"""Base handler classes and registry for Verisk download file processing.

Handlers are invoked by the download worker after a file has been
successfully cached to disk. They receive the DownloadTaskMessage (full
context) and the local file path, and return parsed data to be merged
into the CachedDownloadMessage metadata.

Routing is by (status_subtype, file_type). Unmatched files are silently
skipped; matched files are logged at INFO on entry and completion.

The FileHandlerRunner owns handler lookup, execution, side-effect
production, and producer lifecycle. The download worker just calls
``await runner.run(task, file_path)`` and receives metadata to merge.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel

from pipeline.common.logging import extract_log_context
from pipeline.verisk.schemas.tasks import DownloadTaskMessage, XACTEnrichmentTask

logger = logging.getLogger(__name__)


@dataclass
class FileHandlerSideEffect:
    """A message a handler wants to produce to a side-effect topic."""

    topic_key: str          # e.g. "reinspections" → resolved to verisk.reinspections
    message: BaseModel      # Pydantic model to serialize and send


@dataclass
class FileHandlerResult:
    """Result from a download file handler."""

    success: bool
    parsed_data: dict[str, Any] = field(default_factory=dict)
    side_effects: list[FileHandlerSideEffect] = field(default_factory=list)
    error: str | None = None


class DownloadFileHandler(ABC):
    """Base class for Verisk download file handlers.

    Subclasses declare which (status_subtype, file_type) combinations they
    handle via ``status_subtypes`` and ``file_types`` class attributes.
    Empty lists are treated as wildcards (match all values).

    Example::

        @register_handler
        class MyHandler(DownloadFileHandler):
            status_subtypes = ["estimatePackageReceived"]
            file_types = ["xml"]

            async def handle(self, task, file_path):
                ...
    """

    status_subtypes: list[str] = []
    file_types: list[str] = []

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def handle(
        self,
        task: DownloadTaskMessage,
        file_path: Path,
    ) -> FileHandlerResult:
        """Parse / process a downloaded file.

        Args:
            task: The original download task (carries trace_id, assignment_id,
                  status_subtype, is_reinspection, metadata, etc.)
            file_path: Absolute path to the locally cached file.

        Returns:
            FileHandlerResult whose ``parsed_data`` is merged into
            CachedDownloadMessage.metadata by the caller.
        """


# Module-level registry: (status_subtype, file_type) → handler class
# An empty string is used as the wildcard sentinel.
_HANDLERS: dict[tuple[str, str], type[DownloadFileHandler]] = {}
_WILDCARD = ""


class FileHandlerRegistry:
    """Registry mapping (status_subtype, file_type) pairs to handler classes."""

    def get_handler(
        self,
        status_subtype: str,
        file_type: str,
    ) -> DownloadFileHandler | None:
        """Return an instantiated handler for the given routing key, or None."""
        key = (status_subtype.lower(), file_type.lower())

        # Exact match first, then wildcard fallbacks
        for candidate in [
            key,
            (_WILDCARD, file_type.lower()),
            (status_subtype.lower(), _WILDCARD),
            (_WILDCARD, _WILDCARD),
        ]:
            handler_class = _HANDLERS.get(candidate)
            if handler_class is not None:
                return handler_class()

        return None

    def get_registered_handlers(self) -> dict[tuple[str, str], str]:
        return {key: cls.__name__ for key, cls in _HANDLERS.items()}


def get_handler_registry() -> FileHandlerRegistry:
    return FileHandlerRegistry()


def register_handler(cls: type[DownloadFileHandler]) -> type[DownloadFileHandler]:
    """Decorator to register a handler class for its status_subtypes × file_types."""
    subtypes = cls.status_subtypes or [_WILDCARD]
    types = cls.file_types or [_WILDCARD]

    for subtype in subtypes:
        for file_type in types:
            key = (subtype.lower(), file_type.lower())
            if key in _HANDLERS:
                logger.warning(
                    "Overwriting handler registration",
                    extra={
                        "status_subtype": subtype,
                        "file_type": file_type,
                        "old_handler": _HANDLERS[key].__name__,
                        "new_handler": cls.__name__,
                    },
                )
            _HANDLERS[key] = cls
            logger.debug(
                "Registered download file handler",
                extra={
                    "handler_name": cls.__name__,
                    "status_subtype": subtype or "(any)",
                    "file_type": file_type or "(any)",
                },
            )
    return cls


class FileHandlerRunner:
    """Runs file handlers and dispatches side-effect messages.

    Owns the full handler lifecycle: registry lookup, handler execution,
    lazy producer creation for side-effect topics, and producer shutdown.

    The download worker holds one instance and calls::

        metadata = await self.handler_runner.run(task, file_path)

    That is the only handler-related code the worker needs.
    """

    def __init__(self, producer_factory: Callable[[str], Any]):
        """
        Args:
            producer_factory: Callable that accepts a ``topic_key`` string and
                returns an uninitialised producer. Called lazily on first use.
        """
        self._registry = FileHandlerRegistry()
        self._producer_factory = producer_factory
        self._producers: dict[str, Any] = {}

    async def _get_producer(self, topic_key: str) -> Any:
        """Return a started producer for topic_key, creating it on first use."""
        if topic_key not in self._producers:
            producer = self._producer_factory(topic_key)
            await producer.start()
            self._producers[topic_key] = producer
        return self._producers[topic_key]

    async def run(
        self,
        task: DownloadTaskMessage,
        file_path: Path,
    ) -> dict[str, Any]:
        """Run the handler for this file (if any) and return metadata to merge.

        Side-effect messages declared by the handler are produced before
        returning. Returns an empty dict when no handler is registered.
        """
        handler = self._registry.get_handler(task.status_subtype, task.file_type)
        if handler is None:
            return {}

        logger.debug(
            "Running download file handler",
            extra={
                "handler_name": handler.name,
                "file_path": str(file_path),
                **extract_log_context(task),
            },
        )

        result = await handler.handle(task, file_path)
        if not result.success:
            logger.warning(
                "Download file handler failed",
                extra={
                    "handler_name": handler.name,
                    "error": result.error,
                    **extract_log_context(task),
                },
            )
            return {}

        if result.side_effects:
            for se in result.side_effects:
                producer = await self._get_producer(se.topic_key)
                await producer.send(value=se.message, key=task.trace_id)
                logger.info(
                    "Produced handler side-effect message",
                    extra={
                        "handler_name": handler.name,
                        "topic_key": se.topic_key,
                        "eventhub_name": producer.eventhub_name,
                        **extract_log_context(task),
                    },
                )
        else:
            logger.debug(
                "Download file handler produced no side-effect",
                extra={
                    "handler_name": handler.name,
                    **extract_log_context(task),
                },
            )

        return result.parsed_data

    async def close(self) -> None:
        """Stop all lazily created producers."""
        for topic_key, producer in self._producers.items():
            try:
                await producer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping handler runner producer",
                    extra={"topic_key": topic_key, "error": str(e)},
                )
        self._producers.clear()


# ---------------------------------------------------------------------------
# Event handlers — status-only events that carry no downloadable attachments
# ---------------------------------------------------------------------------

@dataclass
class EventHandlerResult:
    """Result from a status event handler."""

    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    side_effect: FileHandlerSideEffect | None = None
    error: str | None = None


class EventHandler(ABC):
    """Base class for Verisk status event handlers.

    Subclasses declare which status_subtype(s) they handle via the
    ``status_subtypes`` class attribute.  An empty list is treated as a
    wildcard (matches all subtypes).

    Example::

        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                ...
    """

    status_subtypes: list[str] = []

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def handle(self, task: XACTEnrichmentTask) -> EventHandlerResult:
        """Process a status event.

        Args:
            task: The enrichment task containing the full event context.

        Returns:
            EventHandlerResult whose ``data`` may carry extracted values and
            whose optional ``side_effect`` is produced by EventHandlerRunner.
        """


# Module-level registry: status_subtype (lower) → handler class.
# Empty string is the wildcard sentinel.
_EVENT_HANDLERS: dict[str, type[EventHandler]] = {}


def register_event_handler(cls: type[EventHandler]) -> type[EventHandler]:
    """Decorator to register an EventHandler for its status_subtypes."""
    subtypes = cls.status_subtypes or [_WILDCARD]
    for subtype in subtypes:
        key = subtype.lower()
        if key in _EVENT_HANDLERS:
            logger.warning(
                "Overwriting event handler registration",
                extra={
                    "status_subtype": subtype,
                    "old_handler": _EVENT_HANDLERS[key].__name__,
                    "new_handler": cls.__name__,
                },
            )
        _EVENT_HANDLERS[key] = cls
        logger.debug(
            "Registered status event handler",
            extra={
                "handler_name": cls.__name__,
                "status_subtype": subtype or "(any)",
            },
        )
    return cls


class EventHandlerRegistry:
    """Registry mapping status_subtype strings to EventHandler classes."""

    def get_handler(self, status_subtype: str) -> EventHandler | None:
        """Return an instantiated handler for the given subtype, or None."""
        key = status_subtype.lower()
        handler_class = _EVENT_HANDLERS.get(key) or _EVENT_HANDLERS.get(_WILDCARD)
        return handler_class() if handler_class is not None else None

    def get_registered_handlers(self) -> dict[str, str]:
        return {k: cls.__name__ for k, cls in _EVENT_HANDLERS.items()}


class EventHandlerRunner:
    """Runs event handlers for status-only events and dispatches side-effects.

    Mirrors FileHandlerRunner but operates on XACTEnrichmentTask directly —
    no local file path is involved.  The enrichment worker holds one instance
    and calls::

        await self.event_handler_runner.run(task)
    """

    def __init__(self, producer_factory: Callable[[str], Any]):
        self._registry = EventHandlerRegistry()
        self._producer_factory = producer_factory
        self._producers: dict[str, Any] = {}

    async def _get_producer(self, topic_key: str) -> Any:
        """Return a started producer for topic_key, creating it on first use."""
        if topic_key not in self._producers:
            producer = self._producer_factory(topic_key)
            await producer.start()
            self._producers[topic_key] = producer
        return self._producers[topic_key]

    async def run(self, task: XACTEnrichmentTask) -> dict[str, Any]:
        """Run the handler for this event (if any) and return extracted data.

        Side-effect messages declared by the handler are produced before
        returning.  Returns an empty dict when no handler is registered.
        """
        handler = self._registry.get_handler(task.status_subtype)
        if handler is None:
            return {}

        logger.debug(
            "Running status event handler",
            extra={
                "handler_name": handler.name,
                **extract_log_context(task),
            },
        )

        result = await handler.handle(task)

        if not result.success:
            logger.warning(
                "Status event handler failed",
                extra={
                    "handler_name": handler.name,
                    "error": result.error,
                    **extract_log_context(task),
                },
            )
            return {}

        if result.side_effect is not None:
            se = result.side_effect
            producer = await self._get_producer(se.topic_key)
            await producer.send(value=se.message, key=task.trace_id)
            logger.info(
                "Produced event handler side-effect message",
                extra={
                    "handler_name": handler.name,
                    "topic_key": se.topic_key,
                    "eventhub_name": producer.eventhub_name,
                    **extract_log_context(task),
                },
            )

        logger.debug(
            "Status event handler complete",
            extra={
                "handler_name": handler.name,
                "parsed_fields": list(result.data.keys()),
                **extract_log_context(task),
            },
        )
        return result.data

    async def close(self) -> None:
        """Stop all lazily created producers."""
        for topic_key, producer in self._producers.items():
            try:
                await producer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping event handler runner producer",
                    extra={"topic_key": topic_key, "error": str(e)},
                )
        self._producers.clear()
