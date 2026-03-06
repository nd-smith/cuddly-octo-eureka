"""Verisk download file handlers and status event handlers."""

from pipeline.verisk.handlers.base import (
    DownloadFileHandler,
    EventHandler,
    EventHandlerRegistry,
    EventHandlerResult,
    EventHandlerRunner,
    FileHandlerRegistry,
    FileHandlerResult,
    FileHandlerRunner,
    FileHandlerSideEffect,
    get_handler_registry,
    register_event_handler,
    register_handler,
)

# Import handlers to trigger @register_handler / @register_event_handler decoration
import pipeline.verisk.handlers.fnol  # noqa: F401, E402
import pipeline.verisk.handlers.reinspection  # noqa: F401, E402
import pipeline.verisk.handlers.assignment_notes  # noqa: F401, E402
import pipeline.verisk.handlers.status  # noqa: F401, E402

__all__ = [
    "DownloadFileHandler",
    "EventHandler",
    "EventHandlerRegistry",
    "EventHandlerResult",
    "EventHandlerRunner",
    "FileHandlerRegistry",
    "FileHandlerResult",
    "FileHandlerRunner",
    "FileHandlerSideEffect",
    "get_handler_registry",
    "register_event_handler",
    "register_handler",
]
