"""
XACT Enrichment Worker - Enriches and produces download tasks.

This worker provides a dedicated enrichment stage for the XACT pipeline:
1. Consumes XACTEnrichmentTask from enrichment pending topic
2. Produces DownloadTaskMessage for each attachment
3. Routes status-only events to event handler

Consumer group: {prefix}-xact-enrichment-worker
Input topic: xact.enrichment.pending
Output topic: xact.downloads.pending
"""

import asyncio
import orjson
import logging
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.paths.resolver import generate_blob_path
from core.security.exceptions import URLValidationError
from core.security.url_validation import sanitize_url, validate_download_url
from core.types import ErrorCategory
from pipeline.common.health import HealthCheckServer
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.handlers import EventHandlerRunner
from pipeline.verisk.retry import RetryHandler
from pipeline.verisk.schemas.tasks import (
    DownloadTaskMessage,
    XACTEnrichmentTask,
)
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS

from core.errors.exceptions import PermanentError

logger = logging.getLogger(__name__)


class XACTEnrichmentWorker:
    """
    Worker to consume XACT enrichment tasks and produce download tasks.

    Architecture:
    - Single-task processing (no batching needed - simple flow)
    - Download task generation for each attachment

    Transport Layer:
    - Message consumption is handled by the transport layer (pipeline.common.transport)
    - The transport layer calls _handle_enrichment_task() for EACH message individually
    - Offsets are committed AFTER the handler completes successfully
    - All work must be awaited synchronously to prevent data loss (Issue #38)
    - No background task tracking - the handler must complete all work before returning
    - Transport type (EventHub/Kafka) is selected via PIPELINE_TRANSPORT env var
    """

    WORKER_NAME = "enrichment_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "verisk",
        enrichment_topic: str = "",
        download_topic: str = "",
        producer_config: MessageConfig | None = None,
        instance_id: str | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.download_topic = download_topic or config.get_topic(domain, "downloads_pending")

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = (
                f"{self.WORKER_NAME}-{instance_id}"  # e.g., "enrichment_worker-happy-tiger"
            )
        else:
            self.worker_id = self.WORKER_NAME

        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        self.producer = None
        self.consumer = None
        self.retry_handler: RetryHandler | None = None

        self.event_handler_runner: EventHandlerRunner | None = None

        # Health check server
        health_port = 8081
        health_enabled = True
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name=self.WORKER_NAME,
            enabled=health_enabled,
        )

        self._running = False

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._stats_logger: PeriodicStatsLogger | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        # UUID namespace for deterministic media_id generation
        self.MEDIA_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://xactPipeline/media_id")

        logger.info(
            "Initialized XACTEnrichmentWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": "enrichment_worker",
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "enrichment_worker"),
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
            },
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running")
            return

        logger.info("Starting XACTEnrichmentWorker")
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "enrichment-worker")

        try:
            self._stats_logger = PeriodicStatsLogger(
                interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
                get_stats=self._get_cycle_stats,
                stage="enrichment",
                worker_id=self.worker_id,
            )
            self._stats_logger.start()

            self.producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic_key="downloads_pending",
            )
            await self.producer.start()

            # Sync topic with producer's actual entity name (Event Hub entity may
            # differ from the Kafka topic name resolved by get_topic()).
            if hasattr(self.producer, "eventhub_name"):
                self.download_topic = self.producer.eventhub_name

            self.event_handler_runner = EventHandlerRunner(
                producer_factory=lambda topic_key: create_producer(
                    config=self.producer_config,
                    domain=self.domain,
                    worker_name="enrichment_worker",
                    topic_key=topic_key,
                ),
            )

            self.retry_handler = RetryHandler(
                config=self.consumer_config,
            )
            await self.retry_handler.start()
            logger.info(
                "Retry handler initialized",
                extra={
                    "dlq_topic": self.retry_handler.dlq_topic,
                },
            )

            self.consumer = await create_consumer(
                config=self.consumer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topics=[self.enrichment_topic],
                message_handler=self._handle_enrichment_task,
                topic_key="enrichment_pending",
                consumer_config=ConsumerConfig(instance_id=self.instance_id),
            )

            self.health_server.set_ready(transport_connected=True)

            await self.consumer.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.stop()
            raise
        finally:
            self._running = False

    @staticmethod
    async def _stop_component(
        coro, name: str, catch_cancelled: bool = True,
    ) -> None:
        """Stop a single component, logging errors."""
        try:
            await coro
        except asyncio.CancelledError:
            if catch_cancelled:
                logger.warning("Cancelled while stopping %s", name)
            else:
                raise
        except Exception as e:
            logger.error("Error stopping %s", name, extra={"error": str(e)})

    async def stop(self) -> None:
        logger.info("Stopping XACTEnrichmentWorker")
        self._running = False

        if self._stats_logger:
            await self._stop_component(self._stats_logger.stop(), "stats logger", catch_cancelled=False)

        for attr, name in [("consumer", "consumer"), ("retry_handler", "retry handler"), ("producer", "producer")]:
            component = getattr(self, attr)
            if component:
                await self._stop_component(component.stop(), name)
                setattr(self, attr, None)

        if self.event_handler_runner:
            await self._stop_component(self.event_handler_runner.close(), "event handler runner", catch_cancelled=False)

        await self._stop_component(self.health_server.stop(), "health server", catch_cancelled=False)

        logger.info("XACTEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    async def _handle_enrichment_task(self, message: PipelineMessage) -> None:
        """Process enrichment task and create download tasks."""
        try:
            message_data = orjson.loads(message.value)
            task = XACTEnrichmentTask.model_validate(message_data)
        except (orjson.JSONDecodeError, ValidationError) as e:
            raw_preview = message.value[:1000].decode("utf-8", errors="replace") if message.value else ""
            logger.error(
                "Failed to parse XACTEnrichmentTask",
                extra={
                    "trace_id": message.key.decode("utf-8") if message.key else None,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                    "raw_payload_preview": raw_preview,
                    "raw_payload_bytes": len(message.value) if message.value else 0,
                },
                exc_info=True,
            )
            raise PermanentError(str(e)) from e

        from core.logging.context import set_log_context

        set_log_context(trace_id=task.trace_id)

        self._records_processed += 1
        ts = message.timestamp
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

        logger.info(
            "Processing enrichment task",
            extra={
                "trace_id": task.trace_id,
                "event_type": task.event_type,
                "status_subtype": task.status_subtype,
                "retry_count": task.retry_count,
            },
        )

        await self._process_single_task(task)

    async def _process_single_task(self, task: XACTEnrichmentTask) -> None:
        start_time = datetime.now(UTC)

        try:
            # Create download tasks for each attachment
            download_tasks = await self._create_download_tasks_from_attachments(task)
            if download_tasks:
                await self._produce_download_tasks(download_tasks)
            elif self.event_handler_runner:
                # No attachments — this is a status-only event; dispatch to event handler
                await self.event_handler_runner.run(task)

            self._records_succeeded += 1

            elapsed_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
            logger.info(
                "Enrichment task complete",
                extra={
                    "trace_id": task.trace_id,
                    "assignment_id": task.assignment_id,
                    "status_subtype": task.status_subtype,
                    "download_tasks_produced": len(download_tasks),
                    "attachments_received": len(task.attachments),
                    "duration_ms": round(elapsed_ms, 2),
                },
            )

        except Exception as e:
            error_category = ErrorCategory.UNKNOWN
            log_worker_error(
                logger,
                "Enrichment task failed with unexpected error",
                trace_id=task.trace_id,
                error_category=error_category.value,
                exc=e,
                status_subtype=task.status_subtype,
                error_type=type(e).__name__,
            )
            await self._handle_enrichment_failure(task, e, error_category)

    @staticmethod
    def _compute_is_reinspection(task: XACTEnrichmentTask) -> bool:
        """Return True if this is a reinspection estimatePackageReceived event.

        Checks whether any attachment URL's filename is REINSPECTION_FORM.XML
        (case-insensitive). Only applicable to estimatePackageReceived events;
        all other subtypes always return False.
        """
        if task.status_subtype != "estimatePackageReceived":
            return False
        return any(
            urlparse(url).path.rsplit("/", 1)[-1].upper().endswith("REINSPECTION_FORM.XML")
            for url in task.attachments
        )

    async def _create_download_tasks_from_attachments(
        self,
        task: XACTEnrichmentTask,
    ) -> list[DownloadTaskMessage]:
        """Create download tasks from enrichment task attachments."""
        download_tasks = []
        is_reinspection = self._compute_is_reinspection(task)

        for attachment_url in task.attachments:
            media_id = str(uuid.uuid5(self.MEDIA_ID_NAMESPACE, f"{task.trace_id}:{attachment_url}"))
            try:
                # Validate attachment URL
                try:
                    validate_download_url(
                        attachment_url,
                        allow_localhost=False,
                    )
                except URLValidationError as e:
                    logger.warning(
                        "Invalid attachment URL, skipping",
                        extra={
                            "media_id": media_id,
                            "trace_id": task.trace_id,
                            "url": sanitize_url(attachment_url),
                            "validation_error": str(e),
                        },
                    )
                    continue

                # Generate blob storage path
                blob_path, file_type = generate_blob_path(
                    status_subtype=task.status_subtype,
                    trace_id=task.trace_id,
                    assignment_id=task.assignment_id,
                    download_url=attachment_url,
                    estimate_version=task.estimate_version,
                )

                # Create download task
                download_task = DownloadTaskMessage(
                    media_id=media_id,
                    trace_id=task.trace_id,
                    attachment_url=attachment_url,
                    blob_path=blob_path,
                    status_subtype=task.status_subtype,
                    file_type=file_type,
                    assignment_id=task.assignment_id,
                    estimate_version=task.estimate_version,
                    retry_count=0,
                    event_type=self.domain,
                    event_subtype=task.status_subtype,
                    original_timestamp=task.original_timestamp,
                    is_reinspection=is_reinspection,
                )
                download_tasks.append(download_task)

            except Exception as e:
                logger.error(
                    "Failed to create download task for attachment",
                    extra={
                        "media_id": media_id,
                        "trace_id": task.trace_id,
                        "url": sanitize_url(attachment_url),
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Continue with other attachments

        logger.debug(
            "Created download tasks from attachments",
            extra={
                "trace_id": task.trace_id,
                "media_ids": [download_task.media_id for download_task in download_tasks],
                "attachments": len(task.attachments),
                "download_tasks": len(download_tasks),
            },
        )

        return download_tasks

    async def _produce_download_tasks(
        self,
        download_tasks: list[DownloadTaskMessage],
    ) -> None:
        """Produce download tasks to downloads.pending topic."""
        logger.info(
            "Producing download tasks",
            extra={
                "trace_id": download_tasks[0].trace_id if download_tasks else None,
                "task_count": len(download_tasks),
            },
        )

        for task in download_tasks:
            try:
                metadata = await self.producer.send(
                    value=task,
                    key=task.trace_id,
                    headers={"trace_id": task.trace_id, "media_id": task.media_id},
                )

                logger.info(
                    "Produced download task",
                    extra={
                        "media_id": task.media_id,
                        "trace_id": task.trace_id,
                        "blob_path": task.blob_path,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "media_id": task.media_id,
                        "trace_id": task.trace_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Re-raise to trigger retry of the entire enrichment task
                raise

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=self._records_failed,
            skipped=self._records_skipped,
        )
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return msg, extra

    async def _handle_enrichment_failure(
        self,
        task: XACTEnrichmentTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed task to retry topic or DLQ based on error category and retry count.
        """
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        log_worker_error(
            logger,
            "Enrichment task failed",
            trace_id=task.trace_id,
            error_category=error_category.value,
            exc=error,
            status_subtype=task.status_subtype,
            retry_count=task.retry_count,
        )

        self._records_failed += 1

        # Ensure metadata dict exists (XACTEnrichmentTask defaults to None,
        # but RetryHandler writes to task.metadata[...])
        if task.metadata is None:
            task.metadata = {}

        await self.retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=error_category,
        )


__all__ = ["XACTEnrichmentWorker"]
