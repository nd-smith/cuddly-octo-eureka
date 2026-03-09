"""
Delta Events Worker - Writes events to Delta Lake xact_events table.

Consumes enrichment tasks from the enrichment_pending topic (downstream of
the event ingester's deduplication) and extracts the raw_event payload for
Delta Lake writes via flatten_events().

Features:
- Deduplication inherited from upstream event ingester
- Batch accumulation for efficient Delta writes
- Configurable batch size via delta_events_batch_size
- Optional batch limit for testing via delta_events_max_batches
- Retry via Event Hub topics with exponential backoff

Input topic: enrichment_pending
Output: Delta table xact_events (no Event Hub output)
"""

import asyncio
import contextlib
import orjson
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.errors.exceptions import PermanentError
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import record_delta_write
from pipeline.common.retry.delta_handler import DeltaRetryHandler
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import create_consumer
from pipeline.common.types import PipelineMessage
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.verisk.workers.worker_defaults import MAX_POLL_RECORDS
from pipeline.verisk.writers import DeltaEventsWriter

logger = logging.getLogger(__name__)


class DeltaEventsWorker:
    """
    Worker to consume deduplicated enrichment tasks and write raw events to Delta Lake.

    Consumes XACTEnrichmentTask records from the enrichment_pending topic
    (downstream of the event ingester's deduplication) and extracts the
    raw_event payload for Delta Lake writes via flatten_events().

    Features:
    - Deduplication inherited from upstream event ingester
    - Batch accumulation for efficient Delta writes
    - Configurable batch size
    - Graceful shutdown with pending batch flush
    - Failed batches route to retry topics
    """

    WORKER_NAME = "delta_events_writer"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
        producer: Any,
        events_table_path: str,
        domain: str = "verisk",
        instance_id: str | None = None,
    ):
        """
        Initialize Delta events worker.

        Args:
            config: Message broker configuration for consumer (topic names, connection settings).
                    Also provides delta_events_batch_size and delta_events_max_batches.
            producer: Message producer for retry topic routing (required).
            events_table_path: Full abfss:// path to xact_events Delta table
            domain: Domain identifier (default: "verisk")
        """
        self.config = config
        self.domain = domain
        self.instance_id = instance_id
        self.events_table_path = events_table_path
        self.consumer = None
        self.producer = producer

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Batch configuration
        self.batch_size = MAX_POLL_RECORDS
        self.max_batches = None  # unlimited
        self.batch_timeout_seconds = 10.0

        # Retry configuration
        self._retry_delays = [300, 600, 1200, 2400]
        self._retry_topic_prefix = "verisk-retry"
        self._dlq_topic = "verisk-dlq"

        # Batch state
        self._batch: list[dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: asyncio.Task | None = None
        self._batches_written = 0
        self._total_events_written = 0

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._stats_logger: PeriodicStatsLogger | None = None
        self._running = False

        # Initialize Delta writer
        if not events_table_path:
            raise ValueError("events_table_path is required for DeltaEventsWorker")

        self.delta_writer = DeltaEventsWriter(
            table_path=events_table_path,
        )

        # Initialize retry handler
        self.retry_handler = DeltaRetryHandler(
            config=config,
            table_path=events_table_path,
            retry_delays=self._retry_delays,
            retry_topic_prefix=self._retry_topic_prefix,
            dlq_topic=self._dlq_topic,
            domain=self.domain,
        )

        # Health check server
        health_port = 8093
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-delta-events",
        )

        logger.info(
            "Initialized DeltaEventsWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "delta_events_writer"),
                "source_topic": config.get_topic(domain, "enrichment_pending"),
                "events_table_path": events_table_path,
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "max_batches": self.max_batches,
                "retry_delays": self._retry_delays,
                "retry_topic_prefix": self._retry_topic_prefix,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the delta events worker.

        Initializes consumer and begins consuming enrichment tasks from the
        enrichment_pending topic. Runs until stop() is called or max_batches
        is reached (if configured).
        """
        logger.info(
            "Starting DeltaEventsWorker",
            extra={
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
            },
        )
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "delta-events-worker")

        try:
            # Start retry handler producers
            await self.retry_handler.start()

            # Start periodic stats logger
            self._stats_logger = PeriodicStatsLogger(
                interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
                get_stats=self._get_cycle_stats,
                stage="delta_write",
                worker_id=self.worker_id,
            )
            self._stats_logger.start()

            # Start batch timer for periodic flushing
            self._reset_batch_timer()

            # Create and start consumer on enrichment_pending topic (already deduplicated)
            # Disable per-message commits - we commit after batch writes to ensure
            # offsets are only committed after data is durably written to Delta Lake
            self.consumer = await create_consumer(
                config=self.config,
                domain=self.domain,
                worker_name="delta_events_writer",
                topics=[self.config.get_topic(self.domain, "enrichment_pending")],
                message_handler=self._handle_event_message,
                topic_key="enrichment_pending",
                consumer_config=ConsumerConfig(
                    enable_message_commit=False,
                    instance_id=self.instance_id,
                ),
            )

            # Update health check readiness
            self.health_server.set_ready(transport_connected=True)

            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.stop()
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the delta events worker.

        Flushes any pending batch, then gracefully shuts down consumer,
        committing any pending offsets.
        """
        logger.info("Stopping DeltaEventsWorker")
        self._running = False

        await self._close_resource("stats logger", self._stop_stats_logger)
        await self._close_resource("batch timer", self._cancel_batch_timer)
        await self._close_resource("pending batch", self._flush_pending_batch)
        await self._close_resource("consumer", self._stop_consumer)
        await self._close_resource("retry handler", self._stop_retry_handler)
        await self._close_resource("health server", self.health_server.stop)

        logger.info(
            "DeltaEventsWorker stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "records_succeeded": self._records_succeeded,
            },
        )

    async def _close_resource(self, name: str, method: Callable[[], Awaitable[None]]) -> None:
        try:
            await method()
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})

    async def _stop_stats_logger(self) -> None:
        if self._stats_logger:
            await self._stats_logger.stop()

    async def _cancel_batch_timer(self) -> None:
        if self._batch_timer and not self._batch_timer.done():
            self._batch_timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._batch_timer

    async def _flush_pending_batch(self) -> None:
        if self._batch:
            logger.info(
                "Flushing remaining batch on shutdown",
                extra={"batch_size": len(self._batch)},
            )
            await self._flush_batch()

    async def _stop_consumer(self) -> None:
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None

    async def _stop_retry_handler(self) -> None:
        if self.retry_handler:
            await self.retry_handler.stop()

    def _update_cycle_offsets(self, ts: Any) -> None:
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    async def _handle_event_message(self, record: PipelineMessage) -> None:
        """
        Process a single enrichment task message from the enrichment_pending topic.

        Extracts the raw_event payload and adds it to the batch for Delta writes.
        Messages are already deduplicated by the upstream event ingester.
        """
        self._records_processed += 1
        self._update_cycle_offsets(record.timestamp)

        # Check if we've reached max batches limit
        if self.max_batches is not None and self._batches_written >= self.max_batches:
            logger.info(
                "Reached max_batches limit, stopping consumer",
                extra={
                    "max_batches": self.max_batches,
                    "batches_written": self._batches_written,
                },
            )
            if self.consumer:
                await self.consumer.stop()
            return

        try:
            message_data = orjson.loads(record.value)
        except orjson.JSONDecodeError as e:
            log_worker_error(
                logger,
                "Failed to parse message JSON",
                error_category="PERMANENT",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                trace_id=record.key.decode("utf-8") if record.key else None,
            )
            raise PermanentError(
                f"Failed to parse message JSON: {e}",
                cause=e,
                context={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            ) from e

        # Parse enrichment task and extract the raw event for flatten_events()
        try:
            task = XACTEnrichmentTask(**message_data)
        except ValidationError as e:
            log_worker_error(
                logger,
                "Failed to parse XACTEnrichmentTask",
                error_category="PERMANENT",
                exc=e,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                trace_id=record.key.decode("utf-8") if record.key else None,
            )
            raise PermanentError(
                f"Failed to parse XACTEnrichmentTask: {e}",
                cause=e,
                context={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                },
            ) from e

        # Set logging context for correlation
        set_log_context(trace_id=task.trace_id)

        # Add raw event to batch (same format flatten_events() expects)
        flush_needed = False
        async with self._batch_lock:
            self._batch.append(task.raw_event)
            if len(self._batch) >= self.batch_size:
                flush_needed = True

        # Flush outside lock so the batch lock isn't held during
        # the potentially long write + retry cycle
        if flush_needed:
            await self._flush_batch()
            self._reset_batch_timer()

    async def _flush_batch(self) -> None:
        """
        Write the accumulated batch to Delta Lake.

        On success: clears batch and updates counters.
        On failure: routes batch to retry topic with classified error.
        """
        # Snapshot and clear batch under lock, then write outside lock
        # so the lock isn't held during the potentially long write + retry cycle
        async with self._batch_lock:
            if not self._batch:
                return
            batch_id = uuid.uuid4().hex[:8]
            batch_size = len(self._batch)
            batch_to_write = self._batch
            self._batch = []

        success = await self._write_batch(batch_to_write, batch_id)

        if success:
            await self._handle_successful_batch(batch_id, batch_size)
        else:
            await self._handle_failed_batch(batch_to_write, batch_id, batch_size)

    async def _handle_successful_batch(self, batch_id: str, batch_size: int) -> None:
        self._batches_written += 1
        self._records_succeeded += batch_size

        if self.consumer:
            await self.consumer.commit()

        progress = (
            f"Batch {self._batches_written}/{self.max_batches}"
            if self.max_batches
            else f"Batch {self._batches_written}"
        )

        logger.info(
            f"{progress}: Successfully wrote {batch_size} events to Delta",
            extra={
                "batch_id": batch_id,
                "batch_size": batch_size,
                "batches_written": self._batches_written,
                "records_succeeded": self._records_succeeded,
                "max_batches": self.max_batches,
            },
        )

        if self.max_batches and self._batches_written >= self.max_batches:
            logger.info(
                "Reached max_batches limit, stopping consumer",
                extra={
                    "batch_id": batch_id,
                    "max_batches": self.max_batches,
                    "batches_written": self._batches_written,
                },
            )
            if self.consumer:
                await self.consumer.stop()

    async def _handle_failed_batch(
        self, batch_to_write: list[dict[str, Any]], batch_id: str, batch_size: int
    ) -> None:
        error = getattr(self, "_last_write_error", None) or Exception("Delta write failed")
        error_category = getattr(self, "_last_error_category", None)
        category_str = (
            error_category.value if hasattr(error_category, "value") else "transient"
        )

        trace_ids = [
            event_dict.get("traceId") or event_dict.get("trace_id")
            for event_dict in batch_to_write[:10]
            if event_dict.get("traceId") or event_dict.get("trace_id")
        ]
        logger.warning(
            "Batch write failed, routing to retry topic",
            extra={
                "batch_id": batch_id,
                "batch_size": batch_size,
                "error_category": category_str,
                "error": str(error)[:500],
                "error_type": type(error).__name__,
                "table": "xact_events",
                "trace_id": trace_ids[0] if trace_ids else None,
                "trace_ids": trace_ids,
                "retry_count": 0,
                "records_failed": batch_size,
            },
        )
        try:
            await self.retry_handler.handle_batch_failure(
                batch=batch_to_write,
                error=error,
                retry_count=0,
                error_category=category_str,
                batch_id=batch_id,
            )
        except Exception:
            logger.error(
                "Failed to route batch to retry handler, events will be "
                "redelivered from last checkpoint",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "error_category": category_str,
                    "table": "xact_events",
                    "trace_id": trace_ids[0] if trace_ids else None,
                    "records_failed": batch_size,
                },
                exc_info=True,
            )

    async def _write_batch(self, batch: list[dict[str, Any]], batch_id: str) -> bool:
        """
        Attempt to write a batch to Delta Lake.

        Returns True if write succeeded, False otherwise.
        """
        batch_size = len(batch)

        from pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("delta.write") as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("span.kind", "client")
            span.set_tag("batch_id", batch_id)
            span.set_tag("batch_size", batch_size)
            span.set_tag("table", "xact_events")
            try:
                success = await self.delta_writer.write_raw_events(batch, batch_id=batch_id)

                span.set_tag("write.success", success)
                record_delta_write(
                    table="xact_events",
                    event_count=batch_size,
                    success=success,
                )

                return success

            except Exception as e:
                # Classify error using DeltaRetryHandler for proper DLQ routing
                error_category = self.retry_handler.classify_delta_error(e)

                # Store error info for _flush_batch to use
                self._last_write_error = e
                self._last_error_category = error_category

                trace_ids = []
                event_types = set()
                for evt in batch[:10]:
                    tid = evt.get("traceId") or evt.get("trace_id")
                    if tid:
                        trace_ids.append(tid)
                    etype = evt.get("type") or evt.get("event_type")
                    if etype:
                        event_types.add(etype)

                span.set_tag("write.success", False)
                span.set_tag("error.category", error_category.value)
                span.set_tag("error.type", type(e).__name__)
                span.set_tag("error.message", str(e)[:200])

                log_worker_error(
                    logger,
                    "Delta write error - classified for routing",
                    error_category=error_category.value,
                    exc=e,
                    error=str(e)[:500],
                    error_type=type(e).__name__,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    table="xact_events",
                    trace_id=trace_ids[0] if trace_ids else None,
                    trace_ids=trace_ids,
                    event_type=", ".join(sorted(event_types)) if event_types else None,
                    retry_count=0,
                    records_failed=batch_size,
                    rows_written=0,
                )
                record_delta_write(
                    table="xact_events",
                    event_count=batch_size,
                    success=False,
                )
                return False

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=0,
        )
        extra = {
            "records_processed": self._records_processed,
            "batches_written": self._batches_written,
            "records_succeeded": self._records_succeeded,
            "pending_batch_size": len(self._batch),
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return msg, extra

    async def _periodic_flush(self) -> None:
        """
        Timer callback to periodically flush batch regardless of size.

        This ensures events are written even during low-traffic periods,
        improving latency for the last events in a batch.
        """
        try:
            while self._running:
                await asyncio.sleep(self.batch_timeout_seconds)
                await self._flush_batch()
        except asyncio.CancelledError:
            logger.debug("Periodic flush task cancelled")
            raise

    def _reset_batch_timer(self) -> None:
        """Reset the batch flush timer."""
        if self._batch_timer and not self._batch_timer.done():
            self._batch_timer.cancel()
        self._batch_timer = asyncio.create_task(self._periodic_flush())


__all__ = ["DeltaEventsWorker"]
