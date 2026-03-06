"""
Result processor worker for batching download results.

Consumes from downloads.results topic and batches results
for efficient Delta table writes.

Features:
- Size-based batching (default: 100 records)
- Timeout-based batching (default: 5 seconds)
- Thread-safe batch accumulation
- Graceful shutdown with pending batch flush
- Writes successful downloads to xact_attachments table
- Writes permanent failures to xact_attachments_failed table (optional)
- Batch ID for log correlation
- Delta write metrics
- Optional max_batches limit for testing
- Retry via topics on Delta write failure
"""

import asyncio
import contextlib
import logging
import time
import uuid
from typing import Any

from config.config import MessageConfig
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import log_worker_error
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import record_delta_write
from pipeline.common.retry.delta_handler import DeltaRetryHandler
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import create_consumer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS

from core.errors.exceptions import PermanentError
from pipeline.verisk.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)

logger = logging.getLogger(__name__)


async def _safe_stop(name: str, coro) -> None:
    """Await a coroutine, logging and swallowing any exception."""
    try:
        await coro
    except Exception as e:
        logger.error("Error stopping %s", name, extra={"error": str(e)})


class ResultProcessor:
    """
    Consumes download results and batches for Delta table writes.

    The result processor is the final stage of the pipeline:
    1. Consumes from downloads.results topic
    2. Routes results by status:
       - success → xact_attachments table
       - failed_permanent → xact_attachments_failed table (if configured)
       - failed_transient → skipped (still retrying)
    3. Batches results by size or timeout
    4. Flushes batches to Delta tables

    Batching Strategy:
    - Size flush: When batch reaches BATCH_SIZE (default: 100)
    - Timeout flush: When BATCH_TIMEOUT_SECONDS elapsed (default: 5s)
    - Shutdown flush: Pending batches flushed on graceful shutdown

    Thread Safety:
    - Uses asyncio.Lock for batch accumulation
    - Safe for concurrent message processing

    Usage:
        >>> config = MessageConfig.from_env()
        >>> producer = MessageProducer(config)
        >>> await producer.start()
        >>> processor = ResultProcessor(
        ...     config=config,
        ...     producer=producer,
        ...     inventory_table_path="abfss://.../xact_attachments",
        ...     failed_table_path="abfss://.../xact_attachments_failed",
        ... )
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    WORKER_NAME = "result_processor"

    # Batching configuration
    BATCH_SIZE = 2000
    BATCH_TIMEOUT_SECONDS = 5

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
        producer=None,
        inventory_table_path: str = "",
        failed_table_path: str | None = None,
        batch_size: int | None = None,
        batch_timeout_seconds: float | None = None,
        instance_id: str | None = None,
        domain: str = "verisk",
    ):
        """
        Initialize result processor.

        Args:
            config: Message broker configuration
            producer: Message producer for retry topic routing
            inventory_table_path: Full abfss:// path to xact_attachments Delta table
            failed_table_path: Optional path to xact_attachments_failed Delta table.
                               If provided, permanent failures will be written here.
            batch_size: Optional custom batch size (default: 100)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
        """
        self.config = config
        self.producer = producer
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS
        self.max_batches: int | None = None

        # Domain and worker configuration (must be set before using them)
        self.domain = domain
        self.worker_name = "result_processor"
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Delta writers
        self._inventory_writer = DeltaInventoryWriter(table_path=inventory_table_path)
        self._failed_writer: DeltaFailedAttachmentsWriter | None = None
        if failed_table_path:
            self._failed_writer = DeltaFailedAttachmentsWriter(table_path=failed_table_path)

        # Retry handler for failed Delta writes
        self._retry_handler = DeltaRetryHandler(
            config=config,
            table_path=inventory_table_path,
            retry_topic_prefix="result-processor.retry",
            dlq_topic="result-processor.dlq",
            domain=self.domain,
        )

        # Batching state - separate batches for success and failed
        self._batch: list[DownloadResultMessage] = []
        self._failed_batch: list[DownloadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()

        # Progress tracking
        self._batches_written = 0
        self._failed_batches_written = 0
        self._total_records_written = 0

        # Stats tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._stats_logger: PeriodicStatsLogger | None = None

        # Store topics for logging
        self._results_topic = config.get_topic(self.domain, "downloads_results")

        # Consumer created in start() since create_consumer is async
        self._consumer = None

        # Background flush task
        self._flush_task: asyncio.Task | None = None
        self._running = False

        # Double-buffer write state: allows consumption to continue
        # while a Delta write runs in the background
        self._pending_write_task: asyncio.Task | None = None
        self._pending_write_meta: dict | None = None

        # Health check server
        health_port = 8094
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-result-processor",
        )

        logger.info(
            "Initialized result processor",
            extra={
                "domain": self.domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "max_batches": self.max_batches,
                "results_topic": self._results_topic,
                "inventory_table_path": inventory_table_path,
                "failed_table_path": failed_table_path,
                "failed_tracking_enabled": failed_table_path is not None,
            },
        )

    async def start(self) -> None:
        """
        Start the result processor.

        Starts consuming from results topic and begins background flush timer.
        This method runs until stop() is called.

        Raises Exception if consumer fails to start.
        """
        if self._running:
            logger.warning("Result processor already running, ignoring duplicate start")
            return

        logger.info("Starting result processor")
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "result-processor")

        # Start retry handler producers
        await self._retry_handler.start()

        # Create consumer via transport factory (Event Hub support)
        self._consumer = await create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self._results_topic],
            message_handler=self._handle_result,
            topic_key="downloads_results",
            consumer_config=ConsumerConfig(
                enable_message_commit=False,
                instance_id=self.instance_id,
                on_partition_revoked=self._on_partition_revoked,
            ),
        )

        # Start periodic background tasks
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="result_processing",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()
        self._flush_task = asyncio.create_task(self._periodic_flush())

        # Update health check readiness
        self.health_server.set_ready(transport_connected=True)

        try:
            # Start consumer (blocks until stopped)
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Result processor cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Result processor terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def _cancel_flush_task(self) -> None:
        """Cancel the periodic flush background task."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task

    async def _flush_pending_on_shutdown(self) -> None:
        """Flush any in-flight write and remaining batches during shutdown."""
        # Resolve any in-flight background write first
        await self._resolve_pending_write()

        async with self._batch_lock:
            if self._batch:
                logger.info(
                    "Flushing pending success batch on shutdown",
                    extra={"batch_size": len(self._batch)},
                )
                await self._flush_batch()
            if self._failed_batch:
                logger.info(
                    "Flushing pending failed batch on shutdown",
                    extra={"batch_size": len(self._failed_batch)},
                )
                await self._flush_failed_batch()

        # Resolve writes started by the final flush above
        await self._resolve_pending_write()

    async def _on_partition_revoked(self, partition_id: str) -> None:
        """Flush pending work before partition context is cleared on rebalance.

        Called by EventHubConsumer._on_partition_close BEFORE it clears
        _current_partition_context, so commit() can still checkpoint.
        """
        # 1. Resolve any in-flight background Delta write
        await self._resolve_pending_write()

        # 2. Flush remaining batch under lock
        async with self._batch_lock:
            if self._batch:
                logger.info(
                    "Flushing batch on partition revocation",
                    extra={
                        "partition_id": partition_id,
                        "batch_size": len(self._batch),
                    },
                )
                await self._flush_batch()
            if self._failed_batch:
                logger.info(
                    "Flushing failed batch on partition revocation",
                    extra={
                        "partition_id": partition_id,
                        "batch_size": len(self._failed_batch),
                    },
                )
                await self._flush_failed_batch()

        # 3. Resolve the write started by the flush above
        await self._resolve_pending_write()

    async def stop(self) -> None:
        """
        Stop the result processor gracefully.

        Flushes any pending batch before stopping consumer.
        Safe to call multiple times.
        """
        logger.info("Stopping result processor")
        self._running = False

        if self._stats_logger:
            await _safe_stop("stats_logger", self._stats_logger.stop())
        await _safe_stop("flush_task", self._cancel_flush_task())
        await _safe_stop("pending_batches", self._flush_pending_on_shutdown())

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error("Error stopping consumer", extra={"error": str(e)})
            finally:
                self._consumer = None

        if self._retry_handler:
            await _safe_stop("retry_handler", self._retry_handler.stop())
        if self.health_server:
            await _safe_stop("health_server", self.health_server.stop())

        logger.info(
            "Result processor stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "failed_batches_written": self._failed_batches_written,
                "total_records_written": self._total_records_written,
            },
        )

    async def _add_to_batch_and_flush(
        self, batch: list, result: DownloadResultMessage, flush_fn, label: str
    ) -> None:
        """Add a result to a batch and flush if size threshold is reached."""
        # Eagerly resolve completed writes before taking the lock to
        # reduce contention during backpressure
        self._resolve_pending_write_if_done()
        async with self._batch_lock:
            batch.append(result)
            if len(batch) >= self.batch_size:
                logger.debug(
                    "%s batch size threshold reached, flushing",
                    label,
                    extra={"batch_size": len(batch)},
                )
                await flush_fn()

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Track earliest/latest message timestamps for cycle logging."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    async def _handle_result(self, message: PipelineMessage) -> None:
        """
        Handle a single result message.

        Routes results by status:
        - success → inventory batch
        - failed_permanent → failed batch (if tracking enabled)
        - failed_transient → skip (still retrying)

        Triggers flush if size or timeout threshold reached.
        """
        self._records_processed += 1
        self._update_cycle_offsets(message.timestamp)

        try:
            result = DownloadResultMessage.model_validate_json(message.value)
        except Exception as e:
            log_worker_error(
                logger,
                "Failed to parse result message",
                error_category="PERMANENT",
                exc=e,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                trace_id=message.key.decode("utf-8") if message.key else None,
            )
            raise PermanentError(str(e)) from e

        set_log_context(trace_id=result.trace_id)

        if result.status == "completed":
            self._records_succeeded += 1
            await self._add_to_batch_and_flush(
                self._batch, result, self._flush_batch, "Success"
            )
        elif result.status == "failed_permanent" and self._failed_writer:
            self._records_failed += 1
            await self._add_to_batch_and_flush(
                self._failed_batch, result, self._flush_failed_batch, "Failed"
            )
        else:
            self._records_skipped += 1
            logger.debug(
                "Skipping result",
                extra={
                    "trace_id": result.trace_id,
                    "status": result.status,
                    "reason": "transient" if result.status == "failed" else "no_failed_writer",
                    "attachment_url": result.attachment_url[:100],
                },
            )

    async def _flush_if_timed_out(self) -> None:
        """Check timeout and flush batches if threshold reached. Caller must hold lock."""
        elapsed = time.monotonic() - self._last_flush
        if elapsed < self.batch_timeout_seconds:
            return
        if self._batch:
            logger.debug(
                "Success batch timeout threshold reached, flushing",
                extra={"batch_size": len(self._batch), "elapsed_seconds": elapsed},
            )
            await self._flush_batch()
        if self._failed_batch:
            logger.debug(
                "Failed batch timeout threshold reached, flushing",
                extra={"batch_size": len(self._failed_batch), "elapsed_seconds": elapsed},
            )
            await self._flush_failed_batch()

    async def _periodic_flush(self) -> None:
        """Background task for timeout-based batch flushing."""
        try:
            while self._running:
                await asyncio.sleep(1)
                # Eagerly resolve completed writes outside the lock to
                # commit checkpoints faster (~1s instead of next batch flush)
                self._resolve_pending_write_if_done()
                async with self._batch_lock:
                    await self._flush_if_timed_out()
        except asyncio.CancelledError:
            logger.debug("Periodic flush task cancelled")
            raise

    def _resolve_pending_write_if_done(self) -> None:
        """Non-blocking: resolve the pending write only if the task is already done.

        Called from hot paths (periodic flush, add-to-batch) to commit
        checkpoints eagerly without blocking on in-flight Delta writes.
        """
        if self._pending_write_task is not None and self._pending_write_task.done():
            # Schedule resolution as a fire-and-forget task so we don't
            # need to make callers async-aware of the result handling.
            # _resolve_pending_write will take-and-clear atomically.
            asyncio.ensure_future(self._resolve_pending_write())

    async def _resolve_pending_write(self) -> None:
        """Await the in-flight background write and handle its result.

        On success: record metrics, commit offsets via _handle_flush_success.
        On failure: route to retry topic via _handle_flush_failure.

        Safe to call when no write is pending (no-op).
        Uses take-and-clear to prevent double-resolution from concurrent callers.
        """
        if self._pending_write_task is None:
            return

        # Take-and-clear: grab references and clear state BEFORE awaiting,
        # so concurrent callers see None and no-op instead of double-resolving.
        task = self._pending_write_task
        meta = self._pending_write_meta
        self._pending_write_task = None
        self._pending_write_meta = None

        try:
            success = await task
        except Exception:
            logger.exception("Background Delta write raised an exception")
            success = False

        record_delta_write(
            table=meta["table_name"],
            event_count=meta["batch_size"],
            success=success,
        )

        if success:
            await self._handle_flush_success(
                meta["batch_snapshot"],
                meta["batch_id"],
                meta["batch_size"],
                meta["table_name"],
                meta["counter_attr"],
            )
        else:
            await self._handle_flush_failure(
                meta["batch_snapshot"],
                meta["batch_id"],
                meta["batch_size"],
                meta["table_name"],
            )

    async def _flush_batch_common(
        self,
        batch: list[DownloadResultMessage],
        writer: DeltaInventoryWriter | DeltaFailedAttachmentsWriter,
        table_name: str,
        counter_attr: str,
    ) -> None:
        """
        Unified batch flush logic with double-buffering.

        Resolves any in-flight background write first (backpressure), then
        snapshots the current batch and starts the Delta write as a background
        task so consumption can continue immediately.

        Args:
            batch: Batch list to flush
            writer: Delta writer to use
            table_name: Table name for logging and metrics
            counter_attr: Attribute name for tracking written batches
        """
        if not batch:
            return

        # Backpressure: wait for previous write to finish before starting new one
        await self._resolve_pending_write()

        # Generate batch ID for log correlation
        batch_id = uuid.uuid4().hex[:8]

        # Snapshot current batch and reset
        batch_snapshot = batch.copy()
        batch.clear()
        self._last_flush = time.monotonic()

        batch_size = len(batch_snapshot)

        # Start Delta write as background task — consumption continues immediately
        self._pending_write_task = asyncio.create_task(
            writer.write_results(batch_snapshot)
        )
        self._pending_write_meta = {
            "batch_snapshot": batch_snapshot,
            "batch_id": batch_id,
            "batch_size": batch_size,
            "table_name": table_name,
            "counter_attr": counter_attr,
        }

    async def _handle_flush_success(
        self,
        batch_snapshot: list[DownloadResultMessage],
        batch_id: str,
        batch_size: int,
        table_name: str,
        counter_attr: str,
    ) -> None:
        """Handle counters, offset commit, and logging after a successful Delta write."""
        # Update counters
        setattr(self, counter_attr, getattr(self, counter_attr) + 1)
        self._total_records_written += batch_size

        # Commit offsets after successful Delta write
        if self._consumer:
            await self._consumer.commit()

        log_message = self._build_success_log_message(batch_size, table_name)
        logger.info(
            log_message,
            extra={
                "batch_id": batch_id,
                "batch_size": batch_size,
                "batches_written": getattr(self, counter_attr),
                "total_records_written": self._total_records_written,
                "first_trace_id": batch_snapshot[0].trace_id,
                "last_trace_id": batch_snapshot[-1].trace_id,
            },
        )

    def _build_success_log_message(self, batch_size: int, table_name: str) -> str:
        """Build the log message for a successful batch write."""
        if table_name != "xact_attachments":
            return f"Successfully wrote {batch_size} records to {table_name}"

        if self.max_batches:
            progress = f"Batch {self._batches_written}/{self.max_batches}"
        else:
            progress = f"Batch {self._batches_written}"
        return f"{progress}: Successfully wrote {batch_size} records to {table_name}"

    async def _handle_flush_failure(
        self,
        batch_snapshot: list[DownloadResultMessage],
        batch_id: str,
        batch_size: int,
        table_name: str,
    ) -> None:
        """Route a failed batch to retry topics with appropriate logging."""
        extra = {
            "batch_id": batch_id,
            "batch_size": batch_size,
            "first_trace_id": batch_snapshot[0].trace_id,
            "last_trace_id": batch_snapshot[-1].trace_id,
        }
        if table_name != "xact_attachments":
            extra["table"] = table_name

        logger.warning(
            "Delta write failed, routing batch to retry topic",
            extra=extra,
        )
        # Convert DownloadResultMessage objects to dicts for retry handler
        batch_dicts = [msg.model_dump() for msg in batch_snapshot]
        await self._retry_handler.handle_batch_failure(
            batch=batch_dicts,
            error=Exception(f"Delta write to {table_name} failed"),
            retry_count=0,
            error_category="transient",
            batch_id=batch_id,
        )

    async def _flush_batch(self) -> None:
        """
        Flush current batch (internal, assumes lock held).

        Converts batch to inventory records and writes to Delta.
        On success: commits offsets, updates counters.
        On failure: routes batch to retry topics.

        Note: This method assumes the caller holds self._batch_lock
        """
        await self._flush_batch_common(
            self._batch,
            self._inventory_writer,
            "xact_attachments",
            "_batches_written",
        )

    async def _flush_failed_batch(self) -> None:
        """
        Flush current failed batch (internal, assumes lock held).

        Converts failed batch to records and writes to Delta.
        On success: commits offsets, updates counters.
        On failure: routes batch to retry topics.

        Note: This method assumes the caller holds self._batch_lock
        """
        if not self._failed_writer:
            return

        await self._flush_batch_common(
            self._failed_batch,
            self._failed_writer,
            "xact_attachments_failed",
            "_failed_batches_written",
        )

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Return current stats for PeriodicStatsLogger."""
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "records_deduplicated": 0,
            "batches_written": self._batches_written,
            "failed_batches_written": self._failed_batches_written,
            "total_records_written": self._total_records_written,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return "", extra

    @property
    def is_running(self) -> bool:
        """Check if result processor is running."""
        return self._running and self._consumer is not None and self._consumer.is_running


__all__ = [
    "ResultProcessor",
]
