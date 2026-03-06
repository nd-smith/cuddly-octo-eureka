"""ClaimX enrichment worker.

Enriches events with ClaimX API data and produces download tasks.
Routes events by type to specialized handlers.
"""

import asyncio
import hashlib
import orjson
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import (
    detect_log_output_mode,
    log_startup_banner,
    log_worker_error,
)
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from pipeline.claimx.handlers import HandlerRegistry, get_handler_registry
from pipeline.claimx.handlers.project_cache import ProjectCache
from pipeline.claimx.retry import EnrichmentRetryHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.schemas.tasks import (
    ClaimXDownloadTask,
    ClaimXEnrichmentTask,
)
from pipeline.claimx.workers.download_factory import create_download_tasks_from_media
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS
from pipeline.common.health import HealthCheckServer
from pipeline.common.log_fields import producer_log_fields
from pipeline.common.metrics import (
    record_delta_write,
    record_processing_error,
)
from pipeline.common.storage.delta import DeltaTableReader
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import create_batch_consumer, create_consumer, create_producer
from pipeline.common.types import BatchResult, PipelineMessage

from core.errors.exceptions import PermanentError

logger = logging.getLogger(__name__)


class ClaimXEnrichmentWorker:
    """
    Worker to consume ClaimX enrichment tasks and enrich them with API data.

    Architecture:
    - Event routing via handler registry
    - Single-task processing (no batching - delta writer handles batching)
    - Entity data writes to 7 Delta tables
    - Download task generation for media files

    Transport Layer:
    - Message consumption is handled by the transport layer (pipeline.common.transport)
    - The transport layer calls _handle_enrichment_task() for EACH message individually
    - Offsets are committed AFTER the handler completes successfully
    - All work must be awaited synchronously to prevent data loss (Issue #38)
    - No background task tracking - the handler must complete all work before returning
    - Transport type (EventHub/Kafka) is selected via PIPELINE_TRANSPORT env var
    """

    WORKER_NAME = "enrichment_worker"

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "claimx",
        enable_delta_writes: bool = True,
        producer_config: MessageConfig | None = None,
        projects_table_path: str = "",
        instance_id: str | None = None,
        api_client: Any | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id
        self.enrichment_topic = config.get_topic(domain, "enrichment_pending")
        self.download_topic = config.get_topic(domain, "downloads_pending")
        self.entity_rows_topic = config.get_topic(domain, "enriched")
        self.enable_delta_writes = enable_delta_writes

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [self.enrichment_topic]

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self.batch_size = 50
        self.batch_timeout_ms = 1000

        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        self.producer = None
        self.download_producer = None
        self.task_event_producer = None
        self.video_event_producer = None
        self.consumer = None
        self.api_client: Any | None = None
        self._injected_api_client = api_client
        self.retry_handler: EnrichmentRetryHandler | None = None

        self.handler_registry: HandlerRegistry = get_handler_registry()

        # ITEL event routing: publishes matching task events to ITEL pending topic
        self.itel_producer = None

        # Project cache prevents redundant API calls for in-flight verification
        self._preload_cache_from_delta = False
        self.project_cache = ProjectCache()

        # port=0 for dynamic port assignment (avoids conflicts with multiple workers)
        health_port = 0
        health_enabled = True
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-enricher",
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

        self._projects_table_path = projects_table_path

        logger.info(
            "Initialized ClaimXEnrichmentWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "enrichment_worker"),
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "project_cache_preload": self._preload_cache_from_delta,
                "api_client_injected": api_client is not None,
            },
        )

    async def _preload_project_cache(self) -> None:
        """Preload project cache with existing project IDs from Delta table to reduce API calls."""
        if not self._projects_table_path:
            logger.warning("Cannot preload project cache - projects_table_path not configured")
            return

        try:
            logger.info(
                "Preloading project cache from Delta",
                extra={"table_path": self._projects_table_path},
            )

            reader = DeltaTableReader(self._projects_table_path)
            if not reader.exists():
                logger.info("Projects table does not exist yet, skipping preload")
                return

            df = reader.read(columns=["project_id"])
            if df.is_empty():
                logger.info("Projects table is empty, skipping preload")
                return

            project_ids = df["project_id"].drop_nulls().unique().to_list()
            loaded_count = self.project_cache.load_from_ids(project_ids)

            logger.info(
                "Project cache preloaded from Delta",
                extra={
                    "table_path": self._projects_table_path,
                    "unique_project_ids": len(project_ids),
                    "loaded_to_cache": loaded_count,
                    "cache_size": self.project_cache.size(),
                },
            )

        except Exception as e:
            logger.warning(
                "Failed to preload project cache from Delta, continuing without preload",
                extra={
                    "table_path": self._projects_table_path,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

    async def _cleanup_stale_resources(self) -> None:
        """Close resources from a previous failed start attempt to prevent leaks.

        _start_with_retry() may call start() multiple times; without cleanup,
        each attempt overwrites self.api_client and orphans the old session.
        """
        await self._close_resource("_stats_logger")
        if self.api_client and not self._injected_api_client:
            await self._close_resource("api_client", action="close")
        await self._close_resource("producer")
        await self._close_resource("download_producer")
        await self._close_resource("task_event_producer")
        await self._close_resource("video_event_producer")
        await self._close_resource("retry_handler")

    def _init_api_client(self) -> None:
        """Initialize or attach the API client."""
        if self._injected_api_client is not None:
            self.api_client = self._injected_api_client
            logger.info(
                "Using injected API client",
                extra={
                    "api_client_type": type(self.api_client).__name__,
                    "worker_id": self.worker_id,
                },
            )
        else:
            self.api_client = ClaimXApiClient(
                base_url=self.consumer_config.claimx_api_url,
                token=self.consumer_config.claimx_api_token,
                timeout_seconds=self.consumer_config.claimx_api_timeout_seconds,
                max_concurrent=self.consumer_config.claimx_api_concurrency,
            )

    async def _init_producers(self) -> None:
        """Create and start Kafka producers."""
        started: list[str] = []
        try:
            self.producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic_key="enriched",
            )
            await self.producer.start()
            started.append("producer")

            self.download_producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic_key="downloads_pending",
            )
            await self.download_producer.start()
            started.append("download_producer")

            self.task_event_producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic_key="task_events",
            )
            await self.task_event_producer.start()
            started.append("task_event_producer")

            self.video_event_producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic_key="video_events",
            )
            await self.video_event_producer.start()
        except Exception:
            for attr in reversed(started):
                await self._close_resource(attr)
            raise

    # Task IDs that route to the ITEL cabinet pending topic
    ITEL_TASK_IDS = {32615, 24454}
    ITEL_PENDING_TOPIC = "pcesdopodappv1-itel-cabinet-pending"
    ITEL_CABINET_DAMAGE_QUESTION = "Kitchen cabinet damage present"

    async def _init_itel_routing(self) -> None:
        """Create producer for ITEL event routing."""
        try:
            self.itel_producer = create_producer(
                config=self.producer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topic=self.ITEL_PENDING_TOPIC,
            )
            await self.itel_producer.start()
            logger.info(
                "ITEL event routing initialized",
                extra={
                    **producer_log_fields(output_topic=self.ITEL_PENDING_TOPIC),
                    "task_ids": list(self.ITEL_TASK_IDS),
                },
            )
        except Exception as e:
            logger.warning(
                "Failed to init ITEL routing, continuing without it",
                extra={"error": str(e)},
                exc_info=True,
            )
            self.itel_producer = None

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running")
            return

        logger.info("Starting ClaimXEnrichmentWorker")
        self._running = True

        await self.health_server.start()

        log_output_mode = detect_log_output_mode()
        log_startup_banner(
            logger,
            worker_name="ClaimX Enrichment Worker",
            instance_id=self.worker_id,
            domain=self.domain,
            input_topic=self.enrichment_topic,
            output_topic=self.download_topic,
            health_port=self.health_server.port,
            log_output_mode=log_output_mode,
        )

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "enrichment-worker")

        await self._cleanup_stale_resources()

        try:
            self._stats_logger = PeriodicStatsLogger(
                interval_seconds=CYCLE_LOG_INTERVAL_SECONDS,
                get_stats=self._get_cycle_stats,
                stage="enrichment",
                worker_id=self.worker_id,
            )
            self._stats_logger.start()

            self._init_api_client()
            await self.api_client._ensure_session()

            api_reachable = not self.api_client.is_circuit_open
            self.health_server.set_ready(transport_connected=False, api_reachable=api_reachable)

            await self._init_producers()

            self.retry_handler = EnrichmentRetryHandler(config=self.consumer_config)
            await self.retry_handler.start()
            logger.info(
                "Retry handler initialized",
                extra={
                    "retry_topics": [t for t in self.topics if "retry" in t],
                    "dlq_topic": self.retry_handler.dlq_topic,
                },
            )

            await self._init_itel_routing()

            if self._preload_cache_from_delta:
                await self._preload_project_cache()

            self.consumer = await create_batch_consumer(
                config=self.consumer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topics=[self.enrichment_topic],
                batch_handler=self._process_batch,
                topic_key="enrichment_pending",
                consumer_config=ConsumerConfig(
                    batch_size=self.batch_size,
                    batch_timeout_ms=self.batch_timeout_ms,
                    instance_id=self.instance_id,
                ),
            )

            self.health_server.set_ready(
                transport_connected=True,
                api_reachable=api_reachable,
                circuit_open=self.api_client.is_circuit_open,
            )

            await self.consumer.start()

        except asyncio.CancelledError:
            raise
        except Exception:
            await self.stop()
            raise
        finally:
            self._running = False

    async def _close_resource(self, attr_name: str, action: str = "stop") -> None:
        """Close a resource by attribute name, logging errors and clearing the reference."""
        resource = getattr(self, attr_name, None)
        if not resource:
            return
        try:
            await getattr(resource, action)()
        except asyncio.CancelledError:
            logger.warning("Cancelled while closing %s", attr_name)
        except Exception as e:
            logger.error("Error closing %s", attr_name, extra={"error": str(e)})
        finally:
            setattr(self, attr_name, None)

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEnrichmentWorker")
        self._running = False

        await self._close_resource("_stats_logger")
        await self._close_resource("consumer")
        await self._close_resource("retry_handler")
        await self._close_resource("producer")
        await self._close_resource("download_producer")
        await self._close_resource("task_event_producer")
        await self._close_resource("video_event_producer")
        await self._close_resource("itel_producer")
        await self._close_resource("api_client", action="close")
        await self.health_server.stop()

        logger.info("ClaimXEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    def _parse_enrichment_messages(
        self, messages: list[PipelineMessage]
    ) -> tuple[
        list[tuple[PipelineMessage, ClaimXEnrichmentTask, ClaimXEventMessage]],
        list[tuple[PipelineMessage, Exception]],
    ]:
        """Parse raw messages into validated tasks and events."""
        parsed = []
        failures = []
        for msg in messages:
            try:
                data = orjson.loads(msg.value)
                task = ClaimXEnrichmentTask.model_validate(data)
                event = ClaimXEventMessage(
                    trace_id=task.trace_id,
                    event_type=task.event_type,
                    project_id=task.project_id,
                    media_id=task.media_id,
                    task_assignment_id=task.task_assignment_id,
                    video_collaboration_id=task.video_collaboration_id,
                    master_file_name=task.master_file_name,
                    media_ids=task.media_ids,
                    ingested_at=task.created_at,
                )
                parsed.append((msg, task, event))
            except (orjson.JSONDecodeError, ValidationError) as e:
                failures.append((msg, PermanentError(str(e))))
        return parsed, failures

    async def _preflight_project_check(
        self, parsed: list[tuple[PipelineMessage, ClaimXEnrichmentTask, ClaimXEventMessage]]
    ) -> None:
        """Verify projects exist and dispatch project entity rows (deduplicated)."""
        project_ids = {task.project_id for _, task, _ in parsed if task.project_id}
        if not project_ids:
            return

        merged_rows = await self._fetch_and_merge_project_rows(project_ids)

        if self.enable_delta_writes and not merged_rows.is_empty():
            tasks_for_dispatch = [task for _, task, _ in parsed]
            await self._produce_entity_rows(merged_rows, tasks_for_dispatch)

    async def _fetch_and_merge_project_rows(
        self, project_ids: set[str],
    ) -> EntityRowsMessage:
        """Fetch project rows for each project ID and merge into a single message."""
        merged = EntityRowsMessage()
        for pid in project_ids:
            try:
                rows = await self._ensure_project_exists(pid)
                if not rows.is_empty():
                    merged.merge(rows)
            except Exception as e:
                logger.warning(
                    "Failed to fetch project rows for project",
                    extra={"project_id": pid, "error": str(e)[:200]},
                )
        return merged

    async def _execute_handler_groups(
        self,
        parsed: list[tuple[PipelineMessage, ClaimXEnrichmentTask, ClaimXEventMessage]],
    ) -> list[tuple[Any, PipelineMessage, ClaimXEnrichmentTask]]:
        """Group events by handler, execute each group, return results."""
        event_to_info: dict[int, tuple[PipelineMessage, ClaimXEnrichmentTask]] = {
            id(event): (msg, task) for msg, task, event in parsed
        }

        events = [event for _, _, event in parsed]
        grouped = self.handler_registry.group_events_by_handler(events)
        all_results = []
        handled_count = 0

        for handler_class, handler_events in grouped.items():
            handled_count += len(handler_events)
            handler = handler_class(
                self.api_client,
                project_cache=self.project_cache,
                task_event_producer=self.task_event_producer,
                video_event_producer=self.video_event_producer,
            )
            try:
                results = await handler.handle_batch(handler_events)
                for result in results:
                    msg, task = event_to_info[id(result.event)]
                    all_results.append((result, msg, task))
            except Exception as e:
                for evt in handler_events:
                    msg, task = event_to_info[id(evt)]
                    await self._handle_enrichment_failure(task, e, ErrorCategory.TRANSIENT)

        self._records_skipped += len(parsed) - handled_count

        return all_results

    async def _tally_and_route_results(
        self,
        all_results: list[tuple[Any, PipelineMessage, ClaimXEnrichmentTask]],
    ) -> tuple[EntityRowsMessage, list[tuple[PipelineMessage, Exception]]]:
        """Tally results and route failures. Returns (entity_rows, permanent_failures)."""
        all_entity_rows = EntityRowsMessage()
        permanent_failures: list[tuple[PipelineMessage, Exception]] = []

        for result, msg, task in all_results:
            self._records_processed += 1
            if result.success:
                self._records_succeeded += 1
                if result.rows:
                    all_entity_rows.merge(result.rows)
            else:
                error = result.original_error if hasattr(result, 'original_error') and result.original_error else Exception(result.error or "Handler returned failure")
                category = result.error_category or ErrorCategory.TRANSIENT
                if not result.is_retryable:
                    permanent_failures.append((msg, error))
                else:
                    await self._handle_enrichment_failure(task, error, category)

        return all_entity_rows, permanent_failures

    async def _dispatch_batch_results(
        self,
        all_results: list[tuple[Any, PipelineMessage, ClaimXEnrichmentTask]],
        parsed: list[tuple[PipelineMessage, ClaimXEnrichmentTask, ClaimXEventMessage]],
        permanent_failures: list[tuple[PipelineMessage, Exception]],
    ) -> None:
        """Tally results, dispatch entity rows and download tasks."""
        all_entity_rows, new_failures = await self._tally_and_route_results(all_results)
        permanent_failures.extend(new_failures)

        if self.enable_delta_writes and not all_entity_rows.is_empty():
            all_tasks = [task for _, task, _ in parsed]
            await self._produce_entity_rows(all_entity_rows, all_tasks)

        if all_entity_rows.media:
            download_tasks = create_download_tasks_from_media(all_entity_rows.media)
            if download_tasks:
                await self._produce_download_tasks(download_tasks)

    async def _process_batch(self, messages: list[PipelineMessage]) -> BatchResult:
        """Process a batch of enrichment messages.

        Groups events by handler so MediaHandler's project-level batching
        (1 API call per project instead of N) is utilized.
        """
        parsed, permanent_failures = self._parse_enrichment_messages(messages)
        if not parsed:
            return BatchResult(commit=True, permanent_failures=permanent_failures)

        await self._preflight_project_check(parsed)
        all_results = await self._execute_handler_groups(parsed)

        for result, _msg, task in all_results:
            if result.success:
                await self._route_itel_events(task, result.event, result.rows)

        await self._dispatch_batch_results(all_results, parsed, permanent_failures)

        return BatchResult(commit=True, permanent_failures=permanent_failures)

    async def _handle_enrichment_task(self, record: PipelineMessage) -> None:
        """Process enrichment task. No batching at enricher level - delta writer handles batching."""
        try:
            message_data = orjson.loads(record.value)
            task = ClaimXEnrichmentTask.model_validate(message_data)
        except (orjson.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXEnrichmentTask",
                extra={
                    "trace_id": record.key.decode("utf-8") if record.key else None,
                    "message_topic": record.topic,
                    "message_partition": record.partition,
                    "message_offset": record.offset,
                    "message_key": record.key.decode("utf-8") if record.key else None,
                    "error": str(e),
                    "raw_payload_bytes": len(record.value) if record.value else 0,
                    "payload_sha256": (
                        hashlib.sha256(record.value).hexdigest() if record.value else None
                    ),
                },
                exc_info=True,
            )
            raise PermanentError(str(e)) from e

        self._records_processed += 1

        ts = record.timestamp
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

        set_log_context(trace_id=task.trace_id)

        logger.info(
            "Processing enrichment task",
            extra={
                "trace_id": task.trace_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
            },
        )

        await self._process_single_task(task)

    async def _dispatch_entity_rows(
        self,
        task: ClaimXEnrichmentTask,
        entity_rows: EntityRowsMessage,
    ) -> None:
        """
        Dispatch entity rows to Kafka for Delta Lake writing.

        Awaits entity row production to ensure writes complete before offset commit.
        Critical for preventing data loss (Issue #38).

        Args:
            task: Original enrichment task
            entity_rows: Entity rows to produce
        """
        if not self.enable_delta_writes or entity_rows.is_empty():
            return

        await self._produce_entity_rows(entity_rows, [task])

    async def _dispatch_download_tasks(
        self,
        entity_rows: EntityRowsMessage,
    ) -> int:
        """
        Create and dispatch download tasks for media files.

        Args:
            entity_rows: Entity rows containing media metadata

        Returns:
            Number of download tasks produced.
        """
        if not entity_rows.media:
            return 0

        download_tasks = create_download_tasks_from_media(entity_rows.media)
        if download_tasks:
            await self._produce_download_tasks(download_tasks)
        return len(download_tasks)

    async def _handle_handler_failure(self, task, handler_result, handler_class) -> None:
        """Route a failed handler result to retry/DLQ."""
        error_msg = (
            handler_result.errors[0] if handler_result.errors else "Handler returned failure"
        )
        error = Exception(error_msg)
        error_category = (
            ErrorCategory.PERMANENT if handler_result.failed_permanent > 0
            else ErrorCategory.TRANSIENT
        )
        log_worker_error(
            logger,
            "Handler returned failure result",
            error_category=error_category.value,
            trace_id=task.trace_id,
            handler=handler_class.__name__,
            error_detail=error_msg[:200],
            succeeded=handler_result.succeeded,
            failed=handler_result.failed,
            failed_permanent=handler_result.failed_permanent,
        )
        await self._handle_enrichment_failure(task, error, error_category)

    @staticmethod
    def _has_cabinet_damage(task_row: dict) -> bool:
        """Check if the form response indicates kitchen cabinet damage is present.

        Inspects _form_response_groups attached to the task_row for the
        "Kitchen cabinet damage present" question.  Returns True only when the
        answer is an affirmative value (e.g. "Yes"); returns False for "No",
        missing question, or missing form data.
        """
        groups = task_row.get("_form_response_groups", [])
        for group in groups:
            for qa in group.get("questionAndAnswers", []):
                q_text = (qa.get("questionText") or "").strip()
                if q_text.lower() != ClaimXEnrichmentWorker.ITEL_CABINET_DAMAGE_QUESTION.lower():
                    continue
                export = qa.get("responseAnswerExport", {})
                answer_type = export.get("type", "")
                if answer_type == "option":
                    name = (export.get("optionAnswer") or {}).get("name", "")
                    return name.lower().startswith("yes")
                if answer_type == "text":
                    text = (export.get("text") or "").strip().lower()
                    return text.startswith("yes") or text == "true"
                return bool(export.get("text") or export.get("optionAnswer"))
        return False

    async def _route_itel_events(self, task, event, entity_rows) -> None:
        """Route matching task events to the ITEL cabinet pending topic (best-effort).

        Replaces the former plugin-based TaskTriggerPlugin. Checks if any task
        in the enriched entities matches ITEL task IDs and publishes to the
        ITEL pending topic for downstream processing.

        Skips routing on retries to prevent duplicate ITEL events when
        downstream steps (e.g. entity row production) fail and trigger a retry.
        """
        if not self.itel_producer:
            return

        if task.retry_count > 0:
            logger.debug(
                "ITEL event routing skipped on retry",
                extra={"trace_id": task.trace_id, "retry_count": task.retry_count},
            )
            return

        try:
            tasks = entity_rows.tasks if entity_rows else []
            for task_row in tasks:
                task_id = task_row.get("task_id")
                if task_id not in self.ITEL_TASK_IDS:
                    continue

                if not self._has_cabinet_damage(task_row):
                    logger.info(
                        "ITEL event skipped: no cabinet damage",
                        extra={
                            "trace_id": task.trace_id,
                            "task_id": task_id,
                            "assignment_id": task_row.get("assignment_id"),
                        },
                    )
                    continue

                payload = {
                    "trigger_name": "iTel Cabinet Repair Form Task",
                    "event_id": task.trace_id,
                    "event_type": task.event_type,
                    "project_id": task.project_id,
                    "task_id": task_id,
                    "assignment_id": task_row.get("assignment_id"),
                    "task_name": task_row.get("task_name"),
                    "task_status": task_row.get("status"),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "media_ids": task.media_ids,
                    "task": task_row,
                }
                # Include project data if available
                if entity_rows.projects:
                    payload["project"] = entity_rows.projects[0]

                await self.itel_producer.send(
                    value=payload,
                    key=task.trace_id,
                    headers={
                        "x-trigger-name": "iTel Cabinet Repair Form Task",
                        "x-task-id": str(task_id),
                        "x-event-type": task.event_type or "",
                    },
                )
                logger.info(
                    "ITEL event routed",
                    extra={
                        "trace_id": task.trace_id,
                        "task_id": task_id,
                        "assignment_id": task_row.get("assignment_id"),
                        "event_type": task.event_type,
                        "project_id": task.project_id,
                    },
                )
        except Exception as e:
            logger.error(
                "ITEL event routing failed",
                extra={"trace_id": task.trace_id, "error": str(e)},
                exc_info=True,
            )

    async def _process_single_task(self, task: ClaimXEnrichmentTask) -> None:
        """
        Process single enrichment task through the pipeline.

        Flow:
        1. Ensure project exists in cache
        2. Create event message from task
        3. Execute handler to fetch API data
        4. Dispatch entity rows to Kafka
        5. Dispatch download tasks for media files
        """
        start_time = datetime.now(UTC)

        if task.project_id:
            try:
                project_rows = await self._ensure_project_exists(task.project_id)
                if not project_rows.is_empty():
                    await self._dispatch_entity_rows(task, project_rows)
            except Exception:
                logger.debug(
                    "Pre-flight project verification failed, handler will retry",
                    extra={"project_id": task.project_id, "trace_id": task.trace_id},
                )

        event = ClaimXEventMessage(
            trace_id=task.trace_id,
            event_type=task.event_type,
            project_id=task.project_id,
            media_id=task.media_id,
            task_assignment_id=task.task_assignment_id,
            video_collaboration_id=task.video_collaboration_id,
            master_file_name=task.master_file_name,
            media_ids=task.media_ids,
            ingested_at=task.created_at,
        )

        handler_class = self.handler_registry.get_handler_class(event.event_type)
        if not handler_class:
            logger.warning(
                "No handler found for event",
                extra={"trace_id": event.trace_id, "event_type": event.event_type},
            )
            self._records_skipped += 1
            return

        handler = handler_class(
            self.api_client,
            project_cache=self.project_cache,
            task_event_producer=self.task_event_producer,
            video_event_producer=self.video_event_producer,
        )

        try:
            handler_result = await handler.process([event])

            if handler_result.failed > 0:
                await self._handle_handler_failure(task, handler_result, handler_class)
                return

            entity_rows = handler_result.rows
            self._records_succeeded += 1

            await self._route_itel_events(task, event, entity_rows)
            await self._dispatch_entity_rows(task, entity_rows)
            download_task_count = await self._dispatch_download_tasks(entity_rows)

            elapsed_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
            logger.info(
                "Enrichment task complete",
                extra={
                    "trace_id": task.trace_id,
                    "event_type": task.event_type,
                    "project_id": task.project_id,
                    "handler": handler_class.__name__,
                    "entity_rows": entity_rows.row_count(),
                    "download_tasks_produced": download_task_count,
                    "api_calls": handler_result.api_calls,
                    "duration_ms": round(elapsed_ms, 2),
                },
            )

        except ClaimXApiError as e:
            log_worker_error(
                logger, "Handler failed with API error",
                error_category=e.category.value, trace_id=task.trace_id,
                exc=e, handler=handler_class.__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=e.category.value,
            )
            await self._handle_enrichment_failure(task, e, e.category)

        except Exception as e:
            error_category = ErrorCategory.UNKNOWN
            log_worker_error(
                logger, "Handler failed with unexpected error",
                error_category=error_category.value, trace_id=task.trace_id,
                exc=e, handler=handler_class.__name__, error_type=type(e).__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=error_category.value,
            )
            await self._handle_enrichment_failure(task, e, error_category)

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        # PeriodicStatsLogger will format the message with deltas
        # We just need to return the extra fields with cumulative counts
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "records_deduplicated": 0,
            "project_cache_size": self.project_cache.size(),
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None

        # Message will be replaced by PeriodicStatsLogger
        return "", extra

    async def _ensure_project_exists(
        self,
        project_id: str,
    ) -> EntityRowsMessage:
        """Fetch project data from API if not already in cache.

        Universal pre-flight check that guarantees every event's project
        exists in the Delta table, regardless of which handler processes
        the event or whether that handler succeeds.
        """
        from pipeline.claimx.handlers.project import ProjectHandler

        project_handler = ProjectHandler(self.api_client, project_cache=self.project_cache)
        return await project_handler.fetch_project_data(
            int(project_id),
            trace_id=None,
        )

    async def _produce_entity_rows(
        self,
        entity_rows: EntityRowsMessage,
        tasks: list[ClaimXEnrichmentTask],
    ) -> None:
        """Write entity rows to Kafka. On failure, routes all tasks to retry/DLQ."""
        batch_id = uuid.uuid4().hex[:8]
        trace_ids = [task.trace_id for task in tasks[:5]]

        try:
            trace_id = tasks[0].trace_id if tasks else batch_id
            entity_rows.trace_id = trace_id
            await self.producer.send(
                value=entity_rows,
                key=trace_id,
                headers={"trace_id": trace_id},
            )

            logger.info(
                "Produced entity rows batch",
                extra={
                    "batch_id": batch_id,
                    "trace_id": trace_id,
                    "trace_ids": trace_ids,
                    "row_count": entity_rows.row_count(),
                },
            )

        except Exception as e:
            logger.error(
                "Error writing entities to Delta - routing batch to retry",
                extra={
                    "batch_id": batch_id,
                    "trace_id": trace_id,
                    "trace_ids": trace_ids,
                    "row_count": entity_rows.row_count(),
                    "task_count": len(tasks),
                    "error_category": ErrorCategory.TRANSIENT.value,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

            record_delta_write(
                table="claimx_entities_produce",
                event_count=entity_rows.row_count(),
                success=False,
            )

            error_category = ErrorCategory.TRANSIENT
            for task in tasks:
                await self._handle_enrichment_failure(task, e, error_category)

    async def _produce_download_tasks(
        self,
        download_tasks: list[ClaimXDownloadTask],
    ) -> None:
        trace_ids = list({t.trace_id for t in download_tasks})
        logger.info(
            "Producing download tasks",
            extra={
                "task_count": len(download_tasks),
                "trace_ids": trace_ids,
            },
        )

        for task in download_tasks:
            try:
                metadata = await self.download_producer.send(
                    value=task,
                    key=task.trace_id,
                    headers={"trace_id": task.trace_id},
                )

                logger.info(
                    "Produced download task",
                    extra={
                        "trace_id": task.trace_id,
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "blob_path": task.blob_path,
                        **producer_log_fields(self.download_topic, metadata),
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "trace_id": task.trace_id,
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def _handle_enrichment_failure(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: "ErrorCategory",
    ) -> None:
        """
        Route failed task to retry topic or DLQ based on error category and retry count.
        TRANSIENT/AUTH/CIRCUIT_OPEN/UNKNOWN -> retry with backoff, PERMANENT -> DLQ immediately.
        """
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        log_worker_error(
            logger,
            "Enrichment task failed",
            error_category=error_category.value,
            trace_id=task.trace_id,
            exc=error,
            event_type=task.event_type,
            project_id=task.project_id,
            retry_count=task.retry_count,
        )

        self._records_failed += 1

        await self.retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=error_category,
        )


__all__ = ["ClaimXEnrichmentWorker"]
