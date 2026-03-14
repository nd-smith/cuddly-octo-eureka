"""Verisk domain worker runners.

Contains all runner functions for XACT pipeline workers:
- Event ingestion (Event Hub)
- Enrichment
- Download/Upload
- Retry scheduling
"""

import asyncio
import logging
from pathlib import Path

from pipeline.runners.common import (
    execute_worker_with_shutdown,
)

logger = logging.getLogger(__name__)


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    shutdown_event: asyncio.Event,
    domain: str = "verisk",
    instance_id: int | None = None,
):
    """Reads events from Event Hub and produces download tasks to local Kafka."""
    from pipeline.verisk.workers.event_ingester import EventIngesterWorker

    worker = EventIngesterWorker(
        config=eventhub_config,
        domain=domain,
        producer_config=local_kafka_config,
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-event-ingester",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_xact_retry_scheduler(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Unified retry scheduler for all XACT retry types.
    Routes messages from xact.retry topic to target topics based on headers."""
    from pipeline.common.retry.unified_scheduler import UnifiedRetryScheduler

    scheduler = UnifiedRetryScheduler(
        config=kafka_config,
        domain="verisk",
        target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
        persistence_dir=kafka_config.retry_persistence_dir,
    )
    await execute_worker_with_shutdown(
        scheduler,
        stage_name="xact-retry-scheduler",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_xact_enrichment_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Run XACT enrichment worker with plugin-based enrichment."""
    from pipeline.verisk.workers.enrichment_worker import XACTEnrichmentWorker

    worker = XACTEnrichmentWorker(
        config=kafka_config,
        domain="verisk",
        instance_id=instance_id,
    )

    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-enricher",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_download_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Download files from external sources."""
    from pipeline.verisk.workers.download_worker import DownloadWorker

    worker = DownloadWorker(
        config=kafka_config,
        domain="verisk",
        temp_dir=Path(kafka_config.temp_dir),
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-download",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_upload_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Upload cached files to storage."""
    from pipeline.verisk.workers.upload_worker import UploadWorker

    worker = UploadWorker(config=kafka_config, domain="verisk", instance_id=instance_id)

    await execute_worker_with_shutdown(
        worker,
        stage_name="xact-upload",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )
