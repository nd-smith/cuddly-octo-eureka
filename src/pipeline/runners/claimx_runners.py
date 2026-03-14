"""ClaimX domain worker runners.

Contains all runner functions for ClaimX pipeline workers:
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


async def run_claimx_event_ingester(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX event ingester worker."""
    from pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

    worker = ClaimXEventIngesterWorker(
        config=kafka_config,
        domain="claimx",
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-ingester",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_enrichment_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    claimx_projects_table_path: str = "",
    instance_id: int | None = None,
):
    """ClaimX enrichment worker with entity extraction."""
    from pipeline.claimx.workers.enrichment_worker import (
        ClaimXEnrichmentWorker,
    )

    worker = ClaimXEnrichmentWorker(
        config=kafka_config,
        domain="claimx",
        projects_table_path=claimx_projects_table_path,
        instance_id=instance_id,
    )

    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-enricher",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_download_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX download worker."""
    from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    worker = ClaimXDownloadWorker(
        config=kafka_config,
        domain="claimx",
        temp_dir=Path(kafka_config.temp_dir),
        instance_id=instance_id,
    )
    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-downloader",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_upload_worker(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """ClaimX upload worker."""
    from pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

    worker = ClaimXUploadWorker(config=kafka_config, domain="claimx", instance_id=instance_id)

    await execute_worker_with_shutdown(
        worker,
        stage_name="claimx-uploader",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )


async def run_claimx_retry_scheduler(
    kafka_config,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
):
    """Unified retry scheduler for all ClaimX retry types.
    Routes messages from claimx.retry topic to target topics based on headers."""
    from pipeline.common.retry.unified_scheduler import UnifiedRetryScheduler

    scheduler = UnifiedRetryScheduler(
        config=kafka_config,
        domain="claimx",
        target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
        persistence_dir=kafka_config.retry_persistence_dir,
    )
    await execute_worker_with_shutdown(
        scheduler,
        stage_name="claimx-retry-scheduler",
        shutdown_event=shutdown_event,
        instance_id=instance_id,
    )
