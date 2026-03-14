"""Tests for claimx domain worker runners.

Verifies that each runner function:
- Instantiates the correct worker/poller class with expected arguments
- Delegates to the correct execute_* helper from common
- Passes through shutdown_event, instance_id, and domain correctly

Note: Runner functions use local imports for worker classes, so we patch
at the source module (e.g., pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker).
The execute_* helpers are imported at module level and patched on the runners module.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from pipeline.runners.claimx_runners import (
    run_claimx_download_worker,
    run_claimx_enrichment_worker,
    run_claimx_event_ingester,
    run_claimx_retry_scheduler,
    run_claimx_upload_worker,
)

# ---------------------------------------------------------------------------
# run_claimx_event_ingester
# ---------------------------------------------------------------------------


class TestRunClaimxEventIngester:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_event_ingester(kafka_config, shutdown, instance_id=3)

        MockWorker.assert_called_once_with(config=kafka_config, domain="claimx", instance_id=3)
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-ingester",
            shutdown_event=shutdown,
            instance_id=3,
        )

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_event_ingester(Mock(), shutdown)

        assert MockWorker.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_claimx_enrichment_worker
# ---------------------------------------------------------------------------


class TestRunClaimxEnrichmentWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.enrichment_worker.ClaimXEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_enrichment_worker(
                kafka_config,
                shutdown,
                claimx_projects_table_path="/delta/projects",
                instance_id=2,
            )

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            projects_table_path="/delta/projects",
            instance_id=2,
        )
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-enricher",
            shutdown_event=shutdown,
            instance_id=2,
        )


# ---------------------------------------------------------------------------
# run_claimx_download_worker
# ---------------------------------------------------------------------------


class TestRunClaimxDownloadWorker:
    async def test_creates_worker_with_temp_dir_as_path(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp/claimx-dl"

        with (
            patch("pipeline.claimx.workers.download_worker.ClaimXDownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_download_worker(kafka_config, shutdown, instance_id=1)

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            temp_dir=Path("/tmp/claimx-dl"),
            instance_id=1,
        )

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp"

        with (
            patch("pipeline.claimx.workers.download_worker.ClaimXDownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_download_worker(kafka_config, shutdown)

        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-downloader",
            shutdown_event=shutdown,
            instance_id=None,
        )


# ---------------------------------------------------------------------------
# run_claimx_upload_worker
# ---------------------------------------------------------------------------


class TestRunClaimxUploadWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.upload_worker.ClaimXUploadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_upload_worker(kafka_config, shutdown, instance_id=4)

        MockWorker.assert_called_once_with(config=kafka_config, domain="claimx", instance_id=4)

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.upload_worker.ClaimXUploadWorker"),
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_upload_worker(Mock(), shutdown)

        assert mock_exec.call_args[1]["stage_name"] == "claimx-uploader"


# ---------------------------------------------------------------------------
# run_claimx_retry_scheduler
# ---------------------------------------------------------------------------


class TestRunClaimxRetryScheduler:
    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.common.retry.unified_scheduler.UnifiedRetryScheduler") as MockScheduler,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_retry_scheduler(kafka_config, shutdown, instance_id=1)

        MockScheduler.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
            persistence_dir=kafka_config.retry_persistence_dir,
        )
        mock_exec.assert_awaited_once_with(
            MockScheduler.return_value,
            stage_name="claimx-retry-scheduler",
            shutdown_event=shutdown,
            instance_id=1,
        )
