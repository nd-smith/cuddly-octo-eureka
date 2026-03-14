"""Tests for verisk domain worker runners.

Verifies that each runner function:
- Instantiates the correct worker/poller class with expected arguments
- Delegates to the correct execute_* helper from common
- Passes through shutdown_event, instance_id, and domain correctly

Note: Runner functions use local imports for worker classes, so we patch
at the source module (e.g., pipeline.verisk.workers.event_ingester.EventIngesterWorker).
The execute_* helpers are imported at module level and patched on the runners module.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from pipeline.runners.verisk_runners import (
    run_download_worker,
    run_event_ingester,
    run_upload_worker,
    run_xact_enrichment_worker,
    run_xact_retry_scheduler,
)

# ---------------------------------------------------------------------------
# run_event_ingester
# ---------------------------------------------------------------------------


class TestRunEventIngester:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        eh_config = Mock()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_event_ingester(
                eh_config, kafka_config, shutdown, domain="verisk", instance_id=None
            )

        MockWorker.assert_called_once_with(
            config=eh_config,
            domain="verisk",
            producer_config=kafka_config,
            instance_id=None,
        )
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-event-ingester",
            shutdown_event=shutdown,
            instance_id=None,
        )

    async def test_passes_instance_id(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_event_ingester(Mock(), Mock(), shutdown, domain="verisk", instance_id=7)

        assert MockWorker.call_args[1]["instance_id"] == 7
        assert mock_exec.call_args[1]["instance_id"] == 7

    async def test_passes_custom_domain(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_event_ingester(Mock(), Mock(), shutdown, domain="custom")

        assert MockWorker.call_args[1]["domain"] == "custom"


# ---------------------------------------------------------------------------
# run_xact_retry_scheduler
# ---------------------------------------------------------------------------


class TestRunXactRetryScheduler:
    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.common.retry.unified_scheduler.UnifiedRetryScheduler") as MockScheduler,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_xact_retry_scheduler(kafka_config, shutdown, instance_id=1)

        MockScheduler.assert_called_once_with(
            config=kafka_config,
            domain="verisk",
            target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
            persistence_dir=kafka_config.retry_persistence_dir,
        )
        mock_exec.assert_awaited_once_with(
            MockScheduler.return_value,
            stage_name="xact-retry-scheduler",
            shutdown_event=shutdown,
            instance_id=1,
        )


# ---------------------------------------------------------------------------
# run_xact_enrichment_worker
# ---------------------------------------------------------------------------


class TestRunXactEnrichmentWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.enrichment_worker.XACTEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_xact_enrichment_worker(kafka_config, shutdown, instance_id=3)

        MockWorker.assert_called_once_with(config=kafka_config, domain="verisk", instance_id=3)
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-enricher",
            shutdown_event=shutdown,
            instance_id=3,
        )

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.enrichment_worker.XACTEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_xact_enrichment_worker(Mock(), shutdown)

        assert MockWorker.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_download_worker
# ---------------------------------------------------------------------------


class TestRunDownloadWorker:
    async def test_creates_worker_with_temp_dir_as_path(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp/downloads"

        with (
            patch("pipeline.verisk.workers.download_worker.DownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_download_worker(kafka_config, shutdown, instance_id=5)

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="verisk",
            temp_dir=Path("/tmp/downloads"),
            instance_id=5,
        )

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp"

        with (
            patch("pipeline.verisk.workers.download_worker.DownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_download_worker(kafka_config, shutdown, instance_id=None)

        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-download",
            shutdown_event=shutdown,
            instance_id=None,
        )


# ---------------------------------------------------------------------------
# run_upload_worker
# ---------------------------------------------------------------------------


class TestRunUploadWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.upload_worker.UploadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_upload_worker(kafka_config, shutdown, instance_id=2)

        MockWorker.assert_called_once_with(config=kafka_config, domain="verisk", instance_id=2)

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.upload_worker.UploadWorker"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_upload_worker(kafka_config, shutdown)

        mock_exec.assert_awaited_once()
        assert mock_exec.call_args[1]["stage_name"] == "xact-upload"
