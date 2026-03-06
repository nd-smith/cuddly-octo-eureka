"""
Unit tests for ClaimX Delta Events Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop)
    - Message parsing and validation
    - Batch accumulation (size-based and time-based)
    - Delta writes with ClaimXEventsDeltaWriter
    - Retry handler integration for failed Delta writes
    - Graceful shutdown with batch flushing
    - Periodic logging and metrics

No infrastructure required - all dependencies mocked.
"""

import contextlib
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from core.errors.exceptions import PermanentError
from pipeline.claimx.workers.delta_events_worker import ClaimXDeltaEventsWorker
from pipeline.common.types import PipelineMessage


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "claimx.events"
    config.get_consumer_group.return_value = "claimx-delta-events-writer"

    return config


@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def sample_enrichment_task():
    """Sample enrichment task message (as produced by the event ingester)."""
    from datetime import UTC, datetime

    task_data = {
        "trace_id": "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
        "event_type": "PROJECT_CREATED",
        "project_id": "proj-456",
        "retry_count": 0,
        "created_at": datetime.now(UTC).isoformat(),
        "ingested_at": "2024-01-01T00:00:00+00:00",
        "media_id": None,
        "task_assignment_id": None,
        "video_collaboration_id": None,
        "master_file_name": None,
    }

    return PipelineMessage(
        topic="claimx.enrichment_pending",
        partition=0,
        offset=1,
        key=b"abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
        value=json.dumps(task_data).encode(),
        timestamp=None,
        headers=None,
    )


class TestDeltaEventsWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, mock_producer):
        """Worker initializes with default configuration."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            assert worker.domain == "claimx"
            assert worker.worker_id == "delta_events_writer"
            assert worker.instance_id is None
            assert worker.batch_size == 100
            assert worker.batch_timeout_seconds == 30.0

    def test_initialization_with_instance_id(self, mock_config, mock_producer):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
                instance_id="happy-tiger",
            )

            assert worker.worker_id == "delta_events_writer-happy-tiger"
            assert worker.instance_id == "happy-tiger"

    def test_initialization_requires_table_path(self, mock_config, mock_producer):
        """Worker requires events_table_path."""
        with (
            patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"),
            pytest.raises(ValueError, match="events_table_path is required"),
        ):
            ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="",
            )

    def test_metrics_initialized_to_zero(self, mock_config, mock_producer):
        """Worker initializes metrics to zero."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            assert worker._records_processed == 0
            assert worker._records_succeeded == 0
            assert worker._records_failed == 0
            assert worker._batches_written == 0


class TestDeltaEventsWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config, mock_producer):
        """Worker start initializes all components."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            with (
                patch(
                    "pipeline.claimx.workers.delta_events_worker.create_consumer"
                ) as mock_create_consumer,
                patch("pipeline.common.telemetry.initialize_worker_telemetry"),
                patch.object(worker.health_server, "start", new_callable=AsyncMock),
                patch.object(worker.retry_handler, "start", new_callable=AsyncMock),
            ):
                # Setup mock consumer
                mock_consumer = AsyncMock()
                mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
                mock_create_consumer.return_value = mock_consumer

                with contextlib.suppress(Exception):
                    await worker.start()

                # Verify components were initialized
                assert worker._running is False  # Reset in finally
                assert worker.consumer is not None

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_batch(self, mock_config, mock_producer):
        """Worker stop flushes pending batch."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Setup mocked components
            worker._running = True
            worker.consumer = AsyncMock()
            worker.consumer.stop = AsyncMock()
            worker.retry_handler = AsyncMock()
            worker.retry_handler.stop = AsyncMock()

            # Add pending batch
            worker._batch = [{"eventId": "evt-123"}]

            # Mock flush
            worker._flush_batch = AsyncMock()

            # Stop worker
            await worker.stop()

            # Verify flush was called
            worker._flush_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, mock_producer):
        """Worker stop handles None components gracefully."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Mock retry handler stop
            worker.retry_handler = AsyncMock()
            worker.retry_handler.stop = AsyncMock()

            # All components are None
            assert worker.consumer is None

            # Should not raise
            await worker.stop()


class TestDeltaEventsWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_enrichment_task_parsed_successfully(
        self, mock_config, mock_producer, sample_enrichment_task
    ):
        """Worker parses enrichment task message and extracts delta-relevant fields."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            await worker._handle_event_message(sample_enrichment_task)

            # Verify message was processed
            assert worker._records_processed == 1
            assert len(worker._batch) == 1

            # Verify batch contains only delta-relevant fields
            batch_event = worker._batch[0]
            assert batch_event["trace_id"] == "abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
            assert batch_event["event_type"] == "PROJECT_CREATED"
            assert batch_event["project_id"] == "proj-456"
            assert batch_event["ingested_at"] is not None
            # Enrichment-only fields should be excluded
            assert "created_at" not in batch_event
            assert "retry_count" not in batch_event
            assert "metadata" not in batch_event

    @pytest.mark.asyncio
    async def test_invalid_json_logs_error(self, mock_config, mock_producer):
        """Worker handles invalid JSON gracefully."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            invalid_message = PipelineMessage(
                topic="claimx.events",
                partition=0,
                offset=1,
                key=b"key",
                value=b"invalid json{",
                timestamp=None,
                headers=None,
            )

            # Should raise PermanentError wrapping JSONDecodeError
            with pytest.raises(PermanentError):
                await worker._handle_event_message(invalid_message)

    @pytest.mark.asyncio
    async def test_validation_error_logs_and_skips(self, mock_config, mock_producer):
        """Worker raises PermanentError on validation errors."""
        # Event missing all required fields
        event_data = {"some_unknown_field": "value"}

        message = PipelineMessage(
            topic="claimx.events",
            partition=0,
            offset=1,
            key=b"key",
            value=json.dumps(event_data).encode(),
            timestamp=None,
            headers=None,
        )

        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Should raise PermanentError wrapping ValidationError
            with pytest.raises(PermanentError):
                await worker._handle_event_message(message)


class TestDeltaEventsWorkerBatching:
    """Test batch accumulation and flushing."""

    @pytest.mark.asyncio
    async def test_batch_accumulates_events(self, mock_producer, sample_enrichment_task):
        """Worker accumulates events in batch."""
        # Create config with batch_size=10
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "claimx.events"
        config.get_consumer_group.return_value = "claimx-delta-events-writer"

        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Process multiple messages
            for _ in range(3):
                await worker._handle_event_message(sample_enrichment_task)

            # Verify batch accumulation
            assert len(worker._batch) == 3
            assert worker._records_processed == 3

    @pytest.mark.asyncio
    async def test_batch_flushes_on_size_threshold(self, mock_producer, sample_enrichment_task):
        """Worker flushes batch when size threshold reached."""
        # Create config with batch_size=2
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "claimx.events"
        config.get_consumer_group.return_value = "claimx-delta-events-writer"

        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )
            worker.batch_size = 2

            # Mock flush method
            worker._flush_batch = AsyncMock()

            # Process messages up to threshold
            await worker._handle_event_message(sample_enrichment_task)
            await worker._handle_event_message(sample_enrichment_task)

            # Verify flush was triggered
            worker._flush_batch.assert_called()


class TestDeltaEventsWorkerDeltaWrites:
    """Test Delta table writes."""

    @pytest.mark.asyncio
    async def test_flush_batch_writes_to_delta(self, mock_config, mock_producer):
        """Worker writes batch to Delta table."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Mock delta writer
            worker.delta_writer = AsyncMock()
            worker.delta_writer.write_events = AsyncMock(return_value=True)
            worker.consumer = AsyncMock()
            worker.consumer.commit = AsyncMock()

            # Add to batch
            worker._batch = [{"eventId": "evt-123"}]

            # Flush batch
            await worker._flush_batch()

            # Verify Delta write was called
            assert worker.delta_writer.write_events.called
            assert worker.consumer.commit.called
            assert worker._batches_written == 1
            assert worker._records_succeeded == 1

    @pytest.mark.asyncio
    async def test_flush_batch_routes_to_retry_on_failure(self, mock_config, mock_producer):
        """Worker routes batch to retry handler on Delta write failure."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            # Mock delta writer to return failure
            worker.delta_writer = AsyncMock()
            worker.delta_writer.write_events = AsyncMock(return_value=False)
            worker._handle_failed_batch = AsyncMock()

            # Add to batch
            worker._batch = [{"eventId": "evt-123"}]

            # Flush batch
            await worker._flush_batch()

            # Verify retry handler was called
            assert worker._handle_failed_batch.called
            assert worker._batches_written == 0  # Not incremented on failure
            assert worker._records_failed == 1

    @pytest.mark.asyncio
    async def test_flush_batch_with_empty_batch_is_noop(self, mock_config, mock_producer):
        """Worker handles empty batch flush gracefully."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            worker.delta_writer = AsyncMock()
            worker._batch = []

            await worker._flush_batch()

            # Verify no writes occurred
            assert not worker.delta_writer.write_events.called

    @pytest.mark.asyncio
    async def test_transient_error_clears_batch_after_retry_send(self, mock_config, mock_producer):
        """Transient error clears batch after successful retry send (H5 fix)."""
        with patch("pipeline.claimx.workers.delta_events_worker.DeltaRetryHandler"):
            worker = ClaimXDeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/claimx_events",
            )

            worker.retry_handler = AsyncMock()
            worker.retry_handler.handle_batch_failure = AsyncMock()

            batch = [{"trace_id": "evt-1"}, {"trace_id": "evt-2"}]

            # Simulate leftover batch data (as if transient error happened)
            worker._batch = [{"trace_id": "stale"}]

            await worker._handle_failed_batch(
                batch, Exception("connection timeout")
            )

            # Batch should be cleared after retry send succeeds
            assert len(worker._batch) == 0
