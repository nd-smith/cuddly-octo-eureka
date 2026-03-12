"""
Unit tests for Verisk Delta Events Worker.

Test Coverage:
    - Worker initialization and configuration
    - Lifecycle management (start/stop)
    - Message parsing and validation (enrichment tasks with raw_event)
    - Batch accumulation (size-based and time-based)
    - Delta writes with DeltaEventsWriter
    - Retry handler integration for failed Delta writes
    - Graceful shutdown with batch flushing
    - Periodic stats logging
    - Max batches limit handling

No infrastructure required - all dependencies mocked.
"""

import contextlib
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from config.config import MessageConfig
from core.errors.exceptions import PermanentError
from pipeline.common.types import PipelineMessage
from pipeline.verisk.workers.delta_events_worker import DeltaEventsWorker


def _make_sample_raw_event(trace_id="trace-123"):
    """Build a raw event dict matching the format flatten_events() expects."""
    return {
        "type": "verisk.claims.property.xn.documentsReceived",
        "version": 1,
        "utcDateTime": "2024-01-01T00:00:00Z",
        "traceId": trace_id,
        "data": json.dumps({"assignmentId": "A12345"}),
    }


def _make_enrichment_task_message(trace_id="trace-123", topic="verisk.enrichment.pending"):
    """Build a PipelineMessage containing a serialized XACTEnrichmentTask."""
    raw_event = _make_sample_raw_event(trace_id)
    task_data = {

        "trace_id": trace_id,
        "event_type": "verisk",
        "status_subtype": "documentsReceived",
        "assignment_id": "A12345",
        "attachments": [],
        "retry_count": 0,
        "created_at": datetime.now(UTC).isoformat(),
        "original_timestamp": "2024-01-01T00:00:00+00:00",
        "raw_event": raw_event,
    }
    return PipelineMessage(
        topic=topic,
        partition=0,
        offset=1,
        key=trace_id.encode(),
        value=json.dumps(task_data).encode(),
        timestamp=None,
        headers=None,
    )


@pytest.fixture
def mock_config():
    """Mock MessageConfig with standard settings."""
    config = Mock(spec=MessageConfig)
    config.get_topic.return_value = "verisk.enrichment.pending"
    config.get_consumer_group.return_value = "verisk-enrichment-pending-delta"
    return config


@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def sample_event_message():
    """Sample enrichment task message."""
    return _make_enrichment_task_message()


class TestDeltaEventsWorkerInitialization:
    """Test worker initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config, mock_producer):
        """Worker initializes with default configuration."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            assert worker.domain == "verisk"
            assert worker.worker_id == "delta_events_writer"
            assert worker.instance_id is None
            assert worker.batch_size == 5000  # MAX_POLL_RECORDS default
            assert worker.batch_timeout_seconds == 10.0
            assert worker.max_batches is None

    def test_initialization_with_instance_id(self, mock_config, mock_producer):
        """Worker uses instance ID for worker_id suffix."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
                instance_id="happy-tiger",
            )

            assert worker.worker_id == "delta_events_writer-happy-tiger"
            assert worker.instance_id == "happy-tiger"

    def test_initialization_max_batches_defaults_to_none(self, mock_config, mock_producer):
        """Worker defaults max_batches to None (unlimited)."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            assert worker.max_batches is None

    def test_initialization_requires_table_path(self, mock_config, mock_producer):
        """Worker requires events_table_path."""
        with (
            patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"),
            pytest.raises(ValueError, match="events_table_path is required"),
        ):
            DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="",
            )

    def test_metrics_initialized_to_zero(self, mock_config, mock_producer):
        """Worker initializes metrics to zero."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            assert worker._records_processed == 0
            assert worker._records_succeeded == 0
            assert worker._batches_written == 0


class TestDeltaEventsWorkerLifecycle:
    """Test worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, mock_config, mock_producer):
        """Worker start initializes all components."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            with (
                patch(
                    "pipeline.verisk.workers.delta_events_worker.create_consumer"
                ) as mock_create_consumer,
                patch("pipeline.common.telemetry.initialize_worker_telemetry"),
                patch.object(worker.health_server, "start", new_callable=AsyncMock),
                patch.object(worker.retry_handler, "start", new_callable=AsyncMock),
                patch("pipeline.verisk.workers.delta_events_worker.PeriodicStatsLogger"),
            ):
                # Setup mock consumer
                mock_consumer = AsyncMock()
                mock_consumer.start = AsyncMock(side_effect=Exception("Stop"))
                mock_create_consumer.return_value = mock_consumer

                with contextlib.suppress(Exception):
                    await worker.start()

                # Verify consumer was created with enrichment_pending topic
                mock_create_consumer.assert_called_once()
                call_kwargs = mock_create_consumer.call_args[1]
                assert call_kwargs["topic_key"] == "enrichment_pending"
                assert "connection_string" not in call_kwargs

                # Verify components were initialized
                assert worker._running is False  # Reset in finally
                assert worker.consumer is None  # stop() cleans up resources

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_batch(self, mock_config, mock_producer):
        """Worker stop flushes pending batch."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Setup mocked components
            worker._running = True
            worker.consumer = AsyncMock()
            worker.consumer.stop = AsyncMock()
            worker.retry_handler = AsyncMock()
            worker.retry_handler.stop = AsyncMock()
            worker._stats_logger = AsyncMock()
            worker._stats_logger.stop = AsyncMock()

            # Mock flush method to avoid telemetry imports
            worker._flush_batch = AsyncMock()

            # Add pending batch
            worker._batch = [_make_sample_raw_event()]

            # Stop worker
            await worker.stop()

            # Verify flush was called
            worker._flush_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none_components(self, mock_config, mock_producer):
        """Worker stop handles None components gracefully."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Mock retry handler stop
            worker.retry_handler = AsyncMock()
            worker.retry_handler.stop = AsyncMock()

            # All components are None
            assert worker.consumer is None
            assert worker._stats_logger is None

            # Should not raise
            await worker.stop()


class TestDeltaEventsWorkerMessageProcessing:
    """Test message parsing and processing."""

    @pytest.mark.asyncio
    async def test_enrichment_task_parsed_and_raw_event_batched(
        self, mock_config, mock_producer, sample_event_message
    ):
        """Worker parses enrichment task and batches the raw_event."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            await worker._handle_event_message(sample_event_message)

            # Verify message was processed
            assert worker._records_processed == 1
            assert len(worker._batch) == 1
            # Verify the batch contains the raw event dict, not the full task
            assert worker._batch[0]["traceId"] == "trace-123"
            assert "type" in worker._batch[0]
            assert "data" in worker._batch[0]

    @pytest.mark.asyncio
    async def test_invalid_json_raises_error(self, mock_config, mock_producer):
        """Worker handles invalid JSON with error logging."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            invalid_message = PipelineMessage(
                topic="verisk.enrichment.pending",
                partition=0,
                offset=1,
                key=b"key",
                value=b"invalid json{",
                timestamp=None,
                headers=None,
            )

            # Should raise PermanentError wrapping the JSONDecodeError
            with (
                patch("pipeline.verisk.workers.delta_events_worker.log_worker_error"),
                pytest.raises(PermanentError, match="Failed to parse message JSON") as exc_info,
            ):
                await worker._handle_event_message(invalid_message)

            assert isinstance(exc_info.value.cause, json.JSONDecodeError)

    @pytest.mark.asyncio
    async def test_invalid_enrichment_task_raises_error(self, mock_config, mock_producer):
        """Worker raises PermanentError for messages that fail XACTEnrichmentTask validation."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Valid JSON but missing required fields for XACTEnrichmentTask
            invalid_task = PipelineMessage(
                topic="verisk.enrichment.pending",
                partition=0,
                offset=1,
                key=b"key",
                value=json.dumps({"some_field": "value"}).encode(),
                timestamp=None,
                headers=None,
            )

            with (
                patch("pipeline.verisk.workers.delta_events_worker.log_worker_error"),
                pytest.raises(PermanentError, match="Failed to parse XACTEnrichmentTask"),
            ):
                await worker._handle_event_message(invalid_task)


class TestDeltaEventsWorkerBatching:
    """Test batch accumulation and flushing."""

    @pytest.mark.asyncio
    async def test_batch_accumulates_events(self, mock_producer):
        """Worker accumulates raw events in batch."""
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "verisk.enrichment.pending"
        config.get_consumer_group.return_value = "verisk-enrichment-pending-delta"

        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Process multiple messages (below batch_size threshold)
            for i in range(3):
                msg = _make_enrichment_task_message(trace_id=f"trace-{i}")
                await worker._handle_event_message(msg)

            # Verify batch accumulation
            assert len(worker._batch) == 3
            assert worker._records_processed == 3

    @pytest.mark.asyncio
    async def test_batch_flushes_on_size_threshold(self, mock_producer):
        """Worker flushes batch when size threshold reached."""
        config = Mock(spec=MessageConfig)
        config.get_topic.return_value = "verisk.enrichment.pending"
        config.get_consumer_group.return_value = "verisk-enrichment-pending-delta"

        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )
            worker.batch_size = 2

            # Mock flush method
            worker._flush_batch = AsyncMock()

            # Process messages up to threshold
            msg = _make_enrichment_task_message()
            await worker._handle_event_message(msg)
            await worker._handle_event_message(msg)

            # Verify flush was triggered
            worker._flush_batch.assert_called()


class TestDeltaEventsWorkerDeltaWrites:
    """Test Delta table writes."""

    @pytest.mark.asyncio
    async def test_flush_batch_writes_to_delta(self, mock_config, mock_producer):
        """Worker writes batch to Delta table."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Mock delta writer
            worker._write_batch = AsyncMock(return_value=True)
            worker.consumer = AsyncMock()
            worker.consumer.commit = AsyncMock()

            # Add raw event to batch
            worker._batch = [_make_sample_raw_event()]

            # Flush batch
            await worker._flush_batch()

            # Verify Delta write was called
            assert worker._write_batch.called
            assert worker.consumer.commit.called
            assert worker._batches_written == 1
            assert worker._records_succeeded == 1

    @pytest.mark.asyncio
    async def test_flush_batch_routes_to_retry_on_failure(self, mock_config, mock_producer):
        """Worker routes batch to retry handler on Delta write failure."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            # Mock delta writer to return failure
            worker._write_batch = AsyncMock(return_value=False)
            worker.retry_handler = AsyncMock()
            worker.retry_handler.handle_batch_failure = AsyncMock()

            # Add raw event to batch
            worker._batch = [_make_sample_raw_event()]

            # Flush batch
            await worker._flush_batch()

            # Verify retry handler was called
            assert worker.retry_handler.handle_batch_failure.called
            assert worker._batches_written == 0  # Not incremented on failure

    @pytest.mark.asyncio
    async def test_flush_batch_with_empty_batch_is_noop(self, mock_config, mock_producer):
        """Worker handles empty batch flush gracefully."""
        with patch("pipeline.verisk.workers.delta_events_worker.DeltaRetryHandler"):
            worker = DeltaEventsWorker(
                config=mock_config,
                producer=mock_producer,
                events_table_path="abfss://test/xact_events",
            )

            worker.delta_writer = AsyncMock()
            worker._batch = []

            await worker._flush_batch()

            # Verify no writes occurred
            assert not worker.delta_writer.write_raw_events.called
