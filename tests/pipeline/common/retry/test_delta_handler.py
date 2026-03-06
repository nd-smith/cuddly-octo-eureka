"""
Unit tests for DeltaRetryHandler.

Test Coverage:
    - Handler initialization and configuration
    - Lifecycle management (start/stop)
    - Error classification (TRANSIENT, PERMANENT, UNKNOWN)
    - Batch failure handling (routing logic)
    - Retry topic routing with exponential backoff
    - DLQ routing for permanent errors and exhausted retries
    - FailedDeltaBatch message creation
    - Empty batch handling

No infrastructure required - all dependencies mocked.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from core.types import ErrorCategory
from pipeline.common.retry.delta_handler import DeltaRetryHandler


@pytest.fixture
def mock_config():
    """Mock MessageConfig."""
    config = Mock()
    # _send_to_retry_topic needs these methods
    config.get_retry_topic.return_value = "test.retry"
    config.get_topic.return_value = "test.downloads.results"
    return config


class TestDeltaRetryHandlerInitialization:
    """Test handler initialization and configuration."""

    def test_initialization_with_default_config(self, mock_config):
        """Handler initializes with default configuration."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        assert handler.config is mock_config
        assert handler.table_path == "abfss://test/table"
        assert handler.domain == "verisk"
        assert handler._retry_delays == [300, 600, 1200, 2400]
        assert handler._max_retries == 4
        assert handler._retry_topic_prefix == "verisk-retry"
        assert handler._dlq_topic == "verisk-dlq"

    def test_initialization_with_custom_retry_delays(self, mock_config):
        """Handler accepts custom retry delays."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            retry_delays=[60, 120, 240],
            domain="verisk",
        )

        assert handler._retry_delays == [60, 120, 240]
        assert handler._max_retries == 3

    def test_initialization_with_custom_topics(self, mock_config):
        """Handler accepts custom retry topic prefix and DLQ topic."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            retry_topic_prefix="custom.retry",
            dlq_topic="custom.dlq",
            domain="verisk",
        )

        assert handler._retry_topic_prefix == "custom.retry"
        assert handler._dlq_topic == "custom.dlq"

    def test_initialization_with_custom_domain(self, mock_config):
        """Handler accepts custom domain."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="claimx",
        )

        assert handler.domain == "claimx"


class TestDeltaRetryHandlerLifecycle:
    """Test handler lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_creates_producers(self, mock_config):
        """Handler start creates retry and DLQ producers."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        with patch("pipeline.common.retry.delta_handler.create_producer") as mock_create:
            mock_retry_producer = AsyncMock()
            mock_retry_producer.start = AsyncMock()
            mock_dlq_producer = AsyncMock()
            mock_dlq_producer.start = AsyncMock()

            mock_create.side_effect = [mock_retry_producer, mock_dlq_producer]

            await handler.start()

            # Verify both producers were created and started
            assert mock_create.call_count == 2
            assert handler._retry_producer is mock_retry_producer
            assert handler._dlq_producer is mock_dlq_producer
            mock_retry_producer.start.assert_called_once()
            mock_dlq_producer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_stops_producers(self, mock_config):
        """Handler stop stops both producers."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        # Mock producers
        mock_retry_producer = AsyncMock()
        mock_dlq_producer = AsyncMock()
        handler._retry_producer = mock_retry_producer
        handler._dlq_producer = mock_dlq_producer

        await handler.stop()

        # Verify both producers were stopped (save references before stop() sets them to None)
        mock_retry_producer.stop.assert_called_once()
        mock_dlq_producer.stop.assert_called_once()
        assert handler._retry_producer is None
        assert handler._dlq_producer is None

    @pytest.mark.asyncio
    async def test_stop_handles_none_producers(self, mock_config):
        """Handler stop handles None producers gracefully."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        # Producers are None
        assert handler._retry_producer is None
        assert handler._dlq_producer is None

        # Should not raise
        await handler.stop()


class TestDeltaRetryHandlerErrorClassification:
    """Test error classification logic."""

    def test_classify_permanent_schema_error(self, mock_config):
        """Schema mismatch errors are classified as PERMANENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("Schema mismatch: column type incompatible")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_permission_error(self, mock_config):
        """Permission denied errors are classified as PERMANENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("403 Permission denied")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.PERMANENT

    def test_classify_permanent_configuration_error(self, mock_config):
        """Table not found errors are classified as PERMANENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("Table not found: invalid path")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.PERMANENT

    def test_classify_transient_timeout_error(self, mock_config):
        """Timeout errors are classified as TRANSIENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = TimeoutError("Operation timed out")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_connection_error(self, mock_config):
        """Connection errors are classified as TRANSIENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("Connection refused")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_throttling_error(self, mock_config):
        """Throttling errors are classified as TRANSIENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("429 Rate limit exceeded")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_service_error(self, mock_config):
        """Service unavailable errors are classified as TRANSIENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("503 Service unavailable")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.TRANSIENT

    def test_classify_transient_delta_conflict(self, mock_config):
        """Delta commit conflicts are classified as TRANSIENT."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("Commit conflict: concurrent writes detected")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.TRANSIENT

    def test_classify_unknown_error(self, mock_config):
        """Unrecognized errors are classified as UNKNOWN."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        error = Exception("Something went wrong")
        category = handler.classify_delta_error(error)

        assert category == ErrorCategory.UNKNOWN


class TestDeltaRetryHandlerBatchFailureHandling:
    """Test batch failure handling and routing logic."""

    @pytest.mark.asyncio
    async def test_handle_batch_failure_with_empty_batch(self, mock_config):
        """Handler ignores empty batch."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        # Should not raise
        await handler.handle_batch_failure(
            batch=[],
            error=Exception("Test error"),
            retry_count=0,
            error_category="transient",
        )

    @pytest.mark.asyncio
    async def test_handle_batch_failure_permanent_goes_to_dlq(self, mock_config):
        """PERMANENT errors go straight to DLQ without retry."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        handler._send_to_dlq = AsyncMock()

        batch = [{"traceId": "123"}]
        error = Exception("Schema mismatch")

        await handler.handle_batch_failure(
            batch=batch,
            error=error,
            retry_count=0,
            error_category="permanent",
            batch_id="batch-1",
        )

        # Verify sent to DLQ, not retry
        handler._send_to_dlq.assert_called_once()
        assert handler._send_to_dlq.call_args[1]["batch"] == batch
        assert handler._send_to_dlq.call_args[1]["error"] == error

    @pytest.mark.asyncio
    async def test_handle_batch_failure_exhausted_goes_to_dlq(self, mock_config):
        """Exhausted retries go to DLQ."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            retry_delays=[60, 120],  # Max 2 retries
            domain="verisk",
        )

        handler._send_to_dlq = AsyncMock()

        batch = [{"traceId": "123"}]
        error = Exception("Timeout")

        # Retry count >= max_retries
        await handler.handle_batch_failure(
            batch=batch,
            error=error,
            retry_count=2,  # Exhausted (max is 2)
            error_category="transient",
            batch_id="batch-1",
        )

        # Verify sent to DLQ
        handler._send_to_dlq.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_batch_failure_transient_goes_to_retry(self, mock_config):
        """TRANSIENT errors go to retry topic."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        handler._send_to_retry_topic = AsyncMock()

        batch = [{"traceId": "123"}]
        error = Exception("Timeout")

        await handler.handle_batch_failure(
            batch=batch,
            error=error,
            retry_count=0,
            error_category="transient",
            batch_id="batch-1",
        )

        # Verify sent to retry topic
        handler._send_to_retry_topic.assert_called_once()
        assert handler._send_to_retry_topic.call_args[1]["batch"] == batch
        assert handler._send_to_retry_topic.call_args[1]["error"] == error
        assert handler._send_to_retry_topic.call_args[1]["retry_count"] == 0

    @pytest.mark.asyncio
    async def test_handle_batch_failure_normalizes_string_category(self, mock_config):
        """Handler normalizes string error categories to ErrorCategory enum."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        handler._send_to_retry_topic = AsyncMock()

        batch = [{"traceId": "123"}]
        error = Exception("Timeout")

        # Pass string instead of ErrorCategory
        await handler.handle_batch_failure(
            batch=batch,
            error=error,
            retry_count=0,
            error_category="transient",  # String, not enum
            batch_id="batch-1",
        )

        # Should still work - normalized to enum
        handler._send_to_retry_topic.assert_called_once()


class TestDeltaRetryHandlerRetryRouting:
    """Test retry topic routing."""

    @pytest.mark.asyncio
    async def test_send_to_retry_topic_creates_failed_batch(self, mock_config):
        """Sending to retry topic creates FailedDeltaBatch message."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            retry_delays=[300],
            domain="verisk",
        )

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()
        handler._retry_producer = mock_producer

        batch = [{"traceId": "123", "data": "test"}]
        error = Exception("Timeout")

        with patch("pipeline.common.retry.delta_handler.datetime") as mock_datetime:
            now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
            mock_datetime.now.return_value = now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            await handler._send_to_retry_topic(
                batch=batch,
                error=error,
                error_category=ErrorCategory.TRANSIENT,
                retry_count=0,
                batch_id="batch-1",
            )

        # Verify producer.send was called
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args

        # Verify FailedDeltaBatch message
        failed_batch = call_args[1]["value"]
        assert failed_batch.batch_id == "batch-1"
        assert failed_batch.events == batch
        assert failed_batch.retry_count == 1  # Incremented
        assert failed_batch.error_category == "transient"
        assert failed_batch.table_path == "abfss://test/table"
        assert failed_batch.event_count == 1

        # Verify headers
        headers = call_args[1]["headers"]
        assert headers["retry_count"] == "1"
        assert headers["error_category"] == "transient"
        assert headers["domain"] == "verisk"

    @pytest.mark.asyncio
    async def test_send_to_retry_topic_calculates_retry_at(self, mock_config):
        """Retry topic message includes correct retry_at timestamp."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            retry_delays=[300, 600],  # 5m, 10m
            domain="verisk",
        )

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()
        handler._retry_producer = mock_producer

        batch = [{"traceId": "123"}]
        error = Exception("Timeout")

        with patch("pipeline.common.retry.delta_handler.datetime") as mock_datetime:
            now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
            mock_datetime.now.return_value = now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            await handler._send_to_retry_topic(
                batch=batch,
                error=error,
                error_category=ErrorCategory.TRANSIENT,
                retry_count=1,  # Second retry (use 600s delay)
                batch_id="batch-1",
            )

        # Verify retry_at is now + delay
        failed_batch = mock_producer.send.call_args[1]["value"]
        expected_retry_at = now + timedelta(seconds=600)
        assert failed_batch.retry_at == expected_retry_at


class TestDeltaRetryHandlerDLQRouting:
    """Test DLQ routing."""

    @pytest.mark.asyncio
    async def test_send_to_dlq_creates_failed_batch(self, mock_config):
        """Sending to DLQ creates FailedDeltaBatch message."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()
        handler._dlq_producer = mock_producer

        batch = [{"traceId": "123", "data": "test"}]
        error = Exception("Schema mismatch")

        await handler._send_to_dlq(
            batch=batch,
            error=error,
            error_category=ErrorCategory.PERMANENT,
            retry_count=0,
            batch_id="batch-1",
        )

        # Verify producer.send was called
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args

        # Verify FailedDeltaBatch message
        failed_batch = call_args[1]["value"]
        assert failed_batch.batch_id == "batch-1"
        assert failed_batch.events == batch
        assert failed_batch.retry_count == 0
        assert failed_batch.error_category == "permanent"
        assert failed_batch.table_path == "abfss://test/table"
        assert failed_batch.retry_at is None  # No retry scheduled

        # Verify headers
        headers = call_args[1]["headers"]
        assert headers["retry_count"] == "0"
        assert headers["error_category"] == "permanent"
        assert headers["failed"] == "true"

    @pytest.mark.asyncio
    async def test_send_to_dlq_truncates_long_error(self, mock_config):
        """DLQ messages truncate long error messages."""
        handler = DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()
        handler._dlq_producer = mock_producer

        batch = [{"traceId": "123"}]
        # Error message longer than 500 chars
        long_error = Exception("x" * 600)

        await handler._send_to_dlq(
            batch=batch,
            error=long_error,
            error_category=ErrorCategory.PERMANENT,
            retry_count=0,
            batch_id="batch-1",
        )

        # Verify error message was truncated
        failed_batch = mock_producer.send.call_args[1]["value"]
        assert len(failed_batch.last_error) == 500  # Truncated to 500
        assert failed_batch.last_error.endswith("...")


class TestDeltaRetryHandlerChunkedSending:
    """Test _send_chunked logic for oversized batch splitting."""

    def _make_handler(self, mock_config):
        return DeltaRetryHandler(
            config=mock_config,
            table_path="abfss://test/table",
            domain="verisk",
        )

    def _make_failed_batch(self, event_count):
        from pipeline.common.schemas.delta_batch import FailedDeltaBatch

        events = [{"traceId": f"trace-{i}", "data": "x" * 100} for i in range(event_count)]
        return FailedDeltaBatch(
            batch_id="batch-1",
            events=events,
            retry_count=1,
            first_failure_at=datetime(2024, 1, 1, tzinfo=UTC),
            last_error="Test error",
            error_category="transient",
            table_path="abfss://test/table",
        )

    @pytest.mark.asyncio
    async def test_small_batch_sends_as_single_message(self, mock_config):
        """Batches under size limit are sent as a single message."""
        handler = self._make_handler(mock_config)
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()

        batch = self._make_failed_batch(5)
        headers = {"retry_count": "1"}

        await handler._send_chunked(mock_producer, batch, "key1", headers)

        mock_producer.send.assert_called_once()
        sent_batch = mock_producer.send.call_args[1]["value"]
        assert sent_batch.batch_id == "batch-1"
        assert len(sent_batch.events) == 5

    @pytest.mark.asyncio
    async def test_oversized_batch_is_split_into_chunks(self, mock_config):
        """Batches exceeding size limit are split into multiple chunks."""
        handler = self._make_handler(mock_config)
        # Use a very small threshold to force chunking
        handler._MAX_MESSAGE_BYTES = 500
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()

        batch = self._make_failed_batch(20)
        headers = {"retry_count": "1"}

        await handler._send_chunked(mock_producer, batch, "key1", headers)

        # Should have been called multiple times
        assert mock_producer.send.call_count > 1

        # Verify all chunks together contain all events
        all_events = []
        for call in mock_producer.send.call_args_list:
            chunk = call[1]["value"]
            all_events.extend(chunk.events)
            # Chunk batch_id should have suffix
            assert "_chunk_" in chunk.batch_id

        assert len(all_events) == 20

    @pytest.mark.asyncio
    async def test_single_oversized_event_raises_error(self, mock_config):
        """A single event exceeding the limit raises ValueError."""
        handler = self._make_handler(mock_config)
        handler._MAX_MESSAGE_BYTES = 100  # Very small to force error
        mock_producer = AsyncMock()

        batch = self._make_failed_batch(1)

        with pytest.raises(ValueError, match="Single event too large"):
            await handler._send_chunked(mock_producer, batch, "key1", {})

    @pytest.mark.asyncio
    async def test_chunk_metadata_preserves_fields(self, mock_config):
        """Chunks preserve retry metadata from original batch."""
        handler = self._make_handler(mock_config)
        handler._MAX_MESSAGE_BYTES = 500
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock()

        batch = self._make_failed_batch(20)
        headers = {"retry_count": "1", "error_category": "transient"}

        await handler._send_chunked(mock_producer, batch, "key1", headers)

        for call in mock_producer.send.call_args_list:
            chunk = call[1]["value"]
            assert chunk.retry_count == 1
            assert chunk.error_category == "transient"
            assert chunk.table_path == "abfss://test/table"
            # Headers passed through
            assert call[1]["headers"] == headers

    @pytest.mark.asyncio
    async def test_retry_topic_uses_chunked_sending(self, mock_config):
        """_send_to_retry_topic routes through _send_chunked."""
        handler = self._make_handler(mock_config)
        handler._send_chunked = AsyncMock()

        mock_producer = AsyncMock()
        handler._retry_producer = mock_producer

        batch = [{"traceId": "123"}]
        error = Exception("Timeout")

        await handler._send_to_retry_topic(
            batch=batch,
            error=error,
            error_category=ErrorCategory.TRANSIENT,
            retry_count=0,
            batch_id="batch-1",
        )

        handler._send_chunked.assert_called_once()
        assert handler._send_chunked.call_args[1]["producer"] is mock_producer

    @pytest.mark.asyncio
    async def test_dlq_uses_chunked_sending(self, mock_config):
        """_send_to_dlq routes through _send_chunked."""
        handler = self._make_handler(mock_config)
        handler._send_chunked = AsyncMock()

        mock_producer = AsyncMock()
        handler._dlq_producer = mock_producer

        batch = [{"traceId": "123"}]
        error = Exception("Schema mismatch")

        await handler._send_to_dlq(
            batch=batch,
            error=error,
            error_category=ErrorCategory.PERMANENT,
            retry_count=0,
            batch_id="batch-1",
        )

        handler._send_chunked.assert_called_once()
        assert handler._send_chunked.call_args[1]["producer"] is mock_producer
