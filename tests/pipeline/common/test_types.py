"""
Tests for pipeline message types.

Tests PipelineMessage, ProduceResult, PartitionInfo dataclasses.
These are pure unit tests with no external dependencies.
"""

from dataclasses import FrozenInstanceError

import pytest

from pipeline.common.types import (
    PartitionInfo,
    PipelineMessage,
    ProduceResult,
)


class TestPipelineMessage:
    """Tests for PipelineMessage dataclass."""

    def test_creation_minimal(self):
        """Test creating PipelineMessage with minimal required fields."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
        )

        assert msg.topic == "test-topic"
        assert msg.partition == 0
        assert msg.offset == 100
        assert msg.timestamp == 1234567890
        assert msg.key is None
        assert msg.value is None
        assert msg.headers is None

    def test_creation_full(self):
        """Test creating PipelineMessage with all fields."""
        headers = [("header1", b"value1"), ("header2", b"value2")]

        msg = PipelineMessage(
            topic="test-topic",
            partition=3,
            offset=12345,
            timestamp=9876543210,
            key=b"test-key",
            value=b'{"test": "data"}',
            headers=headers,
        )

        assert msg.topic == "test-topic"
        assert msg.partition == 3
        assert msg.offset == 12345
        assert msg.timestamp == 9876543210
        assert msg.key == b"test-key"
        assert msg.value == b'{"test": "data"}'
        assert msg.headers == headers

    def test_immutability(self):
        """Test that PipelineMessage is immutable (frozen dataclass)."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
        )

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(FrozenInstanceError):
            msg.topic = "modified-topic"

        with pytest.raises(FrozenInstanceError):
            msg.partition = 999

        with pytest.raises(FrozenInstanceError):
            msg.offset = 999

    def test_equality(self):
        """Test PipelineMessage equality comparison."""
        msg1 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        msg2 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        msg3 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=101,  # Different offset
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        assert msg1 == msg2
        assert msg1 != msg3

    def test_headers_format(self):
        """Test headers are stored as List[Tuple[str, bytes]]."""
        headers = [
            ("trace-id", b"abc123"),
            ("correlation-id", b"xyz789"),
            ("content-type", b"application/json"),
        ]

        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            headers=headers,
        )

        assert len(msg.headers) == 3
        assert msg.headers[0] == ("trace-id", b"abc123")
        assert msg.headers[1] == ("correlation-id", b"xyz789")
        assert msg.headers[2] == ("content-type", b"application/json")

    def test_bytes_fields(self):
        """Test key and value are bytes type."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"binary-key",
            value=b"\x00\x01\x02\x03",  # Binary data
        )

        assert isinstance(msg.key, bytes)
        assert isinstance(msg.value, bytes)
        assert msg.key == b"binary-key"
        assert msg.value == b"\x00\x01\x02\x03"

    def test_empty_headers(self):
        """Test empty headers list."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            headers=[],
        )

        assert msg.headers == []


class TestProduceResult:
    """Tests for ProduceResult dataclass."""

    def test_creation(self):
        """Test creating ProduceResult."""
        result = ProduceResult(
            topic="test-topic",
            partition=2,
            offset=54321,
        )

        assert result.topic == "test-topic"
        assert result.partition == 2
        assert result.offset == 54321

    def test_immutability(self):
        """Test that ProduceResult is immutable (frozen dataclass)."""
        result = ProduceResult(
            topic="test-topic",
            partition=1,
            offset=100,
        )

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(FrozenInstanceError):
            result.topic = "modified-topic"

        with pytest.raises(FrozenInstanceError):
            result.partition = 999

        with pytest.raises(FrozenInstanceError):
            result.offset = 999

    def test_equality(self):
        """Test ProduceResult equality comparison."""
        result1 = ProduceResult(topic="test-topic", partition=0, offset=100)
        result2 = ProduceResult(topic="test-topic", partition=0, offset=100)
        result3 = ProduceResult(topic="test-topic", partition=1, offset=100)

        assert result1 == result2
        assert result1 != result3

    def test_use_as_confirmation(self):
        """Test ProduceResult can be used to verify message was persisted."""
        result = ProduceResult(topic="events", partition=3, offset=12345)

        # Verify we can extract confirmation details
        assert result.topic == "events"
        assert result.partition == 3
        assert result.offset == 12345

        # Can be used to construct unique message identifier
        message_id = f"{result.topic}-{result.partition}-{result.offset}"
        assert message_id == "events-3-12345"


class TestPartitionInfo:
    """Tests for PartitionInfo dataclass."""

    def test_creation(self):
        """Test creating PartitionInfo."""
        partition = PartitionInfo(topic="test-topic", partition=5)

        assert partition.topic == "test-topic"
        assert partition.partition == 5

    def test_immutability(self):
        """Test that PartitionInfo is immutable (frozen dataclass)."""
        partition = PartitionInfo(topic="test-topic", partition=0)

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(FrozenInstanceError):
            partition.topic = "modified-topic"

        with pytest.raises(FrozenInstanceError):
            partition.partition = 999

    def test_equality(self):
        """Test PartitionInfo equality comparison."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)
        p3 = PartitionInfo(topic="test-topic", partition=1)
        p4 = PartitionInfo(topic="other-topic", partition=0)

        assert p1 == p2
        assert p1 != p3
        assert p1 != p4

    def test_hashable(self):
        """Test PartitionInfo is hashable (can be used as dict key)."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=1)
        p3 = PartitionInfo(topic="other-topic", partition=0)

        # Should be able to use as dict key
        partition_offsets = {
            p1: 100,
            p2: 200,
            p3: 300,
        }

        assert partition_offsets[p1] == 100
        assert partition_offsets[p2] == 200
        assert partition_offsets[p3] == 300

    def test_use_in_set(self):
        """Test PartitionInfo can be used in sets."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)  # Duplicate
        p3 = PartitionInfo(topic="test-topic", partition=1)

        partition_set = {p1, p2, p3}

        # p1 and p2 are equal, so set should only contain 2 items
        assert len(partition_set) == 2
        assert p1 in partition_set
        assert p3 in partition_set

    def test_hash_consistency(self):
        """Test PartitionInfo hash is consistent."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)

        # Equal objects should have equal hashes
        assert hash(p1) == hash(p2)


class TestTypeCompatibility:
    """Tests for type compatibility and usage patterns."""

    def test_pipeline_message_in_handler(self):
        """Test PipelineMessage can be used in message handler pattern."""
        import json

        msg = PipelineMessage(
            topic="events",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"entity-123",
            value=json.dumps({"event": "test", "data": "value"}).encode("utf-8"),
        )

        # Simulate message handler usage
        payload = json.loads(msg.value)
        assert payload["event"] == "test"
        assert payload["data"] == "value"

        # Simulate key extraction
        entity_id = msg.key.decode("utf-8")
        assert entity_id == "entity-123"

    def test_produce_result_in_producer_pattern(self):
        """Test ProduceResult can be used in producer pattern."""
        result = ProduceResult(topic="events", partition=3, offset=12345)

        # Simulate producer confirmation logging
        confirmation_msg = (
            f"Published to {result.topic} partition {result.partition} at offset {result.offset}"
        )
        assert confirmation_msg == "Published to events partition 3 at offset 12345"

    def test_partition_info_for_offset_management(self):
        """Test PartitionInfo can be used for offset management."""
        # Simulate offset tracking per partition
        offsets = {}

        p1 = PartitionInfo(topic="events", partition=0)
        p2 = PartitionInfo(topic="events", partition=1)
        p3 = PartitionInfo(topic="events", partition=2)

        offsets[p1] = 100
        offsets[p2] = 200
        offsets[p3] = 300

        # Simulate offset lookup
        assert offsets[p1] == 100
        assert offsets[PartitionInfo(topic="events", partition=1)] == 200
