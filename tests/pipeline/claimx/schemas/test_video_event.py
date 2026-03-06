"""
Tests for ClaimXVideoCollabEvent schema.

Validates Pydantic model behavior, JSON serialization, and field validation.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from pipeline.claimx.schemas.video_event import ClaimXVideoCollabEvent


class TestClaimXVideoCollabEventCreation:
    """Test ClaimXVideoCollabEvent instantiation with valid data."""

    def test_create_with_all_fields(self):
        """ClaimXVideoCollabEvent can be created with all fields populated."""
        now = datetime.now(UTC)
        event = ClaimXVideoCollabEvent(
            trace_id="abc123",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            project_id="5996293",
            mfn="06QYL5W",
            ntid="jsmith",
            claim_number="0820045896",
            policy_number="POL-123456",
            number_of_videos=3,
            number_of_photos=12,
            total_time_seconds=1845.00,
            session_count=2,
            total_time="0:30:45",
            created_at=now,
        )

        assert event.trace_id == "abc123"
        assert event.event_type == "VIDEO_COLLABORATION_COMPLETED"
        assert event.project_id == "5996293"
        assert event.mfn == "06QYL5W"
        assert event.ntid == "jsmith"
        assert event.total_time_seconds == 1845.00
        assert event.session_count == 2
        assert event.created_at == now

    def test_create_with_minimal_fields(self):
        """ClaimXVideoCollabEvent can be created with only required fields."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_INVITE_SENT",
        )

        assert event.trace_id == "evt_001"
        assert event.event_type == "VIDEO_COLLABORATION_INVITE_SENT"
        assert event.project_id is None
        assert event.mfn is None
        assert event.total_time_seconds is None
        assert event.created_at is not None

    def test_created_at_defaults_to_utc(self):
        """created_at field defaults to current UTC time."""
        before = datetime.now(UTC)
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
        )
        after = datetime.now(UTC)

        assert before <= event.created_at <= after


class TestClaimXVideoCollabEventValidation:
    """Test field validation rules."""

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXVideoCollabEvent(
                trace_id="",
                event_type="VIDEO_COLLABORATION_COMPLETED",
            )

    def test_empty_event_type_raises_error(self):
        """Empty event_type raises ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXVideoCollabEvent(
                trace_id="evt_001",
                event_type="",
            )

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXVideoCollabEvent(event_type="VIDEO_COLLABORATION_COMPLETED")


class TestClaimXVideoCollabEventTotalTimeCoercion:
    """Test total_time_seconds string-to-float coercion (H2 regression)."""

    def test_string_coerced_to_float(self):
        """String total_time_seconds is coerced to float."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            total_time_seconds="1845.00",
        )

        assert event.total_time_seconds == 1845.00
        assert isinstance(event.total_time_seconds, float)

    def test_float_passes_through(self):
        """Float total_time_seconds passes through unchanged."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            total_time_seconds=1845.00,
        )

        assert event.total_time_seconds == 1845.00

    def test_int_coerced_to_float(self):
        """Int total_time_seconds is coerced to float."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            total_time_seconds=1845,
        )

        assert event.total_time_seconds == 1845.0

    def test_none_passes_through(self):
        """None total_time_seconds passes through unchanged."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            total_time_seconds=None,
        )

        assert event.total_time_seconds is None

    def test_zero_string_coerced(self):
        """Zero string is coerced to 0.0."""
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            total_time_seconds="0",
        )

        assert event.total_time_seconds == 0.0


class TestClaimXVideoCollabEventSerialization:
    """Test JSON serialization and deserialization."""

    def test_round_trip_serialization(self):
        """ClaimXVideoCollabEvent round-trips through JSON correctly."""
        now = datetime.now(UTC)
        original = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            project_id="5996293",
            mfn="06QYL5W",
            total_time_seconds=1845.00,
            session_count=2,
            created_at=now,
        )

        json_str = original.model_dump_json()
        restored = ClaimXVideoCollabEvent.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.event_type == original.event_type
        assert restored.project_id == original.project_id
        assert restored.mfn == original.mfn
        assert restored.total_time_seconds == original.total_time_seconds
        assert restored.session_count == original.session_count

    def test_created_at_serializes_as_iso(self):
        """created_at field serializes as ISO 8601 string."""
        now = datetime.now(UTC)
        event = ClaimXVideoCollabEvent(
            trace_id="evt_001",
            event_type="VIDEO_COLLABORATION_COMPLETED",
            created_at=now,
        )

        json_str = event.model_dump_json()
        assert now.isoformat() in json_str

    def test_model_validate_from_dict(self):
        """Can validate and create from dict."""
        data = {
            "trace_id": "evt_001",
            "event_type": "VIDEO_COLLABORATION_COMPLETED",
            "project_id": "5996293",
            "total_time_seconds": "1845.00",
        }

        event = ClaimXVideoCollabEvent.model_validate(data)
        assert event.trace_id == "evt_001"
        assert event.total_time_seconds == 1845.00
