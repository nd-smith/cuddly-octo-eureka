"""
Tests for ClaimXTaskEvent schema.

Validates Pydantic model behavior, JSON serialization, and field validation.
"""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from pipeline.claimx.schemas.task_event import ClaimXTaskEvent


class TestClaimXTaskEventCreation:
    """Test ClaimXTaskEvent instantiation with valid data."""

    def test_create_with_all_fields(self):
        """ClaimXTaskEvent can be created with all fields populated."""
        now = datetime.now(UTC)
        event = ClaimXTaskEvent(
            trace_id="abc123",
            event_type="CUSTOM_TASK_COMPLETED",
            project_id="5996293",
            claim_number="0820045896",
            project_status="IN_PROGRESS",
            customer_firstname="ROBERT",
            customer_lastname="GUILLIAMS",
            loss_address_street="16891 SE 76TH CHATHAM AVE",
            loss_address_city="THE VILLAGES",
            loss_address_state_province="FL",
            loss_address_zip_postcode="32162",
            mfn="06QYL5W",
            task_assignment_id="5550810",
            task_id=7085,
            form_id="5e4eb0a494511a44fd3851b9",
            task_name="File Request - Property",
            task_status="COMPLETED",
            date_task_assigned="2026-02-25T17:34:39.559+00:00",
            date_task_completed="2026-02-25T18:13:01.623+00:00",
            cancelled_date=None,
            form_response='{"groups": []}',
            report_id=98055035,
            report_url="https://s3.amazonaws.com/...",
            report_name="File Request - Property Report",
            media_id="98055035",
            created_at=now,
        )

        assert event.trace_id == "abc123"
        assert event.event_type == "CUSTOM_TASK_COMPLETED"
        assert event.project_id == "5996293"
        assert event.claim_number == "0820045896"
        assert event.task_assignment_id == "5550810"
        assert event.task_id == 7085
        assert event.task_status == "COMPLETED"
        assert event.created_at == now

    def test_create_with_minimal_fields(self):
        """ClaimXTaskEvent can be created with only required fields."""
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
        )

        assert event.trace_id == "evt_001"
        assert event.event_type == "CUSTOM_TASK_ASSIGNED"
        assert event.project_id == "proj_001"
        assert event.claim_number is None
        assert event.task_assignment_id is None
        assert event.form_response is None
        assert event.report_id is None
        assert event.created_at is not None

    def test_created_at_defaults_to_utc(self):
        """created_at field defaults to current UTC time."""
        before = datetime.now(UTC)
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
        )
        after = datetime.now(UTC)

        assert before <= event.created_at <= after


class TestClaimXTaskEventValidation:
    """Test field validation rules."""

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXTaskEvent(
                trace_id="",
                event_type="CUSTOM_TASK_ASSIGNED",
                project_id="proj_001",
            )

    def test_empty_event_type_raises_error(self):
        """Empty event_type raises ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXTaskEvent(
                trace_id="evt_001",
                event_type="",
                project_id="proj_001",
            )

    def test_empty_project_id_raises_error(self):
        """Empty project_id raises ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXTaskEvent(
                trace_id="evt_001",
                event_type="CUSTOM_TASK_ASSIGNED",
                project_id="",
            )

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError):
            ClaimXTaskEvent(event_type="CUSTOM_TASK_ASSIGNED", project_id="proj_001")

    def test_task_assignment_id_is_string(self):
        """task_assignment_id must be str | None (C2 regression)."""
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            task_assignment_id="5550810",
        )
        assert event.task_assignment_id == "5550810"
        assert isinstance(event.task_assignment_id, str)


class TestClaimXTaskEventFormResponse:
    """Test form_response stringify validator."""

    def test_form_response_dict_is_stringified(self):
        """Dict form_response is JSON-stringified."""
        data = {"groups": [{"name": "Group1"}]}
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            form_response=data,
        )

        assert isinstance(event.form_response, str)
        assert json.loads(event.form_response) == data

    def test_form_response_list_is_stringified(self):
        """List form_response is JSON-stringified."""
        data = [{"question": "Q1", "answer": "A1"}]
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            form_response=data,
        )

        assert isinstance(event.form_response, str)
        assert json.loads(event.form_response) == data

    def test_form_response_string_passes_through(self):
        """String form_response passes through unchanged."""
        json_str = '{"groups": []}'
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            form_response=json_str,
        )

        assert event.form_response == json_str

    def test_form_response_none_passes_through(self):
        """None form_response passes through unchanged."""
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            form_response=None,
        )

        assert event.form_response is None


class TestClaimXTaskEventSerialization:
    """Test JSON serialization and deserialization."""

    def test_round_trip_serialization(self):
        """ClaimXTaskEvent round-trips through JSON correctly."""
        now = datetime.now(UTC)
        original = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_COMPLETED",
            project_id="proj_001",
            claim_number="CLM-001",
            task_assignment_id="5550810",
            task_name="Review photos",
            task_status="COMPLETED",
            created_at=now,
        )

        json_str = original.model_dump_json()
        restored = ClaimXTaskEvent.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.event_type == original.event_type
        assert restored.project_id == original.project_id
        assert restored.claim_number == original.claim_number
        assert restored.task_assignment_id == original.task_assignment_id
        assert restored.task_name == original.task_name

    def test_created_at_serializes_as_iso(self):
        """created_at field serializes as ISO 8601 string."""
        now = datetime.now(UTC)
        event = ClaimXTaskEvent(
            trace_id="evt_001",
            event_type="CUSTOM_TASK_ASSIGNED",
            project_id="proj_001",
            created_at=now,
        )

        json_str = event.model_dump_json()
        assert now.isoformat() in json_str

    def test_model_validate_from_dict(self):
        """Can validate and create from dict."""
        data = {
            "trace_id": "evt_001",
            "event_type": "CUSTOM_TASK_ASSIGNED",
            "project_id": "proj_001",
            "task_assignment_id": "5550810",
        }

        event = ClaimXTaskEvent.model_validate(data)
        assert event.trace_id == "evt_001"
        assert event.task_assignment_id == "5550810"
