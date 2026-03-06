"""Tests for AssignmentNoteMessage schema."""

from datetime import UTC, datetime

from pipeline.verisk.schemas.assignment_notes import AssignmentNoteMessage
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask

SAMPLE_DATA = {
    "description": "Assignment Note",
    "assignmentId": "06QY0RT",
    "dateTime": "2026-02-27T13:13:40.0000000Z",
    "note": "Target Completion Date",
    "author": "XactNet System",
    "adm": {
        "coverageLoss": {"claimNumber": "0819940122"},
    },
}


def _make_task() -> XACTEnrichmentTask:
    return XACTEnrichmentTask(
        event_id="evt-001",
        trace_id="trace-001",
        event_type="verisk",
        status_subtype="assignmentNoteAdded",
        assignment_id="A001",
        attachments=[],
        retry_count=0,
        created_at=datetime.now(UTC),
        original_timestamp=datetime.now(UTC),
        raw_event={"type": "verisk.claims.property.xn.assignmentNoteAdded", "data": "{}"},
    )


class TestAssignmentNoteMessage:
    def test_from_handler_data_maps_all_fields(self):
        task = _make_task()
        produced = datetime.now(UTC)
        msg = AssignmentNoteMessage.from_handler_data(task, SAMPLE_DATA, produced)

        assert msg.trace_id == "trace-001"
        assert msg.assignment_id == "A001"
        assert msg.status_subtype == "assignmentNoteAdded"
        assert msg.description == "Assignment Note"
        assert msg.note == "Target Completion Date"
        assert msg.author == "XactNet System"
        assert msg.event_date_time == "2026-02-27T13:13:40.0000000Z"
        assert msg.claim_number == "0819940122"
        assert msg.produced_at == produced

    def test_from_handler_data_handles_missing_adm(self):
        data = {**SAMPLE_DATA, "adm": None}
        task = _make_task()
        msg = AssignmentNoteMessage.from_handler_data(task, data, datetime.now(UTC))

        assert msg.claim_number is None

    def test_from_handler_data_handles_empty_data(self):
        task = _make_task()
        msg = AssignmentNoteMessage.from_handler_data(task, {}, datetime.now(UTC))

        assert msg.description is None
        assert msg.note is None
        assert msg.author is None
        assert msg.event_date_time is None
        assert msg.claim_number is None

    def test_from_handler_data_handles_missing_optional_fields(self):
        data = {"description": "Assignment Note", "assignmentId": "06QY0RT"}
        task = _make_task()
        msg = AssignmentNoteMessage.from_handler_data(task, data, datetime.now(UTC))

        assert msg.description == "Assignment Note"
        assert msg.note is None
        assert msg.author is None
        assert msg.claim_number is None

    def test_serialization_produces_iso_timestamps(self):
        task = _make_task()
        produced = datetime(2026, 3, 3, 20, 0, 0, tzinfo=UTC)
        msg = AssignmentNoteMessage.from_handler_data(task, SAMPLE_DATA, produced)
        dumped = msg.model_dump()

        assert isinstance(dumped["produced_at"], str)
        assert dumped["produced_at"].startswith("2026-03-03")
