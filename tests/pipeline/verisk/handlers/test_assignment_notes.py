"""Tests for AssignmentNoteAddedHandler."""

import json
from datetime import UTC, datetime

import pytest

from pipeline.verisk.handlers.base import (
    EventHandlerResult,
    FileHandlerSideEffect,
    _EVENT_HANDLERS,
)
from pipeline.verisk.handlers.assignment_notes import (
    AssignmentNoteAddedHandler,
    _XN_ASSIGNMENT_NOTE_MARKER,
)
from pipeline.verisk.schemas.assignment_notes import AssignmentNoteMessage
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask

# ---------------------------------------------------------------------------
# Sample event payload matching ASSIGNMENT_NOTE_ADDED.JSON
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate event handler registry state between tests."""
    original = dict(_EVENT_HANDLERS)
    yield
    _EVENT_HANDLERS.clear()
    _EVENT_HANDLERS.update(original)


def _make_task(
    status_subtype: str = "assignmentNoteAdded",
    event_type: str = "verisk.claims.property.xn.assignmentNoteAdded",
    data: dict | None = None,
) -> XACTEnrichmentTask:
    raw_data = json.dumps(data or SAMPLE_DATA)
    return XACTEnrichmentTask(
        event_id="evt-001",
        trace_id="trace-001",
        event_type="verisk",
        status_subtype=status_subtype,
        assignment_id="A001",
        attachments=[],
        retry_count=0,
        created_at=datetime.now(UTC),
        original_timestamp=datetime.now(UTC),
        raw_event={"type": event_type, "data": raw_data},
    )


# ---------------------------------------------------------------------------
# AssignmentNoteAddedHandler tests
# ---------------------------------------------------------------------------


class TestAssignmentNoteAddedHandler:
    def test_handler_is_registered_for_assignment_note_added(self):
        assert "assignmentnoteadded" in _EVENT_HANDLERS
        assert _EVENT_HANDLERS["assignmentnoteadded"] is AssignmentNoteAddedHandler

    def test_handle_returns_success_with_side_effect(self):
        handler = AssignmentNoteAddedHandler()
        task = _make_task()

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert isinstance(result, EventHandlerResult)
        assert result.success is True
        assert result.side_effect is not None
        assert isinstance(result.side_effect, FileHandlerSideEffect)
        assert result.side_effect.topic_key == "verisk_notes"
        assert isinstance(result.side_effect.message, AssignmentNoteMessage)

    def test_handle_extracts_correct_fields(self):
        handler = AssignmentNoteAddedHandler()
        task = _make_task()

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        msg: AssignmentNoteMessage = result.side_effect.message
        assert msg.description == "Assignment Note"
        assert msg.note == "Target Completion Date"
        assert msg.author == "XactNet System"
        assert msg.event_date_time == "2026-02-27T13:13:40.0000000Z"
        assert msg.claim_number == "0819940122"

    def test_handle_noops_for_non_matching_event_type(self):
        """Handler must return empty no-op when event type is not xn.assignmentNoteAdded."""
        handler = AssignmentNoteAddedHandler()
        task = _make_task(
            status_subtype="assignmentNoteAdded",
            event_type="verisk.claims.property.xn.status.delivered",
        )

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert result.success is True
        assert result.side_effect is None
        assert result.data == {}

    def test_handle_returns_failure_on_invalid_json(self):
        """Handler returns failure when raw_event data is malformed JSON."""
        task = _make_task()
        task.raw_event["data"] = "not valid json {"

        import asyncio

        handler = AssignmentNoteAddedHandler()
        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert result.success is False
        assert result.error is not None
        assert result.side_effect is None

    def test_xn_assignment_note_marker_value(self):
        """Smoke-test the private sentinel used for event type gating."""
        assert _XN_ASSIGNMENT_NOTE_MARKER == ".xn.assignmentNoteAdded"
        assert _XN_ASSIGNMENT_NOTE_MARKER in "verisk.claims.property.xn.assignmentNoteAdded"
        assert _XN_ASSIGNMENT_NOTE_MARKER not in "verisk.claims.property.xn.status.delivered"
