"""Tests for assignment_note rule matching, parsing, and action."""

import json
from datetime import UTC, datetime

import pytest

from pipeline.verisk.handlers.parsers import parse_event_json
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_assignment_note,
    produce_assignment_note,
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


def _make_task(
    status_subtype: str = "assignmentNoteAdded",
    event_type: str = "verisk.claims.property.xn.assignmentNoteAdded",
    data: dict | None = None,
) -> XACTEnrichmentTask:
    raw_data = json.dumps(data or SAMPLE_DATA)
    return XACTEnrichmentTask(
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
# Rule matching tests
# ---------------------------------------------------------------------------


class TestMatchesAssignmentNote:
    def test_matches_assignment_note_event(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=None)
        assert matches_assignment_note(ctx) is True

    def test_no_match_non_assignment_note_event(self):
        task = _make_task(
            status_subtype="assignmentNoteAdded",
            event_type="verisk.claims.property.xn.status.delivered",
        )
        ctx = RuleContext(task=task, file_path=None)
        assert matches_assignment_note(ctx) is False

    def test_no_match_wrong_subtype(self):
        task = _make_task(
            status_subtype="delivered",
            event_type="verisk.claims.property.xn.assignmentNoteAdded",
        )
        ctx = RuleContext(task=task, file_path=None)
        assert matches_assignment_note(ctx) is False

    def test_no_match_when_file_path_present(self):
        from pathlib import Path
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/file.xml"))
        assert matches_assignment_note(ctx) is False


# ---------------------------------------------------------------------------
# Action tests
# ---------------------------------------------------------------------------


class TestProduceAssignmentNote:
    @pytest.mark.asyncio
    async def test_produces_assignment_note_side_effect(self):
        task = _make_task()
        parsed = parse_event_json(task)
        ctx = RuleContext(task=task, file_path=None, parsed_data=parsed)

        side_effects = await produce_assignment_note(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "verisk_notes"

        msg = side_effects[0].message
        assert isinstance(msg, AssignmentNoteMessage)
        assert msg.description == "Assignment Note"
        assert msg.note == "Target Completion Date"
        assert msg.author == "XactNet System"
        assert msg.event_date_time == "2026-02-27T13:13:40.0000000Z"
        assert msg.claim_number == "0819940122"

    @pytest.mark.asyncio
    async def test_handles_invalid_json(self):
        import json

        task = _make_task()
        task.raw_event["data"] = "not valid json {"

        with pytest.raises(json.JSONDecodeError):
            parse_event_json(task)
