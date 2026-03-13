"""Tests for xact_status rule matching, parsing, and action."""

import json
from datetime import UTC, datetime

import pytest

from pipeline.verisk.handlers.parsers import parse_event_json
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_xact_status,
    produce_xact_status,
)
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.verisk.schemas.xact_status import XactStatusMessage

# ---------------------------------------------------------------------------
# Sample event payload matching STATUS.JSON
# ---------------------------------------------------------------------------

SAMPLE_DATA = {
    "description": "Delivered",
    "assignmentId": "AAAAAAA",
    "xnAddress": "EXAMPLE@CARRIER_XF",
    "originalAssignmentId": "BBBBBBB",
    "dateTime": "2026-01-01T00:00:00.0000000Z",
    "contact": {
        "contactMethods": {
            "phone": {"type": "Office", "number": "000-000-0000", "extension": ""},
            "email": {"address": "adjuster@example.com"},
        },
        "type": "ClaimRep",
        "name": "Jane Smith",
    },
    "adm": {
        "coverageLoss": {"claimNumber": "0000000000"},
    },
}


def _make_task(
    status_subtype: str = "delivered",
    event_type: str = "verisk.claims.property.xn.status.delivered",
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
# XactStatusMessage schema tests
# ---------------------------------------------------------------------------


class TestXactStatusMessage:
    def test_from_handler_data_maps_all_fields(self):
        task = _make_task()
        produced = datetime.now(UTC)
        msg = XactStatusMessage.from_handler_data(task, SAMPLE_DATA, produced)

        assert msg.trace_id == "trace-001"
        assert msg.assignment_id == "A001"
        assert msg.status_subtype == "delivered"
        assert msg.description == "Delivered"
        assert msg.xn_address == "EXAMPLE@CARRIER_XF"
        assert msg.original_assignment_id == "BBBBBBB"
        assert msg.is_reassignment is True
        assert msg.event_date_time == "2026-01-01T00:00:00.0000000Z"
        assert msg.contact_name == "Jane Smith"
        assert msg.contact_type == "ClaimRep"
        assert msg.contact_email == "adjuster@example.com"
        assert msg.contact_phone == "000-000-0000"
        assert msg.claim_number == "0000000000"
        assert msg.produced_at == produced

    def test_is_reassignment_false_when_original_matches_assignment(self):
        data = {**SAMPLE_DATA, "originalAssignmentId": "A001"}
        task = _make_task(data=data)
        msg = XactStatusMessage.from_handler_data(task, data, datetime.now(UTC))
        assert msg.is_reassignment is False

    def test_is_reassignment_false_when_original_absent(self):
        data = {k: v for k, v in SAMPLE_DATA.items() if k != "originalAssignmentId"}
        task = _make_task(data=data)
        msg = XactStatusMessage.from_handler_data(task, data, datetime.now(UTC))
        assert msg.is_reassignment is False

    def test_from_handler_data_handles_missing_contact(self):
        data = {**SAMPLE_DATA, "contact": None}
        task = _make_task(data=data)
        msg = XactStatusMessage.from_handler_data(task, data, datetime.now(UTC))

        assert msg.contact_name is None
        assert msg.contact_type is None
        assert msg.contact_email is None
        assert msg.contact_phone is None

    def test_from_handler_data_handles_missing_adm(self):
        data = {**SAMPLE_DATA, "adm": None}
        task = _make_task(data=data)
        msg = XactStatusMessage.from_handler_data(task, data, datetime.now(UTC))

        assert msg.claim_number is None

    def test_from_handler_data_handles_empty_data(self):
        task = _make_task(data={})
        msg = XactStatusMessage.from_handler_data(task, {}, datetime.now(UTC))

        assert msg.description is None
        assert msg.xn_address is None
        assert msg.claim_number is None

    def test_serialization_produces_iso_timestamps(self):
        task = _make_task()
        produced = datetime(2026, 2, 28, 14, 0, 0, tzinfo=UTC)
        msg = XactStatusMessage.from_handler_data(task, SAMPLE_DATA, produced)
        dumped = msg.model_dump()

        assert isinstance(dumped["produced_at"], str)
        assert dumped["produced_at"].startswith("2026-02-28")


# ---------------------------------------------------------------------------
# Rule matching tests
# ---------------------------------------------------------------------------


class TestMatchesXactStatus:
    def test_matches_xn_status_event(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=None)
        assert matches_xact_status(ctx) is True

    def test_no_match_non_status_event(self):
        task = _make_task(
            status_subtype="documentsReceived",
            event_type="verisk.claims.property.xn.documentsReceived",
        )
        ctx = RuleContext(task=task, file_path=None)
        assert matches_xact_status(ctx) is False

    def test_matches_any_xn_status_subtype(self):
        task = _make_task(
            status_subtype="accepted",
            event_type="verisk.claims.property.xn.status.accepted",
        )
        ctx = RuleContext(task=task, file_path=None)
        assert matches_xact_status(ctx) is True

    def test_no_match_when_file_path_present(self):
        from pathlib import Path
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/file.xml"))
        assert matches_xact_status(ctx) is False


# ---------------------------------------------------------------------------
# Action tests
# ---------------------------------------------------------------------------


class TestProduceXactStatus:
    @pytest.mark.asyncio
    async def test_produces_xact_status_side_effect(self):
        task = _make_task()
        parsed = parse_event_json(task)
        ctx = RuleContext(task=task, file_path=None, parsed_data=parsed)

        side_effects = await produce_xact_status(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "verisk_xact_status"

        msg = side_effects[0].message
        assert isinstance(msg, XactStatusMessage)
        assert msg.description == "Delivered"
        assert msg.xn_address == "EXAMPLE@CARRIER_XF"
        assert msg.original_assignment_id == "BBBBBBB"
        assert msg.contact_name == "Jane Smith"
        assert msg.claim_number == "0000000000"

    @pytest.mark.asyncio
    async def test_handles_invalid_json(self):
        import json

        task = _make_task()
        task.raw_event["data"] = "not valid json {"

        with pytest.raises(json.JSONDecodeError):
            parse_event_json(task)
