"""Tests for XactStatusEventHandler and XactStatusMessage schema."""

import json
from datetime import UTC, datetime

import pytest

from pipeline.verisk.handlers.base import (
    EventHandlerResult,
    FileHandlerSideEffect,
    _EVENT_HANDLERS,
)
from pipeline.verisk.handlers.status import XactStatusEventHandler, _XN_STATUS_MARKER
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
    status_subtype: str = "delivered",
    event_type: str = "verisk.claims.property.xn.status.delivered",
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
        data = {**SAMPLE_DATA, "originalAssignmentId": "A001"}  # matches task.assignment_id
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
# XactStatusEventHandler tests
# ---------------------------------------------------------------------------


class TestXactStatusEventHandler:
    def test_handler_is_registered_as_wildcard(self):
        assert "" in _EVENT_HANDLERS
        assert _EVENT_HANDLERS[""] is XactStatusEventHandler

    def test_handle_returns_success_with_side_effect(self):
        handler = XactStatusEventHandler()
        task = _make_task()

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert isinstance(result, EventHandlerResult)
        assert result.success is True
        assert result.side_effect is not None
        assert isinstance(result.side_effect, FileHandlerSideEffect)
        assert result.side_effect.topic_key == "verisk_xact_status"
        assert isinstance(result.side_effect.message, XactStatusMessage)

    def test_handle_extracts_correct_fields(self):
        handler = XactStatusEventHandler()
        task = _make_task()

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        msg: XactStatusMessage = result.side_effect.message
        assert msg.description == "Delivered"
        assert msg.xn_address == "EXAMPLE@CARRIER_XF"
        assert msg.original_assignment_id == "BBBBBBB"
        assert msg.contact_name == "Jane Smith"
        assert msg.claim_number == "0000000000"

    def test_handle_noops_for_non_status_event_type(self):
        """Handler must return empty no-op when event type is not xn.status.*."""
        handler = XactStatusEventHandler()
        task = _make_task(
            status_subtype="documentsReceived",
            event_type="verisk.claims.property.xn.documentsReceived",
        )

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert result.success is True
        assert result.side_effect is None
        assert result.data == {}

    def test_handle_processes_any_xn_status_subtype(self):
        """Wildcard applies to any xn.status.* event, not just 'delivered'."""
        handler = XactStatusEventHandler()
        task = _make_task(
            status_subtype="accepted",
            event_type="verisk.claims.property.xn.status.accepted",
        )

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert result.success is True
        assert result.side_effect is not None
        assert result.side_effect.message.status_subtype == "accepted"

    def test_handle_returns_failure_on_invalid_json(self):
        """Handler returns failure=False when raw_event data is malformed JSON."""
        task = _make_task()
        task.raw_event["data"] = "not valid json {"

        import asyncio

        handler = XactStatusEventHandler()
        result = asyncio.get_event_loop().run_until_complete(handler.handle(task))

        assert result.success is False
        assert result.error is not None
        assert result.side_effect is None

    def test_xn_status_marker_value(self):
        """Smoke-test the private sentinel used for event type gating."""
        assert _XN_STATUS_MARKER == ".xn.status."
        assert _XN_STATUS_MARKER in "verisk.claims.property.xn.status.delivered"
        assert _XN_STATUS_MARKER not in "verisk.claims.property.xn.documentsReceived"
