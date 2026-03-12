"""Tests for ContentsPal routing logic and FNOL handler integration."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.base import _HANDLERS
from pipeline.verisk.handlers.contentspal_router import (
    CONTENTSPAL_DATASET,
    CONTENTSPAL_JOB_TYPES,
    CONTENTSPAL_TOPIC_KEY,
    build_contentspal_side_effect,
)
from pipeline.verisk.handlers.fnol import FnolXactdocXmlHandler
from pipeline.verisk.schemas.fnol import FnolMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate registry state between tests."""
    original = dict(_HANDLERS)
    yield
    _HANDLERS.clear()
    _HANDLERS.update(original)


def _make_task(status_subtype: str = "firstNoticeOfLossReceived", file_type: str = "xml") -> DownloadTaskMessage:
    return DownloadTaskMessage(
        event_id="evt-001",
        media_id="media-001",
        trace_id="trace-001",
        attachment_url="https://example.com/A001_FNOL_XACTDOC.XML",
        blob_path="firstNoticeOfLossReceived/A001/trace-001",
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id="A001",
        retry_count=0,
        event_type="xact",
        event_subtype=status_subtype,
        original_timestamp=datetime.now(UTC),
    )


def _make_contentspal_xml(carrier_id: str = "3372667", rotation_trade: str = "INVENTORY") -> bytes:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO
    carrierId="{carrier_id}"
    rotationTrade="{rotation_trade}"
    transactionId="AAAAAAA"
    originalTransactionId="AAAAAAA"
    sendersXNAddress="EXAMPLE.HOME.WEB"
    recipientsXNAddress="EXAMPLE.ONLINE" />
  <ADM dateOfLoss="2026-02-24T22:30:00Z" dateReceived="2026-02-26T17:53:06Z">
    <COVERAGE_LOSS claimNumber="0000000000" policyNumber="000000000000" />
  </ADM>
</XACTDOC>""".encode()


class TestBuildContentsPalSideEffect:
    def test_matching_dataset_and_inventory_job_type(self):
        task = _make_task()
        parsed_data = {
            "dataset": "3372667",
            "job_type": "INVENTORY",
            "blob_url": "test/blob/url",
        }

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is not None
        assert result.topic_key == CONTENTSPAL_TOPIC_KEY
        assert isinstance(result.message, FnolMessage)

    def test_matching_dataset_and_pricing_task_job_type(self):
        task = _make_task()
        parsed_data = {
            "dataset": "3372667",
            "job_type": "PRICING TASK",
            "blob_url": "test/blob/url",
        }

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is not None
        assert result.topic_key == CONTENTSPAL_TOPIC_KEY

    def test_non_matching_dataset_returns_none(self):
        task = _make_task()
        parsed_data = {
            "dataset": "9999999",
            "job_type": "INVENTORY",
            "blob_url": "test/blob/url",
        }

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is None

    def test_non_matching_job_type_returns_none(self):
        task = _make_task()
        parsed_data = {
            "dataset": "3372667",
            "job_type": "WIND/HAIL INSPECTION",
            "blob_url": "test/blob/url",
        }

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is None

    def test_missing_dataset_returns_none(self):
        task = _make_task()
        parsed_data = {"job_type": "INVENTORY", "blob_url": "test/blob/url"}

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is None

    def test_missing_job_type_returns_none(self):
        task = _make_task()
        parsed_data = {"dataset": "3372667", "blob_url": "test/blob/url"}

        result = build_contentspal_side_effect(task, parsed_data)

        assert result is None

    def test_constants(self):
        assert CONTENTSPAL_DATASET == "3372667"
        assert CONTENTSPAL_JOB_TYPES == frozenset({"INVENTORY", "PRICING TASK"})
        assert CONTENTSPAL_TOPIC_KEY == "contentspal_delivery"


class TestFnolHandlerContentsPalIntegration:
    @pytest.mark.asyncio
    async def test_matching_fnol_produces_two_side_effects(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "INVENTORY"))

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert len(result.side_effects) == 2
        assert result.side_effects[0].topic_key == "fnol"
        assert result.side_effects[1].topic_key == "contentspal_delivery"

    @pytest.mark.asyncio
    async def test_matching_fnol_pricing_task(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "PRICING TASK"))

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert len(result.side_effects) == 2
        assert result.side_effects[1].topic_key == "contentspal_delivery"

    @pytest.mark.asyncio
    async def test_non_matching_fnol_produces_only_fnol_side_effect(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("0000001", "WIND/HAIL INSPECTION"))

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert len(result.side_effects) == 1
        assert result.side_effects[0].topic_key == "fnol"

    @pytest.mark.asyncio
    async def test_contentspal_side_effect_message_has_correct_fields(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "INVENTORY"))

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        msg = result.side_effects[1].message
        assert isinstance(msg, FnolMessage)
        assert msg.dataset == "3372667"
        assert msg.job_type == "INVENTORY"
        assert msg.trace_id == "trace-001"
