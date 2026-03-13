"""Tests for ContentsPal rule matching and action."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.parsers import _parse_fnol_xactdoc_sync
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_contentspal,
    produce_contentspal,
)
from pipeline.verisk.schemas.fnol import FnolMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


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


class TestMatchesContentspal:
    def test_matches_with_correct_dataset_and_inventory(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "INVENTORY"))

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        assert matches_contentspal(ctx) is True

    def test_matches_with_pricing_task(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "PRICING TASK"))

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        assert matches_contentspal(ctx) is True

    def test_no_match_wrong_dataset(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("9999999", "INVENTORY"))

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        assert matches_contentspal(ctx) is False

    def test_no_match_wrong_job_type(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "WIND/HAIL INSPECTION"))

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        assert matches_contentspal(ctx) is False

    def test_no_match_missing_dataset(self):
        task = _make_task()
        ctx = RuleContext(
            task=task,
            file_path=Path("/tmp/A001_FNOL_XACTDOC.XML"),
            parsed_data={"job_type": "INVENTORY"},
        )
        assert matches_contentspal(ctx) is False

    def test_no_match_missing_job_type(self):
        task = _make_task()
        ctx = RuleContext(
            task=task,
            file_path=Path("/tmp/A001_FNOL_XACTDOC.XML"),
            parsed_data={"dataset": "3372667"},
        )
        assert matches_contentspal(ctx) is False


class TestProduceContentspal:
    @pytest.mark.asyncio
    async def test_produces_contentspal_side_effect(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(_make_contentspal_xml("3372667", "INVENTORY"))

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_contentspal(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "contentspal_delivery"
        msg = side_effects[0].message
        assert isinstance(msg, FnolMessage)
        assert msg.dataset == "3372667"
        assert msg.job_type == "INVENTORY"
        assert msg.trace_id == "trace-001"
