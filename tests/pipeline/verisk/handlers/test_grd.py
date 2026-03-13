"""Tests for GENERIC_ROUGHDRAFT parsing and rule matching."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.parsers import parse_grd, parse_grd_filename
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_grd,
    produce_grd,
)
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


def _make_task(status_subtype: str = "estimatePackageReceived", file_type: str = "xml") -> DownloadTaskMessage:
    return DownloadTaskMessage(
        media_id="media-grd-001",
        trace_id="trace-grd-001",
        attachment_url="https://example.com/055379P_2__GENERIC_ROUGHDRAFT.xml",
        blob_path="estimatePackageReceived/055379P/trace-grd-001",
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id="055379P",
        retry_count=0,
        event_type="xact",
        event_subtype=status_subtype,
        original_timestamp=datetime.now(UTC),
    )


SAMPLE_GRD_XML = b"""<?xml version='1.0' encoding='UTF-8' ?>
<GENERIC_ROUGHDRAFT xmlns="http://xactware.com/generic_roughdraft.xsd" majorVersion="28" minorVersion="300" transactionId="055379P">
    <HEADER compName="Test_Company" dateCreated="2026-03-03T22:30:40">
        <COMP_INFO><![CDATA[Test Company Info]]></COMP_INFO>
    </HEADER>
    <COVERSHEET>
        <ESTIMATE_INFO estimateName="TEST" insuredName="TEST INSURED" claimNumber="12345" />
    </COVERSHEET>
</GENERIC_ROUGHDRAFT>"""


class TestParseGrd:
    def test_grd_filename_double_underscore(self):
        d = parse_grd_filename("055379P_2__GENERIC_ROUGHDRAFT.xml")
        assert d["assignment_id"] == "055379P"
        assert d["version"] == "2"

    def test_grd_filename_single_underscore(self):
        d = parse_grd_filename("ABC123_5_GENERIC_ROUGHDRAFT.xml")
        assert d["assignment_id"] == "ABC123"
        assert d["version"] == "5"

    @pytest.mark.asyncio
    async def test_parse_grd_reads_content(self, tmp_path):
        xml_file = tmp_path / "055379P_2__GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        d = await parse_grd(xml_file)
        assert d["assignment_id"] == "055379P"
        assert d["version"] == "2"
        assert "GENERIC_ROUGHDRAFT" in d["generic_roughdraft_data"]


class TestMatchesGrd:
    def test_matches_correct_combination(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/055379P_2__GENERIC_ROUGHDRAFT.XML"))
        assert matches_grd(ctx) is True

    def test_no_match_wrong_subtype(self):
        task = _make_task(status_subtype="firstNoticeOfLossReceived")
        ctx = RuleContext(task=task, file_path=Path("/tmp/055379P_2__GENERIC_ROUGHDRAFT.XML"))
        assert matches_grd(ctx) is False

    def test_no_match_wrong_filename(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/other.xml"))
        assert matches_grd(ctx) is False


class TestProduceGrd:
    @pytest.mark.asyncio
    async def test_produces_grd_side_effect(self, tmp_path):
        xml_file = tmp_path / "055379P_2__GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        task = _make_task()
        parsed = await parse_grd(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_grd(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "verisk_grd"

        msg = side_effects[0].message
        assert msg.assignment_id == "055379P"
        assert msg.version == "2"
        assert msg.trace_id == "trace-grd-001"
        assert msg.generic_roughdraft_data is not None
