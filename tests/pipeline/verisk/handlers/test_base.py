"""Tests for reinspection form parsing and rule matching."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.parsers import _parse_reinspection_form_sync, parse_reinspection_form
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_reinspection,
    produce_reinspection,
)
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


def _make_task(status_subtype: str = "estimatePackageReceived", file_type: str = "xml") -> DownloadTaskMessage:
    return DownloadTaskMessage(
        media_id="media-001",
        trace_id="trace-001",
        attachment_url="https://example.com/file.xml",
        blob_path="estimatePackageReceived/A001/trace-001/file.xml",
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id="A001",
        retry_count=0,
        event_type="xact",
        event_subtype=status_subtype,
        original_timestamp=datetime.now(UTC),
    )


SAMPLE_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO
    transactionId="06PR5XM"
    assignmentId="0806720421"
    assignmentType="1"
    recipientsXNAddress="ALLSTATE_XF"
    recipientsXM8UserId="DSHOF"
    recipientsName="David Shoemaker"
    sendersXNAddress="ALLST.HOME.WEB"
    carrierId="3372605"
    senderId="3372600"
    rotationTrade="STRUCTURAL"
    officeDescription1="Zone 2"
    carrierOrgLevel1="Delivery Director"
    carrierOrgLevel2="Kim Miller"
    carrierOrgLevel3="DSM - Resource"
    carrierOrgLevel4="Jeremy Hale"
    carrierOrgLevel5="Team 202"
    carrierOrgLevel6="Lawrence Johnson" />
  <ADM>
    <COVERAGE_LOSS catastrophe="0923202502">
      <TOL code="76" />
    </COVERAGE_LOSS>
  </ADM>
  <REINSPECTION_INFO
    formId="1297130"
    reinspectedEstimateCount="1"
    grossEstimate="33471.51"
    completedDate="2026-02-27T21:08:42Z"
    rating="Effective">
    <FILTERED_TOTALS
      filteredOverwriteCount="4"
      filteredUnderwriteCount="1"
      filteredOverheadAcv=""
      filteredOverheadDep=""
      filteredOverheadRcv=""
      filteredOverheadReason=""
      filteredProfitAcv=""
      filteredProfitDep=""
      filteredProfitRcv=""
      filteredProfitReason=""
      filteredOverAcv="973.87"
      filteredOverDep="34.48"
      filteredOverRcv="978.27"
      filteredUnderAcv="137.94"
      filteredUnderDep="4.4"
      filteredUnderRcv="172.42"
      filteredTotalAcv="1111.81"
      filteredTotalDep="38.88"
      filteredTotalRcv="1150.69"
      filteredOrigAcv="29832.47"
      filteredOrigDep="3493.06"
      filteredOrigRcv="33325.53"
      filteredErrorPercentAcv="3.73"
      filteredErrorPercentDep="1.11"
      filteredErrorPercentRcv="3.45" />
  </REINSPECTION_INFO>
</XACTDOC>"""


class TestParseReinspectionForm:
    def test_reinspection_form_parsed(self, tmp_path):
        xml_file = tmp_path / "2F06PR5XM_1_REINSPECTION_FORM.XML"
        xml_file.write_bytes(SAMPLE_XML)

        d = _parse_reinspection_form_sync(xml_file)

        assert d["is_reinspection"] is True
        assert d["transaction_id"] == "06PR5XM"
        assert d["claim_number"] == "0806720421"
        assert d["assignment_type"] == "1"
        assert d["recipients_xn_address"] == "ALLSTATE_XF"
        assert d["recipients_xm8_user_id"] == "DSHOF"
        assert d["recipients_name"] == "David Shoemaker"
        assert d["dataset"] == "3372605"
        assert d["job_type"] == "STRUCTURAL"
        assert d["office_description_1"] == "Zone 2"
        assert d["carrier_org_level_1"] == "Delivery Director"
        assert d["carrier_org_level_6"] == "Lawrence Johnson"
        assert d["cat_code"] == "0923202502"
        assert d["type_of_loss"] == "76"
        assert d["estimate_version"] == 1
        assert d["filtered_overwrite_count"] == 4
        assert d["filtered_underwrite_count"] == 1
        assert d["filtered_overhead_acv"] is None  # empty string -> None
        assert d["filtered_over_acv"] == 973.87
        assert d["filtered_total_rcv"] == 1150.69
        assert d["filtered_error_percent_acv"] == 3.73
        assert "reinspection_form_data" in d
        assert "XACTDOC" in d["reinspection_form_data"]

    @pytest.mark.asyncio
    async def test_async_wrapper(self, tmp_path):
        xml_file = tmp_path / "REINSPECTION_FORM.XML"
        xml_file.write_bytes(SAMPLE_XML)

        d = await parse_reinspection_form(xml_file)
        assert d["is_reinspection"] is True
        assert d["transaction_id"] == "06PR5XM"

    def test_malformed_xml_raises(self, tmp_path):
        from xml.etree.ElementTree import ParseError

        xml_file = tmp_path / "REINSPECTION_FORM.XML"
        xml_file.write_bytes(b"<not valid xml <<")

        with pytest.raises(ParseError):
            _parse_reinspection_form_sync(xml_file)


class TestMatchesReinspection:
    def test_matches_correct_combination(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/REINSPECTION_FORM.XML"))
        assert matches_reinspection(ctx) is True

    def test_no_match_wrong_subtype(self):
        task = _make_task(status_subtype="firstNoticeOfLossReceived")
        ctx = RuleContext(task=task, file_path=Path("/tmp/REINSPECTION_FORM.XML"))
        assert matches_reinspection(ctx) is False

    def test_no_match_wrong_filename(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/other.xml"))
        assert matches_reinspection(ctx) is False

    def test_no_match_no_file_path(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=None)
        assert matches_reinspection(ctx) is False


class TestProduceReinspection:
    @pytest.mark.asyncio
    async def test_produces_reinspection_side_effect(self, tmp_path):
        xml_file = tmp_path / "REINSPECTION_FORM.XML"
        xml_file.write_bytes(SAMPLE_XML)

        task = _make_task()
        parsed = _parse_reinspection_form_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_reinspection(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "reinspections"
