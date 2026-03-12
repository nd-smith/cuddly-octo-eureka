"""Tests for Verisk download file handler base classes and registry."""

import pytest

from pipeline.verisk.handlers.base import (
    DownloadFileHandler,
    FileHandlerRegistry,
    FileHandlerResult,
    _HANDLERS,
    register_handler,
)
from pipeline.verisk.schemas.tasks import DownloadTaskMessage
from pathlib import Path
from datetime import datetime, UTC


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate registry state between tests."""
    original = dict(_HANDLERS)
    yield
    _HANDLERS.clear()
    _HANDLERS.update(original)


def _make_task(status_subtype: str, file_type: str) -> DownloadTaskMessage:
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


class TestFileHandlerRegistry:
    def test_returns_none_for_unregistered_key(self):
        registry = FileHandlerRegistry()
        assert registry.get_handler("unknownSubtype", "pdf") is None

    def test_exact_match_returned(self):
        @register_handler
        class MyHandler(DownloadFileHandler):
            status_subtypes = ["estimatePackageReceived"]
            file_types = ["xml"]

            async def handle(self, task, file_path):
                return FileHandlerResult(success=True)

        registry = FileHandlerRegistry()
        handler = registry.get_handler("estimatePackageReceived", "xml")
        assert isinstance(handler, MyHandler)

    def test_lookup_is_case_insensitive(self):
        @register_handler
        class MyHandler(DownloadFileHandler):
            status_subtypes = ["estimatePackageReceived"]
            file_types = ["XML"]

            async def handle(self, task, file_path):
                return FileHandlerResult(success=True)

        registry = FileHandlerRegistry()
        assert registry.get_handler("ESTIMATEPACKAGERECEIVED", "xml") is not None

    def test_wildcard_file_type_matches_any(self):
        @register_handler
        class WildcardHandler(DownloadFileHandler):
            status_subtypes = ["estimatePackageReceived"]
            file_types = []  # wildcard

            async def handle(self, task, file_path):
                return FileHandlerResult(success=True)

        registry = FileHandlerRegistry()
        assert registry.get_handler("estimatePackageReceived", "pdf") is not None
        assert registry.get_handler("estimatePackageReceived", "esx") is not None

    def test_no_match_for_different_subtype(self):
        @register_handler
        class MyHandler(DownloadFileHandler):
            status_subtypes = ["estimatePackageReceived"]
            file_types = ["xml"]

            async def handle(self, task, file_path):
                return FileHandlerResult(success=True)

        registry = FileHandlerRegistry()
        assert registry.get_handler("documentsReceived", "xml") is None


class TestEstimatePackageXmlHandler:
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

    @pytest.mark.asyncio
    async def test_reinspection_form_parsed(self, tmp_path):
        from pipeline.verisk.handlers.reinspection import EstimatePackageXmlHandler

        xml_file = tmp_path / "2F06PR5XM_1_REINSPECTION_FORM.XML"
        xml_file.write_bytes(self.SAMPLE_XML)

        handler = EstimatePackageXmlHandler()
        task = _make_task("estimatePackageReceived", "xml")
        result = await handler.handle(task, xml_file)

        assert result.success is True
        d = result.parsed_data
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
        assert d["filtered_overhead_acv"] is None  # empty string → None
        assert d["filtered_over_acv"] == 973.87
        assert d["filtered_total_rcv"] == 1150.69
        assert d["filtered_error_percent_acv"] == 3.73
        assert "reinspection_form_data" in d
        assert "XACTDOC" in d["reinspection_form_data"]
        assert d["blob_url"] == "estimatePackageReceived/A001/trace-001/file.xml/2F06PR5XM_1_REINSPECTION_FORM.XML"
    @pytest.mark.asyncio
    async def test_non_reinspection_xml_returns_empty_data(self, tmp_path):
        from pipeline.verisk.handlers.reinspection import EstimatePackageXmlHandler

        xml_file = tmp_path / "other_attachment.xml"
        xml_file.write_bytes(b"<Root><data>x</data></Root>")

        handler = EstimatePackageXmlHandler()
        task = _make_task("estimatePackageReceived", "xml")
        result = await handler.handle(task, xml_file)

        assert result.success is True
        assert result.parsed_data == {"blob_url": "estimatePackageReceived/A001/trace-001/file.xml/other_attachment.xml"}

    @pytest.mark.asyncio
    async def test_malformed_xml_returns_failure(self, tmp_path):
        from pipeline.verisk.handlers.reinspection import EstimatePackageXmlHandler

        xml_file = tmp_path / "REINSPECTION_FORM.XML"
        xml_file.write_bytes(b"<not valid xml <<")

        handler = EstimatePackageXmlHandler()
        task = _make_task("estimatePackageReceived", "xml")
        result = await handler.handle(task, xml_file)

        assert result.success is False
        assert result.error is not None
