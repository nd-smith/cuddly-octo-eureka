"""Tests for GENERIC_ROUGHDRAFT handling in EstimatePackageXmlHandler."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.base import _HANDLERS
from pipeline.verisk.handlers.reinspection import EstimatePackageXmlHandler
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate registry state between tests."""
    original = dict(_HANDLERS)
    yield
    _HANDLERS.clear()
    _HANDLERS.update(original)


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


class TestGrdHandler:
    @pytest.mark.asyncio
    async def test_grd_parsed(self, tmp_path):
        xml_file = tmp_path / "055379P_2__GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        handler = EstimatePackageXmlHandler()
        task = _make_task()
        result = await handler.handle(task, xml_file)

        assert result.success is True
        d = result.parsed_data

        assert d["assignment_id"] == "055379P"
        assert d["version"] == "2"
        assert "GENERIC_ROUGHDRAFT" in d["generic_roughdraft_data"]
        assert d["blob_url"] == "estimatePackageReceived/055379P/trace-grd-001/055379P_2__GENERIC_ROUGHDRAFT.xml"

    @pytest.mark.asyncio
    async def test_side_effect_topic_key(self, tmp_path):
        xml_file = tmp_path / "055379P_2__GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        handler = EstimatePackageXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.side_effect is not None
        assert result.side_effect.topic_key == "verisk_grd"

    @pytest.mark.asyncio
    async def test_side_effect_message_fields(self, tmp_path):
        xml_file = tmp_path / "055379P_2__GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        handler = EstimatePackageXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        msg = result.side_effect.message
        assert msg.assignment_id == "055379P"
        assert msg.version == "2"
        assert msg.trace_id == "trace-grd-001"
        assert msg.generic_roughdraft_data is not None

    @pytest.mark.asyncio
    async def test_non_grd_xml_passes_through(self, tmp_path):
        xml_file = tmp_path / "other_attachment.xml"
        xml_file.write_bytes(b"<Root><data>x</data></Root>")

        handler = EstimatePackageXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect is None
        assert result.parsed_data == {
            "blob_url": "estimatePackageReceived/055379P/trace-grd-001/other_attachment.xml"
        }

    @pytest.mark.asyncio
    async def test_reinspection_still_works(self, tmp_path):
        """Ensure REINSPECTION_FORM.XML is still handled by the reinspection branch."""
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO transactionId="AAAAAAA" assignmentId="A001" carrierId="C001" />
</XACTDOC>"""
        xml_file = tmp_path / "REINSPECTION_FORM.XML"
        xml_file.write_bytes(xml)

        handler = EstimatePackageXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect is not None
        assert result.side_effect.topic_key == "reinspections"

    @pytest.mark.asyncio
    async def test_grd_filename_single_underscore(self, tmp_path):
        """Handle filenames with single underscore before GENERIC_ROUGHDRAFT."""
        xml_file = tmp_path / "ABC123_5_GENERIC_ROUGHDRAFT.xml"
        xml_file.write_bytes(SAMPLE_GRD_XML)

        handler = EstimatePackageXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.parsed_data["assignment_id"] == "ABC123"
        assert result.parsed_data["version"] == "5"
