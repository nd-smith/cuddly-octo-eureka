"""Tests for FnolXactdocXmlHandler and FNOL XML parsing."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.fnol import FnolXactdocXmlHandler
from pipeline.verisk.handlers.base import _HANDLERS
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


SAMPLE_FNOL_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO
    assignmentType="0"
    businessUnit="064"
    carrierId="0000001"
    createdByEdi="1"
    emergency="0"
    jobSizeCode="2"
    profileCode="71"
    recipientsXM8UserId=""
    recipientsXNAddress="EXAMPLE.ONLINE"
    rotationTrade="WIND/HAIL INSPECTION"
    senderId="0000002"
    sendersXNAddress="EXAMPLE.HOME.WEB"
    transactionId="AAAAAAA"
    originalTransactionId="AAAAAAA"
    transactionType="EST,ASG">
  </XACTNET_INFO>
  <ADM
    dateOfLoss="2026-02-24T22:30:00Z"
    dateReceived="2026-02-26T17:53:06Z">
    <COVERAGE_LOSS
      claimNumber="0000000000"
      dateInitCov="2014-03-27"
      isCommercial="0"
      policyEnd="2026-03-27"
      policyNumber="000000000000"
      policyStart="2025-03-27">
      <TOL
        code="75"
        desc="WINDSTORM AND HAIL" />
    </COVERAGE_LOSS>
  </ADM>
  <CONTACTS>
    <CONTACT
      name="TEST INSURED"
      type="Client">
      <ADDRESSES>
        <ADDRESS
          city="SAMPLE CITY"
          country="US"
          postal="000000000"
          state="TX"
          street="123 EXAMPLE ST"
          type="Home" />
        <ADDRESS
          city="SAMPLE CITY"
          country="US"
          postal="00000-0000"
          state="TX"
          street="123 EXAMPLE ST"
          type="Property" />
      </ADDRESSES>
      <CONTACTMETHODS>
        <EMAIL address="TESTINSURED@EXAMPLE.COM" />
      </CONTACTMETHODS>
    </CONTACT>
  </CONTACTS>
</XACTDOC>"""


class TestFnolXactdocXmlHandler:
    @pytest.mark.asyncio
    async def test_fnol_xactdoc_parsed(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        handler = FnolXactdocXmlHandler()
        task = _make_task()
        result = await handler.handle(task, xml_file)

        assert result.success is True
        d = result.parsed_data

        # XACTNET_INFO
        assert d["transaction_id"] == "AAAAAAA"
        assert d["original_transaction_id"] == "AAAAAAA"
        assert d["assignment_type"] == "0"
        assert d["business_unit"] == "064"
        assert d["dataset"] == "0000001"
        assert d["job_type"] == "WIND/HAIL INSPECTION"
        assert d["senders_xn_address"] == "EXAMPLE.HOME.WEB"
        assert d["recipients_xn_address"] == "EXAMPLE.ONLINE"

        # ADM
        assert d["date_of_loss"] == "2026-02-24T22:30:00Z"
        assert d["date_received"] == "2026-02-26T17:53:06Z"

        # COVERAGE_LOSS
        assert d["claim_number"] == "0000000000"
        assert d["date_init_cov"] == "2014-03-27"
        assert d["policy_number"] == "000000000000"

        # TOL
        assert d["type_of_loss"] == "75"
        assert d["type_of_loss_desc"] == "WINDSTORM AND HAIL"

        # CONTACTS - Property address preferred
        assert d["insured_name"] == "TEST INSURED"
        assert d["insured_email"] == "TESTINSURED@EXAMPLE.COM"
        assert d["insured_street"] == "123 EXAMPLE ST"
        assert d["insured_city"] == "SAMPLE CITY"
        assert d["insured_state"] == "TX"
        assert d["insured_postal"] == "00000-0000"

        # Raw payload
        assert "fnol_xactdoc_data" in d
        assert "XACTDOC" in d["fnol_xactdoc_data"]

        # blob_url
        assert d["blob_url"] == "firstNoticeOfLossReceived/A001/trace-001/A001_FNOL_XACTDOC.XML"

    @pytest.mark.asyncio
    async def test_is_reassignment_false_when_transaction_ids_match(self, tmp_path):
        # Sample XML has transactionId == originalTransactionId → not a reassignment
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect.message.is_reassignment is False

    @pytest.mark.asyncio
    async def test_is_reassignment_true_when_transaction_ids_differ(self, tmp_path):
        xml_reassigned = SAMPLE_FNOL_XML.replace(
            b'originalTransactionId="AAAAAAA"', b'originalTransactionId="BBBBBBB"'
        )
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml_reassigned)

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect.message.is_reassignment is True

    @pytest.mark.asyncio
    async def test_is_reassignment_false_when_original_transaction_id_absent(self, tmp_path):
        xml_no_orig = SAMPLE_FNOL_XML.replace(b'originalTransactionId="AAAAAAA"', b"")
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml_no_orig)

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect.message.is_reassignment is False

    @pytest.mark.asyncio
    async def test_side_effect_topic_key(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.side_effect is not None
        assert result.side_effect.topic_key == "fnol"

    @pytest.mark.asyncio
    async def test_non_fnol_xml_passes_through(self, tmp_path):
        xml_file = tmp_path / "other_attachment.xml"
        xml_file.write_bytes(b"<Root><data>x</data></Root>")

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is True
        assert result.side_effect is None
        assert result.parsed_data == {"blob_url": "firstNoticeOfLossReceived/A001/trace-001/other_attachment.xml"}

    @pytest.mark.asyncio
    async def test_malformed_xml_returns_failure(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(b"<not valid xml <<")

        handler = FnolXactdocXmlHandler()
        result = await handler.handle(_make_task(), xml_file)

        assert result.success is False
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_address_priority_property_over_home(self, tmp_path):
        """Property address takes priority over Home when both are present."""
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO transactionId="AAAAAAA" />
  <CONTACTS>
    <CONTACT name="TEST INSURED" type="Client">
      <ADDRESSES>
        <ADDRESS city="HOME CITY" state="TX" postal="11111" street="1 HOME ST" type="Home" />
        <ADDRESS city="PROPERTY CITY" state="TX" postal="22222" street="2 PROPERTY ST" type="Property" />
      </ADDRESSES>
      <CONTACTMETHODS />
    </CONTACT>
  </CONTACTS>
</XACTDOC>"""
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml)

        result = await FnolXactdocXmlHandler().handle(_make_task(), xml_file)

        assert result.parsed_data["insured_street"] == "2 PROPERTY ST"
        assert result.parsed_data["insured_city"] == "PROPERTY CITY"

    @pytest.mark.asyncio
    async def test_address_priority_home_when_no_property(self, tmp_path):
        """Falls back to Home address when no Property address present."""
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO transactionId="AAAAAAA" />
  <CONTACTS>
    <CONTACT name="TEST INSURED" type="Client">
      <ADDRESSES>
        <ADDRESS city="MAILING CITY" state="TX" postal="33333" street="3 MAILING ST" type="Mailing" />
        <ADDRESS city="HOME CITY" state="TX" postal="11111" street="1 HOME ST" type="Home" />
      </ADDRESSES>
      <CONTACTMETHODS />
    </CONTACT>
  </CONTACTS>
</XACTDOC>"""
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml)

        result = await FnolXactdocXmlHandler().handle(_make_task(), xml_file)

        assert result.parsed_data["insured_street"] == "1 HOME ST"
        assert result.parsed_data["insured_city"] == "HOME CITY"

    @pytest.mark.asyncio
    async def test_address_priority_mailing_when_no_property_or_home(self, tmp_path):
        """Falls back to Mailing address when no Property or Home present."""
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO transactionId="AAAAAAA" />
  <CONTACTS>
    <CONTACT name="TEST INSURED" type="Client">
      <ADDRESSES>
        <ADDRESS city="MAILING CITY" state="TX" postal="33333" street="3 MAILING ST" type="Mailing" />
      </ADDRESSES>
      <CONTACTMETHODS />
    </CONTACT>
  </CONTACTS>
</XACTDOC>"""
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml)

        result = await FnolXactdocXmlHandler().handle(_make_task(), xml_file)

        assert result.parsed_data["insured_street"] == "3 MAILING ST"
        assert result.parsed_data["insured_city"] == "MAILING CITY"
        from pipeline.verisk.handlers.base import FileHandlerRegistry
        import pipeline.verisk.handlers.fnol  # noqa: F401 - trigger registration

        registry = FileHandlerRegistry()
        assert registry.get_handler("firstnoticeoflossreceived", "xml") is not None
        assert registry.get_handler("firstNoticeOfLossReceived", "XML") is not None
        assert isinstance(registry.get_handler("firstNoticeOfLossReceived", "xml"), FnolXactdocXmlHandler)
