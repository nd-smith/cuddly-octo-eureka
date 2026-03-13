"""Tests for FNOL XACTDOC parsing and rule matching."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.verisk.handlers.parsers import _parse_fnol_xactdoc_sync, parse_fnol_xactdoc
from pipeline.verisk.handlers.rules import (
    RuleContext,
    matches_fnol_xactdoc,
    produce_fnol,
)
from pipeline.verisk.schemas.tasks import DownloadTaskMessage


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


class TestParseFnolXactdoc:
    def test_fnol_xactdoc_parsed(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        d = _parse_fnol_xactdoc_sync(xml_file)

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

    @pytest.mark.asyncio
    async def test_async_wrapper(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        d = await parse_fnol_xactdoc(xml_file)
        assert d["transaction_id"] == "AAAAAAA"

    def test_address_priority_property_over_home(self, tmp_path):
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

        d = _parse_fnol_xactdoc_sync(xml_file)
        assert d["insured_street"] == "2 PROPERTY ST"
        assert d["insured_city"] == "PROPERTY CITY"

    def test_address_priority_home_when_no_property(self, tmp_path):
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

        d = _parse_fnol_xactdoc_sync(xml_file)
        assert d["insured_street"] == "1 HOME ST"
        assert d["insured_city"] == "HOME CITY"

    def test_address_priority_mailing_when_no_property_or_home(self, tmp_path):
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

        d = _parse_fnol_xactdoc_sync(xml_file)
        assert d["insured_street"] == "3 MAILING ST"
        assert d["insured_city"] == "MAILING CITY"


class TestMatchesFnolXactdoc:
    def test_matches_correct_combination(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/A001_FNOL_XACTDOC.XML"))
        assert matches_fnol_xactdoc(ctx) is True

    def test_no_match_wrong_subtype(self):
        task = _make_task(status_subtype="estimatePackageReceived")
        ctx = RuleContext(task=task, file_path=Path("/tmp/A001_FNOL_XACTDOC.XML"))
        assert matches_fnol_xactdoc(ctx) is False

    def test_no_match_wrong_filename(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=Path("/tmp/other.xml"))
        assert matches_fnol_xactdoc(ctx) is False

    def test_no_match_no_file_path(self):
        task = _make_task()
        ctx = RuleContext(task=task, file_path=None)
        assert matches_fnol_xactdoc(ctx) is False


class TestProduceFnol:
    @pytest.mark.asyncio
    async def test_produces_fnol_side_effect(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_fnol(ctx)
        assert len(side_effects) == 1
        assert side_effects[0].topic_key == "fnol"

    @pytest.mark.asyncio
    async def test_is_reassignment_false_when_ids_match(self, tmp_path):
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_fnol(ctx)
        assert side_effects[0].message.is_reassignment is False

    @pytest.mark.asyncio
    async def test_is_reassignment_true_when_ids_differ(self, tmp_path):
        xml_reassigned = SAMPLE_FNOL_XML.replace(
            b'originalTransactionId="AAAAAAA"', b'originalTransactionId="BBBBBBB"'
        )
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml_reassigned)

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_fnol(ctx)
        assert side_effects[0].message.is_reassignment is True

    @pytest.mark.asyncio
    async def test_is_reassignment_false_when_original_absent(self, tmp_path):
        xml_no_orig = SAMPLE_FNOL_XML.replace(b'originalTransactionId="AAAAAAA"', b"")
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml_no_orig)

        task = _make_task()
        parsed = _parse_fnol_xactdoc_sync(xml_file)
        ctx = RuleContext(task=task, file_path=xml_file, parsed_data=parsed)

        side_effects = await produce_fnol(ctx)
        assert side_effects[0].message.is_reassignment is False

    @pytest.mark.asyncio
    async def test_malformed_xml_raises(self, tmp_path):
        from xml.etree.ElementTree import ParseError

        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(b"<not valid xml <<")

        with pytest.raises(ParseError):
            _parse_fnol_xactdoc_sync(xml_file)
