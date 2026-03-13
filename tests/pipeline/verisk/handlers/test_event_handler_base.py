"""Tests for unified RuleRunner."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from pipeline.verisk.handlers.rule_runner import RuleRunner
from pipeline.verisk.schemas.tasks import (
    DownloadTaskMessage,
    XACTEnrichmentTask,
)

SAMPLE_FNOL_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO
    carrierId="0000001"
    rotationTrade="WIND/HAIL INSPECTION"
    transactionId="AAAAAAA"
    originalTransactionId="AAAAAAA"
    sendersXNAddress="EXAMPLE.HOME.WEB"
    recipientsXNAddress="EXAMPLE.ONLINE" />
  <ADM dateOfLoss="2026-02-24T22:30:00Z" dateReceived="2026-02-26T17:53:06Z">
    <COVERAGE_LOSS claimNumber="0000000000" policyNumber="000000000000" />
  </ADM>
</XACTDOC>"""

SAMPLE_STATUS_DATA = {
    "description": "Delivered",
    "assignmentId": "AAAAAAA",
    "xnAddress": "EXAMPLE@CARRIER_XF",
    "originalAssignmentId": "BBBBBBB",
    "dateTime": "2026-01-01T00:00:00.0000000Z",
    "contact": {
        "contactMethods": {
            "phone": {"type": "Office", "number": "000-000-0000"},
            "email": {"address": "adjuster@example.com"},
        },
        "type": "ClaimRep",
        "name": "Jane Smith",
    },
    "adm": {
        "coverageLoss": {"claimNumber": "0000000000"},
    },
}


class FakeProducer:
    def __init__(self, key: str = ""):
        self.key = key
        self.sent: list[tuple] = []
        self.eventhub_name = f"test-{key}"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, value: BaseModel, key: str) -> None:
        self.sent.append((value, key))


def _make_download_task(
    status_subtype: str = "firstNoticeOfLossReceived",
    file_type: str = "xml",
) -> DownloadTaskMessage:
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


def _make_enrichment_task(
    status_subtype: str = "delivered",
    event_type: str = "verisk.claims.property.xn.status.delivered",
    data: dict | None = None,
) -> XACTEnrichmentTask:
    raw_data = json.dumps(data or SAMPLE_STATUS_DATA)
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


class TestRuleRunnerDownload:
    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_rules_match(self):
        runner = RuleRunner(producer_factory=lambda k: FakeProducer(k))
        task = _make_download_task(status_subtype="unknownSubtype", file_type="pdf")
        result = await runner.run_for_download(task, __import__("pathlib").Path("/tmp/file.pdf"))
        assert result == {}

    @pytest.mark.asyncio
    async def test_fnol_produces_side_effect(self, tmp_path):
        producers: dict[str, FakeProducer] = {}

        def factory(key: str) -> FakeProducer:
            p = FakeProducer(key)
            producers[key] = p
            return p

        runner = RuleRunner(producer_factory=factory)
        task = _make_download_task()

        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(SAMPLE_FNOL_XML)

        result = await runner.run_for_download(task, xml_file)

        assert "transaction_id" in result
        assert "fnol" in producers
        assert len(producers["fnol"].sent) == 1

    @pytest.mark.asyncio
    async def test_contentspal_fires_when_dataset_matches(self, tmp_path):
        producers: dict[str, FakeProducer] = {}

        def factory(key: str) -> FakeProducer:
            p = FakeProducer(key)
            producers[key] = p
            return p

        runner = RuleRunner(producer_factory=factory)
        task = _make_download_task()

        xml = b"""<?xml version="1.0" encoding="utf-8"?>
<XACTDOC>
  <XACTNET_INFO carrierId="3372667" rotationTrade="INVENTORY"
    transactionId="AAAAAAA" originalTransactionId="AAAAAAA"
    sendersXNAddress="EXAMPLE.HOME.WEB" recipientsXNAddress="EXAMPLE.ONLINE" />
  <ADM dateOfLoss="2026-02-24T22:30:00Z" dateReceived="2026-02-26T17:53:06Z">
    <COVERAGE_LOSS claimNumber="0000000000" policyNumber="000000000000" />
  </ADM>
</XACTDOC>"""
        xml_file = tmp_path / "A001_FNOL_XACTDOC.XML"
        xml_file.write_bytes(xml)

        await runner.run_for_download(task, xml_file)

        # Both fnol and contentspal should fire
        assert "fnol" in producers
        assert "contentspal_delivery" in producers
        assert len(producers["fnol"].sent) == 1
        assert len(producers["contentspal_delivery"].sent) == 1


    @pytest.mark.asyncio
    async def test_documents_received_xml_does_not_trigger_parser(self, tmp_path):
        """Regression: documentsReceived XML files should not trigger the FNOL
        parser via the deferred contentspal rule.  Previously the deferred rule
        caused a parser invocation on every XML download regardless of
        status_subtype, leading to 'not well-formed' errors on non-FNOL files.
        """
        runner = RuleRunner(producer_factory=lambda k: FakeProducer(k))
        task = _make_download_task(
            status_subtype="documentsReceived",
            file_type="xml",
        )

        # Write a non-XML file to ensure no parser attempts to read it
        bad_file = tmp_path / "A001_DOCUMENT.XML"
        bad_file.write_bytes(b"this is not xml at all")

        result = await runner.run_for_download(task, bad_file)
        assert result == {}


class TestRuleRunnerEvent:
    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_rules_match(self):
        runner = RuleRunner(producer_factory=lambda k: FakeProducer(k))
        task = _make_enrichment_task(
            status_subtype="unknownStatus",
            event_type="verisk.claims.property.xn.unknownStatus",
        )
        result = await runner.run_for_event(task)
        assert result == {}

    @pytest.mark.asyncio
    async def test_xact_status_produces_side_effect(self):
        producers: dict[str, FakeProducer] = {}

        def factory(key: str) -> FakeProducer:
            p = FakeProducer(key)
            producers[key] = p
            return p

        runner = RuleRunner(producer_factory=factory)
        task = _make_enrichment_task()
        await runner.run_for_event(task)

        assert "verisk_xact_status" in producers
        assert len(producers["verisk_xact_status"].sent) == 1
        msg = producers["verisk_xact_status"].sent[0][0]
        assert msg.description == "Delivered"

    @pytest.mark.asyncio
    async def test_assignment_note_produces_side_effect(self):
        producers: dict[str, FakeProducer] = {}

        def factory(key: str) -> FakeProducer:
            p = FakeProducer(key)
            producers[key] = p
            return p

        runner = RuleRunner(producer_factory=factory)

        data = {
            "description": "Assignment Note",
            "note": "Target Completion Date",
            "author": "XactNet System",
            "dateTime": "2026-02-27T13:13:40.0000000Z",
            "adm": {"coverageLoss": {"claimNumber": "0819940122"}},
        }
        task = _make_enrichment_task(
            status_subtype="assignmentNoteAdded",
            event_type="verisk.claims.property.xn.assignmentNoteAdded",
            data=data,
        )
        await runner.run_for_event(task)

        assert "verisk_notes" in producers
        assert len(producers["verisk_notes"].sent) == 1


class TestRuleRunnerLifecycle:
    @pytest.mark.asyncio
    async def test_close_stops_all_producers(self):
        stopped: list[str] = []

        class TrackingProducer:
            def __init__(self, key: str):
                self.key = key
                self.eventhub_name = f"test-{key}"

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                stopped.append(self.key)

            async def send(self, **kw) -> None:
                pass

        runner = RuleRunner(producer_factory=lambda k: TrackingProducer(k))
        # Manually prime two producers
        await runner._get_producer("topic_a")
        await runner._get_producer("topic_b")

        await runner.close()
        assert sorted(stopped) == ["topic_a", "topic_b"]
        assert runner._producers == {}
