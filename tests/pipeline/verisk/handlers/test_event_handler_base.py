"""Tests for Verisk status event handler base classes and registry."""

import pytest
from datetime import datetime, UTC

from pipeline.verisk.handlers.base import (
    EventHandler,
    EventHandlerRegistry,
    EventHandlerResult,
    EventHandlerRunner,
    FileHandlerSideEffect,
    _EVENT_HANDLERS,
    register_event_handler,
)
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pydantic import BaseModel


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate registry state between tests."""
    original = dict(_EVENT_HANDLERS)
    yield
    _EVENT_HANDLERS.clear()
    _EVENT_HANDLERS.update(original)


def _make_task(status_subtype: str) -> XACTEnrichmentTask:
    return XACTEnrichmentTask(
        trace_id="trace-001",
        event_type="verisk",
        status_subtype=status_subtype,
        assignment_id="A001",
        attachments=[],
        retry_count=0,
        created_at=datetime.now(UTC),
        original_timestamp=datetime.now(UTC),
        raw_event={"type": f"verisk.claims.property.xn.{status_subtype}"},
    )


class TestEventHandlerRegistry:
    def test_returns_none_for_unregistered_subtype(self):
        _EVENT_HANDLERS.pop("", None)  # remove wildcard for this test
        registry = EventHandlerRegistry()
        assert registry.get_handler("unknownStatus") is None

    def test_exact_match_returned(self):
        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(success=True)

        registry = EventHandlerRegistry()
        handler = registry.get_handler("paymentProcessorAssigned")
        assert isinstance(handler, MyHandler)

    def test_lookup_is_case_insensitive(self):
        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(success=True)

        registry = EventHandlerRegistry()
        assert registry.get_handler("PAYMENTPROCESSORASSIGNED") is not None

    def test_wildcard_matches_any_subtype(self):
        @register_event_handler
        class WildcardHandler(EventHandler):
            status_subtypes = []  # wildcard

            async def handle(self, task):
                return EventHandlerResult(success=True)

        registry = EventHandlerRegistry()
        assert registry.get_handler("paymentProcessorAssigned") is not None
        assert registry.get_handler("documentsReceived") is not None
        assert registry.get_handler("anythingElse") is not None

    def test_no_match_for_different_subtype(self):
        _EVENT_HANDLERS.pop("", None)  # remove wildcard for this test

        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(success=True)

        registry = EventHandlerRegistry()
        assert registry.get_handler("documentsReceived") is None

    def test_each_call_returns_new_handler_instance(self):
        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(success=True)

        registry = EventHandlerRegistry()
        h1 = registry.get_handler("paymentProcessorAssigned")
        h2 = registry.get_handler("paymentProcessorAssigned")
        assert h1 is not h2


class TestEventHandlerRunner:
    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_handler(self):
        runner = EventHandlerRunner(producer_factory=lambda k: None)
        task = _make_task("unknownStatus")
        result = await runner.run(task)
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_handler_data_on_success(self):
        @register_event_handler
        class MyHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(
                    success=True,
                    data={"assignment_id": task.assignment_id},
                )

        runner = EventHandlerRunner(producer_factory=lambda k: None)
        task = _make_task("paymentProcessorAssigned")
        result = await runner.run(task)
        assert result == {"assignment_id": "A001"}

    @pytest.mark.asyncio
    async def test_returns_empty_dict_on_handler_failure(self):
        @register_event_handler
        class FailingHandler(EventHandler):
            status_subtypes = ["badStatus"]

            async def handle(self, task):
                return EventHandlerResult(success=False, error="something broke")

        runner = EventHandlerRunner(producer_factory=lambda k: None)
        task = _make_task("badStatus")
        result = await runner.run(task)
        assert result == {}

    @pytest.mark.asyncio
    async def test_side_effect_produced(self):
        produced = []

        class FakeProducer:
            async def start(self): pass
            async def stop(self): pass
            async def send(self, value, key):
                produced.append((value, key))

        class SideEffectMessage(BaseModel):
            assignment_id: str

        @register_event_handler
        class SideEffectHandler(EventHandler):
            status_subtypes = ["paymentProcessorAssigned"]

            async def handle(self, task):
                return EventHandlerResult(
                    success=True,
                    data={},
                    side_effect=FileHandlerSideEffect(
                        topic_key="verisk_payment",
                        message=SideEffectMessage(assignment_id=task.assignment_id),
                    ),
                )

        runner = EventHandlerRunner(producer_factory=lambda k: FakeProducer())
        task = _make_task("paymentProcessorAssigned")
        await runner.run(task)

        assert len(produced) == 1
        msg, key = produced[0]
        assert isinstance(msg, SideEffectMessage)
        assert msg.assignment_id == "A001"
        assert key == "trace-001"

    @pytest.mark.asyncio
    async def test_close_stops_all_producers(self):
        stopped = []

        class FakeProducer:
            def __init__(self, key):
                self.key = key
            async def start(self): pass
            async def stop(self):
                stopped.append(self.key)
            async def send(self, **kw): pass

        runner = EventHandlerRunner(producer_factory=lambda k: FakeProducer(k))
        # Manually prime two producers
        await runner._get_producer("topic_a")
        await runner._get_producer("topic_b")

        await runner.close()
        assert sorted(stopped) == ["topic_a", "topic_b"]
        assert runner._producers == {}
