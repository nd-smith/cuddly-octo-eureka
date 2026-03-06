"""Tests for pipeline.common.decorators module."""

import pytest

from core.logging.context import clear_message_context, get_message_context, set_message_context
from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.types import PipelineMessage


class TestSetLogContextFromMessage:
    @pytest.fixture(autouse=True)
    def clear_context(self):
        clear_message_context()
        yield
        clear_message_context()

    @pytest.mark.asyncio
    async def test_sets_context_from_message(self):
        msg = PipelineMessage(
            topic="test-topic",
            partition=2,
            offset=100,
            timestamp=1234567890,
        )

        call_log = {}

        class FakeWorker:
            @set_log_context_from_message
            async def handle(self, message):
                from core.logging.context import get_message_context

                ctx = get_message_context()
                call_log.update(ctx)
                return "handled"

        worker = FakeWorker()
        result = await worker.handle(msg)
        assert result == "handled"
        assert call_log["message_topic"] == "test-topic"
        assert call_log["message_partition"] == 2
        assert call_log["message_offset"] == 100

    @pytest.mark.asyncio
    async def test_restores_previous_context_after_call(self):
        set_message_context(
            topic="outer-topic",
            partition=7,
            offset=88,
            key="outer-key",
            consumer_group="outer-group",
        )
        msg = PipelineMessage(
            topic="inner-topic",
            partition=1,
            offset=2,
            timestamp=1234567890,
        )

        class FakeWorker:
            @set_log_context_from_message
            async def handle(self, message):
                ctx = get_message_context()
                assert ctx["message_topic"] == "inner-topic"
                assert ctx["message_partition"] == 1
                assert ctx["message_offset"] == 2
                return "handled"

        worker = FakeWorker()
        result = await worker.handle(msg)

        assert result == "handled"
        restored = get_message_context()
        assert restored["message_topic"] == "outer-topic"
        assert restored["message_partition"] == 7
        assert restored["message_offset"] == 88
        assert restored["message_key"] == "outer-key"
        assert restored["message_consumer_group"] == "outer-group"
