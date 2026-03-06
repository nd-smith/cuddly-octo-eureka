"""Tests for ITEL event routing filter in ClaimXEnrichmentWorker."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker


def _make_task_row(groups=None, task_id=32615):
    """Build a minimal task_row dict with optional form response groups."""
    row = {"task_id": task_id, "assignment_id": 123}
    if groups is not None:
        row["_form_response_groups"] = groups
    return row


def _make_group(question_text, answer_type="option", answer_value="Yes"):
    """Build a single ClaimX response group with one question."""
    export = {"type": answer_type}
    if answer_type == "option":
        export["optionAnswer"] = {"name": answer_value}
    elif answer_type == "text":
        export["text"] = answer_value
    return {
        "name": "Test Group",
        "groupId": "group-test",
        "questionAndAnswers": [
            {
                "questionText": question_text,
                "component": "radio",
                "responseAnswerExport": export,
                "formControl": {"id": "control-test"},
            }
        ],
    }


class TestHasCabinetDamage:
    """Tests for _has_cabinet_damage static method."""

    def test_yes_option_returns_true(self):
        group = _make_group("Kitchen cabinet damage present", "option", "Yes")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True

    def test_yes_comma_variant_returns_true(self):
        group = _make_group("Kitchen cabinet damage present", "option", "Yes, confirmed")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True

    def test_no_option_returns_false(self):
        group = _make_group("Kitchen cabinet damage present", "option", "No")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is False

    def test_text_yes_returns_true(self):
        group = _make_group("Kitchen cabinet damage present", "text", "Yes")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True

    def test_text_no_returns_false(self):
        group = _make_group("Kitchen cabinet damage present", "text", "No")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is False

    def test_missing_form_response_groups_returns_false(self):
        row = _make_task_row(groups=None)
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is False

    def test_empty_groups_returns_false(self):
        row = _make_task_row(groups=[])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is False

    def test_question_not_present_returns_false(self):
        group = _make_group("Some other question", "option", "Yes")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is False

    def test_case_insensitive_question_match(self):
        group = _make_group("kitchen cabinet damage present", "option", "Yes")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True

    def test_whitespace_trimmed_question(self):
        group = _make_group("  Kitchen cabinet damage present  ", "option", "Yes")
        row = _make_task_row(groups=[group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True

    def test_multiple_groups_finds_correct_question(self):
        other_group = _make_group("Unrelated question", "option", "No")
        cabinet_group = _make_group("Kitchen cabinet damage present", "option", "Yes")
        row = _make_task_row(groups=[other_group, cabinet_group])
        assert ClaimXEnrichmentWorker._has_cabinet_damage(row) is True


class TestRouteItelEventsRetrySkip:
    """Verify ITEL routing is skipped on retries to prevent duplicate events."""

    @pytest.fixture()
    def worker(self):
        """Minimal worker with a mocked itel_producer."""
        with patch.object(ClaimXEnrichmentWorker, "__init__", lambda self: None):
            w = ClaimXEnrichmentWorker.__new__(ClaimXEnrichmentWorker)
            w.itel_producer = AsyncMock()
            return w

    @pytest.mark.asyncio
    async def test_skips_routing_when_retry_count_positive(self, worker):
        task = SimpleNamespace(trace_id="abc", retry_count=1, event_type="CUSTOM_TASK_COMPLETED", project_id="1")
        event = SimpleNamespace()
        entity_rows = SimpleNamespace(tasks=[_make_task_row(groups=[_make_group("Kitchen cabinet damage present")])])

        await worker._route_itel_events(task, event, entity_rows)

        worker.itel_producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_routes_when_retry_count_zero(self, worker):
        task = SimpleNamespace(
            trace_id="abc", retry_count=0, event_type="CUSTOM_TASK_COMPLETED",
            project_id="1", media_ids=[],
        )
        event = SimpleNamespace()
        entity_rows = SimpleNamespace(
            tasks=[_make_task_row(groups=[_make_group("Kitchen cabinet damage present")])],
            projects=[],
        )

        await worker._route_itel_events(task, event, entity_rows)

        worker.itel_producer.send.assert_called_once()
