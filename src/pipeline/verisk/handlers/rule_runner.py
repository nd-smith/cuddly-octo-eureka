"""Unified RuleRunner replacing FileHandlerRunner and EventHandlerRunner.

Single class that evaluates rules against download tasks (with file paths)
and enrichment tasks (status-only events). Handles parser invocation,
side-effect production, and producer lifecycle.
"""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pipeline.common.logging import extract_log_context
from pipeline.verisk.handlers.rules import RULE_PARSERS, RULES, Rule, RuleContext
from pipeline.verisk.schemas.tasks import DownloadTaskMessage, XACTEnrichmentTask

logger = logging.getLogger(__name__)


class RuleRunner:
    """Unified rule runner for Verisk download and event handling.

    Replaces both FileHandlerRunner and EventHandlerRunner. Evaluates all
    rules against the context, runs parsers as needed, and produces
    side-effect messages.

    Usage::

        runner = RuleRunner(producer_factory=...)
        metadata = await runner.run_for_download(task, file_path)
        await runner.run_for_event(task)
        await runner.close()
    """

    def __init__(self, producer_factory: Callable[[str], Any]):
        self._producer_factory = producer_factory
        self._producers: dict[str, Any] = {}

    async def _get_producer(self, topic_key: str) -> Any:
        """Return a started producer for topic_key, creating it on first use."""
        if topic_key not in self._producers:
            producer = self._producer_factory(topic_key)
            await producer.start()
            self._producers[topic_key] = producer
        return self._producers[topic_key]

    async def run_for_download(
        self,
        task: DownloadTaskMessage,
        file_path: Path,
    ) -> dict[str, Any]:
        """Run rules for a downloaded file and return parsed metadata to merge.

        Side-effect messages are produced before returning.
        Returns an empty dict when no rules match.
        """
        ctx = RuleContext(task=task, file_path=file_path)
        return await self._evaluate_rules(ctx)

    async def run_for_event(self, task: XACTEnrichmentTask) -> dict[str, Any]:
        """Run rules for a status-only event and return extracted data.

        Side-effect messages are produced before returning.
        Returns an empty dict when no rules match.
        """
        ctx = RuleContext(task=task, file_path=None)
        return await self._evaluate_rules(ctx)

    async def _evaluate_rules(self, ctx: RuleContext) -> dict[str, Any]:
        """Core rule evaluation loop.

        1. Find rules that match without parsed_data (phase 1).
        2. If any matched rule has a parser, run it once and cache the result.
        3. Re-check rules that need parsed_data (phase 2).
        4. Run all matched rules' actions and produce side-effects.
        """
        # Phase 1: match rules that don't need parsed_data
        phase1_matched: list[Rule] = []
        deferred: list[Rule] = []

        for rule in RULES:
            if rule.needs_parsed_data:
                deferred.append(rule)
            elif rule.matches(ctx):
                phase1_matched.append(rule)

        # If no phase-1 rules matched and no deferred rules exist, short-circuit
        if not phase1_matched and not deferred:
            return {}

        # Run parser if any matched rule has one, or if deferred rules need data
        parsed_data: dict[str, Any] | None = None
        parser_name: str | None = None

        # Determine which parser to run from the first matched rule
        for rule in phase1_matched:
            if rule.name in RULE_PARSERS:
                parser_name = rule.name
                break

        if parser_name is None and deferred:
            # We need to run a parser for deferred rules — pick from first phase1 match
            for rule in phase1_matched:
                if rule.name in RULE_PARSERS:
                    parser_name = rule.name
                    break

        # If we still don't have a parser name but have deferred rules, try to
        # find a parser from the deferred rules themselves for pre-parsing.
        # Only do this when phase-1 rules matched — if no phase-1 rule matched,
        # deferred rules (which are refinements of phase-1 rules) cannot match
        # either, and running their parser on unrelated files causes errors
        # (e.g. XML parse failures on non-FNOL documents).
        if parser_name is None and deferred and phase1_matched:
            for rule in deferred:
                if rule.name in RULE_PARSERS:
                    parser_name = rule.name
                    break

        if parser_name is not None:
            try:
                parser = RULE_PARSERS[parser_name]
                parsed_data = await parser(ctx)
                ctx.parsed_data = parsed_data
            except Exception as e:
                logger.error(
                    "Rule parser failed",
                    extra={
                        "parser": parser_name,
                        "error": str(e),
                        **extract_log_context(ctx.task),
                    },
                    exc_info=True,
                )
                return {}

        # Phase 2: check deferred rules now that parsed_data is available
        phase2_matched: list[Rule] = []
        if parsed_data is not None:
            for rule in deferred:
                if rule.matches(ctx):
                    phase2_matched.append(rule)

        all_matched = phase1_matched + phase2_matched

        if not all_matched:
            return {}

        logger.debug(
            "Rules matched",
            extra={
                "rules": [r.name for r in all_matched],
                **extract_log_context(ctx.task),
            },
        )

        # Run parsers for any matched rules that need their own parser
        # (e.g. reinspection has a different parser than fnol)
        # If parsed_data was already set by a shared parser, only run
        # additional parsers for rules with different parser names.
        if parsed_data is None:
            # No parser ran yet — run the first available parser
            for rule in all_matched:
                if rule.name in RULE_PARSERS:
                    try:
                        parser = RULE_PARSERS[rule.name]
                        parsed_data = await parser(ctx)
                        ctx.parsed_data = parsed_data
                    except Exception as e:
                        logger.error(
                            "Rule parser failed",
                            extra={
                                "parser": rule.name,
                                "error": str(e),
                                **extract_log_context(ctx.task),
                            },
                            exc_info=True,
                        )
                        return {}
                    break

        # Execute actions and collect side-effects
        all_side_effects = []
        for rule in all_matched:
            try:
                side_effects = await rule.action(ctx)
                all_side_effects.extend(side_effects)
                logger.debug(
                    "Rule action produced side-effects",
                    extra={
                        "rule": rule.name,
                        "side_effect_count": len(side_effects),
                        **extract_log_context(ctx.task),
                    },
                )
            except Exception as e:
                logger.error(
                    "Rule action failed",
                    extra={
                        "rule": rule.name,
                        "error": str(e),
                        **extract_log_context(ctx.task),
                    },
                    exc_info=True,
                )
                # Continue with other rules — one failure shouldn't block others

        # Produce all side-effects
        for se in all_side_effects:
            try:
                producer = await self._get_producer(se.topic_key)
                await producer.send(value=se.message, key=ctx.task.trace_id)
                logger.info(
                    "Produced rule side-effect message",
                    extra={
                        "topic_key": se.topic_key,
                        "eventhub_name": getattr(producer, "eventhub_name", se.topic_key),
                        **extract_log_context(ctx.task),
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to produce rule side-effect",
                    extra={
                        "topic_key": se.topic_key,
                        "error": str(e),
                        **extract_log_context(ctx.task),
                    },
                    exc_info=True,
                )

        return ctx.parsed_data if ctx.parsed_data else {}

    async def close(self) -> None:
        """Stop all lazily created producers."""
        for topic_key, producer in self._producers.items():
            try:
                await producer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping rule runner producer",
                    extra={"topic_key": topic_key, "error": str(e)},
                )
        self._producers.clear()
