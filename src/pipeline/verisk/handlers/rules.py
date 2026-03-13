"""Unified rule definitions for Verisk handler pipeline.

Each rule is a dataclass with a ``matches()`` predicate and an ``action()``
function. Multiple rules can fire per event. Match functions are plain
Python -- no DSL, fully debuggable.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.verisk.handlers.base import FileHandlerSideEffect
from pipeline.verisk.handlers.parsers import (
    FNOL_FILENAME_SUFFIX,
    GRD_FILENAME_SUFFIX,
    REINSPECTION_FILENAME,
    parse_event_json,
    parse_fnol_xactdoc,
    parse_grd,
    parse_reinspection_form,
)
from pipeline.verisk.schemas.assignment_notes import AssignmentNoteMessage
from pipeline.verisk.schemas.fnol import FnolMessage
from pipeline.verisk.schemas.grd import GrdMessage
from pipeline.verisk.schemas.reinspection import ReinspectionMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage, XACTEnrichmentTask
from pipeline.verisk.schemas.xact_status import XactStatusMessage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rule dataclass and context
# ---------------------------------------------------------------------------

@dataclass
class RuleContext:
    """Context passed to every rule's matches() and action()."""

    task: DownloadTaskMessage | XACTEnrichmentTask
    file_path: Path | None = None
    parsed_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class Rule:
    """A single rule: predicate + async action producing side-effects."""

    name: str
    matches: Callable[[RuleContext], bool]
    action: Callable[[RuleContext], Awaitable[list[FileHandlerSideEffect]]]
    needs_parsed_data: bool = False


# ---------------------------------------------------------------------------
# Match functions
# ---------------------------------------------------------------------------

def matches_fnol_xactdoc(ctx: RuleContext) -> bool:
    return (
        ctx.task.status_subtype == "firstNoticeOfLossReceived"
        and ctx.task.file_type.upper() == "XML"
        and ctx.file_path is not None
        and ctx.file_path.name.upper().endswith(FNOL_FILENAME_SUFFIX)
    )


def matches_contentspal(ctx: RuleContext) -> bool:
    return (
        matches_fnol_xactdoc(ctx)
        and ctx.parsed_data.get("dataset") == "3372667"
        and ctx.parsed_data.get("job_type") in {"INVENTORY", "PRICING TASK"}
    )


def matches_reinspection(ctx: RuleContext) -> bool:
    return (
        ctx.task.status_subtype == "estimatePackageReceived"
        and ctx.task.file_type.upper() == "XML"
        and ctx.file_path is not None
        and ctx.file_path.name.upper() == REINSPECTION_FILENAME
    )


def matches_grd(ctx: RuleContext) -> bool:
    return (
        ctx.task.status_subtype == "estimatePackageReceived"
        and ctx.task.file_type.upper() == "XML"
        and ctx.file_path is not None
        and ctx.file_path.name.upper().endswith(GRD_FILENAME_SUFFIX)
    )


_XN_STATUS_MARKER = ".xn.status."


def matches_xact_status(ctx: RuleContext) -> bool:
    if ctx.file_path is not None:
        return False
    if not isinstance(ctx.task, XACTEnrichmentTask):
        return False
    event_type = ctx.task.raw_event.get("type", "")
    return _XN_STATUS_MARKER in event_type


_XN_ASSIGNMENT_NOTE_MARKER = ".xn.assignmentNoteAdded"


def matches_assignment_note(ctx: RuleContext) -> bool:
    if ctx.file_path is not None:
        return False
    if not isinstance(ctx.task, XACTEnrichmentTask):
        return False
    if ctx.task.status_subtype != "assignmentNoteAdded":
        return False
    event_type = ctx.task.raw_event.get("type", "")
    return _XN_ASSIGNMENT_NOTE_MARKER in event_type


# ---------------------------------------------------------------------------
# Action functions
# ---------------------------------------------------------------------------

async def produce_fnol(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, DownloadTaskMessage)
    assert ctx.file_path is not None
    merged = {**ctx.parsed_data, "blob_url": f"{ctx.task.blob_path}/{ctx.file_path.name}"}
    return [
        FileHandlerSideEffect(
            topic_key="fnol",
            message=FnolMessage.from_handler_data(
                task_message=ctx.task,
                parsed_data=merged,
                produced_at=datetime.now(UTC),
            ),
        ),
    ]


async def produce_contentspal(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, DownloadTaskMessage)
    assert ctx.file_path is not None
    merged = {**ctx.parsed_data, "blob_url": f"{ctx.task.blob_path}/{ctx.file_path.name}"}
    return [
        FileHandlerSideEffect(
            topic_key="contentspal_delivery",
            message=FnolMessage.from_handler_data(
                task_message=ctx.task,
                parsed_data=merged,
                produced_at=datetime.now(UTC),
            ),
        ),
    ]


async def produce_reinspection(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, DownloadTaskMessage)
    assert ctx.file_path is not None
    merged = {**ctx.parsed_data, "blob_url": f"{ctx.task.blob_path}/{ctx.file_path.name}"}
    return [
        FileHandlerSideEffect(
            topic_key="reinspections",
            message=ReinspectionMessage.from_handler_data(
                task_message=ctx.task,
                parsed_data=merged,
                produced_at=datetime.now(UTC),
            ),
        ),
    ]


async def produce_grd(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, DownloadTaskMessage)
    assert ctx.file_path is not None
    merged = {**ctx.parsed_data, "blob_url": f"{ctx.task.blob_path}/{ctx.file_path.name}"}
    return [
        FileHandlerSideEffect(
            topic_key="verisk_grd",
            message=GrdMessage.from_handler_data(
                task_message=ctx.task,
                parsed_data=merged,
                produced_at=datetime.now(UTC),
            ),
        ),
    ]


async def produce_xact_status(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, XACTEnrichmentTask)
    message = XactStatusMessage.from_handler_data(
        task=ctx.task,
        data=ctx.parsed_data,
        produced_at=datetime.now(UTC),
    )
    return [
        FileHandlerSideEffect(
            topic_key="verisk_xact_status",
            message=message,
        ),
    ]


async def produce_assignment_note(ctx: RuleContext) -> list[FileHandlerSideEffect]:
    assert isinstance(ctx.task, XACTEnrichmentTask)
    message = AssignmentNoteMessage.from_handler_data(
        task=ctx.task,
        data=ctx.parsed_data,
        produced_at=datetime.now(UTC),
    )
    return [
        FileHandlerSideEffect(
            topic_key="verisk_notes",
            message=message,
        ),
    ]


# ---------------------------------------------------------------------------
# Parse dispatch — maps rule names to async parse functions
# ---------------------------------------------------------------------------

async def _parse_for_fnol(ctx: RuleContext) -> dict[str, Any]:
    assert ctx.file_path is not None
    return await parse_fnol_xactdoc(ctx.file_path)


async def _parse_for_reinspection(ctx: RuleContext) -> dict[str, Any]:
    assert ctx.file_path is not None
    return await parse_reinspection_form(ctx.file_path)


async def _parse_for_grd(ctx: RuleContext) -> dict[str, Any]:
    assert ctx.file_path is not None
    return await parse_grd(ctx.file_path)


async def _parse_for_event(ctx: RuleContext) -> dict[str, Any]:
    assert isinstance(ctx.task, XACTEnrichmentTask)
    return parse_event_json(ctx.task)


# Maps rule name to its parser. The runner calls the parser once and caches
# the result before running the action.
RULE_PARSERS: dict[str, Callable[[RuleContext], Awaitable[dict[str, Any]]]] = {
    "fnol_xactdoc": _parse_for_fnol,
    "contentspal_delivery": _parse_for_fnol,  # shares parser with fnol
    "reinspection_form": _parse_for_reinspection,
    "generic_roughdraft": _parse_for_grd,
    "xact_status": _parse_for_event,
    "assignment_note": _parse_for_event,
}


# ---------------------------------------------------------------------------
# Rule list — order matters: more specific rules before general ones
# ---------------------------------------------------------------------------

RULES: list[Rule] = [
    Rule(
        name="fnol_xactdoc",
        matches=matches_fnol_xactdoc,
        action=produce_fnol,
        needs_parsed_data=False,
    ),
    Rule(
        name="contentspal_delivery",
        matches=matches_contentspal,
        action=produce_contentspal,
        needs_parsed_data=True,  # needs parsed_data to check dataset/job_type
    ),
    Rule(
        name="reinspection_form",
        matches=matches_reinspection,
        action=produce_reinspection,
        needs_parsed_data=False,
    ),
    Rule(
        name="generic_roughdraft",
        matches=matches_grd,
        action=produce_grd,
        needs_parsed_data=False,
    ),
    Rule(
        name="xact_status",
        matches=matches_xact_status,
        action=produce_xact_status,
        needs_parsed_data=False,
    ),
    Rule(
        name="assignment_note",
        matches=matches_assignment_note,
        action=produce_assignment_note,
        needs_parsed_data=False,
    ),
]
