"""Assignment note event handler for verisk.claims.property.xn.assignmentNoteAdded events.

These events carry no downloadable attachments. The data payload is parsed
directly from the raw event and produced to the verisk_notes topic.
"""

import json
import logging
from datetime import UTC, datetime

from pipeline.common.logging import extract_log_context
from pipeline.verisk.handlers.base import (
    EventHandler,
    EventHandlerResult,
    FileHandlerSideEffect,
    register_event_handler,
)
from pipeline.verisk.schemas.assignment_notes import AssignmentNoteMessage
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask

logger = logging.getLogger(__name__)

_XN_ASSIGNMENT_NOTE_MARKER = ".xn.assignmentNoteAdded"


@register_event_handler
class AssignmentNoteAddedHandler(EventHandler):
    """Handles verisk.claims.property.xn.assignmentNoteAdded events.

    These events carry no attachments. The data payload is parsed directly
    from the raw event and produced to the verisk_notes topic.
    """

    status_subtypes = ["assignmentNoteAdded"]

    async def handle(self, task: XACTEnrichmentTask) -> EventHandlerResult:
        event_type = task.raw_event.get("type", "")

        if _XN_ASSIGNMENT_NOTE_MARKER not in event_type:
            return EventHandlerResult(success=True, data={})

        logger.info(
            "Running assignment note event handler",
            extra={
                "handler_name": self.name,
                **extract_log_context(task),
            },
        )

        try:
            raw_data = task.raw_event.get("data", "{}")
            data: dict = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

            message = AssignmentNoteMessage.from_handler_data(
                task=task,
                data=data,
                produced_at=datetime.now(UTC),
            )

            logger.info(
                "Assignment note event handler complete",
                extra={
                    "handler_name": self.name,
                    "parsed_fields": [
                        f for f in (
                            "description", "note", "author",
                            "event_date_time", "claim_number",
                        )
                        if getattr(message, f) is not None
                    ],
                    **extract_log_context(task),
                },
            )

            return EventHandlerResult(
                success=True,
                data=message.model_dump(exclude={"trace_id", "assignment_id", "status_subtype",
                                                  "original_timestamp", "produced_at"}),
                side_effect=FileHandlerSideEffect(
                    topic_key="verisk_notes",
                    message=message,
                ),
            )

        except Exception as e:
            logger.error(
                "Assignment note event handler failed",
                extra={
                    "handler_name": self.name,
                    "error": str(e),
                    **extract_log_context(task),
                },
                exc_info=True,
            )
            return EventHandlerResult(success=False, error=str(e))
