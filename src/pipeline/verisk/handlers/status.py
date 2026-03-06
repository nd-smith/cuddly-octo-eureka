"""Status event handlers for Verisk xn.status.* events.

These handlers process events that carry no downloadable attachments.
Each handler is keyed on one or more ``status_subtypes`` and is invoked
by the enrichment worker via EventHandlerRunner after plugin execution.

To add a new status handler:
    1. Subclass EventHandler and set ``status_subtypes``.
    2. Implement ``async handle(task) -> EventHandlerResult``.
    3. Decorate with ``@register_event_handler``.
    4. If a side-effect message is required, populate ``result.side_effect``
       with a FileHandlerSideEffect; EventHandlerRunner will produce it.

Example::

    @register_event_handler
    class PaymentProcessorAssignedHandler(EventHandler):
        status_subtypes = ["paymentProcessorAssigned"]

        async def handle(self, task: XACTEnrichmentTask) -> EventHandlerResult:
            return EventHandlerResult(
                success=True,
                data={"assignment_id": task.assignment_id},
                side_effect=FileHandlerSideEffect(
                    topic_key="verisk_payment_assigned",
                    message=MyMessage.from_task(task),
                ),
            )
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
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.verisk.schemas.xact_status import XactStatusMessage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# xn.status.* handler
# ---------------------------------------------------------------------------

_XN_STATUS_MARKER = ".xn.status."


@register_event_handler
class XactStatusEventHandler(EventHandler):
    """Handles all verisk.claims.property.xn.status.* events.

    These events carry no attachments. The data payload is parsed directly
    from the raw event and produced to the verisk_xact_status topic.

    Registered as a wildcard (status_subtypes = []) so it receives every
    no-attachment event; it gates on the raw event type to restrict
    processing to xn.status.* events only, returning a no-op for others.
    """

    status_subtypes = []  # wildcard — filter applied inside handle()

    async def handle(self, task: XACTEnrichmentTask) -> EventHandlerResult:
        event_type = task.raw_event.get("type", "")

        if _XN_STATUS_MARKER not in event_type:
            return EventHandlerResult(success=True, data={})

        logger.info(
            "Running status event handler",
            extra={
                "handler_name": self.name,
                "attachment_filename": None,
                **extract_log_context(task),
            },
        )

        try:
            raw_data = task.raw_event.get("data", "{}")
            data: dict = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

            message = XactStatusMessage.from_handler_data(
                task=task,
                data=data,
                produced_at=datetime.now(UTC),
            )

            logger.info(
                "Status event handler complete",
                extra={
                    "handler_name": self.name,
                    "parsed_fields": [
                        f for f in (
                            "description", "xn_address", "original_assignment_id",
                            "event_date_time", "contact_name", "claim_number",
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
                    topic_key="verisk_xact_status",
                    message=message,
                ),
            )

        except Exception as e:
            logger.error(
                "Status event handler failed",
                extra={
                    "handler_name": self.name,
                    "error": str(e),
                    **extract_log_context(task),
                },
                exc_info=True,
            )
            return EventHandlerResult(success=False, error=str(e))

