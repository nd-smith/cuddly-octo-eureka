"""
AssignmentNoteMessage schema for the verisk_notes topic.

Produced by the assignment note event handler when an
xn.assignmentNoteAdded event is received. These events carry no
downloadable attachments — their data payload is extracted directly
from the raw event.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class AssignmentNoteMessage(BaseModel):
    """Structured xn.assignmentNoteAdded event produced to verisk_notes.

    Carries pipeline identity fields alongside the fields extracted from
    the event data payload.
    """

    # --- Pipeline identity ---------------------------------------------------
    trace_id: str = Field(..., description="Unique event identifier for correlation", min_length=1)
    assignment_id: str = Field(..., description="Assignment ID from the original event", min_length=1)
    status_subtype: str = Field(..., description="Last segment of the event type, e.g. 'assignmentNoteAdded'", min_length=1)
    original_timestamp: datetime = Field(..., description="Timestamp from the original event")
    produced_at: datetime = Field(..., description="Timestamp when this message was produced")

    # --- Event data ----------------------------------------------------------
    description: str | None = Field(default=None, description="Human-readable description (e.g. 'Assignment Note')")
    note: str | None = Field(default=None, description="Note text content")
    author: str | None = Field(default=None, description="Author of the note (e.g. 'XactNet System')")
    event_date_time: str | None = Field(default=None, description="dateTime from the event payload")

    # --- ADM -----------------------------------------------------------------
    claim_number: str | None = Field(default=None, description="adm.coverageLoss.claimNumber")

    @field_validator("trace_id", "assignment_id", "status_subtype")
    @classmethod
    def validate_non_empty_strings(cls, v: str, _info) -> str:
        return v.strip() if isinstance(v, str) else v

    @field_serializer("original_timestamp", "produced_at")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        return timestamp.isoformat()

    @classmethod
    def from_handler_data(
        cls,
        task: Any,
        data: dict[str, Any],
        produced_at: datetime,
    ) -> "AssignmentNoteMessage":
        """Construct from an XACTEnrichmentTask and the parsed data payload."""
        adm = data.get("adm") or {}
        coverage_loss = adm.get("coverageLoss") or {}

        return cls(
            trace_id=task.trace_id,
            assignment_id=task.assignment_id,
            status_subtype=task.status_subtype,
            original_timestamp=task.original_timestamp,
            produced_at=produced_at,
            description=data.get("description"),
            note=data.get("note"),
            author=data.get("author"),
            event_date_time=data.get("dateTime"),
            claim_number=coverage_loss.get("claimNumber"),
        )
