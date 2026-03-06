"""
XactStatusMessage schema for the verisk_xact_status topic.

Produced by the status event handler when an xn.status.* event is received
(e.g. verisk.claims.property.xn.status.delivered). These events carry no
downloadable attachments — their data payload is extracted directly from
the raw event.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class XactStatusMessage(BaseModel):
    """Structured xn.status.* event produced to verisk_xact_status.

    Carries pipeline identity fields alongside the fields extracted from
    the event data payload.
    """

    # --- Pipeline identity ---------------------------------------------------
    trace_id: str = Field(..., description="Unique event identifier for correlation", min_length=1)
    assignment_id: str = Field(..., description="Assignment ID from the original event", min_length=1)
    status_subtype: str = Field(..., description="Last segment of the event type, e.g. 'delivered'", min_length=1)
    original_timestamp: datetime = Field(..., description="Timestamp from the original event")
    produced_at: datetime = Field(..., description="Timestamp when this message was produced")

    # --- Event data ----------------------------------------------------------
    description: str | None = Field(default=None, description="Human-readable status description")
    xn_address: str | None = Field(default=None, description="xnAddress of the recipient")
    original_assignment_id: str | None = Field(default=None, description="originalAssignmentId when reassigned")
    is_reassignment: bool = Field(..., description="True when originalAssignmentId is present and differs from assignmentId")
    event_date_time: str | None = Field(default=None, description="dateTime from the event payload")

    # --- Contact -------------------------------------------------------------
    contact_name: str | None = Field(default=None, description="contact.name")
    contact_type: str | None = Field(default=None, description="contact.type (e.g. 'ClaimRep')")
    contact_email: str | None = Field(default=None, description="contact.contactMethods.email.address")
    contact_phone: str | None = Field(default=None, description="contact.contactMethods.phone.number")

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
    ) -> "XactStatusMessage":
        """Construct from an XACTEnrichmentTask and the parsed data payload."""
        contact = data.get("contact") or {}
        contact_methods = contact.get("contactMethods") or {}
        phone = contact_methods.get("phone") or {}
        email = contact_methods.get("email") or {}
        adm = data.get("adm") or {}
        coverage_loss = adm.get("coverageLoss") or {}

        original_assignment_id = data.get("originalAssignmentId")
        is_reassignment = (
            original_assignment_id is not None
            and original_assignment_id != task.assignment_id
        )

        return cls(
            trace_id=task.trace_id,
            assignment_id=task.assignment_id,
            status_subtype=task.status_subtype,
            original_timestamp=task.original_timestamp,
            produced_at=produced_at,
            description=data.get("description"),
            xn_address=data.get("xnAddress"),
            original_assignment_id=original_assignment_id,
            is_reassignment=is_reassignment,
            event_date_time=data.get("dateTime"),
            contact_name=contact.get("name"),
            contact_type=contact.get("type"),
            contact_email=email.get("address"),
            contact_phone=phone.get("number"),
            claim_number=coverage_loss.get("claimNumber"),
        )
