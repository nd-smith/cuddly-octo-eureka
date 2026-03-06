"""
ClaimX task event schema for downstream EventHub consumers.

Combines enriched project, task, and report data into a single
message for real-time downstream consumption.
"""

import json
from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class ClaimXTaskEvent(BaseModel):
    """Enriched task event combining project, task, and report data.

    Produced by the TaskHandler after enriching a CUSTOM_TASK_ASSIGNED
    or CUSTOM_TASK_COMPLETED event via the ClaimX API.

    Fields are sourced from three API calls:
    - Project export: /export/project/{projectId}
    - Task export: get_custom_task(assignmentId)
    - Media export (conditional): /export/project/{projectId}/media?mediaId={reportId}
    """

    # Event metadata
    trace_id: str = Field(..., description="Correlation ID from source event")
    event_type: str = Field(..., description="CUSTOM_TASK_ASSIGNED or CUSTOM_TASK_COMPLETED")

    # Project fields (from project export)
    project_id: str = Field(..., description="ClaimX project ID")
    claim_number: str | None = Field(default=None, description="Project number / claim number")
    project_status: str | None = Field(default=None, description="Project status")
    customer_firstname: str | None = Field(default=None, description="Customer first name")
    customer_lastname: str | None = Field(default=None, description="Customer last name")
    loss_address_street: str | None = Field(default=None, description="Loss address street")
    loss_address_city: str | None = Field(default=None, description="Loss address city")
    loss_address_state_province: str | None = Field(
        default=None, description="Loss address state/province"
    )
    loss_address_zip_postcode: str | None = Field(
        default=None, description="Loss address zip/postcode"
    )
    mfn: str | None = Field(
        default=None,
        description="Master file name (from project masterFileName or first non-null teamMember mfn)",
    )

    # Task fields (from task export)
    task_assignment_id: int | None = Field(default=None, description="Task assignment ID")
    task_id: int | None = Field(default=None, description="Task template ID")
    form_id: str | None = Field(default=None, description="Form ID")
    task_name: str | None = Field(default=None, description="Task name")
    task_status: str | None = Field(default=None, description="Task status")
    date_task_assigned: str | None = Field(default=None, description="Date task was assigned (ISO)")
    date_task_completed: str | None = Field(
        default=None, description="Date task was completed (ISO)"
    )
    cancelled_date: str | None = Field(default=None, description="Date task was cancelled (ISO)")

    # Form response (from task export with ?full=true)
    form_response: str | None = Field(
        default=None,
        description="JSON-stringified form response containing groups and questionAndAnswers (dynamic column in KQL)",
    )

    @field_validator("form_response", mode="before")
    @classmethod
    def stringify_form_response(cls, v):
        """Ensure form_response is always a JSON string, not a raw dict/list."""
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return v

    # Report fields (from media export, conditional on pdfProjectMediaId)
    report_id: int | None = Field(default=None, description="PDF project media ID")
    report_url: str | None = Field(default=None, description="Full download link for the report")
    report_name: str | None = Field(default=None, description="Report media description")
    media_id: str | None = Field(default=None, description="Media ID from media export response")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "trace_id": "abc123",
                    "event_type": "CUSTOM_TASK_COMPLETED",
                    "project_id": "5996293",
                    "claim_number": "0820045896",
                    "project_status": "IN_PROGRESS",
                    "customer_firstname": "ROBERT",
                    "customer_lastname": "GUILLIAMS",
                    "loss_address_street": "16891 SE 76TH CHATHAM AVE",
                    "loss_address_city": "THE VILLAGES",
                    "loss_address_state_province": "FL",
                    "loss_address_zip_postcode": "32162",
                    "mfn": "06QYL5W",
                    "task_assignment_id": 5550810,
                    "task_id": 7085,
                    "form_id": "5e4eb0a494511a44fd3851b9",
                    "task_name": "File Request - Property",
                    "task_status": "COMPLETED",
                    "date_task_assigned": "2026-02-25T17:34:39.559+00:00",
                    "date_task_completed": "2026-02-25T18:13:01.623+00:00",
                    "cancelled_date": None,
                    "report_id": 98055035,
                    "report_url": "https://s3.amazonaws.com/...",
                    "report_name": "File Request - Property Report",
                    "media_id": "98055035",
                },
            ]
        }
    }
