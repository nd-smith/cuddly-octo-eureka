"""
FNOL message schema for the verisk_fnol topic.

Produced by the download worker when a FNOL_XACTDOC.XML is
successfully parsed from a firstNoticeOfLossReceived event.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class FnolMessage(BaseModel):
    """Structured FNOL event produced to verisk_fnol.

    Carries pipeline identity fields alongside all fields parsed from
    FNOL_XACTDOC.XML. The full raw XML is stored in ``fnol_xactdoc_data``
    for downstream consumers that need it.
    """

    # --- Pipeline identity ---------------------------------------------------
    trace_id: str = Field(..., description="Unique event identifier for correlation", min_length=1)
    media_id: str = Field(..., description="Unique deterministic ID for the attachment", min_length=1)
    assignment_id: str = Field(..., description="Assignment ID from the original event", min_length=1)
    blob_url: str = Field(..., description="Relative blob storage path including filename", min_length=1)
    original_timestamp: datetime = Field(..., description="Timestamp from the original event")
    produced_at: datetime = Field(..., description="Timestamp when this message was produced")

    # --- XACTNET_INFO --------------------------------------------------------
    transaction_id: str | None = Field(default=None, description="transactionId from XACTNET_INFO")
    original_transaction_id: str | None = Field(default=None, description="originalTransactionId from XACTNET_INFO")
    is_reassignment: bool = Field(..., description="True when originalTransactionId is present")
    assignment_type: str | None = Field(default=None, description="assignmentType from XACTNET_INFO")
    business_unit: str | None = Field(default=None, description="businessUnit from XACTNET_INFO")
    dataset: str | None = Field(default=None, description="carrierId from XACTNET_INFO")
    job_type: str | None = Field(default=None, description="rotationTrade from XACTNET_INFO")
    senders_xn_address: str | None = Field(default=None, description="sendersXNAddress from XACTNET_INFO")
    recipients_xn_address: str | None = Field(default=None, description="recipientsXNAddress from XACTNET_INFO")

    # --- ADM -----------------------------------------------------------------
    date_of_loss: str | None = Field(default=None, description="dateOfLoss from ADM")
    date_received: str | None = Field(default=None, description="dateReceived from ADM")

    # --- ADM / COVERAGE_LOSS -------------------------------------------------
    claim_number: str | None = Field(default=None, description="claimNumber from COVERAGE_LOSS")
    date_init_cov: str | None = Field(default=None, description="dateInitCov from COVERAGE_LOSS")
    policy_number: str | None = Field(default=None, description="policyNumber from COVERAGE_LOSS")

    # --- COVERAGE_LOSS / TOL -------------------------------------------------
    type_of_loss: str | None = Field(default=None, description="TOL code from COVERAGE_LOSS")
    type_of_loss_desc: str | None = Field(default=None, description="TOL desc from COVERAGE_LOSS")

    # --- CONTACTS ------------------------------------------------------------
    insured_name: str | None = Field(default=None, description="Client contact name")
    insured_email: str | None = Field(default=None, description="Client contact email address")
    insured_street: str | None = Field(default=None, description="Property address street")
    insured_city: str | None = Field(default=None, description="Property address city")
    insured_state: str | None = Field(default=None, description="Property address state")
    insured_postal: str | None = Field(default=None, description="Property address postal code")

    # --- Raw payload ---------------------------------------------------------
    fnol_xactdoc_data: str | None = Field(
        default=None,
        description="Full XML payload as a string for raw downstream consumption",
    )

    @field_validator("trace_id", "media_id", "assignment_id", "blob_url")
    @classmethod
    def validate_non_empty_strings(cls, v: str, _info) -> str:
        return v.strip() if isinstance(v, str) else v

    @field_serializer("original_timestamp", "produced_at")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        return timestamp.isoformat()

    @classmethod
    def from_handler_data(
        cls,
        task_message: Any,
        parsed_data: dict[str, Any],
        produced_at: datetime,
    ) -> "FnolMessage":
        """Construct from a DownloadTaskMessage + handler parsed_data dict."""
        transaction_id = parsed_data.get("transaction_id")
        original_transaction_id = parsed_data.get("original_transaction_id")
        is_reassignment = (
            original_transaction_id is not None
            and original_transaction_id != transaction_id
        )
        return cls(
            trace_id=task_message.trace_id,
            media_id=task_message.media_id,
            assignment_id=task_message.assignment_id,
            blob_url=parsed_data.get("blob_url", task_message.blob_path),
            original_timestamp=task_message.original_timestamp,
            produced_at=produced_at,
            transaction_id=transaction_id,
            original_transaction_id=original_transaction_id,
            is_reassignment=is_reassignment,
            assignment_type=parsed_data.get("assignment_type"),
            business_unit=parsed_data.get("business_unit"),
            dataset=parsed_data.get("dataset"),
            job_type=parsed_data.get("job_type"),
            senders_xn_address=parsed_data.get("senders_xn_address"),
            recipients_xn_address=parsed_data.get("recipients_xn_address"),
            date_of_loss=parsed_data.get("date_of_loss"),
            date_received=parsed_data.get("date_received"),
            claim_number=parsed_data.get("claim_number"),
            date_init_cov=parsed_data.get("date_init_cov"),
            policy_number=parsed_data.get("policy_number"),
            type_of_loss=parsed_data.get("type_of_loss"),
            type_of_loss_desc=parsed_data.get("type_of_loss_desc"),
            insured_name=parsed_data.get("insured_name"),
            insured_email=parsed_data.get("insured_email"),
            insured_street=parsed_data.get("insured_street"),
            insured_city=parsed_data.get("insured_city"),
            insured_state=parsed_data.get("insured_state"),
            insured_postal=parsed_data.get("insured_postal"),
            fnol_xactdoc_data=parsed_data.get("fnol_xactdoc_data"),
        )
