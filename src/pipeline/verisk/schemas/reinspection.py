"""
Reinspection message schema for the verisk.reinspections topic.

Produced by the download worker when a REINSPECTION_FORM.XML is
successfully parsed from an estimatePackageReceived event.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class ReinspectionMessage(BaseModel):
    """Structured reinspection event produced to verisk.reinspections.

    Carries identity fields from the pipeline alongside all fields parsed
    from REINSPECTION_FORM.XML. The full raw XML is stored in
    ``reinspection_form_data`` for downstream consumers that need it.
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
    claim_number: str | None = Field(default=None, description="assignmentId from XACTNET_INFO")
    assignment_type: str | None = Field(default=None, description="assignmentType from XACTNET_INFO")
    recipients_xn_address: str | None = Field(default=None)
    recipients_xm8_user_id: str | None = Field(default=None)
    recipients_name: str | None = Field(default=None)
    dataset: str | None = Field(default=None, description="carrierId from XACTNET_INFO")
    job_type: str | None = Field(default=None, description="rotationTrade from XACTNET_INFO")
    office_description_1: str | None = Field(default=None)
    carrier_org_level_1: str | None = Field(default=None)
    carrier_org_level_2: str | None = Field(default=None)
    carrier_org_level_3: str | None = Field(default=None)
    carrier_org_level_4: str | None = Field(default=None)
    carrier_org_level_5: str | None = Field(default=None)
    carrier_org_level_6: str | None = Field(default=None)

    # --- ADM / COVERAGE_LOSS -------------------------------------------------
    cat_code: str | None = Field(default=None, description="catastrophe code from COVERAGE_LOSS")
    type_of_loss: str | None = Field(default=None, description="TOL code from COVERAGE_LOSS")

    # --- REINSPECTION_INFO ---------------------------------------------------
    estimate_version: int | None = Field(default=None, description="reinspectedEstimateCount")

    # --- FILTERED_TOTALS -----------------------------------------------------
    filtered_overwrite_count: int | None = Field(default=None)
    filtered_underwrite_count: int | None = Field(default=None)
    filtered_overhead_acv: float | None = Field(default=None)
    filtered_overhead_dep: float | None = Field(default=None)
    filtered_overhead_rcv: float | None = Field(default=None)
    filtered_overhead_reason: str | None = Field(default=None)
    filtered_profit_acv: float | None = Field(default=None)
    filtered_profit_dep: float | None = Field(default=None)
    filtered_profit_rcv: float | None = Field(default=None)
    filtered_profit_reason: str | None = Field(default=None)
    filtered_over_acv: float | None = Field(default=None)
    filtered_over_dep: float | None = Field(default=None)
    filtered_over_rcv: float | None = Field(default=None)
    filtered_under_acv: float | None = Field(default=None)
    filtered_under_dep: float | None = Field(default=None)
    filtered_under_rcv: float | None = Field(default=None)
    filtered_total_acv: float | None = Field(default=None)
    filtered_total_dep: float | None = Field(default=None)
    filtered_total_rcv: float | None = Field(default=None)
    filtered_orig_acv: float | None = Field(default=None)
    filtered_orig_dep: float | None = Field(default=None)
    filtered_orig_rcv: float | None = Field(default=None)
    filtered_error_percent_acv: float | None = Field(default=None)
    filtered_error_percent_dep: float | None = Field(default=None)
    filtered_error_percent_rcv: float | None = Field(default=None)

    # --- Raw payload ---------------------------------------------------------
    reinspection_form_data: str | None = Field(
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
    ) -> "ReinspectionMessage":
        """Construct from a DownloadTaskMessage + handler parsed_data dict."""
        return cls(
            trace_id=task_message.trace_id,
            media_id=task_message.media_id,
            assignment_id=task_message.assignment_id,
            blob_url=parsed_data.get("blob_url", task_message.blob_path),
            original_timestamp=task_message.original_timestamp,
            produced_at=produced_at,
            transaction_id=parsed_data.get("transaction_id"),
            claim_number=parsed_data.get("claim_number"),
            assignment_type=parsed_data.get("assignment_type"),
            recipients_xn_address=parsed_data.get("recipients_xn_address"),
            recipients_xm8_user_id=parsed_data.get("recipients_xm8_user_id"),
            recipients_name=parsed_data.get("recipients_name"),
            dataset=parsed_data.get("dataset"),
            job_type=parsed_data.get("job_type"),
            office_description_1=parsed_data.get("office_description_1"),
            carrier_org_level_1=parsed_data.get("carrier_org_level_1"),
            carrier_org_level_2=parsed_data.get("carrier_org_level_2"),
            carrier_org_level_3=parsed_data.get("carrier_org_level_3"),
            carrier_org_level_4=parsed_data.get("carrier_org_level_4"),
            carrier_org_level_5=parsed_data.get("carrier_org_level_5"),
            carrier_org_level_6=parsed_data.get("carrier_org_level_6"),
            cat_code=parsed_data.get("cat_code"),
            type_of_loss=parsed_data.get("type_of_loss"),
            estimate_version=parsed_data.get("estimate_version"),
            filtered_overwrite_count=parsed_data.get("filtered_overwrite_count"),
            filtered_underwrite_count=parsed_data.get("filtered_underwrite_count"),
            filtered_overhead_acv=parsed_data.get("filtered_overhead_acv"),
            filtered_overhead_dep=parsed_data.get("filtered_overhead_dep"),
            filtered_overhead_rcv=parsed_data.get("filtered_overhead_rcv"),
            filtered_overhead_reason=parsed_data.get("filtered_overhead_reason"),
            filtered_profit_acv=parsed_data.get("filtered_profit_acv"),
            filtered_profit_dep=parsed_data.get("filtered_profit_dep"),
            filtered_profit_rcv=parsed_data.get("filtered_profit_rcv"),
            filtered_profit_reason=parsed_data.get("filtered_profit_reason"),
            filtered_over_acv=parsed_data.get("filtered_over_acv"),
            filtered_over_dep=parsed_data.get("filtered_over_dep"),
            filtered_over_rcv=parsed_data.get("filtered_over_rcv"),
            filtered_under_acv=parsed_data.get("filtered_under_acv"),
            filtered_under_dep=parsed_data.get("filtered_under_dep"),
            filtered_under_rcv=parsed_data.get("filtered_under_rcv"),
            filtered_total_acv=parsed_data.get("filtered_total_acv"),
            filtered_total_dep=parsed_data.get("filtered_total_dep"),
            filtered_total_rcv=parsed_data.get("filtered_total_rcv"),
            filtered_orig_acv=parsed_data.get("filtered_orig_acv"),
            filtered_orig_dep=parsed_data.get("filtered_orig_dep"),
            filtered_orig_rcv=parsed_data.get("filtered_orig_rcv"),
            filtered_error_percent_acv=parsed_data.get("filtered_error_percent_acv"),
            filtered_error_percent_dep=parsed_data.get("filtered_error_percent_dep"),
            filtered_error_percent_rcv=parsed_data.get("filtered_error_percent_rcv"),
            reinspection_form_data=parsed_data.get("reinspection_form_data"),
        )
