"""
Generic Rough Draft message schema for the verisk_grd topic.

Produced by the download worker when a GENERIC_ROUGHDRAFT.XML is
successfully parsed from an estimatePackageReceived event.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class GrdMessage(BaseModel):
    """Structured GRD event produced to verisk_grd.

    Carries pipeline identity fields alongside assignment_id and version
    parsed from the filename, plus the full raw XML payload.
    """

    # --- Pipeline identity ---------------------------------------------------
    trace_id: str = Field(..., description="Unique event identifier for correlation", min_length=1)
    media_id: str = Field(..., description="Unique deterministic ID for the attachment", min_length=1)
    assignment_id: str = Field(..., description="Assignment ID parsed from the filename", min_length=1)
    blob_url: str = Field(..., description="Relative blob storage path including filename", min_length=1)
    original_timestamp: datetime = Field(..., description="Timestamp from the original event")
    produced_at: datetime = Field(..., description="Timestamp when this message was produced")

    # --- Parsed from filename ------------------------------------------------
    version: str = Field(..., description="Estimate version parsed from the filename", min_length=1)

    # --- Raw payload ---------------------------------------------------------
    generic_roughdraft_data: str | None = Field(
        default=None,
        description="Full XML payload as a string for raw downstream consumption",
    )

    @field_validator("trace_id", "media_id", "assignment_id", "blob_url", "version")
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
    ) -> "GrdMessage":
        """Construct from a DownloadTaskMessage + handler parsed_data dict."""
        return cls(
            trace_id=task_message.trace_id,
            media_id=task_message.media_id,
            assignment_id=parsed_data.get("assignment_id", task_message.assignment_id),
            blob_url=parsed_data.get("blob_url", task_message.blob_path),
            original_timestamp=task_message.original_timestamp,
            produced_at=produced_at,
            version=parsed_data["version"],
            generic_roughdraft_data=parsed_data.get("generic_roughdraft_data"),
        )
