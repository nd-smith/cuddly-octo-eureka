"""
ClaimX video collaboration event schema for downstream EventHub consumers.

Combines enriched video collaboration data into a single message
for real-time downstream consumption.
"""

from pydantic import BaseModel, Field


class ClaimXVideoCollabEvent(BaseModel):
    """Enriched video collaboration event produced from the video collab API response.

    Produced by the VideoCollabHandler after enriching a
    VIDEO_COLLABORATION_INVITE_SENT or VIDEO_COLLABORATION_COMPLETED event.

    All fields are sourced from a single API call:
    - Video collaboration report: get_video_collaboration(projectId)
    """

    # Event metadata
    trace_id: str = Field(..., description="Correlation ID from source event")
    event_type: str = Field(
        ...,
        description="VIDEO_COLLABORATION_INVITE_SENT or VIDEO_COLLABORATION_COMPLETED",
    )

    # Identity fields
    project_id: str | None = Field(default=None, description="ClaimX project ID (from claimId)")
    mfn: str | None = Field(default=None, description="Master file name")
    ntid: str | None = Field(
        default=None,
        description="Network ID derived from emailUsername (characters to the left of @)",
    )
    claim_number: str | None = Field(default=None, description="Claim number")
    policy_number: str | None = Field(default=None, description="Policy number")

    # Session metrics
    number_of_videos: int | None = Field(default=None, description="Number of videos captured")
    number_of_photos: int | None = Field(default=None, description="Number of photos captured")
    total_time_seconds: str | None = Field(
        default=None, description="Total session time in seconds (decimal string)"
    )
    session_count: int | None = Field(default=None, description="Number of sessions")
    total_time: str | None = Field(default=None, description="Total time formatted string")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "trace_id": "abc123",
                    "event_type": "VIDEO_COLLABORATION_COMPLETED",
                    "project_id": "5996293",
                    "mfn": "06QYL5W",
                    "ntid": "jsmith",
                    "claim_number": "0820045896",
                    "policy_number": "POL-123456",
                    "number_of_videos": 3,
                    "number_of_photos": 12,
                    "total_time_seconds": "1845.00",
                    "session_count": 2,
                    "total_time": "0:30:45",
                }
            ]
        }
    }
