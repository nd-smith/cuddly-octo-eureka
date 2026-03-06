"""
ClaimX entity row schemas for Kafka pipeline.

Contains Pydantic models for ClaimX entity data extracted from API responses.
Schema aligned with verisk_pipeline EntityRows for compatibility.

Entity Types:
    - projects: Project metadata
    - contacts: Contact/policyholder information
    - media: Attachment metadata
    - tasks: Task information
    - task_templates: Task template definitions
    - external_links: External resource links
    - video_collab: Video collaboration sessions
"""

from typing import Any

from pydantic import BaseModel, Field


class EntityRowsMessage(BaseModel):
    """Entity rows extracted from ClaimX API for Delta table writes.

    Container for entity data grouped by type. Each entity type maps to a
    Delta table in OneLake. Used by enrichment workers to batch entity rows
    before writing to Delta Lake.
    """

    # TODO: Consider making trace_id required once all producers populate it
    trace_id: str = Field(
        default="",
        description="Correlation ID from source event. Empty default for backward compatibility.",
    )
    event_type: str | None = None
    project_id: str | None = None
    projects: list[dict[str, Any]] = Field(default_factory=list)
    contacts: list[dict[str, Any]] = Field(default_factory=list)
    media: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[dict[str, Any]] = Field(default_factory=list)
    task_templates: list[dict[str, Any]] = Field(default_factory=list)
    external_links: list[dict[str, Any]] = Field(default_factory=list)
    video_collab: list[dict[str, Any]] = Field(default_factory=list)

    def is_empty(self) -> bool:
        """Return True if all entity lists are empty."""
        return not any(
            [
                self.projects,
                self.contacts,
                self.media,
                self.tasks,
                self.task_templates,
                self.external_links,
                self.video_collab,
            ]
        )

    def merge(self, other: "EntityRowsMessage") -> "EntityRowsMessage":
        """Merge another EntityRowsMessage into this one. Returns self for chaining."""
        self.projects.extend(other.projects)
        self.contacts.extend(other.contacts)
        self.media.extend(other.media)
        self.tasks.extend(other.tasks)
        self.task_templates.extend(other.task_templates)
        self.external_links.extend(other.external_links)
        self.video_collab.extend(other.video_collab)
        return self

    def row_count(self) -> int:
        """Return total number of rows across all entity types."""
        return (
            len(self.projects)
            + len(self.contacts)
            + len(self.media)
            + len(self.tasks)
            + len(self.task_templates)
            + len(self.external_links)
            + len(self.video_collab)
        )
