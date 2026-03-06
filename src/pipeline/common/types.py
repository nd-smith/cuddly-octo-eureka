"""Message types for the pipeline transport layer."""

from dataclasses import dataclass, field

__all__ = [
    "PipelineMessage",
    "ProduceResult",
    "PartitionInfo",
    "BatchResult",
]


@dataclass(frozen=True)
class PipelineMessage:
    """Transport-agnostic message received from Kafka or Event Hub."""

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None = None
    value: bytes | None = None
    headers: list[tuple[str, bytes]] | None = None


@dataclass(frozen=True)
class ProduceResult:
    """Transport-agnostic confirmation of a published message."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class PartitionInfo:
    """Transport-agnostic partition identifier for Kafka or Event Hub."""

    topic: str
    partition: int


@dataclass
class BatchResult:
    """Structured result from a batch handler with per-message failure reporting."""

    commit: bool
    permanent_failures: list[tuple[PipelineMessage, Exception]] = field(default_factory=list)
