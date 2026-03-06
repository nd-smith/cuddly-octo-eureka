from typing import Any


def message_log_fields(message: Any) -> dict[str, Any]:
    extra: dict[str, Any] = {
        "message_topic": getattr(message, "topic", None),
        "message_partition": getattr(message, "partition", None),
        "message_offset": getattr(message, "offset", None),
    }

    key = getattr(message, "key", None)
    if key:
        extra["message_key"] = key.decode("utf-8") if isinstance(key, bytes) else str(key)

    return {field: value for field, value in extra.items() if value is not None}


def producer_log_fields(output_topic: str | None = None, metadata: Any | None = None) -> dict[str, Any]:
    extra: dict[str, Any] = {}

    if output_topic:
        extra["output_topic"] = output_topic

    if metadata is not None:
        partition = getattr(metadata, "partition", None)
        offset = getattr(metadata, "offset", None)
        if partition is not None:
            extra["producer_partition"] = partition
        if offset is not None:
            extra["producer_offset"] = offset

    return extra