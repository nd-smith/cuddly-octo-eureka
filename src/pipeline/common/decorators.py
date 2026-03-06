"""Common decorators for Kafka pipeline workers."""

from collections.abc import Callable
from functools import wraps
from typing import Any

from core.logging import MessageLogContext
from pipeline.common.types import PipelineMessage


def set_log_context_from_message(func: Callable) -> Callable:
    """Decorator to automatically set log context from message.

    Extracts topic, partition, and offset from PipelineMessage
    and sets them in the logging context before calling the handler.
    """

    @wraps(func)
    async def wrapper(self: Any, message: PipelineMessage, *args: Any, **kwargs: Any) -> Any:
        with MessageLogContext(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        ):
            return await func(self, message, *args, **kwargs)

    return wrapper
