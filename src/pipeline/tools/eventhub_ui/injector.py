"""Ad-hoc EventHub message publishing."""

import json
import logging
from dataclasses import dataclass

from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubProducerClient

logger = logging.getLogger(__name__)


@dataclass
class InjectionResult:
    success: bool
    partition_id: str | None = None
    error: str | None = None


async def send_message(
    conn_str: str,
    eventhub_name: str,
    body: str,
    partition_key: str | None = None,
    ssl_kwargs: dict | None = None,
) -> InjectionResult:
    """Send a single message to an EventHub."""
    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **(ssl_kwargs or {}),
    )

    async with producer:
        event_data = EventData(body)
        batch = await producer.create_batch(partition_key=partition_key)
        batch.add(event_data)
        await producer.send_batch(batch)

        return InjectionResult(
            success=True,
            partition_id=partition_key or "(auto)",
        )


def validate_json(body: str) -> tuple[bool, str, int]:
    """Validate JSON body. Returns (is_valid, formatted_json_or_error, byte_size)."""
    try:
        parsed = json.loads(body)
        formatted = json.dumps(parsed, indent=2, default=str)
        byte_size = len(body.encode("utf-8"))
        return True, formatted, byte_size
    except (json.JSONDecodeError, TypeError) as e:
        return False, str(e), 0
