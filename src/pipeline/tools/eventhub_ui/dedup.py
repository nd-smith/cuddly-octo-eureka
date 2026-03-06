"""Dedup store management helpers for the UI."""

import logging

from pipeline.tools.eventhub_ui.config import load_eventhub_config

logger = logging.getLogger(__name__)


def get_dedup_config() -> dict:
    """Get dedup store config from eventhub config."""
    config = load_eventhub_config()
    return config.get("dedup_store", {})


def get_dedup_workers() -> list[str]:
    """Get list of worker names that use dedup."""
    return [
        "verisk-event-ingester",
        "claimx-event-ingester",
    ]


async def create_dedup_store():
    """Create an ad-hoc BlobDedupStore instance from config."""
    from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

    config = get_dedup_config()
    conn_str = config.get("blob_storage_connection_string", "")
    container = config.get("container_name", "eventhub-dedup-cache")

    if not conn_str:
        raise ValueError("No dedup store blob connection string configured")

    store = BlobDedupStore(connection_string=conn_str, container_name=container)
    await store.initialize()
    return store


def get_dedup_ttl() -> int:
    """Get TTL from config."""
    config = get_dedup_config()
    return int(config.get("ttl_seconds", 86400))
