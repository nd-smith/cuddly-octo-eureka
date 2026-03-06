"""Enhanced message viewer routes."""

import asyncio
import logging
from datetime import UTC

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.config import get_ssl_kwargs, list_eventhubs
from pipeline.tools.eventhub_ui.routes import _helpers
from pipeline.tools.eventhub_ui.routes._helpers import (
    conn_str_for_hub,
    error_response,
    find_hub,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/messages/{eventhub_name}", response_class=HTMLResponse)
async def message_viewer(request: Request, eventhub_name: str):
    """Full page message viewer with controls."""
    from pipeline.tools.eventhub_ui.partitions import get_partition_properties

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        partition_list = await get_partition_properties(
            conn_str, eventhub_name, get_ssl_kwargs()
        )
    except Exception as e:
        logger.exception(f"Failed to get partitions for {eventhub_name}")
        return error_response(request, f"Error connecting to '{eventhub_name}': {e}")

    partition_ids = [p.partition_id for p in partition_list]

    return _helpers.templates.TemplateResponse(
        "message_viewer.html",
        {
            "request": request,
            "hub": hub,
            "partition_ids": partition_ids,
            "hubs": list_eventhubs(),
        },
    )


@router.get("/messages/{eventhub_name}/fetch", response_class=HTMLResponse)
async def message_viewer_fetch(
    request: Request,
    eventhub_name: str,
    partition: str = Query(default="all"),
    count: int = Query(default=10, ge=1, le=100),
    start_time: str = Query(default=""),
):
    """HTMX partial returning sampled messages."""
    from datetime import datetime

    from pipeline.tools.eventhub_ui.partitions import get_partition_properties
    from pipeline.tools.eventhub_ui.sampler import sample_messages

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    starting_time = None
    if start_time:
        try:
            starting_time = datetime.fromisoformat(start_time).replace(tzinfo=UTC)
        except ValueError:
            return error_response(request, f"Invalid datetime: {start_time}", 400)

    try:
        conn_str = conn_str_for_hub(hub)
        ssl_kwargs = get_ssl_kwargs()

        if partition == "all":
            props = await get_partition_properties(conn_str, eventhub_name, ssl_kwargs)
            partition_ids = [p.partition_id for p in props if not p.is_empty]
        else:
            partition_ids = [partition]

        tasks = [
            sample_messages(
                conn_str=conn_str,
                eventhub_name=eventhub_name,
                partition_id=pid,
                count=count,
                starting_time=starting_time,
                ssl_kwargs=ssl_kwargs,
            )
            for pid in partition_ids
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        messages = []
        errors = []
        for pid, result in zip(partition_ids, results, strict=False):
            if isinstance(result, Exception):
                errors.append(f"Partition {pid}: {result}")
            else:
                for msg in result:
                    msg.partition_id = pid  # type: ignore[attr-defined]
                messages.extend(result)

        messages.sort(key=lambda m: m.enqueued_time, reverse=True)

    except Exception as e:
        logger.exception(f"Failed to fetch messages from {eventhub_name}")
        return error_response(request, f"Error fetching messages: {e}")

    return _helpers.templates.TemplateResponse(
        "message_viewer_results.html",
        {
            "request": request,
            "messages": messages,
            "errors": errors,
            "partition": partition,
        },
    )
