"""Overview and partition routes."""

import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.config import get_ssl_kwargs, list_eventhubs
from pipeline.tools.eventhub_ui.routes._helpers import (
    conn_str_for_hub,
    error_response,
    find_hub,
    templates,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    """List all configured EventHubs with basic info."""
    hubs = list_eventhubs()

    domains = {}
    for hub in hubs:
        domains.setdefault(hub.domain, []).append(hub)

    return templates.TemplateResponse(
        "overview.html",
        {
            "request": request,
            "domains": domains,
            "hub_count": len(hubs),
        },
    )


@router.get("/partitions/{eventhub_name}", response_class=HTMLResponse)
async def partitions(request: Request, eventhub_name: str):
    """Show partition details for an EventHub."""
    from pipeline.tools.eventhub_ui.partitions import (
        get_eventhub_properties,
        get_partition_properties,
    )

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        ssl_kwargs = get_ssl_kwargs()
        eh_props = await get_eventhub_properties(conn_str, eventhub_name, ssl_kwargs)
        partition_list = await get_partition_properties(
            conn_str, eventhub_name, ssl_kwargs
        )
    except Exception as e:
        logger.exception(f"Failed to fetch partitions for {eventhub_name}")
        return error_response(request, f"Error connecting to '{eventhub_name}': {e}")

    return templates.TemplateResponse(
        "partitions.html",
        {
            "request": request,
            "hub": hub,
            "eh_props": eh_props,
            "partitions": partition_list,
        },
    )


@router.get("/sampler/{eventhub_name}/{partition_id}", response_class=HTMLResponse)
async def sampler(
    request: Request,
    eventhub_name: str,
    partition_id: str,
    count: int = Query(default=5, ge=1, le=50),
):
    """Sample recent messages from a partition."""
    from pipeline.tools.eventhub_ui.sampler import sample_messages

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        messages = await sample_messages(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            partition_id=partition_id,
            count=count,
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to sample from {eventhub_name}/{partition_id}")
        return error_response(request, f"Error sampling messages: {e}")

    return templates.TemplateResponse(
        "sampler.html",
        {
            "request": request,
            "hub": hub,
            "partition_id": partition_id,
            "messages": messages,
            "count": count,
        },
    )
