"""Payload search routes."""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Form, Request
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


@router.get("/search", response_class=HTMLResponse)
async def search_form(request: Request):
    """Search form."""
    return _helpers.templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "hubs": list_eventhubs(),
        },
    )


@router.post("/search/execute", response_class=HTMLResponse)
async def search_execute(
    request: Request,
    eventhub_name: str = Form(...),
    partition: str = Form(default="all"),
    start_time: str = Form(...),
    end_time: str = Form(...),
    query: str = Form(default=""),
):
    """HTMX partial with search results."""
    from pipeline.tools.eventhub_ui.partitions import get_partition_properties
    from pipeline.tools.eventhub_ui.search import search_eventhub

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(request, f"EventHub '{eventhub_name}' not found", 404)

    try:
        start_dt = datetime.fromisoformat(start_time).replace(tzinfo=UTC)
        end_dt = datetime.fromisoformat(end_time).replace(tzinfo=UTC)
    except ValueError as e:
        return error_response(request, f"Invalid datetime: {e}", 400)

    try:
        conn_str = conn_str_for_hub(hub)
        ssl_kwargs = get_ssl_kwargs()

        if partition == "all":
            props = await get_partition_properties(conn_str, eventhub_name, ssl_kwargs)
            partition_ids = [p.partition_id for p in props if not p.is_empty]
        else:
            partition_ids = [partition]

        results, errors = await search_eventhub(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            partition_ids=partition_ids,
            start_time=start_dt,
            end_time=end_dt,
            query=query,
            ssl_kwargs=ssl_kwargs,
        )
    except Exception as e:
        logger.exception(f"Search failed for {eventhub_name}")
        return error_response(request, f"Search error: {e}")

    return _helpers.templates.TemplateResponse(
        "search_results.html",
        {
            "request": request,
            "results": results,
            "errors": errors,
            "query": query,
        },
    )
