"""Bulk checkpoint operations routes."""

import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.config import (
    extract_fqdn,
    get_blob_connection_string,
    get_checkpoint_container_name,
    get_ssl_kwargs,
    list_eventhubs,
)
from pipeline.tools.eventhub_ui.routes import _helpers
from pipeline.tools.eventhub_ui.routes._helpers import (
    conn_str_for_hub,
    find_hub,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/bulk-checkpoints", response_class=HTMLResponse)
async def bulk_checkpoints_form(request: Request):
    """Checkbox grid of all consumer groups across all EventHubs."""
    hubs = list_eventhubs()

    groups = []
    for hub in hubs:
        for worker_name, cg_name in hub.consumer_groups.items():
            groups.append(
                {
                    "hub": hub,
                    "worker_name": worker_name,
                    "cg_name": cg_name,
                    "key": f"{hub.eventhub_name}|{cg_name}",
                }
            )

    return _helpers.templates.TemplateResponse(
        "bulk_checkpoints.html",
        {
            "request": request,
            "groups": groups,
        },
    )


@router.post("/bulk-checkpoints/preview", response_class=HTMLResponse)
async def bulk_checkpoints_preview(request: Request):
    """Show affected groups."""
    form = await request.form()
    selected = form.getlist("selected")

    groups = []
    for key in selected:
        parts = key.split("|", 1)
        if len(parts) == 2:
            groups.append({"eventhub_name": parts[0], "consumer_group": parts[1]})

    return _helpers.templates.TemplateResponse(
        "bulk_checkpoints_result.html",
        {
            "request": request,
            "groups": groups,
            "preview": True,
            "results": [],
            "selected_keys": selected,
        },
    )


@router.post("/bulk-checkpoints/execute", response_class=HTMLResponse)
async def bulk_checkpoints_execute(request: Request):
    """Execute bulk advance to latest for selected groups."""
    from pipeline.tools.eventhub_ui.checkpoints import advance_checkpoints_to_latest

    form = await request.form()
    selected = form.getlist("selected")

    async def advance_one(eventhub_name: str, consumer_group: str) -> dict:
        hub = find_hub(eventhub_name)
        if not hub:
            return {
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "success": False,
                "error": "Hub not found",
            }
        try:
            conn_str = conn_str_for_hub(hub)
            fqdn = extract_fqdn(conn_str)
            changes = advance_checkpoints_to_latest(
                conn_str=conn_str,
                eventhub_name=eventhub_name,
                consumer_group=consumer_group,
                fqdn=fqdn,
                blob_conn_str=get_blob_connection_string(),
                container_name=get_checkpoint_container_name(),
                ssl_kwargs=get_ssl_kwargs(),
            )
            return {
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "success": True,
                "changes": changes,
            }
        except Exception as e:
            return {
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "success": False,
                "error": str(e),
            }

    tasks = []
    for key in selected:
        parts = key.split("|", 1)
        if len(parts) == 2:
            tasks.append(advance_one(parts[0], parts[1]))

    results = await asyncio.gather(*tasks)

    return _helpers.templates.TemplateResponse(
        "bulk_checkpoints_result.html",
        {
            "request": request,
            "groups": [],
            "preview": False,
            "results": list(results),
            "selected_keys": [],
        },
    )
