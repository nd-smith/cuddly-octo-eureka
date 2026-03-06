"""Checkpoint routes."""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.config import (
    extract_fqdn,
    get_blob_connection_string,
    get_checkpoint_container_name,
    get_ssl_kwargs,
)
from pipeline.tools.eventhub_ui.routes import _helpers
from pipeline.tools.eventhub_ui.routes._helpers import (
    conn_str_for_hub,
    error_response,
    find_hub,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/checkpoints/{eventhub_name}/{consumer_group}", response_class=HTMLResponse
)
async def checkpoints_view(request: Request, eventhub_name: str, consumer_group: str):
    """View checkpoints for a consumer group."""
    from pipeline.tools.eventhub_ui.checkpoints import list_checkpoints

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        entries = list_checkpoints(
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            fqdn=fqdn,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
        )
    except Exception as e:
        logger.exception(
            f"Failed to list checkpoints for {eventhub_name}/{consumer_group}"
        )
        return error_response(request, f"Error listing checkpoints: {e}")

    return _helpers.templates.TemplateResponse(
        "checkpoints.html",
        {
            "request": request,
            "hub": hub,
            "consumer_group": consumer_group,
            "entries": entries,
        },
    )


@router.post(
    "/checkpoints/{eventhub_name}/{consumer_group}/advance", response_class=HTMLResponse
)
async def checkpoints_advance(
    request: Request, eventhub_name: str, consumer_group: str
):
    """Advance all checkpoints to latest."""
    from pipeline.tools.eventhub_ui.checkpoints import advance_checkpoints_to_latest

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

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
    except Exception as e:
        logger.exception(
            f"Failed to advance checkpoints for {eventhub_name}/{consumer_group}"
        )
        return error_response(request, f"Error advancing checkpoints: {e}")

    return _helpers.templates.TemplateResponse(
        "checkpoint_result.html",
        {
            "request": request,
            "hub": hub,
            "consumer_group": consumer_group,
            "action": "advanced to latest",
            "changes": changes,
        },
    )


@router.post(
    "/checkpoints/{eventhub_name}/{consumer_group}/reset-to-time",
    response_class=HTMLResponse,
)
async def checkpoints_reset_to_time(
    request: Request,
    eventhub_name: str,
    consumer_group: str,
    target_datetime: str = Form(...),
):
    """Reset checkpoints to a specific UTC datetime."""
    from pipeline.tools.eventhub_ui.checkpoints import (
        CheckpointConfig,
        reset_checkpoints_to_time,
    )

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        target_time = datetime.fromisoformat(target_datetime).replace(tzinfo=UTC)
    except ValueError:
        return error_response(
            request, f"Invalid datetime format: {target_datetime}", 400
        )

    try:
        conn_str = conn_str_for_hub(hub)
        cfg = CheckpointConfig(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            fqdn=extract_fqdn(conn_str),
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            ssl_kwargs=get_ssl_kwargs(),
        )
        changes = await reset_checkpoints_to_time(cfg, target_time)
    except Exception as e:
        logger.exception(
            f"Failed to reset checkpoints for {eventhub_name}/{consumer_group}"
        )
        return error_response(request, f"Error resetting checkpoints: {e}")

    return _helpers.templates.TemplateResponse(
        "checkpoint_result.html",
        {
            "request": request,
            "hub": hub,
            "consumer_group": consumer_group,
            "action": f"reset to {target_time.isoformat()}",
            "changes": changes,
        },
    )
