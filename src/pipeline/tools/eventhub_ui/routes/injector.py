"""Message injection routes."""

import logging

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


@router.get("/inject", response_class=HTMLResponse)
async def inject_form(request: Request):
    """Form for injecting messages."""
    return _helpers.templates.TemplateResponse(
        "injector.html",
        {
            "request": request,
            "hubs": list_eventhubs(),
        },
    )


@router.post("/inject/preview", response_class=HTMLResponse)
async def inject_preview(
    request: Request,
    eventhub_name: str = Form(...),
    body: str = Form(...),
    partition_key: str = Form(default=""),
):
    """HTMX partial: validate JSON and show preview."""
    from pipeline.tools.eventhub_ui.injector import validate_json

    is_valid, formatted_or_error, byte_size = validate_json(body)

    return _helpers.templates.TemplateResponse(
        "injector_preview.html",
        {
            "request": request,
            "is_valid": is_valid,
            "formatted": formatted_or_error,
            "byte_size": byte_size,
            "eventhub_name": eventhub_name,
            "body": body,
            "partition_key": partition_key,
        },
    )


@router.post("/inject/send", response_class=HTMLResponse)
async def inject_send(
    request: Request,
    eventhub_name: str = Form(...),
    body: str = Form(...),
    partition_key: str = Form(default=""),
):
    """Send the message to EventHub."""
    from pipeline.tools.eventhub_ui.injector import send_message

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        result = await send_message(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            body=body,
            partition_key=partition_key or None,
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to inject message to {eventhub_name}")
        result_data = {"success": False, "error": str(e)}
        return _helpers.templates.TemplateResponse(
            "injector_result.html",
            {
                "request": request,
                "result": result_data,
                "eventhub_name": eventhub_name,
            },
        )

    return _helpers.templates.TemplateResponse(
        "injector_result.html",
        {
            "request": request,
            "result": {
                "success": result.success,
                "partition_id": result.partition_id,
                "error": result.error,
            },
            "eventhub_name": eventhub_name,
        },
    )
