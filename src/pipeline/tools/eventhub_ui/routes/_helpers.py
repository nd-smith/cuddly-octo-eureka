"""Shared helpers for route modules."""

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pipeline.tools.eventhub_ui.config import (
    EventHubInfo,
    get_namespace_connection_string,
    get_source_connection_string,
    list_eventhubs,
)

# Initialized by app.py after template setup
templates: Jinja2Templates = None  # type: ignore[assignment]


def conn_str_for_hub(hub: EventHubInfo) -> str:
    """Get the right connection string based on whether the hub is source or internal."""
    if hub.is_source:
        return get_source_connection_string()
    return get_namespace_connection_string()


def find_hub(eventhub_name: str) -> EventHubInfo | None:
    for hub in list_eventhubs():
        if hub.eventhub_name == eventhub_name:
            return hub
    return None


def error_response(
    request: Request, message: str, status_code: int = 500
) -> HTMLResponse:
    return templates.TemplateResponse(
        "error.html",
        {
            "request": request,
            "message": message,
        },
        status_code=status_code,
    )
