"""Pipeline status routes."""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.routes import _helpers

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/status", response_class=HTMLResponse)
async def pipeline_status(request: Request):
    """Full page pipeline status with HTMX polling."""
    return _helpers.templates.TemplateResponse(
        "pipeline_status.html",
        {
            "request": request,
        },
    )


@router.get("/status/data", response_class=HTMLResponse)
async def pipeline_status_data(request: Request):
    """HTMX partial: worker status grid."""
    from pipeline.tools.eventhub_ui.pipeline_status import get_all_worker_statuses

    try:
        grouped = await get_all_worker_statuses()
    except Exception:
        logger.exception("Failed to probe worker statuses")
        grouped = {}

    return _helpers.templates.TemplateResponse(
        "pipeline_status_partial.html",
        {
            "request": request,
            "grouped": grouped,
        },
    )
