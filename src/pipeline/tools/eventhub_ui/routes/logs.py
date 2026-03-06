"""Log viewer routes."""

import asyncio
import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.routes import _helpers
from pipeline.tools.eventhub_ui.routes._helpers import error_response

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/logs", response_class=HTMLResponse)
async def logs_browse(request: Request):
    """Browse log domains and dates."""
    from pipeline.tools.eventhub_ui.logs import list_log_domains

    try:
        domains = await asyncio.to_thread(list_log_domains)
    except Exception as e:
        return error_response(request, f"Error connecting to OneLake: {e}")

    return _helpers.templates.TemplateResponse(
        "logs_browse.html",
        {
            "request": request,
            "domains": domains,
        },
    )


@router.get("/logs/view", response_class=HTMLResponse)
async def logs_view(
    request: Request,
    path: str = Query(...),
    level: str = Query(default=""),
    search: str = Query(default=""),
    trace_id: str = Query(default=""),
    tail: int = Query(default=500, ge=1, le=5000),
):
    """View and filter a log file."""
    from pipeline.tools.eventhub_ui.logs import read_log_file

    try:
        entries = await asyncio.to_thread(
            read_log_file,
            path=path,
            level=level or None,
            search=search or None,
            trace_id=trace_id or None,
            tail=tail,
        )
    except Exception as e:
        return error_response(request, f"Error reading log file: {e}")

    filename = path.split("/")[-1]

    return _helpers.templates.TemplateResponse(
        "logs_view.html",
        {
            "request": request,
            "path": path,
            "filename": filename,
            "entries": entries,
            "level": level,
            "search": search,
            "trace_id": trace_id,
            "tail": tail,
        },
    )


@router.get("/logs/{domain}", response_class=HTMLResponse)
async def logs_dates(request: Request, domain: str):
    """List available dates for a domain."""
    from pipeline.tools.eventhub_ui.logs import list_log_dates

    try:
        dates = await asyncio.to_thread(list_log_dates, domain)
    except Exception as e:
        return error_response(request, f"Error listing dates for '{domain}': {e}")

    return _helpers.templates.TemplateResponse(
        "logs_dates.html",
        {
            "request": request,
            "domain": domain,
            "dates": dates,
        },
    )


@router.get("/logs/{domain}/{date}", response_class=HTMLResponse)
async def logs_files(request: Request, domain: str, date: str):
    """List log files for a domain/date."""
    from pipeline.tools.eventhub_ui.logs import list_log_files

    try:
        files = await asyncio.to_thread(list_log_files, domain, date)
    except Exception as e:
        return error_response(
            request, f"Error listing files for '{domain}/{date}': {e}"
        )

    return _helpers.templates.TemplateResponse(
        "logs_files.html",
        {
            "request": request,
            "domain": domain,
            "date": date,
            "files": files,
        },
    )
