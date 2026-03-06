"""Dedup store management routes."""

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.routes._helpers import error_response, templates

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/dedup", response_class=HTMLResponse)
async def dedup_overview(request: Request):
    """Dedup store management page."""
    from pipeline.tools.eventhub_ui.dedup import get_dedup_config, get_dedup_workers

    config = get_dedup_config()
    workers = get_dedup_workers()

    return templates.TemplateResponse(
        "dedup.html",
        {
            "request": request,
            "workers": workers,
            "config": config,
        },
    )


@router.get("/dedup/{worker_name}/keys", response_class=HTMLResponse)
async def dedup_keys(request: Request, worker_name: str):
    """HTMX partial: list recent dedup keys."""
    from pipeline.tools.eventhub_ui.dedup import create_dedup_store, get_dedup_ttl

    try:
        store = await create_dedup_store()
        try:
            ttl = get_dedup_ttl()
            keys = await store.list_recent_keys(
                worker_name=worker_name,
                ttl_seconds=ttl,
                max_keys=100,
            )
        finally:
            await store.close()
    except Exception as e:
        logger.exception(f"Failed to list dedup keys for {worker_name}")
        return error_response(request, f"Error listing keys: {e}")

    return templates.TemplateResponse(
        "dedup_keys.html",
        {
            "request": request,
            "worker_name": worker_name,
            "keys": keys,
        },
    )


@router.post("/dedup/{worker_name}/check", response_class=HTMLResponse)
async def dedup_check(
    request: Request,
    worker_name: str,
    key: str = Form(...),
):
    """HTMX partial: check if key is duplicate."""
    from pipeline.tools.eventhub_ui.dedup import create_dedup_store, get_dedup_ttl

    try:
        store = await create_dedup_store()
        try:
            ttl = get_dedup_ttl()
            is_dup, metadata = await store.check_duplicate(
                worker_name=worker_name,
                key=key,
                ttl_seconds=ttl,
            )
        finally:
            await store.close()
    except Exception as e:
        logger.exception(f"Failed to check dedup key {key} for {worker_name}")
        return error_response(request, f"Error checking key: {e}")

    return templates.TemplateResponse(
        "dedup_check_result.html",
        {
            "request": request,
            "worker_name": worker_name,
            "key": key,
            "is_duplicate": is_dup,
            "metadata": metadata,
        },
    )


@router.post("/dedup/{worker_name}/cleanup", response_class=HTMLResponse)
async def dedup_cleanup(request: Request, worker_name: str):
    """Execute cleanup of expired entries."""
    from pipeline.tools.eventhub_ui.dedup import create_dedup_store, get_dedup_ttl

    try:
        store = await create_dedup_store()
        try:
            ttl = get_dedup_ttl()
            removed = await store.cleanup_expired(
                worker_name=worker_name,
                ttl_seconds=ttl,
            )
        finally:
            await store.close()
    except Exception as e:
        logger.exception(f"Failed to cleanup dedup for {worker_name}")
        return error_response(request, f"Error during cleanup: {e}")

    return templates.TemplateResponse(
        "dedup_check_result.html",
        {
            "request": request,
            "worker_name": worker_name,
            "key": None,
            "is_duplicate": None,
            "metadata": None,
            "cleanup_count": removed,
        },
    )
