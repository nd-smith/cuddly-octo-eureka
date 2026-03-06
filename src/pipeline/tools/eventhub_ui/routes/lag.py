"""Consumer lag routes."""

import asyncio
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pipeline.tools.eventhub_ui.config import (
    extract_fqdn,
    get_blob_connection_string,
    get_checkpoint_container_name,
    get_namespace_connection_string,
    get_source_connection_string,
    get_ssl_kwargs,
    list_eventhubs,
)
from pipeline.tools.eventhub_ui.routes._helpers import (
    conn_str_for_hub,
    error_response,
    find_hub,
    templates,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _load_lag_config() -> tuple[dict, list[str]]:
    config = {}
    errors = []

    for key, getter, fqdn_key in [
        ("internal_conn", get_namespace_connection_string, "internal_fqdn"),
        ("source_conn", get_source_connection_string, "source_fqdn"),
    ]:
        try:
            conn = getter()
            config[key] = conn
            config[fqdn_key] = extract_fqdn(conn)
        except Exception as e:
            config[key] = None
            config[fqdn_key] = None
            errors.append(str(e))

    try:
        config["blob_conn"] = get_blob_connection_string()
    except Exception as e:
        config["blob_conn"] = None
        errors.append(str(e))

    return config, errors


def _build_lag_tasks(cfg, hubs, container_name, ssl_kwargs, calculate_lag):
    tasks = []
    for hub in hubs:
        if not hub.consumer_groups:
            continue

        conn_str = cfg["source_conn"] if hub.is_source else cfg["internal_conn"]
        fqdn = cfg["source_fqdn"] if hub.is_source else cfg["internal_fqdn"]

        for worker_name, cg_name in hub.consumer_groups.items():
            tasks.append(
                (
                    hub,
                    worker_name,
                    cg_name,
                    calculate_lag(
                        conn_str=conn_str,
                        eventhub_name=hub.eventhub_name,
                        consumer_group=cg_name,
                        fqdn=fqdn,
                        blob_conn_str=cfg["blob_conn"],
                        container_name=container_name,
                        ssl_kwargs=ssl_kwargs,
                    ),
                )
            )
    return tasks


def _collect_lag_results(tasks, gathered):
    results = []
    errors = []
    for (hub, worker_name, cg_name, _), outcome in zip(tasks, gathered, strict=False):
        if isinstance(outcome, Exception):
            errors.append(f"{hub.eventhub_name}/{cg_name}: {outcome}")
        else:
            results.append(
                {
                    "hub": hub,
                    "worker_name": worker_name,
                    "lag": outcome,
                }
            )
    return results, errors


async def _fetch_all_lag() -> tuple[list[dict], list[str]]:
    from pipeline.tools.eventhub_ui.lag import calculate_lag

    cfg, config_errors = _load_lag_config()
    if config_errors:
        return [], config_errors

    hubs = list_eventhubs()
    ssl_kwargs = get_ssl_kwargs()
    container_name = get_checkpoint_container_name()

    tasks = _build_lag_tasks(cfg, hubs, container_name, ssl_kwargs, calculate_lag)

    gathered = await asyncio.gather(
        *(coro for _, _, _, coro in tasks),
        return_exceptions=True,
    )

    return _collect_lag_results(tasks, gathered)


@router.get("/lag", response_class=HTMLResponse)
async def lag_overview(request: Request):
    """Show lag for all consumer groups across all EventHubs."""
    results, errors = await _fetch_all_lag()

    return templates.TemplateResponse(
        "lag_overview.html",
        {
            "request": request,
            "results": results,
            "errors": errors,
        },
    )


@router.get("/lag/data", response_class=HTMLResponse)
async def lag_overview_data(request: Request):
    """HTMX partial: return just the lag table for polling updates."""
    from pipeline.tools.eventhub_ui.lag_history import get_trends, record_snapshot

    results, errors = await _fetch_all_lag()

    if results:
        try:
            await asyncio.to_thread(record_snapshot, results)
        except Exception:
            logger.exception("Failed to record lag snapshot to CSV")

    try:
        trends = await asyncio.to_thread(get_trends)
    except Exception:
        logger.exception("Failed to load lag trends")
        trends = {}

    now = datetime.now(UTC).strftime("%H:%M:%S UTC")

    return templates.TemplateResponse(
        "lag_overview_partial.html",
        {
            "request": request,
            "results": results,
            "errors": errors,
            "updated_at": now,
            "trends": trends,
        },
    )


@router.get("/lag/history.csv")
async def lag_history_csv():
    """Download the lag history CSV."""
    from fastapi.responses import PlainTextResponse

    from pipeline.tools.eventhub_ui.lag_history import read_csv

    content = await asyncio.to_thread(read_csv)
    return PlainTextResponse(
        content,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=consumer_lag_history.csv"
        },
    )


@router.get("/lag/{eventhub_name}/{consumer_group}", response_class=HTMLResponse)
async def lag(request: Request, eventhub_name: str, consumer_group: str):
    """Show consumer lag for a specific consumer group."""
    from pipeline.tools.eventhub_ui.lag import calculate_lag

    hub = find_hub(eventhub_name)
    if not hub:
        return error_response(
            request, f"EventHub '{eventhub_name}' not found in config", 404
        )

    try:
        conn_str = conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        lag_result = await calculate_lag(
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
            f"Failed to calculate lag for {eventhub_name}/{consumer_group}"
        )
        return error_response(request, f"Error calculating lag: {e}")

    return templates.TemplateResponse(
        "lag.html",
        {
            "request": request,
            "hub": hub,
            "consumer_group": consumer_group,
            "lag": lag_result,
        },
    )
