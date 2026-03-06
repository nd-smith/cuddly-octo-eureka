"""Message replay routes."""

import asyncio
import logging
import uuid
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

# In-memory task tracking: task_id -> {status, total, replayed, error}
_replay_tasks: dict[str, dict] = {}


def _cleanup_old_tasks():
    """Remove tasks older than 1 hour."""
    import time

    now = time.time()
    expired = [
        tid for tid, t in _replay_tasks.items() if now - t.get("created", 0) > 3600
    ]
    for tid in expired:
        _replay_tasks.pop(tid, None)


@router.get("/replay", response_class=HTMLResponse)
async def replay_form(request: Request):
    """Form for replaying messages."""
    hubs = list_eventhubs()
    return _helpers.templates.TemplateResponse(
        "replay.html",
        {
            "request": request,
            "hubs": hubs,
        },
    )


@router.post("/replay/preview", response_class=HTMLResponse)
async def replay_preview(
    request: Request,
    source_hub: str = Form(...),
    source_partition: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    dest_hub: str = Form(...),
):
    """Read source messages and show count + sample."""
    from pipeline.tools.eventhub_ui.sampler import sample_messages

    hub = find_hub(source_hub)
    if not hub:
        return error_response(request, f"Source EventHub '{source_hub}' not found", 404)

    try:
        start_dt = datetime.fromisoformat(start_time).replace(tzinfo=UTC)
        end_dt = datetime.fromisoformat(end_time).replace(tzinfo=UTC)
    except ValueError as e:
        return error_response(request, f"Invalid datetime: {e}", 400)

    try:
        conn_str = conn_str_for_hub(hub)
        messages = await sample_messages(
            conn_str=conn_str,
            eventhub_name=source_hub,
            partition_id=source_partition,
            count=100,
            starting_time=start_dt,
            ssl_kwargs=get_ssl_kwargs(),
        )
        # Filter to end time
        messages = [m for m in messages if m.enqueued_time <= end_dt]
    except Exception as e:
        logger.exception(f"Failed to preview replay from {source_hub}")
        return error_response(request, f"Error reading source: {e}")

    return _helpers.templates.TemplateResponse(
        "replay_preview.html",
        {
            "request": request,
            "messages": messages,
            "source_hub": source_hub,
            "source_partition": source_partition,
            "start_time": start_time,
            "end_time": end_time,
            "dest_hub": dest_hub,
            "count": len(messages),
        },
    )


async def _execute_replay(
    task_id: str,
    source_hub: str,
    source_partition: str,
    start_dt: datetime,
    end_dt: datetime,
    dest_hub: str,
):
    """Background task to replay messages."""
    from pipeline.tools.eventhub_ui.injector import send_message
    from pipeline.tools.eventhub_ui.sampler import sample_messages

    task = _replay_tasks[task_id]
    try:
        src = find_hub(source_hub)
        dst = find_hub(dest_hub)
        if not src or not dst:
            task["status"] = "error"
            task["error"] = "Hub not found"
            return

        ssl_kwargs = get_ssl_kwargs()
        messages = await sample_messages(
            conn_str=conn_str_for_hub(src),
            eventhub_name=source_hub,
            partition_id=source_partition,
            count=100,
            starting_time=start_dt,
            ssl_kwargs=ssl_kwargs,
        )
        messages = [m for m in messages if m.enqueued_time <= end_dt]
        task["total"] = len(messages)

        dst_conn = conn_str_for_hub(dst)
        for msg in messages:
            await send_message(
                conn_str=dst_conn,
                eventhub_name=dest_hub,
                body=msg.body,
                partition_key=msg.partition_key,
                ssl_kwargs=ssl_kwargs,
            )
            task["replayed"] += 1

        task["status"] = "completed"
    except Exception as e:
        logger.exception(f"Replay task {task_id} failed")
        task["status"] = "error"
        task["error"] = str(e)


@router.post("/replay/execute", response_class=HTMLResponse)
async def replay_execute(
    request: Request,
    source_hub: str = Form(...),
    source_partition: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    dest_hub: str = Form(...),
):
    """Start background replay task."""
    import time

    _cleanup_old_tasks()

    try:
        start_dt = datetime.fromisoformat(start_time).replace(tzinfo=UTC)
        end_dt = datetime.fromisoformat(end_time).replace(tzinfo=UTC)
    except ValueError as e:
        return error_response(request, f"Invalid datetime: {e}", 400)

    task_id = str(uuid.uuid4())[:8]
    _replay_tasks[task_id] = {
        "status": "running",
        "total": 0,
        "replayed": 0,
        "error": None,
        "created": time.time(),
    }

    asyncio.create_task(
        _execute_replay(
            task_id, source_hub, source_partition, start_dt, end_dt, dest_hub
        )
    )

    return _helpers.templates.TemplateResponse(
        "replay_progress.html",
        {
            "request": request,
            "task_id": task_id,
            "task": _replay_tasks[task_id],
        },
    )


@router.get("/replay/status/{task_id}", response_class=HTMLResponse)
async def replay_status(request: Request, task_id: str):
    """HTMX-polled progress partial."""
    task = _replay_tasks.get(task_id)
    if not task:
        return error_response(request, f"Replay task '{task_id}' not found", 404)

    return _helpers.templates.TemplateResponse(
        "replay_progress.html",
        {
            "request": request,
            "task_id": task_id,
            "task": task,
        },
    )
