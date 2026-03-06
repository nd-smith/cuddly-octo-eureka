"""Payload search across EventHub partitions."""

import asyncio
import json
import logging
from datetime import datetime

from pipeline.tools.eventhub_ui.sampler import SampledMessage, sample_messages

logger = logging.getLogger(__name__)

MAX_RESULTS = 50
SEARCH_SEMAPHORE = asyncio.Semaphore(4)


def _dot_path_lookup(data: dict, path: str):
    """Attempt dot-path lookup on a dict (e.g., 'body.claim_id')."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _matches_query(msg: SampledMessage, query: str) -> bool:
    """Check if a message body matches the query."""
    if not query:
        return True

    # If query contains '.', try dot-path JSON field lookup first
    if "." in query:
        field_path, _, field_value = query.partition("=")
        if field_value:
            try:
                parsed = json.loads(msg.body)
                actual = _dot_path_lookup(parsed, field_path.strip())
                if actual is not None and str(actual) == field_value.strip():
                    return True
            except (json.JSONDecodeError, TypeError):
                pass

    # Substring search on serialized body
    return query.lower() in msg.body.lower()


async def search_partition(
    conn_str: str,
    eventhub_name: str,
    partition_id: str,
    start_time: datetime,
    end_time: datetime,
    query: str,
    ssl_kwargs: dict | None = None,
    max_results: int = MAX_RESULTS,
) -> list[SampledMessage]:
    """Search a single partition for matching messages."""
    async with SEARCH_SEMAPHORE:
        messages = await sample_messages(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            partition_id=partition_id,
            count=max_results * 10,
            starting_time=start_time,
            ssl_kwargs=ssl_kwargs,
        )

    # Filter by end time and query
    results = []
    for msg in messages:
        if msg.enqueued_time > end_time:
            break
        if _matches_query(msg, query):
            results.append(msg)
            if len(results) >= max_results:
                break

    return results


async def search_eventhub(
    conn_str: str,
    eventhub_name: str,
    partition_ids: list[str],
    start_time: datetime,
    end_time: datetime,
    query: str,
    ssl_kwargs: dict | None = None,
    max_results: int = MAX_RESULTS,
    timeout: float = 30.0,
) -> tuple[list[SampledMessage], list[str]]:
    """Search across partitions. Returns (results, errors)."""
    tasks = [
        search_partition(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            partition_id=pid,
            start_time=start_time,
            end_time=end_time,
            query=query,
            ssl_kwargs=ssl_kwargs,
            max_results=max_results,
        )
        for pid in partition_ids
    ]

    try:
        gathered = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout,
        )
    except TimeoutError:
        return [], [f"Search timed out after {timeout}s"]

    all_results = []
    errors = []
    for pid, result in zip(partition_ids, gathered, strict=False):
        if isinstance(result, Exception):
            errors.append(f"Partition {pid}: {result}")
        else:
            for msg in result:
                msg.partition_id = pid  # type: ignore[attr-defined]
            all_results.extend(result)

    all_results.sort(key=lambda m: m.enqueued_time, reverse=True)
    return all_results[:max_results], errors
