"""Worker registry: maps CLI worker names to runner functions."""

import asyncio
import inspect
from typing import Any

from pipeline.runners import claimx_runners, plugin_runners, verisk_runners

WORKER_REGISTRY: dict[str, dict[str, Any]] = {
    # XACT workers
    "xact-event-ingester": {
        "runner": verisk_runners.run_event_ingester,
    },
    "xact-retry-scheduler": {
        "runner": verisk_runners.run_xact_retry_scheduler,
    },
    "xact-enricher": {
        "runner": verisk_runners.run_xact_enrichment_worker,
    },
    "xact-download": {
        "runner": verisk_runners.run_download_worker,
    },
    "xact-upload": {
        "runner": verisk_runners.run_upload_worker,
    },
    # ClaimX workers
    "claimx-ingester": {
        "runner": claimx_runners.run_claimx_event_ingester,
    },
    "claimx-enricher": {
        "runner": claimx_runners.run_claimx_enrichment_worker,
    },
    "claimx-downloader": {
        "runner": claimx_runners.run_claimx_download_worker,
    },
    "claimx-uploader": {
        "runner": claimx_runners.run_claimx_upload_worker,
    },
    "claimx-retry-scheduler": {
        "runner": claimx_runners.run_claimx_retry_scheduler,
    },
    # App workers
    "itel-cabinet": {
        "runner": plugin_runners.run_itel_cabinet_tracking,
    },
    "eventhub-ui": {
        "runner": plugin_runners.run_eventhub_ui,
    },
}


async def run_worker_from_registry(
    worker_name: str,
    pipeline_config,
    shutdown_event: asyncio.Event,
    eventhub_config=None,
    local_kafka_config=None,
    instance_id: int | None = None,
):
    """Run a worker by looking it up in the registry.

    Raises:
        ValueError: If worker not found in registry or requirements not met
    """
    if worker_name not in WORKER_REGISTRY:
        raise ValueError(f"Unknown worker: {worker_name}")

    worker_def = WORKER_REGISTRY[worker_name]

    kwargs: dict[str, Any] = {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "eventhub_config": eventhub_config,
        "local_kafka_config": local_kafka_config,
        "kafka_config": local_kafka_config,
        "domain": pipeline_config.domain,
    }

    if instance_id is not None:
        kwargs["instance_id"] = instance_id

    # claimx-enricher needs the projects table path for project cache
    if worker_name == "claimx-enricher":
        kwargs["claimx_projects_table_path"] = pipeline_config.claimx_projects_table_path

    # Filter kwargs to match runner's signature (unless it accepts **kwargs)
    runner = worker_def["runner"]
    sig = inspect.signature(runner)
    has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if has_var_keyword:
        filtered_kwargs = kwargs
    else:
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
    await runner(**filtered_kwargs)
