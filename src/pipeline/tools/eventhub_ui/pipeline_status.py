"""Pipeline worker health status probing."""

import asyncio
import logging
import ssl
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)

# Worker health port map from runner configs.
# Workers using port=0 get dynamic ports and are shown as "unknown".
WORKER_HEALTH_PORTS: dict[str, dict[str, int | None]] = {
    "verisk": {
        "xact-event-ingester": None,  # dynamic port
        "xact-enricher": None,
        "xact-download": None,
        "xact-upload": None,
        "xact-result-processor": None,
        "xact-delta-writer": None,
        "xact-retry-scheduler": None,
    },
    "claimx": {
        "claimx-ingester": None,
        "claimx-enricher": None,
        "claimx-downloader": None,
        "claimx-uploader": None,
        "claimx-result-processor": None,
        "claimx-delta-writer": None,
        "claimx-retry-scheduler": None,
        "claimx-entity-writer": None,
    },
    "plugins": {
        "itel-cabinet": 8096,
        "eventhub-ui": 8550,
    },
}


@dataclass
class WorkerStatus:
    name: str
    domain: str
    port: int | None
    live: bool | None = None  # None = unknown/not probed
    ready: bool | None = None
    error: str | None = None


async def _probe_endpoint(url: str, timeout: float = 3.0) -> bool:
    """Probe a health endpoint. Returns True if 2xx response."""
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        async with (
            aiohttp.ClientSession(timeout=client_timeout) as session,
            session.get(url, ssl=ssl_ctx) as resp,
        ):
            return 200 <= resp.status < 300
    except Exception:
        return False


async def probe_worker(name: str, domain: str, port: int | None) -> WorkerStatus:
    """Probe a single worker's health endpoints."""
    if port is None:
        return WorkerStatus(name=name, domain=domain, port=None)

    live = await _probe_endpoint(f"http://localhost:{port}/health/live")
    ready = await _probe_endpoint(f"http://localhost:{port}/health/ready")

    return WorkerStatus(
        name=name,
        domain=domain,
        port=port,
        live=live,
        ready=ready,
    )


async def get_all_worker_statuses() -> dict[str, list[WorkerStatus]]:
    """Probe all workers grouped by domain."""
    tasks = []
    for domain, workers in WORKER_HEALTH_PORTS.items():
        for name, port in workers.items():
            tasks.append(probe_worker(name, domain, port))

    statuses = await asyncio.gather(*tasks)

    grouped: dict[str, list[WorkerStatus]] = {}
    for status in statuses:
        grouped.setdefault(status.domain, []).append(status)

    return grouped
