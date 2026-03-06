"""Common worker execution patterns and utilities.

Provides reusable templates for running workers with consistent:
- Shutdown handling
- Logging context
- Error handling
- Resource cleanup
"""

import asyncio
import logging
import os
from collections.abc import Callable

from core.logging.context import set_log_context
from core.logging.setup import upload_crash_logs
from pipeline.common.health import HealthCheckServer

logger = logging.getLogger(__name__)

# Startup retry configuration (overridable via env vars)
DEFAULT_STARTUP_RETRIES = 5
DEFAULT_STARTUP_BACKOFF_BASE = 5  # seconds



async def _cleanup_watcher_task(task: asyncio.Task) -> None:
    """Cancel and await watcher task, suppressing expected exceptions.

    Handles CancelledError and RuntimeError (when event loop closed during shutdown).
    """
    try:
        task.cancel()
        await task
    except (asyncio.CancelledError, RuntimeError):
        pass


async def _start_with_retry(
    start_fn: Callable,
    label: str,
    max_retries: int | None = None,
    backoff_base: int | None = None,
    use_constant_backoff: bool = False,
) -> None:
    """Retry an async start function with exponential backoff.

    On exhaustion, re-raises the last exception so the caller's fatal error
    handler can log it and enter health-server error mode.

    Args:
        start_fn: Async callable (e.g. worker.start, producer.start)
        label: Human-readable label for log messages
        max_retries: Number of retry attempts (default: 5, env: STARTUP_MAX_RETRIES)
        backoff_base: Base seconds for backoff (default: 5, env: STARTUP_BACKOFF_SECONDS)
        use_constant_backoff: If True, use constant delays instead of linear (default: False)
    """
    max_retries = max_retries or int(os.getenv("STARTUP_MAX_RETRIES", str(DEFAULT_STARTUP_RETRIES)))
    backoff_base = backoff_base or int(
        os.getenv("STARTUP_BACKOFF_SECONDS", str(DEFAULT_STARTUP_BACKOFF_BASE))
    )

    for attempt in range(1, max_retries + 1):
        try:
            await start_fn()
            return
        except Exception as e:
            if attempt == max_retries:
                logger.error(
                    f"Failed to start {label} after {max_retries} attempts, giving up",
                    extra={"error": str(e), "attempts": max_retries},
                )
                raise
            delay = backoff_base if use_constant_backoff else backoff_base * attempt
            logger.warning(
                f"Failed to start {label} (attempt {attempt}/{max_retries}), retrying in {delay}s",
                extra={"error": str(e), "attempt": attempt, "delay": delay},
            )
            await asyncio.sleep(delay)


async def _enter_worker_error_mode(
    health_server: HealthCheckServer,
    stage_name: str,
    error_msg: str,
    shutdown_event: asyncio.Event,
) -> None:
    """Keep worker's health server alive in error state until shutdown.

    Instead of exiting immediately on fatal error, the worker enters error mode where:
    - Health server continues running for debugging
    - Liveness probe passes (pod stays alive)
    - Readiness probe fails with error details (pod not ready for traffic)
    - Waits for shutdown signal before exiting

    This enables debugging in containerized environments where inspecting
    pod state after errors is needed.

    Args:
        health_server: Worker's health check server instance
        stage_name: Worker stage name for logging
        error_msg: Error message describing the failure
        shutdown_event: Event to wait on for graceful shutdown
    """
    logger.warning(f"Entering ERROR MODE for {stage_name} - health endpoint will remain alive")

    health_server.set_error(error_msg)

    # Hold a reference to prevent "Task was destroyed" warnings on shutdown
    crash_log_task = asyncio.create_task(asyncio.to_thread(upload_crash_logs, error_msg))

    def _log_crash_upload_error(t: asyncio.Task) -> None:
        if not t.cancelled():
            exc = t.exception()
            if exc:
                logger.warning("Crash log upload failed", extra={"error": str(exc)})

    crash_log_task.add_done_callback(_log_crash_upload_error)

    logger.info(
        "Health server running in error mode",
        extra={
            "stage": stage_name,
            "health_port": health_server.actual_port,
            "error": error_msg,
        },
    )

    await shutdown_event.wait()
    logger.info(f"Shutdown signal received in error mode for {stage_name}")


async def execute_worker_with_shutdown(
    worker_instance,
    stage_name: str,
    shutdown_event: asyncio.Event,
    instance_id: int | None = None,
) -> None:
    """Execute a worker with standard shutdown handling.

    On fatal error, if worker has a health_server attribute, enters error mode
    to keep health endpoint alive for debugging. Otherwise re-raises for
    top-level error handling.

    Args:
        worker_instance: Worker instance with start() and stop() methods
        stage_name: Name for logging context
        shutdown_event: Event to signal graceful shutdown
        instance_id: Instance identifier for multi-instance deployments (optional)
    """
    context = {"stage": stage_name}
    if instance_id is not None:
        context["instance_id"] = instance_id
        context["worker_id"] = f"{stage_name}-{instance_id}"
        logger_suffix = f" (instance {instance_id})"
    else:
        logger_suffix = ""

    set_log_context(**context)
    logger.info("Starting %s%s...", stage_name, logger_suffix)

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info(f"Shutdown signal received, stopping {stage_name}{logger_suffix}...")
        await worker_instance.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await _start_with_retry(worker_instance.start, stage_name)
    except Exception as e:
        if hasattr(worker_instance, "health_server"):
            await _cleanup_watcher_task(watcher_task)
            await _enter_worker_error_mode(
                worker_instance.health_server,
                stage_name,
                f"Fatal error: {e}",
                shutdown_event,
            )
            # Fall through to finally for cleanup
        else:
            raise
    finally:
        await _cleanup_watcher_task(watcher_task)
        try:
            await worker_instance.stop()
        except Exception as e:
            logger.error("Error stopping worker", extra={"error": str(e), "stage": stage_name})


async def execute_worker_with_producer(
    worker_class,
    kafka_config,
    domain: str,
    stage_name: str,
    shutdown_event: asyncio.Event,
    *,
    worker_kwargs: dict | None = None,
    producer_worker_name: str | None = None,
    instance_id: int | None = None,
) -> None:
    """Execute a worker that requires a producer with shutdown handling.

    On fatal error, if worker has a health_server attribute, enters error mode
    to keep health endpoint alive for debugging. Otherwise re-raises for
    top-level error handling.

    Args:
        worker_class: Worker class to instantiate
        kafka_config: Message configuration
        domain: Domain name (xact/claimx)
        stage_name: Name for logging context
        shutdown_event: Event to signal graceful shutdown
        worker_kwargs: Additional kwargs for worker instantiation
        producer_worker_name: Name for producer (defaults to stage_name)
        instance_id: Instance identifier for multi-instance deployments (optional)
    """
    context = {"stage": stage_name}
    if instance_id is not None:
        context["instance_id"] = instance_id
        context["worker_id"] = f"{stage_name}-{instance_id}"
        logger_suffix = f" (instance {instance_id})"
    else:
        logger_suffix = ""

    set_log_context(**context)
    logger.info("Starting %s%s...", stage_name, logger_suffix)

    worker_kwargs = worker_kwargs or {}
    producer_worker_name = producer_worker_name or stage_name.replace("-", "_")

    if instance_id is not None:
        producer_worker_name = f"{producer_worker_name}_{instance_id}"
        worker_kwargs["instance_id"] = instance_id

    from pipeline.common.transport import create_producer

    producer = create_producer(
        config=kafka_config,
        domain=domain,
        worker_name=producer_worker_name,
        topic_key="retry",
    )
    await _start_with_retry(producer.start, f"{stage_name}-producer")

    worker = None
    watcher_task = None

    try:
        worker = worker_class(
            config=kafka_config,
            producer=producer,
            domain=domain,
            **worker_kwargs,
        )

        async def shutdown_watcher():
            await shutdown_event.wait()
            logger.info(f"Shutdown signal received, stopping {stage_name}{logger_suffix}...")
            await worker.stop()

        watcher_task = asyncio.create_task(shutdown_watcher())

        await _start_with_retry(worker.start, stage_name)
    except Exception as e:
        if worker is not None and hasattr(worker, "health_server"):
            if watcher_task is not None:
                await _cleanup_watcher_task(watcher_task)
                watcher_task = None
            await _enter_worker_error_mode(
                worker.health_server,
                stage_name,
                f"Fatal error: {e}",
                shutdown_event,
            )
            # Fall through to finally for cleanup
        else:
            raise
    finally:
        if watcher_task is not None:
            await _cleanup_watcher_task(watcher_task)
        if worker is not None:
            try:
                await worker.stop()
            except Exception as e:
                logger.error("Error stopping worker", extra={"error": str(e), "stage": stage_name})
        try:
            await producer.stop()
        except Exception as e:
            logger.error("Error stopping producer", extra={"error": str(e), "stage": stage_name})


