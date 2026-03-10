# ClaimX Pipeline — Production Readiness TODO

> Generated from end-to-end walkthrough on 2026-03-08.
> Fix order: C1 → H4 → H1 → H3 → M4 → M8 → H2 → M6 → M1 → M2 → H5 → M7 → M3

---

## Critical

### C1. SSL bypass fails open when env vars are unset
**File**: `src/core/security/ssl_dev_bypass.py:112-119`

The guard only blocks `"production"`. If both `ENVIRONMENT` and `APP_ENV` are unset (empty string), the check passes and SSL verification is disabled.

```python
# CURRENT — fails open:
env = os.getenv("ENVIRONMENT", "").lower()
app_env = os.getenv("APP_ENV", "").lower()
if env == "production" or app_env == "production":
    logger.error(
        "DISABLE_SSL_VERIFY is set but ENVIRONMENT or APP_ENV is 'production'. "
        "Refusing to disable SSL verification in production."
    )
    return
```

**Fix** — Invert to allowlist. Only permit bypass in explicitly recognized dev environments:

```python
ALLOWED_ENVS = {"development", "local", "dev", "test"}
env = os.getenv("ENVIRONMENT", "").lower()
app_env = os.getenv("APP_ENV", "").lower()
if env not in ALLOWED_ENVS and app_env not in ALLOWED_ENVS:
    logger.error(
        "DISABLE_SSL_VERIFY is set but ENVIRONMENT=%s / APP_ENV=%s "
        "is not a recognized development environment. "
        "Refusing to disable SSL verification.",
        env, app_env,
    )
    return
```

---

## High Priority

### H1. No async operation timeouts on external calls

Workers can deadlock if any external service hangs. Wrap each call with `asyncio.wait_for()`.

#### H1a. Handler batch processing — no timeout
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:529`

```python
# CURRENT:
results = await handler.handle_batch(handler_events)
```

```python
# FIX:
results = await asyncio.wait_for(
    handler.handle_batch(handler_events),
    timeout=120.0,
)
```

#### H1b. Handler single-event processing — no timeout
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:876`

```python
# CURRENT:
handler_result = await handler.process([event])
```

```python
# FIX:
handler_result = await asyncio.wait_for(
    handler.process([event]),
    timeout=120.0,
)
```

#### H1c. Entity rows producer send — no timeout
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:980-984`

```python
# CURRENT:
await self.producer.send(
    value=entity_rows,
    key=trace_id,
    headers={"trace_id": trace_id},
)
```

```python
# FIX:
await asyncio.wait_for(
    self.producer.send(
        value=entity_rows,
        key=trace_id,
        headers={"trace_id": trace_id},
    ),
    timeout=30.0,
)
```

#### H1d. Download task producer send — no timeout
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:1036-1040`

```python
# CURRENT:
metadata = await self.download_producer.send(
    value=task,
    key=task.trace_id,
    headers={"trace_id": task.trace_id},
)
```

```python
# FIX:
metadata = await asyncio.wait_for(
    self.download_producer.send(
        value=task,
        key=task.trace_id,
        headers={"trace_id": task.trace_id},
    ),
    timeout=30.0,
)
```

#### H1e. OneLake upload — no timeout
**File**: `src/pipeline/claimx/workers/upload_worker.py:528-532`

```python
# CURRENT:
blob_path = await self.onelake_client.async_upload_file(
    relative_path=cached_message.destination_path,
    local_path=cache_path,
    overwrite=True,
)
```

```python
# FIX:
blob_path = await asyncio.wait_for(
    self.onelake_client.async_upload_file(
        relative_path=cached_message.destination_path,
        local_path=cache_path,
        overwrite=True,
    ),
    timeout=300.0,
)
```

#### H1f. Result processor Delta merge — no timeout
**File**: `src/pipeline/claimx/workers/result_processor.py:421-425`

```python
# CURRENT:
await self.inventory_writer._async_merge(
    df,
    merge_keys=["media_id"],
    preserve_columns=["created_at"],
)
```

```python
# FIX:
await asyncio.wait_for(
    self.inventory_writer._async_merge(
        df,
        merge_keys=["media_id"],
        preserve_columns=["created_at"],
    ),
    timeout=300.0,
)
```

#### H1g. Consumer close — no timeout
**File**: `src/pipeline/common/eventhub/consumer.py:339-340`

```python
# CURRENT:
if self._consumer:
    await self._consumer.close()
```

```python
# FIX:
if self._consumer:
    await asyncio.wait_for(self._consumer.close(), timeout=30.0)
```

#### H1h. DLQ producer send — no timeout
**File**: `src/pipeline/common/eventhub/consumer.py:866-868`

```python
# CURRENT:
metadata = await self._dlq_producer.send(
    topic=dlq_entity_name,
    key=dlq_key,
```

Wrap the full send call with `asyncio.wait_for(..., timeout=30.0)`.

---

### H2. Handlers don't retry transient API errors locally
**Files**:
- `src/pipeline/claimx/handlers/project.py:128-150`
- `src/pipeline/claimx/handlers/task.py:216-233`
- `src/pipeline/claimx/handlers/media.py:280-285`
- `src/pipeline/claimx/handlers/video.py:133-154`
- `src/pipeline/claimx/handlers/project_update.py:154-175`

All handlers catch `ClaimXApiError` and immediately return a failure result. A transient 503 triggers full pipeline-level retry (minimum 5-minute delay) instead of a quick 1-2 second local retry.

```python
# CURRENT (project.py:128-150):
except ClaimXApiError as e:
    duration_ms = elapsed_ms(start_time)
    logger.warning(
        "API error for project",
        extra={...},
    )
    return EnrichmentResult(
        event=event,
        success=False,
        error=str(e),
        error_category=e.category,
        is_retryable=e.is_retryable,
        api_calls=1,
        duration_ms=duration_ms,
    )
```

**Fix** — Add a retry utility to `handlers/base.py` and use in all handlers:

```python
# src/pipeline/claimx/handlers/base.py — new method on EventHandler:
async def _call_api_with_retry(
    self,
    coro_factory: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> T:
    """Retry transient API errors locally before escalating to pipeline retry."""
    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except ClaimXApiError as e:
            if not e.is_retryable or attempt == max_attempts - 1:
                raise
            delay = base_delay * (2 ** attempt)
            logger.info(
                "Retrying transient API error",
                extra={
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "delay": delay,
                    "status_code": e.status_code,
                },
            )
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")  # satisfy type checker
```

Then in each handler, wrap API calls:
```python
# Instead of:
response = await self.api_client.get_project(project_id)
# Use:
response = await self._call_api_with_retry(
    lambda: self.api_client.get_project(project_id)
)
```

---

### H3. Multi-table Delta writes lack atomicity — append-only tables produce duplicates
**File**: `src/pipeline/claimx/writers/delta_entities.py:336-367`

```python
# CURRENT — 7 tables written concurrently, no rollback:
results = await asyncio.gather(
    *(self._write_table(entity_type, rows) for entity_type, rows in pending),
    return_exceptions=True,
)

for (entity_type, _rows), result in zip(pending, results):
    if isinstance(result, BaseException):
        self.logger.error(...)
        failed_tables.append(entity_type)
    elif result is not None:
        counts[entity_type] = result
    else:
        failed_tables.append(entity_type)
```

**File**: `src/pipeline/claimx/writers/delta_entities.py:367`
```python
_APPEND_ONLY_TABLES = frozenset({"contacts", "media"})
```

If 3 tables succeed and 4 fail, offsets are not committed → reprocessing → duplicate rows in `contacts` and `media` (append-only).

**Fix** — Convert `contacts` and `media` to merge-based writes to make reprocessing idempotent:

```python
# Change:
_APPEND_ONLY_TABLES = frozenset({"contacts", "media"})

# To:
_APPEND_ONLY_TABLES = frozenset()  # All tables now use merge for idempotency
```

Ensure merge keys are defined for both tables:
- `contacts`: merge on `project_id` + `email` + `type`
- `media`: merge on `media_id`

Verify the existing `MERGE_KEYS` dict already has entries for these tables. If not, add them.

---

### H4. Download task production failures silently swallowed (data loss)
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:1053-1063`

```python
# CURRENT — failure logged but not routed to retry/DLQ:
except Exception as e:
    logger.error(
        "Failed to produce download task",
        extra={
            "trace_id": task.trace_id,
            "media_id": task.media_id,
            "project_id": task.project_id,
            "error": str(e),
        },
        exc_info=True,
    )
    # ← Nothing happens here. Task is lost forever.
```

**Fix** — Route failed download tasks to the enrichment retry handler:

```python
except Exception as e:
    logger.error(
        "Failed to produce download task - routing to retry",
        extra={
            "trace_id": task.trace_id,
            "media_id": task.media_id,
            "project_id": task.project_id,
            "error": str(e),
        },
        exc_info=True,
    )
    # Build an enrichment task from the download task for retry routing
    enrichment_task = ClaimXEnrichmentTask(
        trace_id=task.trace_id,
        project_id=task.project_id,
        event_type=task.event_type,
        retry_count=task.retry_count,
    )
    await self._handle_enrichment_failure(
        enrichment_task, e, ErrorCategory.TRANSIENT,
    )
```

---

### H5. Silent exception swallowing in multiple locations

#### H5a. Process pool reset — bare except pass
**File**: `src/pipeline/common/writers/base.py:141-149`

```python
# CURRENT:
try:
    for proc in list(pool._processes.values()):
        if proc.is_alive():
            proc.kill()
    pool.shutdown(wait=False, cancel_futures=True)
except Exception:
    pass
```

```python
# FIX — at minimum log the failure:
try:
    for proc in list(pool._processes.values()):
        if proc.is_alive():
            proc.kill()
    pool.shutdown(wait=False, cancel_futures=True)
except Exception:
    logger.warning("Failed to cleanly reset process pool", exc_info=True)
```

#### H5b. Fire-and-forget blob dedup writes
**File**: `src/pipeline/claimx/workers/event_ingester.py:655-659`

```python
# CURRENT — blob failure swallowed with warning only:
except Exception as e:
    logger.warning(
        "Error persisting to blob storage (memory cache still updated)",
        extra={"trace_id": trace_id, "error": str(e)},
    )
```

```python
# FIX — add a metric counter so degradation is observable:
except Exception as e:
    logger.warning(
        "Error persisting to blob storage (memory cache still updated)",
        extra={"trace_id": trace_id, "error": str(e)},
    )
    record_processing_error(
        worker="claimx-event-ingester",
        error_type="blob_dedup_write_failure",
    )
```

#### H5c. Partition revoke callback exceptions swallowed
**File**: `src/pipeline/common/eventhub/consumer.py:489-495`

Verify the exception handler in `_on_partition_close` logs the error. If it only logs at debug level, upgrade to warning.

---

### H6. Missing failure scenario tests

Create these test files:

- [ ] `tests/pipeline/claimx/handlers/test_error_scenarios.py` — 429/5xx API responses, circuit breaker trips
- [ ] `tests/pipeline/claimx/writers/test_delta_entities_failures.py` — partial multi-table write failures, concurrent write conflicts
- [ ] `tests/pipeline/claimx/workers/test_download_worker_errors.py` — presigned URL expiration during download, network failures
- [ ] `tests/pipeline/common/test_consumer_failures.py` — reconnection loop, checkpoint failure fallback, partition rebalance
- [ ] `tests/core/security/test_ssl_bypass_guard.py` — production guard with unset env vars

---

## Medium Priority

### M1. Hardcoded tuning values — extract to config

#### M1a. Delta events worker
**File**: `src/pipeline/claimx/workers/delta_events_worker.py:96-100`

```python
# CURRENT:
self.batch_size = 100
self.batch_timeout_seconds = 30.0
self._retry_delays = [300, 600, 1200, 2400]
```

```python
# FIX — load from config:
self.batch_size = config.get_worker_setting(domain, "delta_events", "batch_size", default=100)
self.batch_timeout_seconds = config.get_worker_setting(domain, "delta_events", "batch_timeout_seconds", default=30.0)
```

#### M1b. Entity delta worker
**File**: `src/pipeline/claimx/workers/entity_delta_worker.py:92-97`

```python
# CURRENT:
self.batch_size = 100
self.batch_timeout_seconds = 30.0
self.max_retries = 3
self._retry_delays = [60, 300, 900]
```

#### M1c. Enrichment worker
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:105-106`

```python
# CURRENT:
self.batch_size = 50
self.batch_timeout_ms = 1000
```

#### M1d. Download worker timeout
**File**: `src/pipeline/claimx/workers/download_worker.py:708`

```python
# CURRENT:
timeout=300,
```

#### M1e. Upload worker concurrency
**File**: `src/pipeline/claimx/workers/upload_worker.py:116-117`

```python
# CURRENT:
self.concurrency = 10
self.batch_size = 100
```

#### M1f. Dedup cache tuning
**File**: `src/pipeline/claimx/workers/event_ingester.py:98-106`

```python
# CURRENT:
self._dedup_cache_ttl_seconds = 86400  # 24 hours
self._dedup_cache_max_size = 100_000
self._blob_semaphore = asyncio.Semaphore(50)
```

#### M1g. ITEL routing config
**File**: `src/pipeline/claimx/workers/enrichment_worker.py:292-294`

```python
# CURRENT:
ITEL_TASK_IDS = {32615, 24454}
ITEL_PENDING_TOPIC = "pcesdopodappv1-itel-cabinet-pending"
ITEL_CABINET_DAMAGE_QUESTION = "Kitchen cabinet damage present"
```

Move all to `pipeline_config.py` or `config.yaml` with sensible defaults.

---

### M2. Incomplete metrics — add observability gaps ✅ DONE

**File**: `src/pipeline/common/metrics.py`

Add:
- [ ] ClaimX API latency histograms (per-handler, per-endpoint)
- [ ] Dedup cache hit/miss rate counters
- [ ] Blob dedup write failure counter (see H5b)
- [ ] Retry-per-message tracking (retries before success/DLQ)
- [ ] Per-table Delta write latency histograms
- [ ] EventHub partition rebalance duration

---

### M3. In-memory retry queue loses messages on crash
**File**: `src/pipeline/common/retry/unified_scheduler.py:86-93,146-148`

```python
# CURRENT:
# Crash safety:
# - In-memory queue persisted to disk every 10 seconds

base_dir = persistence_dir or "/tmp"
persistence_file = Path(base_dir) / f"{PERSISTENCE_FILE_PREFIX}{domain}_retry_queue.json"
self._delay_queue = DelayQueue(domain, persistence_file)
```

**Fix**:
1. Reduce `persistence_interval_seconds` from 10 to 2-3 seconds
2. Use a durable path (blob storage or mounted volume) instead of `/tmp`
3. Document the trade-off in operational runbooks

---

### M4. Config fails open on unresolved env vars
**File**: `src/config/config.py:64-85`

```python
# CURRENT — logs warning, returns unexpanded string:
if "${" in expanded and "}" in expanded:
    logger.warning(
        "Config value contains unexpanded environment variable: %s. "
        "Ensure the environment variable is set before starting the worker.",
        expanded,
    )
result = expanded
```

**Fix** — Add a startup validation function that scans all resolved config and fails hard:

```python
def validate_no_unresolved_vars(config: dict, path: str = "") -> None:
    """Fail fast if any config values contain unexpanded ${VAR} references."""
    for key, value in config.items():
        full_key = f"{path}.{key}" if path else key
        if isinstance(value, dict):
            validate_no_unresolved_vars(value, full_key)
        elif isinstance(value, str) and "${" in value and "}" in value:
            raise ConfigurationError(
                f"Unresolved env var in config key '{full_key}': {value}"
            )
```

Call this at worker startup after loading config.

---

### M5. Downstream dedup gap — enrichment/entity workers don't deduplicate
**Files**:
- `src/pipeline/claimx/workers/event_ingester.py` — deduplicates ✓
- `src/pipeline/claimx/workers/enrichment_worker.py` — no dedup ✗
- `src/pipeline/claimx/workers/entity_delta_worker.py` — no dedup ✗

If EventHub redelivers after ingester checkpoint but before enrichment completes, duplicates reach downstream workers. For merge-based tables this is idempotent, but append-only tables (`contacts`, `media`) get duplicate rows.

**Fix**: Addressed by H3 (converting append-only tables to merge). Alternatively, add trace_id-based dedup in entity_delta_worker.

---

### M6. Presigned URL expiration not checked before download
**File**: `src/core/security/presigned_urls.py:46-50`

```python
# DEFINED but never called in production code:
def expires_within(self, seconds: int) -> bool:
    """Check if URL expires within N seconds (for buffer logic)."""
    if not self.expires_at:
        return False
    return datetime.now(UTC) + timedelta(seconds=seconds) >= self.expires_at
```

**File**: `src/core/download/downloader.py:88-92`

```python
# CURRENT — only checks already-expired, not about-to-expire:
url_info = check_presigned_url(url)
if not (url_info.url_type == "s3" and url_info.is_expired):
    return None
```

**Fix** — In `download_worker.py`, before starting download, check buffer:

```python
url_info = check_presigned_url(task_message.download_url)

# Check if URL will expire during the download window
download_timeout = 300  # seconds
if url_info.expires_within(download_timeout):
    logger.info(
        "Presigned URL expires within download window, requesting refresh",
        extra={
            "trace_id": task_message.trace_id,
            "seconds_remaining": url_info.seconds_remaining,
            "download_timeout": download_timeout,
        },
    )
    # Route to retry handler which will refresh the URL
    return DownloadOutcome(
        success=False,
        error="presigned_url_expiring",
        error_category=ErrorCategory.TRANSIENT,
    )
```

Also extend the check to cover ClaimX URLs (currently only S3 URLs are validated in `downloader.py:91`).

---

### M7. No startup resource validation ✅ DONE

Workers start successfully even when dependencies are unreachable. Failures only surface minutes later during message processing.

**Fix** — Add a `_validate_resources()` method called during each worker's `start()`:

```python
async def _validate_resources(self) -> None:
    """Fail fast if critical dependencies are unreachable."""
    # Test EventHub connectivity
    if self.producer:
        await asyncio.wait_for(self.producer.ping(), timeout=10.0)

    # Test Delta table path accessibility
    if self.delta_writer:
        await asyncio.wait_for(self.delta_writer.validate_path(), timeout=10.0)

    # Test API connectivity (enrichment worker only)
    if hasattr(self, 'api_client'):
        await asyncio.wait_for(self.api_client.health_check(), timeout=10.0)
```

---

### M8. Checkpoint store silently falls back to in-memory
**File**: `src/pipeline/common/transport.py:432-451`

```python
# CURRENT — logs error but continues with in-memory:
except Exception as e:
    logger.error(
        f"Failed to initialize checkpoint store for Event Hub {consumer_label}: "
        f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
        f"Falling back to in-memory checkpointing. Error: {e}"
    )
```

**Fix** — Fail hard in production:

```python
except Exception as e:
    env = os.getenv("ENVIRONMENT", "").lower()
    if env == "production":
        raise RuntimeError(
            f"Checkpoint store initialization failed in production: {e}. "
            f"In-memory fallback is not safe for production deployments."
        ) from e
    logger.error(
        f"Failed to initialize checkpoint store for Event Hub {consumer_label}: "
        f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
        f"Falling back to in-memory checkpointing. Error: {e}"
    )
```
