# ClaimX Pipeline — Production Readiness Review

**Date**: 2026-03-08
**Scope**: End-to-end walkthrough of ClaimX pipeline, all stages (ingestion → enrichment → Delta writes → download → upload → result processing)

---

## Executive Summary

The ClaimX pipeline is architecturally sound with good separation of concerns, comprehensive schema validation, and proper error classification. However, several production hardening gaps must be addressed before go-live, organized below by priority.

**Critical**: 1 issue (SSL fail-open)
**High**: 5 issues (timeouts, atomicity, silent failures, missing tests)
**Medium**: 8 issues (config, metrics, crash loss, dedup gaps, URL expiration)

---

## Critical

### C1. SSL bypass activates when environment variables are unset
**File**: `src/core/security/ssl_dev_bypass.py:108-119`

The production guard checks `ENVIRONMENT == "production"` or `APP_ENV == "production"`. If **both are unset** (empty string), the guard passes and SSL bypass activates whenever `DISABLE_SSL_VERIFY=true`. A misconfigured production deployment missing these env vars would silently disable TLS verification.

**Fix**: Invert the logic — only allow bypass when `ENVIRONMENT` or `APP_ENV` explicitly equals `"development"` or `"local"`. Fail-closed.

```python
# Current (fail-open):
if env == "production" or app_env == "production":
    logger.error("Refusing to disable SSL in production.")
    return

# Proposed (fail-closed):
ALLOWED_ENVS = {"development", "local", "dev", "test"}
if env not in ALLOWED_ENVS and app_env not in ALLOWED_ENVS:
    logger.error(
        "DISABLE_SSL_VERIFY is set but ENVIRONMENT=%s / APP_ENV=%s "
        "is not a recognized development environment. "
        "Refusing to disable SSL verification.", env, app_env
    )
    return
```

---

## High Priority

### H1. No async operation timeouts on API calls and long-running ops
**Files**:
- `src/pipeline/claimx/workers/enrichment_worker.py:529` — `handler.handle_batch()` no timeout
- `src/pipeline/claimx/workers/enrichment_worker.py:876` — `handler.process()` no timeout
- `src/pipeline/claimx/workers/enrichment_worker.py:980,1036` — `producer.send()` no timeout
- `src/pipeline/claimx/workers/upload_worker.py:527` — `onelake_client.async_upload_file()` no timeout
- `src/pipeline/claimx/workers/result_processor.py:426` — `inventory_writer._async_merge()` no timeout
- `src/pipeline/common/eventhub/consumer.py:339` — `consumer.close()` no timeout
- `src/pipeline/common/eventhub/consumer.py:864` — `dlq_producer.send()` no timeout

If any external call hangs (ClaimX API, OneLake, EventHub), the worker deadlocks. No watchdog or circuit breaker triggers because the async task never completes or raises.

**Fix**: Wrap all external async calls with `asyncio.wait_for(coro, timeout=N)`. Suggested timeouts:
- API handler calls: 120s
- Producer sends: 30s
- OneLake uploads: 300s
- Delta merges: 300s (already have subprocess timeout, but add outer guard)
- Consumer/producer lifecycle: 60s

### H2. Handlers don't retry transient API errors locally
**Files**:
- `src/pipeline/claimx/handlers/project.py:126-148`
- `src/pipeline/claimx/handlers/task.py:217-234`
- `src/pipeline/claimx/handlers/media.py:280-283`
- `src/pipeline/claimx/handlers/video.py:133-154`
- `src/pipeline/claimx/handlers/project_update.py:154-175`

A transient 503 from the ClaimX API causes the entire message to be routed through pipeline-level retry (re-enqueue to EventHub with backoff delays of 5m/10m/20m/40m). A simple 1-2 second local retry with 2-3 attempts would resolve most transient errors immediately, avoiding the 5-minute minimum delay.

**Fix**: Add a local retry decorator or utility for API calls in the handler base class:
```python
async def _call_api_with_retry(self, coro_factory, max_attempts=3, base_delay=1.0):
    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except ClaimXApiError as e:
            if not e.retryable or attempt == max_attempts - 1:
                raise
            await asyncio.sleep(base_delay * (2 ** attempt))
```

### H3. Multi-table Delta writes lack atomicity
**File**: `src/pipeline/claimx/writers/delta_entities.py:337-367`

`write_all()` uses `asyncio.gather(..., return_exceptions=True)` to write 7 entity tables concurrently. If 3 tables succeed and 4 fail:
- No rollback on succeeded tables
- Consumer offset is NOT committed (correct), so messages will be reprocessed
- But reprocessing causes **duplicate rows** in the 3 tables that already succeeded (merge-on-key mitigates for most tables, but `contacts` and `media` use append-only writes per line 370)

**File**: `src/pipeline/claimx/writers/delta_entities.py:370`
```python
_APPEND_ONLY_TABLES = frozenset({"contacts", "media"})
```

**Fix (two options)**:
1. **Preferred**: Change `contacts` and `media` to merge-based writes (like other tables) to make reprocessing idempotent. This eliminates the duplicate risk entirely.
2. **Alternative**: Write tables sequentially with a partial-success tracking mechanism that records which tables completed, allowing targeted retry of only failed tables.

### H4. Silent exception swallowing
**Files**:
- `src/pipeline/common/writers/base.py:148-149` — Bare `except Exception: pass` when killing stuck process pool workers. If `proc.kill()` fails, dead processes leak Azure storage locks with zero indication.
- `src/pipeline/claimx/workers/enrichment_worker.py:1053-1063` — Download task production failures are logged but **not routed to retry or DLQ**. Failed download tasks silently vanish — media files never get downloaded.
- `src/pipeline/claimx/workers/event_ingester.py:654-658` — Blob dedup persistence failures are fire-and-forget. If blob write fails, memory cache succeeds but restart loses dedup state, causing duplicate processing.
- `src/pipeline/common/eventhub/consumer.py:487-492` — Partition revoke callback exceptions swallowed.

**Fix**: At minimum, add logging to `base.py:148`. For `enrichment_worker.py:1053-1063`, route individual download task production failures to the retry handler (this is a data loss bug). For `event_ingester.py`, add a metric counter for blob write failures.

### H5. Missing failure scenario tests
**Existing test files cover** happy paths well, but no tests exist for:
- 429/5xx API responses in handlers (rate limiting, server errors)
- Delta write conflicts and partial multi-table failures
- Presigned URL expiration during download
- EventHub producer send failures during enrichment
- Checkpoint store initialization failure (in-memory fallback)
- Process pool `BrokenProcessPool` recovery

**Fix**: Add test files:
- `tests/pipeline/claimx/handlers/test_error_scenarios.py`
- `tests/pipeline/claimx/writers/test_delta_entities_failures.py`
- `tests/pipeline/claimx/workers/test_download_worker_errors.py`

---

## Medium Priority

### M1. Hardcoded tuning values throughout
Many operational parameters are baked into code rather than loaded from config:

| File | Line | Value | Description |
|------|------|-------|-------------|
| `workers/delta_events_worker.py` | 96-97 | `batch_size=100`, `timeout=30s` | Delta events batching |
| `workers/entity_delta_worker.py` | 92-93 | `batch_size=100`, `timeout=30s` | Entity batching |
| `workers/enrichment_worker.py` | 105-106 | `batch_size=50`, `timeout=1000ms` | Enrichment batching |
| `workers/download_worker.py` | 709 | `timeout=300s` | Download timeout |
| `workers/upload_worker.py` | 115-116 | `concurrency=10`, `batch=100` | Upload tuning |
| `workers/result_processor.py` | 141 | `health_port=8087` | Health check port |
| `workers/event_ingester.py` | 99-100 | `cache_ttl=86400`, `max_size=100000` | Dedup cache |
| `workers/event_ingester.py` | 107 | `Semaphore(50)` | Blob concurrency |

**Fix**: Move all tuning values to `pipeline_config.py` or `config.yaml` with sensible defaults. This enables tuning without redeployment.

### M2. Incomplete metrics coverage
**File**: `src/pipeline/common/metrics.py`

Missing:
- ClaimX API latency histograms (per-handler, per-endpoint)
- Circuit breaker state change events
- Retry-per-message tracking (how many retries before success/DLQ)
- Dedup cache hit/miss rates
- Blob dedup write failure counter
- EventHub partition rebalance duration
- Per-table Delta write latency

**Fix**: Add targeted histogram/counter metrics in handlers and workers.

### M3. In-memory delayed message queue loses messages on crash
**File**: `src/pipeline/common/retry/unified_scheduler.py:86-93,146`

Documented trade-off: up to 10 seconds of delayed messages lost on crash. Persistence file is in `/tmp` which survives container restart but not host reboot.

**Fix**: Reduce persistence interval to 2-3 seconds. Consider using a durable store (blob storage) for persistence instead of local `/tmp`. At minimum, document this limitation in operational runbooks.

### M4. Config fails open on unresolved env vars
**File**: `src/config/config.py:77-82`

`get_config_value()` logs a warning when `${VAR}` syntax remains unexpanded but returns the unexpanded string. Downstream code receives literal `${MY_SECRET}` as a connection string, causing confusing runtime errors far from the root cause.

**Fix**: Add a startup validation pass that scans all resolved config values for unexpanded `${...}` patterns and fails hard before any workers start:
```python
def validate_config_no_unresolved(config: dict) -> None:
    for key, value in flatten(config):
        if isinstance(value, str) and "${" in value:
            raise ConfigurationError(f"Unresolved env var in config key '{key}': {value}")
```

### M5. Downstream dedup gap
**File**: `src/pipeline/claimx/workers/event_ingester.py` — deduplicates at ingestion.
**Files**: `workers/enrichment_worker.py`, `workers/entity_delta_worker.py` — no deduplication.

If EventHub redelivers a message after the ingester checkpoint but before enrichment completes, the enrichment and entity workers process it again. For merge-based tables this is idempotent, but `contacts` and `media` (append-only, per `delta_entities.py:370`) will produce duplicates.

**Fix**: Same as H3 — convert append-only tables to merge-based writes. Alternatively, add a lightweight dedup check (trace_id-based) in the entity delta worker.

### M6. Presigned URL expiration not checked before download
**File**: `src/core/security/presigned_urls.py:46-50` — `expires_within()` is defined and tested but **never called** in production code.
**File**: `src/core/download/downloader.py:90` — only checks `is_expired` (already expired), not `expires_within()`.

A URL that expires in 5 seconds will start a 300-second download that fails partway through, wasting bandwidth and time. The retry handler then refreshes the URL (good), but the initial wasted attempt is avoidable.

**Fix**: In `download_worker.py`, before starting a download, check `url_info.expires_within(download_timeout_seconds)` and proactively refresh URLs that will expire during the transfer window.

### M7. No startup resource validation
Workers don't test connectivity to EventHub, Delta tables, or ClaimX API at boot. Failures only surface during message processing, potentially minutes after startup.

**Fix**: Add a `_validate_resources()` method called during `start()` that:
- Tests EventHub producer/consumer connectivity
- Verifies Delta table paths are accessible
- Pings ClaimX API health endpoint
- Fails fast with clear error messages

### M8. Checkpoint store silently falls back to in-memory
**File**: `src/pipeline/common/transport.py:432-451`

If blob checkpoint store initialization fails, the consumer silently falls back to in-memory checkpointing. On restart, all offsets are lost and messages are reprocessed from `@latest` (or `@earliest` depending on config).

**Fix**: Make this a hard failure in production. If `ENVIRONMENT == "production"` and checkpoint store fails, raise instead of falling back. In-memory checkpoint should only be allowed in development.

---

## Test Coverage Gaps

| Area | Has Tests | Missing Tests |
|------|-----------|---------------|
| Schemas (events, tasks, entities, results, cached) | Yes | Edge cases for malformed raw_data |
| Handlers (project, media, task, video, project_update) | Yes | 429/5xx responses, circuit breaker trips |
| Workers (all 7) | Yes | Timeout scenarios, partial batch failures |
| Writers (delta_events, delta_entities) | Yes | Multi-table partial failure, concurrent write conflicts |
| Retry handlers (enrichment, download) | Yes | URL refresh failure, DLQ routing edge cases |
| Consumer/Transport | No dedicated tests | Reconnection loop, checkpoint failure, partition rebalance |
| SSL bypass | No | Production guard bypass scenarios |
| Config validation | No | Unresolved env var detection, empty path validation |

---

## Recommended Fix Order

1. **C1** — SSL fail-closed (quick fix, highest risk)
2. **H4** — Download task production failure routing (data loss bug)
3. **H1** — Add timeouts to critical async operations
4. **H3/M5** — Convert append-only tables to merge-based writes
5. **M4** — Config startup validation
6. **M8** — Checkpoint store hard failure in production
7. **H2** — Local handler retry for transient API errors
8. **M6** — Presigned URL expiration pre-check
9. **M1** — Extract hardcoded values to config
10. **M2** — Add missing metrics
11. **H5** — Failure scenario tests
12. **M7** — Startup resource validation
13. **M3** — Reduce retry scheduler persistence interval
