# ClaimX Pipeline — Production Readiness Review Log

Tracking review progress layer-by-layer through the ClaimX pipeline.

## Completed

### 1. Schemas (`src/pipeline/claimx/schemas/`)
- **Reviewed:** 2026-03-05
- **Commit:** `f88e9f2`
- **Files:** `__init__.py`, `events.py`, `task_event.py`, `video_event.py`, `results.py`, `tasks.py`, `entities.py`, `cached.py`
- **Issues found:** 3 critical, 6 high, 5 medium, 4 low — all fixed
- **Tests added:** `test_task_event.py`, `test_video_event.py`
- **Docs created:** `docs/claimx/schemas.md`
- **Key fixes:**
  - Timezone-naive `datetime.now()` fallback → `datetime.now(UTC)`
  - `task_assignment_id` type mismatch (`int` → `str`) across schema + transformer
  - Added missing `final_error` field to `FailedDownloadMessage` + wired into download handler
  - Normalized `expires_at` (`str` → `datetime`) and `total_time_seconds` (`str` → `float`) with backward-compat validators
  - Enforced `error_message` max_length and `error_category` Literal types on DLQ models
  - Added missing `__init__.py` exports, `created_at` timestamps, docstrings, field validators

---

### 2. Handlers (`src/pipeline/claimx/handlers/`)
- **Reviewed:** 2026-03-05
- **Files:** `base.py`, `project.py`, `project_update.py`, `project_cache.py`, `media.py`, `task.py`, `video.py`, `transformers.py`, `utils.py`, `__init__.py`
- **Issues found:** 3 critical, 4 high, 6 medium, 3 low — all fixed
- **Tests added/updated:** 8 new tests, 2 updated
- **Key fixes:**
  - **C1:** `build_task_event()` extracted address from `customer` instead of `project` — all downstream task event address fields were empty
  - **C2:** `MediaHandler.process()` silently dropped events when `asyncio.gather` returned an exception for a group — now creates error `EnrichmentResult` for every event in the failed group
  - **C3:** `ProjectUpdateHandler` returned retryable error for unknown event types, causing infinite retry — now `PERMANENT` / non-retryable
  - **H1:** `TaskHandler` and `VideoCollabHandler` returned `success=True` when EventHub production failed — now returns failure with `TRANSIENT` category
  - **H2:** `HandlerRegistry.get_handler()` omitted `project_cache`, `task_event_producer`, `video_event_producer` — added optional kwargs passthrough
  - **H3:** `int(m)` on non-numeric media_id raised `ValueError` — replaced with `safe_int` filter
  - **H4:** `exc_info=True` outside except block captured no traceback — changed to `exc_info=result`
  - **M1:** Misleading `logger.error` with `exc_info=True` for None check → `logger.warning`
  - **M2:** `EnrichmentResult.rows` typed `EntityRowsMessage | None` but never actually None — simplified type and removed `__post_init__`
  - **M3:** `or` fallback masked zero values for `project_id`/`assignment_id` — explicit `is not None` checks
  - **M4:** Unguarded `json.dumps` on task form response — added `_safe_json_dumps` with `TypeError`/`ValueError` fallback
  - **M5:** Redundant truthiness check inside guarded block — simplified
  - **L1:** Added `logger.warning` for unknown event type rejection
  - **L2:** `project_cache.load_from_ids` parameter type `list[str]` → `list[str | int]`
  - **L3:** Manual dict counting → `Counter`
  - Added docstrings across `utils.py` (all `safe_*` converters, timestamp functions), `base.py` (registry methods, `process`), `transformers.py` (target Delta tables, response shapes), `media.py` (fetch strategy, error attribution), `task.py` (multi-entity extraction)

---

### 3. Workers (`src/pipeline/claimx/workers/`)
- **Reviewed:** 2026-03-05
- **Files:** `entity_delta_worker.py`, `enrichment_worker.py`, `delta_events_worker.py`, `result_processor.py`, `download_worker.py`, `download_factory.py`, `upload_worker.py`, `event_ingester.py`
- **Issues found:** 3 critical, 5 high, 6 medium, 4 low — 13 fixed, 5 deferred
- **Tests added/updated:** 11 new tests, 1 updated
- **Key fixes:**
  - **C1:** `entity_delta_worker._flush_batch()` — dead `self._batch.copy()` result discarded; removed
  - **C2:** `entity_delta_worker._handle_message()` — parse failure silently swallowed; now raises `PermanentError`
  - **C3:** `enrichment_worker._preflight_project_check()` — only first task passed to dispatch; now passes all tasks
  - **H1:** `result_processor._flush_batch()` — all flush errors hardcoded as "transient"; added `_classify_flush_error()` method
  - **H3:** `download_worker._process_single_task()` — `task_message=None` on parse failure; now creates sentinel `ClaimXDownloadTask`
  - **H4:** `enrichment_worker._fetch_and_merge_project_rows()` — bare `except: pass`; now logs warning with project_id
  - **H5:** `delta_events_worker._handle_failed_batch()` — transient error kept batch intact causing unbounded growth; now clears batch after retry send
  - **M1:** `enrichment_worker._tally_and_route_results()` — preserves original error type when available
  - **M3:** `entity_delta_worker._handle_flush_error()` — `error_category` passed as enum to `handle_batch_failure` expecting str; now passes `.value`
  - **M5:** `download_factory.create_download_tasks_from_media()` — validates `media_id` non-empty, skips with warning if empty
  - **M6:** `upload_worker.start()` — removed `loop.set_default_executor()` that modified global event loop state
  - **L1:** `event_ingester._parse_and_dedup_events()` — `latest_timestamp` now tracks actual max instead of last event
  - **L2:** Added docstring to `entity_delta_worker._handle_flush_error` (already present)
- **Deferred:**
  - **H2:** Batch data "loss" on flush error mitigated by consumer redelivery
  - **M2:** Private `_validate_pre_download` access — internal to project, out of scope
  - **M4:** Failed results not written to Delta inventory — feature request
  - **L3:** Hardcoded health ports — consistent pattern, broader refactor needed
  - **L4:** Hardcoded ITEL task IDs — acceptable for now

---

### 4. Retry (`src/pipeline/claimx/retry/`)
- **Reviewed:** 2026-03-05
- **Files:** `enrichment_handler.py`, `download_handler.py`
- **Issues found:** 2 high, 3 medium, 2 low — all fixed
- **Tests added:** 6 new tests (3 per handler)
- **Key fixes:**
  - **H1:** `_retry_delays[retry_count]` IndexError when `max_retries > len(delays)` — clamped to last delay + `__init__` warning
  - **H2:** `handle_failure()` called `.send()` on `None` producers before `start()` — added `RuntimeError` guard
  - **M1:** `download_handler` used `str(error)[:500]` instead of `truncate_error_message()` — fixed for consistency
  - **M2:** `enrichment_handler._send_to_dlq` never called `record_dlq_message()` — added import + call
  - **M3:** `download_handler` DLQ headers missing `trace_id` kwarg — added `trace_id=task.trace_id`
  - **L1:** `download_handler._send_to_dlq` malformed docstring — added `Args:`/`Raises:` sections
  - **L2:** Unused `retry_topic` variable in both handlers — removed from code and log extras

---

### 5. Writers (`src/pipeline/claimx/writers/`)
- **Reviewed:** 2026-03-05
- **Files:** `delta_entities.py`, `delta_events.py`, `__init__.py`
- **Issues found:** 2 high, 2 medium, 2 low — all fixed
- **Tests added:** 7 new tests
- **Key fixes:**
  - **H3:** `write_all` returned only counts, caller committed offsets on partial failure — now returns `(counts, failed_tables)` tuple; `entity_delta_worker` skips commit when tables fail
  - **H4:** Columns not in `TABLE_SCHEMAS` silently dropped — now logs warning with dropped column names
  - **M5:** `_coerce_value` only handled Datetime/Date strings — extended for `Int32`/`Int64`/`Float64`/`Boolean`
  - **M6:** `pl.lit(now)` could lose UTC timezone — added explicit `.cast(pl.Datetime("us", "UTC"))`
  - **L6:** `TABLE_SCHEMAS` not exported from `writers/__init__.py` — added to exports
  - **L7:** `sample_event` in log extras may contain PII — removed from `delta_events.py`
- **Deferred:**
  - **L3:** `"unauthorized"` in URL expiration indicators — needs product decision
  - **L5:** Contacts in `_APPEND_ONLY_TABLES` despite having merge keys — needs product confirmation
  - **L4:** `TABLE_SCHEMAS` type annotation style (class refs vs instances) — cosmetic

---

### 6. API Client (`src/pipeline/claimx/api_client.py`)
- **Reviewed:** 2026-03-05
- **Files:** `api_client.py`, `core/resilience/circuit_breaker.py`, `handlers/video.py`
- **Issues found:** 2 high, 4 medium, 3 low — all fixed
- **Tests updated:** `test_api_client.py` (signature changes, param name fix)
- **Key fixes:**
  - **H1:** `get_video_collaboration(project_id: str)` inconsistent with all other methods (`int`) — changed to `int`, removed `int(project_id)` cast, updated handler call site
  - **H2:** `_request` return type `dict[str, Any]` wrong — `response.json()` can return `list`, `int`, etc. Changed to `Any`
  - **M1:** Hardcoded default `sender_username="nsmkd@allstate.com"` (PII) — changed to `""`
  - **M2:** `_auth_retry` parameter in `_request()` was dead code — removed
  - **M3:** `if media_ids:` skipped empty list `[]` — changed to `if media_ids is not None:`
  - **M4:** Only HTTP 200 treated as success — changed to `200 <= status < 300` for all 2xx
  - **L1:** Added missing docstrings to `get_project_contacts`, `get_project_tasks`, `get_project_conversations`
  - **L2:** Private `self._circuit._get_retry_after()` access — added public `get_retry_after()` to `CircuitBreaker`
  - **L3:** Deprecated `asyncio.get_event_loop().time()` — replaced with `asyncio.get_running_loop().time()`
- **Bonus:** Fixed pre-existing test bug: `mediaIds` → `mediaId` param name mismatch; fixed import sort order

---

## Up Next

### 7. Pipeline Initialization & Orchestration
- `src/pipeline/__main__.py` — Entry point: arg parsing, env setup, logging, signal handlers, worker startup
- `src/pipeline/__init__.py` — Package docs, version
- `src/pipeline/runners/registry.py` — Worker registry (CLI name → runner function mapping)
- `src/pipeline/runners/common.py` — Shared execution patterns (shutdown handling, retry, error mode)
- `src/pipeline/runners/claimx_runners.py` — ClaimX worker runners
- `src/pipeline/runners/verisk_runners.py` — XACT/Verisk worker runners
- `src/pipeline/runners/plugin_runners.py` — Plugin runners (iTel Cabinet, EventHub UI)
- `src/config/__init__.py` — Config API (singleton, YAML loading)
- `src/config/config.py` — Core config classes, env var expansion
- `src/config/pipeline_config.py` — EventHub + Delta Lake path configuration

### 8. Shared Infrastructure (`src/pipeline/common/`)
- EventHub consumer/producer
- Delta Lake storage
- OneLake storage
- Retry infrastructure
- Metrics, health, monitoring

---

## Review Checklist (per layer)
- [ ] Read all files, identify bugs and issues
- [ ] Fix critical and high-severity issues
- [ ] Fix medium and low issues
- [ ] Add inline docstrings where missing
- [ ] Add missing tests
- [ ] Create standalone docs (`docs/claimx/<layer>.md`)
- [ ] Run tests, ruff, mypy
- [ ] Verify backward compatibility
