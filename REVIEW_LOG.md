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

## Up Next

### 3. Workers (`src/pipeline/claimx/workers/`)
- `event_ingester.py` — EventHub consumer → enrichment task producer
- `enrichment_worker.py` — Enrichment task consumer → API calls → entity/download tasks
- `download_worker.py` — Download task consumer → file downloads
- `download_factory.py` — Download task construction
- `upload_worker.py` — Cached file → OneLake upload
- `delta_events_worker.py` — Event data → Delta Lake writes
- `entity_delta_worker.py` — Entity data → Delta Lake writes
- `result_processor.py` — Upload result processing

### 4. Retry (`src/pipeline/claimx/retry/`)
- `enrichment_handler.py` — Enrichment retry/DLQ routing
- `download_handler.py` — Download retry/DLQ routing (partially reviewed in schemas pass)

### 5. Writers (`src/pipeline/claimx/writers/`)
- `delta_entities.py` — Entity rows → Delta table writes
- `delta_events.py` — Event data → Delta table writes

### 6. API Client (`src/pipeline/claimx/`)
- `api_client.py` — ClaimX REST API client (auth, endpoints, error handling)

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
