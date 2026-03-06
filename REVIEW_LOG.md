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

## Up Next

### 2. Handlers (`src/pipeline/claimx/handlers/`)
- `base.py` — Base handler class (shared error handling, retry logic)
- `project.py` — PROJECT_CREATED handler
- `project_update.py` — PROJECT_FILE_ADDED / PROJECT_MFN_ADDED handler
- `project_cache.py` — Project data caching layer
- `media.py` — Media/file download task creation
- `task.py` — CUSTOM_TASK_ASSIGNED / CUSTOM_TASK_COMPLETED handler
- `video.py` — VIDEO_COLLABORATION handler
- `transformers.py` — API response → schema transformers
- `utils.py` — Shared handler utilities (safe_str, safe_int, etc.)

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
