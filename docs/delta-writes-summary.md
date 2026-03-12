# Delta Table Writes Summary

All delta table writes across the codebase, organized by domain.

## Verisk Domain

| Table | Strategy | Merge Keys | Writer File |
|-------|----------|------------|-------------|
| `xact_events` | **APPEND** | N/A | `src/pipeline/verisk/writers/delta_events.py` |
| `xact_attachments` | **APPEND** | N/A | `src/pipeline/verisk/writers/delta_inventory.py` |
| `xact_attachments_failed` | **MERGE** | `media_id` | `src/pipeline/verisk/writers/delta_inventory.py` |

### xact_events

- **Strategy:** Append
- **Partition column:** `event_date`
- **Z-order columns:** `event_date`, `trace_id`, `event_id`, `type`
- **Worker:** `DeltaEventsWorker` consuming from `enrichment_pending` topic
- **Notes:** Raw Eventhouse events are flattened and appended. Deduplication is handled by a separate daily maintenance job.

### xact_attachments

- **Strategy:** Append
- **Partition column:** `event_date`
- **Worker:** `ResultProcessor` consuming from `downloads.results` topic
- **Notes:** Append-only inventory table tracking where downloaded files are stored in OneLake. Records include media_id, trace_id, attachment_url, blob_path, file_type, status_subtype, assignment_id, bytes_downloaded, downloaded_at, and created_at.

### xact_attachments_failed

- **Strategy:** Merge (upsert on `media_id`)
- **Z-order columns:** `media_id`, `failed_at`
- **Preserved columns on update:** `created_at`
- **Worker:** `ResultProcessor` consuming from `downloads.results` topic
- **Notes:** Tracks permanent download failures for monitoring and potential replay. Merge provides idempotent upsert semantics.

---

## ClaimX Domain

| Table | Strategy | Merge Keys | Writer File |
|-------|----------|------------|-------------|
| `claimx_events` | **APPEND** | N/A | `src/pipeline/claimx/writers/delta_events.py` |
| `claimx_projects` | **MERGE** | `project_id` | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_contacts` | **MERGE** | `project_id`, `contact_email`, `contact_type` | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_attachment_metadata` | **MERGE** | `media_id` | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_tasks` | **MERGE** | `assignment_id` | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_task_templates` | **MERGE** | `task_id` (conditional on `modified_date`) | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_external_links` | **MERGE** | `link_id` | `src/pipeline/claimx/writers/delta_entities.py` |
| `claimx_video_collab` | **MERGE** | `video_collaboration_id` | `src/pipeline/claimx/writers/delta_entities.py` |

### claimx_events

- **Strategy:** Append
- **Partition column:** `event_date`
- **Z-order columns:** `project_id`
- **Worker:** `ClaimXDeltaEventsWorker` consuming from `enrichment_pending` topic
- **Notes:** Append-only event log. Deduplication handled upstream by EventHub.

### ClaimX Entity Tables (7 tables)

All entity tables are written by `ClaimXEntityDeltaWorker` consuming from the `enriched` topic. They share common characteristics:

- **Strategy:** Merge (upsert)
- **Preserved columns on update:** `created_at`
- **Concurrency:** All 7 tables are written concurrently via `asyncio.gather()`
- **Batch settings:** Size-based (default 100) or timeout-based (30 seconds)

**Special behavior:**
- `claimx_task_templates` has a conditional update — rows are only updated when `modified_date` has changed.

---

## Write Strategy Totals

| Strategy | Count | Tables |
|----------|-------|--------|
| **APPEND** | 3 | `xact_events`, `xact_attachments`, `claimx_events` |
| **MERGE** | 8 | `xact_attachments_failed`, `claimx_projects`, `claimx_contacts`, `claimx_attachment_metadata`, `claimx_tasks`, `claimx_task_templates`, `claimx_external_links`, `claimx_video_collab` |

---

## Infrastructure

- **Core write implementation:** `src/pipeline/common/storage/delta.py` — append (~line 574), merge (~line 700)
- **Base writer abstraction:** `src/pipeline/common/writers/base.py` — async subprocess execution
- **Process pool:** `ProcessPoolExecutor` with max 4 workers (forkserver context) to avoid GIL contention from Rust-based deltalake library
- **Write timeout:** 300 seconds per operation
- **Retry policy:** 3 max attempts, 1–10 second exponential backoff; permanent failures route to dead-letter queue
- **All tables** use partition pruning and z-ordering for query performance
