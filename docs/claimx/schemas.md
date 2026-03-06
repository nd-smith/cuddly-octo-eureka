# ClaimX Pipeline Schemas

## Overview

The ClaimX schemas (`src/pipeline/claimx/schemas/`) define the data contracts for the ClaimX event-driven pipeline. Every message flowing through the pipeline — from raw webhook events through enrichment, download, upload, and dead-letter queues — is validated against these Pydantic models.

All handlers, workers, and writers depend on these schemas. Changes to schemas are **breaking changes** and require coordinated updates across the pipeline.

## Data Flow

```
Webhook → EventHub
    │
    ▼
ClaimXEventMessage          (events.py)
    │                        Raw event from EventHub topic
    ▼
ClaimXEnrichmentTask        (tasks.py)
    │                        Work item for enrichment worker
    ├──► EntityRowsMessage   (entities.py)
    │     │                  Entity data for Delta table writes
    │     ▼
    │    Delta Lake
    │
    ├──► ClaimXTaskEvent         (task_event.py)
    │     │                       Enriched task event → downstream EventHub
    │     ▼
    │    EventHub (downstream consumers)
    │
    ├──► ClaimXVideoCollabEvent  (video_event.py)
    │     │                       Enriched video collab event → downstream EventHub
    │     ▼
    │    EventHub (downstream consumers)
    │
    ├──► ClaimXDownloadTask      (tasks.py)
    │     │                       Media download work item
    │     ▼
    │    ClaimXCachedDownloadMessage  (cached.py)
    │     │                           File downloaded to local cache
    │     ▼
    │    ClaimXUploadResultMessage    (results.py)
    │                                 Upload outcome (success/failure)
    │
    ├──► FailedEnrichmentMessage     (results.py)
    │                                 Enrichment failure → DLQ
    │
    └──► FailedDownloadMessage       (results.py)
                                      Download failure → DLQ
```

## Schema Reference

| Schema | File | Topic/Destination | Key Fields | Pipeline Stage |
|--------|------|-------------------|------------|----------------|
| `ClaimXEventMessage` | `events.py` | `claimx.events` (source) | `trace_id`, `event_type`, `project_id` | Ingestion |
| `ClaimXEnrichmentTask` | `tasks.py` | `claimx.enrichment.pending` | `trace_id`, `event_type`, `project_id`, `retry_count` | Enrichment |
| `EntityRowsMessage` | `entities.py` | Delta Lake tables | `trace_id`, entity lists (projects, contacts, media, etc.) | Storage |
| `ClaimXTaskEvent` | `task_event.py` | Downstream EventHub | `trace_id`, `event_type`, `project_id`, task/project fields | Output |
| `ClaimXVideoCollabEvent` | `video_event.py` | Downstream EventHub | `trace_id`, `event_type`, video session metrics | Output |
| `ClaimXDownloadTask` | `tasks.py` | `claimx.downloads.pending` | `media_id`, `download_url`, `blob_path`, `retry_count` | Download |
| `ClaimXCachedDownloadMessage` | `cached.py` | `claimx.downloads.cached` | `media_id`, `local_cache_path`, `destination_path` | Upload |
| `ClaimXUploadResultMessage` | `results.py` | `claimx.downloads.results` | `media_id`, `status`, `bytes_uploaded` | Results |
| `FailedEnrichmentMessage` | `results.py` | `claimx.enrichment.dlq` | `original_task`, `final_error`, `error_category` | DLQ |
| `FailedDownloadMessage` | `results.py` | `claimx.downloads.dlq` | `original_task`, `final_error`, `error_category` | DLQ |

## Design Decisions

### Pydantic v2 for Validation
All schemas use Pydantic `BaseModel` with strict field validation. This catches type mismatches and missing fields at construction time rather than at write/consume time.

### Backward-Compatible Evolution
Schema changes follow additive-only rules:
- New fields must have defaults
- Type changes require `mode="before"` validators to accept old formats (e.g., `expires_at` accepts ISO strings, `total_time_seconds` accepts decimal strings)
- Existing serialized messages must deserialize without errors

### ISO 8601 Timestamps
All `datetime` fields serialize to ISO 8601 via `field_serializer`. This ensures consistent formatting across Kafka topics and Delta Lake columns.

### Untyped Entity Dicts
`EntityRowsMessage` uses `list[dict[str, Any]]` for entity rows rather than typed models. This is intentional — entity schemas vary by ClaimX API version and change frequently. Validation happens at the Delta write layer instead.

### Error Category Literals
DLQ message `error_category` fields use `Literal["transient", "permanent", "auth", "circuit_open", "unknown"]` to match the `ErrorCategory` enum values used by retry handlers. This prevents category drift.

### Raw Data Preservation
`ClaimXEventMessage.raw_data` stores the full original event payload. This increases message size but avoids information loss during event routing — handlers can extract event-type-specific fields without schema changes.

## Known Limitations and Deferred Work

1. **`EntityRowsMessage.trace_id` defaults to empty string** — Should be required once all producers populate it. Currently kept optional for backward compatibility.

2. **No schema versioning** — Messages don't carry a schema version field. If breaking changes are needed, they require coordinated deployment of all producers and consumers.

3. **`ClaimXTaskEvent` date fields are strings** — `date_task_assigned`, `date_task_completed`, and `cancelled_date` are `str | None` rather than `datetime | None`. These pass through raw ISO strings from the ClaimX API without parsing.

4. **No message size limits** — `raw_data` and `form_response` can be arbitrarily large. Consider adding size guards if Kafka message size becomes a concern.
