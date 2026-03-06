#!/usr/bin/env python3
"""
inject_itel_message.py — iTel Cabinet test message injector

Takes a raw ClaimX-style webhook payload, transforms it into the format
expected by the ItelCabinetTrackingWorker, and publishes it directly to
the itel-cabinet-pending Event Hub / Kafka topic — bypassing the
ClaimX event ingester (and its dedup) entirely.

Usage:
    # From a JSON file:
    python tools/inject_itel_message.py payload.json

    # From stdin:
    echo '{"eventType": "CUSTOM_TASK_COMPLETED", ...}' | python tools/inject_itel_message.py -

    # Dry-run (print transformed payload, do not send):
    python tools/inject_itel_message.py payload.json --dry-run

    # Override EventHub entity name:
    python tools/inject_itel_message.py payload.json --eventhub-name my-test-eventhub

    # Override task-id or task-name:
    python tools/inject_itel_message.py payload.json --task-id 32615 --task-name "Cabinet Repair"

Auth / Transport
----------------
The script mirrors the application's PIPELINE_TRANSPORT env var:

  eventhub (default):
    Requires: EVENTHUB_NAMESPACE_CONNECTION_STRING
    Uses: azure-eventhub SDK over AMQP/WebSocket

  kafka:
    Requires: KAFKA_BOOTSTRAP_SERVERS
    Auth:     SASL_SSL + OAUTHBEARER via Azure AD
              (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)

Input Payload Format (ClaimX webhook / event_ingester output):
    {
        "eventType": "CUSTOM_TASK_COMPLETED",
        "projectId": 601109,
        "taskAssignmentId": 5563715,
        "mediaIds": [98269686, 98269668, ...],
        "masterFileName": "",
        "mediaId": "",
        "videoCollaborationId": "",
        "IngestionTime": ""
    }

Output Payload Format (itel-cabinet-pending topic — TaskEvent schema):
    {
        "trigger_name": "iTel Cabinet Repair Form Task",
        "event_id": "<uuid4>",
        "event_type": "CUSTOM_TASK_COMPLETED",
        "project_id": 601109,
        "task_id": 32615,
        "assignment_id": 5563715,
        "task_name": "Cabinet Repair",
        "task_status": "COMPLETED",
        "timestamp": "2024-...",
        "media_ids": [98269686, ...],
        "task": {
            "assignment_id": 5563715,
            "task_id": 32615,
            "task_name": "Cabinet Repair",
            "status": "COMPLETED"
        }
    }

Dedup note:
    A fresh UUID is generated for event_id on every run, so this script
    will never be blocked by the ClaimX event ingester's dedup store.
    If you need to test with a specific event_id, set --event-id manually.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

# Load .env from the project root (same directory as this script's parent)
try:
    from dotenv import load_dotenv as _load_dotenv
    _env_file = Path(__file__).resolve().parent.parent / ".env"
    if _env_file.exists():
        _load_dotenv(_env_file, override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on env vars being set manually

# Apply SSL dev bypass (for local dev behind corporate TLS-intercepting proxy)
_src_dir = str(Path(__file__).resolve().parent.parent / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
try:
    from core.security.ssl_dev_bypass import apply_ssl_dev_bypass, get_eventhub_ssl_kwargs
    apply_ssl_dev_bypass()
except ImportError:
    def get_eventhub_ssl_kwargs() -> dict:  # type: ignore[misc]
        return {}


# ---------------------------------------------------------------------------
# Loose-JSON parser
# ---------------------------------------------------------------------------

def _parse_loose_json(text: str) -> dict:
    """Parse the "as-received" ClaimX payload format into a Python dict.

    The source payloads are not strictly valid JSON — they may have:
    - No outer ``{`` / ``}`` wrapping
    - Unquoted bare-word string values  (e.g. ``"eventType": CUSTOM_TASK_COMPLETED``)
    - Empty / missing values            (e.g. ``"masterFileName": ,`` or ``"IngestionTime":`` at EOF)

    Strategy:
    1. Try ``json.loads`` first (handles already-valid JSON).
    2. On failure, apply fixes and retry.
    """
    # --- fast path ---------------------------------------------------------
    stripped = text.strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # --- fix-up pass -------------------------------------------------------
    fixed = stripped

    # 1. Wrap in {} if the text is a bare key-value list (no outer braces)
    if not fixed.startswith("{"):
        fixed = "{\n" + fixed + "\n}"

    # 2. Replace empty values before a comma:  `": ,`  →  `": null,`
    fixed = re.sub(r':\s*,', ': null,', fixed)

    # 3. Replace empty values at end-of-line (before newline or closing brace):
    #    `"key":  \n`  →  `"key": null\n`
    fixed = re.sub(r':\s*\n', ': null\n', fixed)

    # 4. Replace empty values at very end of the object (before `}`):
    #    `"key": }` or `"key":}` — shouldn't normally happen but guard it
    fixed = re.sub(r':\s*\}', ': null\n}', fixed)

    # 5. Quote unquoted bare-word string values that follow `: `.
    #    Matches  `": WORD`  where WORD is alphanumeric/underscore and NOT a JSON keyword.
    _JSON_LITERALS = {"null", "true", "false"}

    def _quote_bareword(m: re.Match) -> str:
        word = m.group(1)
        # Leave JSON literals and numbers as-is
        if word in _JSON_LITERALS:
            return m.group(0)
        try:
            float(word)
            return m.group(0)
        except ValueError:
            pass
        suffix = m.group(2)  # comma, newline, or closing brace
        return f': "{word}"{suffix}'

    fixed = re.sub(r':\s*([A-Za-z_][A-Za-z0-9_]*)([,\n\}])', _quote_bareword, fixed)

    try:
        return json.loads(fixed)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Could not parse payload as JSON (even after loose-format fixes).\n"
            f"Parse error: {exc}\n\n"
            f"Fixed text:\n{fixed}"
        ) from exc


# ---------------------------------------------------------------------------
# Payload transformation
# ---------------------------------------------------------------------------

DEFAULT_TASK_ID = 32615
DEFAULT_TASK_NAME = "Cabinet Repair"
DEFAULT_EVENTHUB_NAME = "pcesdopodappv1-itel-cabinet-pending"
DEFAULT_KAFKA_BOOTSTRAP = "localhost:9094"

_EVENT_TYPE_TO_STATUS: dict[str, str] = {
    "CUSTOM_TASK_COMPLETED": "COMPLETED",
    "CUSTOM_TASK_ASSIGNED": "ASSIGNED",
    "CUSTOM_TASK_IN_PROGRESS": "IN_PROGRESS",
    "CUSTOM_TASK_CREATED": "ASSIGNED",
}


def transform_payload(
    raw: dict,
    *,
    task_id: int = DEFAULT_TASK_ID,
    task_name: str = DEFAULT_TASK_NAME,
    event_id: str | None = None,
) -> dict:
    """Transform a raw ClaimX webhook dict into the itel-cabinet-pending message format.

    Args:
        raw: Incoming ClaimX webhook payload.
        task_id: iTel task ID (defaults to 32615 — the ITEL cabinet task).
        task_name: Human-readable task name.
        event_id: Optional explicit event_id; if omitted a UUID4 is generated.

    Returns:
        Transformed dict ready for publishing to itel-cabinet-pending.

    Raises:
        ValueError: If required fields (eventType, projectId, taskAssignmentId) are missing.
    """
    missing = [f for f in ("eventType", "projectId", "taskAssignmentId") if f not in raw]
    if missing:
        raise ValueError(
            f"Input payload is missing required fields: {missing}\n"
            f"Expected keys: eventType, projectId, taskAssignmentId"
        )

    event_type: str = raw["eventType"]
    project_id = str(raw["projectId"])
    assignment_id = raw["taskAssignmentId"]
    media_ids: list = [str(m) for m in raw["mediaIds"]] if raw.get("mediaIds") else []

    # Derive task_status from eventType, with fallback
    task_status = _EVENT_TYPE_TO_STATUS.get(event_type, "IN_PROGRESS")

    # Parse IngestionTime if present; fall back to now
    ingestion_time: str = raw.get("IngestionTime") or ""
    if ingestion_time:
        timestamp = ingestion_time
    else:
        timestamp = datetime.now(UTC).isoformat()

    generated_event_id = event_id or str(uuid.uuid4())

    return {
        "trigger_name": "iTel Cabinet Repair Form Task",
        "event_id": generated_event_id,
        "event_type": event_type,
        "project_id": project_id,
        "task_id": task_id,
        "assignment_id": assignment_id,
        "task_name": task_name,
        "task_status": task_status,
        "timestamp": timestamp,
        "media_ids": media_ids,
        # Mirrors the "task" sub-object published by enrichment_worker._route_itel_events
        "task": {
            "assignment_id": assignment_id,
            "task_id": task_id,
            "task_name": task_name,
            "status": task_status,
        },
    }


# ---------------------------------------------------------------------------
# EventHub transport
# ---------------------------------------------------------------------------

async def _send_eventhub(payload: dict, eventhub_name: str) -> None:
    """Publish payload to an Azure Event Hub using the namespace connection string.

    Requires:
        EVENTHUB_NAMESPACE_CONNECTION_STRING — namespace-level connection string
        (no EntityPath).
    """

    try:
        from azure.eventhub.aio import EventHubProducerClient
        from azure.eventhub import EventData, TransportType
    except ImportError as exc:
        raise RuntimeError(
            "azure-eventhub is required for EventHub transport.\n"
            "Install it with: pip install azure-eventhub"
        ) from exc

    conn_str = os.environ.get("EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError(
            "EVENTHUB_NAMESPACE_CONNECTION_STRING is not set.\n"
            "Export the namespace-level connection string (without EntityPath)."
        )

    # Strip EntityPath if present (namespace-level required)
    parts = [p for p in conn_str.split(";") if not p.lower().startswith("entitypath=")]
    conn_str = ";".join(parts)

    body = json.dumps(payload, default=str).encode("utf-8")
    event_id = payload.get("event_id", "")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
        **get_eventhub_ssl_kwargs(),
    )

    async with producer:
        batch = await producer.create_batch()
        event = EventData(body)
        # Set the event_id as a property so downstream can read it
        event.properties = {"_key": event_id.encode("utf-8")}
        batch.add(event)
        await producer.send_batch(batch)

    print(f"✓ Published to EventHub '{eventhub_name}' (event_id={event_id})")


# ---------------------------------------------------------------------------
# Kafka transport
# ---------------------------------------------------------------------------

async def _send_kafka(payload: dict, bootstrap_servers: str) -> None:
    """Publish payload to Kafka using SASL_SSL + OAUTHBEARER (Azure AD).

    Requires:
        KAFKA_BOOTSTRAP_SERVERS — comma-separated broker list
        AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID — for OAuth token
    """

    try:
        from aiokafka import AIOKafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "aiokafka is required for Kafka transport.\n"
            "Install it with: pip install aiokafka"
        ) from exc

    # Lazy import so EventHub-only users don't need these
    try:
        from core.auth.credentials import AzureCredentialProvider
        from core.auth.eventhub_oauth import create_eventhub_oauth_callback
    except ImportError:
        # Fallback: build oauth callback from env vars manually
        AzureCredentialProvider = None  # type: ignore[assignment]
        create_eventhub_oauth_callback = None  # type: ignore[assignment]

    servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP)
    event_id = payload.get("event_id", "")
    body = json.dumps(payload, default=str).encode("utf-8")
    key = event_id.encode("utf-8") if event_id else None

    if create_eventhub_oauth_callback is not None:
        oauth_cb = create_eventhub_oauth_callback()
        producer = AIOKafkaProducer(
            bootstrap_servers=servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=oauth_cb,
        )
    else:
        # No auth (local dev Kafka without SSL)
        print("⚠  core.auth not importable — connecting without SASL (local dev only)")
        producer = AIOKafkaProducer(bootstrap_servers=servers)

    topic = os.environ.get("ITEL_PENDING_TOPIC", DEFAULT_EVENTHUB_NAME)

    await producer.start()
    try:
        await producer.send_and_wait(topic, value=body, key=key)
    finally:
        await producer.stop()

    print(f"✓ Published to Kafka topic '{topic}' on {servers} (event_id={event_id})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Transform a raw ClaimX webhook payload and inject it into the "
            "itel-cabinet-pending topic, bypassing dedup."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "payload_file",
        metavar="FILE",
        help="Path to a JSON file containing the raw ClaimX webhook payload, "
             "or '-' to read from stdin.",
    )
    parser.add_argument(
        "--task-id",
        type=int,
        default=DEFAULT_TASK_ID,
        metavar="ID",
        help=f"iTel task ID to embed in the message (default: {DEFAULT_TASK_ID}).",
    )
    parser.add_argument(
        "--task-name",
        default=DEFAULT_TASK_NAME,
        metavar="NAME",
        help=f"Task name to embed in the message (default: '{DEFAULT_TASK_NAME}').",
    )
    parser.add_argument(
        "--event-id",
        default=None,
        metavar="UUID",
        help="Explicit event_id to use instead of a generated UUID. "
             "Use this only when you need to replay a specific event_id.",
    )
    parser.add_argument(
        "--eventhub-name",
        default=DEFAULT_EVENTHUB_NAME,
        metavar="NAME",
        help=f"Azure Event Hub entity name (default: '{DEFAULT_EVENTHUB_NAME}'). "
             "Only used when PIPELINE_TRANSPORT=eventhub (the default).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        metavar="SERVERS",
        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP_SERVERS or localhost:9094). "
             "Only used when PIPELINE_TRANSPORT=kafka.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the transformed payload and exit without sending.",
    )
    return parser


async def _main(args: argparse.Namespace) -> int:
    # Read input payload
    if args.payload_file == "-":
        raw_text = sys.stdin.read()
        source = "stdin"
    else:
        try:
            with open(args.payload_file) as fh:
                raw_text = fh.read()
            source = args.payload_file
        except FileNotFoundError:
            print(f"Error: file not found: {args.payload_file}", file=sys.stderr)
            return 1

    try:
        raw_payload = _parse_loose_json(raw_text)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"Error: invalid payload from {source}: {exc}", file=sys.stderr)
        return 1

    # Transform
    try:
        transformed = transform_payload(
            raw_payload,
            task_id=args.task_id,
            task_name=args.task_name,
            event_id=args.event_id,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    # Always print the transformed payload for inspection
    print("Transformed payload:")
    print(json.dumps(transformed, indent=2, default=str))
    print()

    if args.dry_run:
        print("--dry-run: not sending.")
        return 0

    # Determine transport
    transport = os.environ.get("PIPELINE_TRANSPORT", "eventhub").lower()

    try:
        if transport == "eventhub":
            await _send_eventhub(transformed, eventhub_name=args.eventhub_name)
        elif transport == "kafka":
            await _send_kafka(transformed, bootstrap_servers=args.bootstrap_servers)
        else:
            print(
                f"Error: unknown PIPELINE_TRANSPORT='{transport}'. "
                "Expected 'eventhub' or 'kafka'.",
                file=sys.stderr,
            )
            return 1
    except (ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
