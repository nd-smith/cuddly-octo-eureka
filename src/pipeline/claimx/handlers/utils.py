"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
used across handler modules.
"""

import logging
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# String truncation limits for logging
LOG_VALUE_TRUNCATE = 100  # For truncating values in log messages
LOG_ERROR_TRUNCATE_SHORT = 200  # For shorter error messages
LOG_ERROR_TRUNCATE_LONG = 500  # For longer error messages

# API call counts for error handling decorator
API_CALLS_SINGLE = 1  # Single API fetch (e.g., project verification only)
API_CALLS_WITH_VERIFICATION = 2  # Primary fetch + project verification


def safe_int(value: Any) -> int | None:
    """Convert value to int, returning None on failure. Logs a warning on conversion error."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:LOG_VALUE_TRUNCATE], "target_type": "int"},
        )
        return None


def safe_str(value: Any) -> str | None:
    """Convert value to stripped string. Returns None for None or empty/whitespace-only strings."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_str_id(value: Any) -> str | None:
    """Convert value to string ID. Numeric values are truncated to int first (e.g. 123.0 → "123")."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> bool | None:
    """Convert value to bool. Strings "true", "1", "yes" (case-insensitive) are truthy."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> float | None:
    """Convert value to float, returning None on failure. Logs a warning on conversion error."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:LOG_VALUE_TRUNCATE], "target_type": "float"},
        )
        return None


def safe_decimal_str(value: Any) -> str | None:
    """Convert value to a decimal string representation for lossless storage."""
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(
            "Type conversion failed",
            extra={"value": str(value)[:LOG_VALUE_TRUNCATE], "target_type": "decimal"},
        )
        return None


def parse_timestamp(value: Any) -> str | None:
    """Convert timestamp to ISO 8601 string. Normalizes 'Z' suffix to '+00:00'."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        return s.replace("Z", "+00:00") if s else None
    return None


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(UTC).isoformat()


def now_datetime() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


def today_date() -> date:
    """Return current UTC date."""
    return datetime.now(UTC).date()


def _parse_timestamp_str(s: str) -> datetime | None:
    """Parse a timestamp string to datetime, returning None on failure."""
    s = s.strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_timestamp_dt(value: Any) -> datetime | None:
    """Convert timestamp to datetime object (unlike parse_timestamp which returns a string)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return _parse_timestamp_str(value)
    return None


def elapsed_ms(start: datetime) -> int:
    """Return milliseconds elapsed since start time."""
    return int((datetime.now(UTC) - start).total_seconds() * 1000)


def inject_metadata(
    row: dict[str, Any],
    trace_id: str,
    include_last_enriched: bool = True,
) -> dict[str, Any]:
    """Inject common metadata fields into a row dictionary."""
    now = now_datetime()
    row["trace_id"] = trace_id
    row["created_at"] = now
    row["updated_at"] = now
    if include_last_enriched:
        row["last_enriched_at"] = now
    return row
