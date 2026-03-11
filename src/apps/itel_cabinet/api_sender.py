"""
iTel Cabinet API Sender

Handles transformation of enriched task data into iTel vendor API format,
sending to the iTel API endpoint, and recording results (success/error topics
+ OneLake upload).

Extracted from api_worker.py to be used directly by the tracking worker pipeline
in a single-worker architecture.
"""

import json
import logging
import os

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson

from config.config import MessageConfig, expand_env_var_string
from core.errors.exceptions import PermanentError, TransientError
from pipeline.common.connections import ConnectionManager, is_http_error
from pipeline.common.transport import create_producer
from pipeline.common.config_loader import load_connections, load_yaml_config

logger = logging.getLogger(__name__)

# Image type mapping: question_key -> vendor image_type
IMAGE_TYPE_MAP = {
    "overview_photos": "overview",
    # Lower Cabinet
    "lower_cabinet_box": "low_overview",
    "lower_face_frames_doors_drawers": "low_face",
    "lower_cabinet_end_panels": "low_end",
    # Upper Cabinet
    "upper_cabinet_box": "upp_overview",
    "upper_face_frames_doors_drawers": "upp_face",
    "upper_cabinet_end_panels": "upp_end",
    # Full Height Cabinet
    "full_height_cabinet_box": "fh_overview",
    "full_height_face_frames_doors_drawers": "fh_face",
    "full_height_end_panels": "fh_end",
    # Island Cabinet
    "island_cabinet_box": "isl_overview",
    "island_face_frames_doors_drawers": "isl_face",
    "island_cabinet_end_panels": "isl_end",
}

# Configuration paths
CONFIG_DIR = Path(__file__).parent / "config"
API_CONNECTIONS_CONFIG_PATH = CONFIG_DIR / "app.itel.yaml"
WORKERS_CONFIG_PATH = CONFIG_DIR / "workers.yaml"


def load_api_worker_config() -> dict:
    """Load the itel_cabinet_api worker configuration from workers.yaml."""
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if "itel_cabinet_api" not in workers:
        raise ValueError(f"Worker 'itel_cabinet_api' not found in {WORKERS_CONFIG_PATH}")

    return workers["itel_cabinet_api"]


def _is_duplicate_response(response: dict) -> bool:
    """Return True when the API response indicates a duplicate submission."""
    return "already exists" in str(response).lower()


class ItelApiSender:
    """Transforms enriched task data to iTel vendor format, sends to iTel API,
    and records results to success/error topics and OneLake.

    This class encapsulates all API-specific logic that was previously in
    ItelCabinetApiWorker, making it usable as a component within the
    combined tracking worker pipeline.
    """

    def __init__(
        self,
        api_config: dict,
        connection_manager: ConnectionManager,
        success_producer,
        error_producer,
        *,
        simulation_config: Any | None = None,
        onelake_results_path: str | None = None,
        output_config: dict | None = None,
        debug_output_path: str | None = None,
    ):
        self.api_config = api_config
        self.connections = connection_manager
        self._success_producer = success_producer
        self._error_producer = error_producer
        self.simulation_config = simulation_config
        self.onelake_results_path = onelake_results_path
        self.onelake_client = None
        self.output_config = output_config or {}

        self._success_topic = self.output_config.get(
            "success_topic", "pcesdopodappv1-itel-api-success"
        )
        self._error_topic = self.output_config.get(
            "error_topic", "pcesdopodappv1-itel-api-errors"
        )

        # Debug output dir
        self.debug_output_dir = Path(debug_output_path or "/tmp/itel_cabinet_api_debug")
        self.debug_output_dir.mkdir(parents=True, exist_ok=True)

        # Simulation output dir
        if self.simulation_config:
            self.output_dir = self.simulation_config.local_storage_path / "itel_submissions"
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "ItelApiSender running in simulation mode",
                extra={"output_dir": str(self.output_dir)},
            )
        else:
            self.output_dir = None

        logger.info(
            "ItelApiSender initialized",
            extra={
                "simulation_mode": bool(self.simulation_config),
                "connection": api_config.get("connection"),
            },
        )

    async def start(self):
        """Initialize async resources (OneLake client)."""
        if self.onelake_results_path:
            from pipeline.common.storage import OneLakeClient

            logger.info(
                "Initializing OneLake client for result uploads",
                extra={"results_path": self.onelake_results_path},
            )
            self.onelake_client = OneLakeClient(self.onelake_results_path)
            await self.onelake_client.__aenter__()
            logger.info(
                "OneLake client initialized for result uploads",
                extra={"results_path": self.onelake_results_path},
            )
        else:
            logger.warning(
                "No OneLake results path configured — API result files will not be uploaded",
            )

    async def stop(self):
        """Clean up async resources."""
        if self.onelake_client:
            try:
                await self.onelake_client.__aexit__(None, None, None)
                logger.info("OneLake client stopped")
            except Exception as e:
                logger.error("Error stopping OneLake client", extra={"error": str(e)})

        for name, producer in [
            ("Success producer", self._success_producer),
            ("Error producer", self._error_producer),
        ]:
            if producer:
                try:
                    await producer.stop()
                    logger.info("%s stopped", name)
                except Exception as e:
                    logger.error("Error stopping %s", name, extra={"error": str(e)})

    async def submit(self, payload: dict) -> bool:
        """Transform payload to iTel vendor format, send to API, and record results.

        Args:
            payload: Enriched task payload with submission, attachments, readable_report,
                     insured_info, adjuster_info, project_number, event_id, etc.

        Returns:
            True if the API call succeeded (or simulation write succeeded), False on error.
        """
        api_payload = self._transform_to_api_format(payload)

        # Always write debug payload
        await self._write_debug_payload(api_payload, payload)

        if self.simulation_config:
            await self._write_simulation_payload(api_payload, payload)
            return True

        try:
            status, response_body = await self._send_to_api(api_payload)
        except PermanentError as e:
            # A PermanentError with a 2xx partial_status means the server
            # accepted the request but dropped the connection before we could
            # read the full response body.  Treat this as success rather than
            # retrying and creating duplicates.
            partial_status = e.context.get("partial_status")
            if partial_status is not None and 200 <= partial_status < 300:
                logger.warning(
                    "Server disconnect with HTTP %d — treating as accepted "
                    "(response body unavailable due to disconnect)",
                    partial_status,
                    extra={
                        "assignment_id": api_payload.get("integration_test_id", "unknown"),
                        "partial_status": partial_status,
                    },
                )
                await self._handle_api_result(
                    status=partial_status,
                    response={"_disconnect": True, "detail": str(e)[:500]},
                    api_payload=api_payload,
                    original_payload=payload,
                )
                return True

            # For 4xx disconnect or other permanent errors, record and re-raise
            logger.error(
                "Permanent error sending to iTel API — publishing error event",
                extra={
                    "assignment_id": api_payload.get("integration_test_id", "unknown"),
                    "error": str(e)[:300],
                    "partial_status": partial_status,
                },
            )
            await self._handle_api_result(
                status=partial_status or 0,
                response={"error": "server_disconnect", "detail": str(e)[:500]},
                api_payload=api_payload,
                original_payload=payload,
            )
            raise
        except Exception as e:
            logger.error(
                "Connection-level error sending to iTel API — publishing error event",
                extra={
                    "assignment_id": api_payload.get("integration_test_id", "unknown"),
                    "error": str(e)[:300],
                },
            )
            await self._handle_api_result(
                status=0,
                response={"error": "connection_error", "detail": str(e)[:500]},
                api_payload=api_payload,
                original_payload=payload,
            )
            raise

        api_error = await self._handle_api_result(status, response_body, api_payload, payload)
        return not api_error

    def _transform_to_api_format(self, payload: dict) -> dict:
        """Transform tracking worker payload into iTel vendor API format."""
        submission = payload.get("submission", {})
        attachments = payload.get("attachments", [])
        readable_report = payload.get("readable_report", {})
        insured_info = payload.get("insured_info", {})
        adjuster_info = payload.get("adjuster_info", {})
        meta = readable_report.get("meta", {})

        logger.info(
            "Transforming payload to vendor format",
            extra={
                "assignment_id": submission.get("assignment_id"),
                "attachment_count": len(attachments),
            },
        )

        images = self._build_images_array(attachments)
        cabinet_specs = self._build_cabinet_repair_specs(submission)
        linear_feet_specs = self._build_linear_feet_specs(submission)

        api_payload = {
            "integration_test_id": f"{submission.get('assignment_id', '')}-{payload.get('event_id', '')}",
            "claim_number": payload.get("project_number", ""),
            "external_claim_id": submission.get("project_id", ""),
            "cat_code": "",
            "claim_type": "Other",
            "loss_type": None,
            "loss_date": meta.get("dates", {}).get("assigned", submission.get("date_assigned")),
            "service_level": "one_hour",
            "insured": self._build_insured(insured_info),
            "adjuster": {
                "carrier_id": self.api_config.get("carrier_id", ""),
                "adjuster_id": adjuster_info.get("adjuster_id", ""),
                "first_name": adjuster_info.get("first_name", ""),
                "last_name": adjuster_info.get("last_name", ""),
                "phone": self._normalize_phone(adjuster_info.get("phone", "")),
                "email": adjuster_info.get("email", ""),
            },
            "images": images,
            "cabinet_repair_specs": cabinet_specs,
            "opinion_replacement_value_specs": linear_feet_specs,
        }

        return api_payload

    def _build_full_name(self, first_name: str | None, last_name: str | None) -> str:
        parts = []
        if first_name:
            parts.append(first_name)
        if last_name:
            parts.append(last_name)
        return " ".join(parts) if parts else ""

    def _normalize_phone(self, phone: str) -> str:
        """Strip non-digit characters and remove leading US country code if present."""
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        return digits

    def _build_insured(self, insured_info: dict) -> dict:
        address = insured_info.get("address", {})
        return {
            "name": self._build_full_name(
                insured_info.get("firstName"),
                insured_info.get("lastName"),
            ),
            "street_number": "",
            "street_name": address.get("street1", ""),
            "city": address.get("city", ""),
            "state": address.get("stateProvince", ""),
            "zip_code": address.get("zipPostcode", ""),
            "country": address.get("country"),
        }

    def _build_images_array(self, attachments: list) -> list[dict]:
        images = []
        for attachment in attachments:
            question_key = attachment.get("question_key", "")
            url = attachment.get("url")

            image_type = IMAGE_TYPE_MAP.get(question_key)

            if image_type and url:
                images.append({"image_type": image_type, "url": url})
            elif url:
                logger.warning(
                    "Unmapped question_key for image",
                    extra={
                        "question_key": question_key,
                        "question_text": attachment.get("question_text"),
                    },
                )

        logger.debug("Built images array", extra={"image_count": len(images)})
        return images

    def _build_cabinet_repair_specs(self, submission: dict) -> dict:
        return {
            "damage_description": self._pad_to_min_words(
                submission.get("damage_description") or "", min_words=10
            ),
            # Upper cabinets
            "upper_cabinets_damaged": submission.get("upper_cabinets_damaged") or False,
            "upper_cabinets_damaged_count": submission.get("num_damaged_upper_boxes") or 0,
            "upper_cabinets_detached": submission.get("upper_cabinets_detached") or False,
            "upper_faces_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("upper_face_frames_doors_drawers_available")
            ),
            "upper_faces_frames_doors_drawers_damaged": submission.get(
                "upper_face_frames_doors_drawers_damaged"
            )
            or False,
            "upper_end_panels_damaged": submission.get("upper_finished_end_panels_damaged")
            or False,
            # Lower cabinets
            "lower_cabinets_damaged": submission.get("lower_cabinets_damaged") or False,
            "lower_cabinets_damaged_count": submission.get("num_damaged_lower_boxes") or 0,
            "lower_cabinets_detached": submission.get("lower_cabinets_detached") or False,
            "lower_faces_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("lower_face_frames_doors_drawers_available")
            ),
            "lower_faces_frames_doors_drawers_damaged": submission.get(
                "lower_face_frames_doors_drawers_damaged"
            )
            or False,
            "lower_end_panels_damaged": submission.get("lower_finished_end_panels_damaged")
            or False,
            "lower_cabinets_counter_top_type": submission.get("lower_counter_type") or "",
            # Full height cabinets
            "full_height_cabinets_damaged": submission.get("full_height_cabinets_damaged")
            or False,
            "full_height_pantry_cabinets_damaged_count": submission.get(
                "num_damaged_full_height_boxes"
            )
            or 0,
            "full_height_pantry_cabinets_detached": submission.get(
                "full_height_cabinets_detached"
            )
            or False,
            "full_height_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("full_height_face_frames_doors_drawers_available")
            ),
            "full_height_frames_doors_drawers_damaged": submission.get(
                "full_height_face_frames_doors_drawers_damaged"
            )
            or False,
            "full_height_end_panels_damaged": submission.get(
                "full_height_finished_end_panels_damaged"
            )
            or False,
            # Island cabinets
            "island_cabinets_damaged": submission.get("island_cabinets_damaged") or False,
            "island_cabinets_damaged_count": submission.get("num_damaged_island_boxes") or 0,
            "island_cabinets_detached": submission.get("island_cabinets_detached") or False,
            "island_frames_doors_drawers_available": self._normalize_yes_no(
                submission.get("island_face_frames_doors_drawers_available")
            ),
            "island_frames_doors_drawers_damaged": submission.get(
                "island_face_frames_doors_drawers_damaged"
            )
            or False,
            "island_end_panels_damaged": submission.get("island_finished_end_panels_damaged")
            or False,
            "island_cabinets_counter_top_type": submission.get("island_counter_type") or "",
            # Other details
            "other_details_and_instructions": submission.get("additional_notes") or "",
        }

    def _build_linear_feet_specs(self, submission: dict) -> dict:
        return {
            "upper_cabinets_linear_ft": self._to_whole_number(submission.get("upper_cabinets_lf")),
            "lower_cabinets_linear_ft": self._to_whole_number(submission.get("lower_cabinets_lf")),
            "full_height_cabinets_linear_ft": self._to_whole_number(submission.get("full_height_cabinets_lf")),
            "island_cabinets_linear_ft": self._to_whole_number(submission.get("island_cabinets_lf")),
            "counter_top_linear_ft": self._to_whole_number(submission.get("countertops_lf")),
        }

    def _to_whole_number(self, value: int | float | None) -> int | None:
        """Round a numeric value to the nearest whole number."""
        if value is None:
            return None
        return round(value)

    def _pad_to_min_words(self, text: str, min_words: int) -> str:
        """Repeat text until it meets the minimum word count."""
        if not text:
            return text
        words = text.split()
        if len(words) >= min_words:
            return text
        repeated = words.copy()
        while len(repeated) < min_words:
            repeated.extend(words)
        return " ".join(repeated)

    def _normalize_yes_no(self, value: str | bool | None) -> str:
        if value is None:
            return "No"
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in ("yes", "true", "1"):
                return "yes"
            return "no"
        return "No"

    async def _send_to_api(self, api_payload: dict) -> tuple[int, dict]:
        """Send payload to iTel Cabinet API."""
        assignment_id = api_payload.get("integration_test_id", "unknown")
        connection_name = self.api_config["connection"]
        conn_config = self.connections.get_connection(connection_name)

        # Diagnostic: log serialized payload details for disconnect debugging
        try:
            serialized = json.dumps(api_payload, default=str)
            payload_bytes = len(serialized.encode("utf-8"))
            image_count = len(api_payload.get("images", []))
            image_urls = [img.get("url", "")[:80] for img in api_payload.get("images", [])]
            has_nulls = [k for k, v in api_payload.items() if v is None]
            logger.info(
                "iTel API payload diagnostics: assignment=%s bytes=%d images=%d null_fields=%s",
                assignment_id,
                payload_bytes,
                image_count,
                has_nulls,
                extra={
                    "assignment_id": assignment_id,
                    "payload_bytes": payload_bytes,
                    "image_count": image_count,
                    "image_urls": image_urls,
                    "null_top_level_fields": has_nulls,
                    "claim_number": api_payload.get("claim_number"),
                    "endpoint": conn_config.endpoint,
                    "method": conn_config.method,
                },
            )
        except Exception:
            logger.warning("Failed to compute payload diagnostics", exc_info=True)

        # ── Experimental: use stdlib requests instead of aiohttp ──
        # Toggle with ITEL_USE_REQUESTS_LIB=true to test whether the
        # server-disconnect issue is aiohttp-specific.  Easily reversible:
        # unset the env var (or set to anything other than "true") to
        # revert to the normal aiohttp path.
        if os.environ.get("ITEL_USE_REQUESTS_LIB", "").lower() == "true":
            return await self._send_to_api_via_requests(
                api_payload, conn_config, assignment_id,
            )

        status, response = await self.connections.request_json(
            connection_name=connection_name,
            method=conn_config.method,
            path=conn_config.endpoint,
            json=api_payload,
            headers={"Ocp-Apim-Subscription-Key": self.api_config.get("subscription_id", "")},
        )

        if is_http_error(status):
            logger.error(
                "iTel API returned error: status=%s body=%s",
                status,
                str(response)[:500],
                extra={
                    "status": status,
                    "assignment_id": assignment_id,
                    "response_body": response,
                },
            )
        else:
            logger.info(
                "iTel API responded: status=%s assignment=%s",
                status,
                assignment_id,
                extra={
                    "status": status,
                    "assignment_id": assignment_id,
                    "outcome": "success",
                    "response_body": str(response)[:500],
                },
            )

        return status, response

    async def _send_to_api_via_requests(
        self,
        api_payload: dict,
        conn_config,
        assignment_id: str,
    ) -> tuple[int, dict]:
        """Send payload using the synchronous `requests` library (experimental).

        Runs the blocking HTTP call in a thread-pool executor so it doesn't
        stall the event loop.  Uses the same OAuth2 token and headers as the
        normal aiohttp path.

        Enable with env var: ITEL_USE_REQUESTS_LIB=true
        Disable by unsetting (or setting to anything other than "true").
        """
        import asyncio
        import functools
        import time as _time

        import requests as req
        from requests.structures import CaseInsensitiveDict

        connection_name = self.api_config["connection"]
        # Reuse the ConnectionManager's header-building (incl. OAuth2 token)
        request_headers = await self.connections._build_request_headers(
            conn_config,
            {"Ocp-Apim-Subscription-Key": self.api_config.get("subscription_id", "")},
        )
        request_headers["Content-Type"] = "application/json"

        url = f"{conn_config.base_url}{conn_config.endpoint}"
        timeout = conn_config.timeout_seconds

        # Log the full outgoing request headers (mask auth tokens)
        safe_headers = {
            k: (v[:12] + "..." if k.lower() in ("authorization", "ocp-apim-subscription-key") else v)
            for k, v in request_headers.items()
        }
        serialized_body = json.dumps(api_payload, default=str)
        logger.info(
            "[REQUESTS-LIB] Sending %s %s (timeout=%ds) request_headers=%s payload_bytes=%d",
            conn_config.method,
            url,
            timeout,
            safe_headers,
            len(serialized_body.encode("utf-8")),
            extra={
                "assignment_id": assignment_id,
                "http_lib": "requests",
                "method": conn_config.method,
                "url": url,
                "request_headers": safe_headers,
                "payload_bytes": len(serialized_body.encode("utf-8")),
            },
        )

        def _do_request() -> req.Response:
            t0 = _time.monotonic()
            try:
                resp = req.request(
                    method=conn_config.method,
                    url=url,
                    json=api_payload,
                    headers=request_headers,
                    timeout=timeout,
                )
            except req.exceptions.ConnectionError as exc:
                elapsed = (_time.monotonic() - t0) * 1000
                # Try to extract any partial info from the underlying urllib3 response
                raw_resp = getattr(exc, "response", None)
                logger.error(
                    "[REQUESTS-LIB] CONNECTION ERROR after %.0fms: %s "
                    "raw_response=%s raw_response_type=%s "
                    "underlying_exception=%r",
                    elapsed,
                    exc,
                    raw_resp,
                    type(raw_resp).__name__ if raw_resp else "None",
                    exc.__cause__,
                    extra={
                        "assignment_id": assignment_id,
                        "http_lib": "requests",
                        "elapsed_ms": round(elapsed, 1),
                        "error_type": type(exc).__name__,
                        "error_cause_type": type(exc.__cause__).__name__ if exc.__cause__ else "None",
                        "raw_response": repr(raw_resp),
                    },
                )
                raise
            except Exception as exc:
                elapsed = (_time.monotonic() - t0) * 1000
                logger.error(
                    "[REQUESTS-LIB] UNEXPECTED ERROR after %.0fms: %r",
                    elapsed,
                    exc,
                    extra={
                        "assignment_id": assignment_id,
                        "http_lib": "requests",
                        "elapsed_ms": round(elapsed, 1),
                        "error_type": type(exc).__name__,
                    },
                )
                raise

            elapsed = (_time.monotonic() - t0) * 1000

            # Log raw response details
            raw_headers = dict(resp.headers) if resp.headers else {}
            raw_body = resp.text[:2000] if resp.text else ""
            logger.info(
                "[REQUESTS-LIB] Response: %s %s -> %d (%.0fms)\n"
                "  response_headers=%s\n"
                "  raw_body=%s\n"
                "  encoding=%s http_version=%s reason=%s",
                conn_config.method,
                url,
                resp.status_code,
                elapsed,
                raw_headers,
                raw_body[:500],
                resp.encoding,
                getattr(resp.raw, "version", "unknown"),
                resp.reason,
                extra={
                    "assignment_id": assignment_id,
                    "http_lib": "requests",
                    "status": resp.status_code,
                    "elapsed_ms": round(elapsed, 1),
                    "response_headers": raw_headers,
                    "raw_body": raw_body,
                    "encoding": resp.encoding,
                    "http_version": getattr(resp.raw, "version", "unknown"),
                    "reason": resp.reason,
                },
            )
            return resp

        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(None, _do_request)

        status = resp.status_code
        try:
            response = resp.json()
        except (json.JSONDecodeError, ValueError):
            response = {"raw_body": resp.text} if resp.text else {}

        if is_http_error(status):
            logger.error(
                "[REQUESTS-LIB] iTel API error: status=%s body=%s",
                status,
                str(response)[:500],
                extra={"status": status, "assignment_id": assignment_id},
            )
        else:
            logger.info(
                "[REQUESTS-LIB] iTel API success: status=%s assignment=%s",
                status,
                assignment_id,
                extra={"status": status, "assignment_id": assignment_id},
            )

        return status, response

    async def _handle_api_result(
        self,
        status: int,
        response: dict,
        api_payload: dict,
        original_payload: dict,
    ) -> bool:
        """Record API result to output topic and OneLake.

        Returns:
            True if the API returned an error status, False otherwise.
        """
        assignment_id = api_payload.get("integration_test_id", "unknown")
        event_id = original_payload.get("event_id", "")
        is_error = is_http_error(status)

        # Treat "already exists" duplicate responses as idempotent success
        if is_error and _is_duplicate_response(response):
            logger.info(
                "Duplicate submission detected — treating as idempotent success",
                extra={"assignment_id": assignment_id, "api_status": status},
            )
            is_error = False

        result_type = "error" if is_error else "success"

        result_message = {
            "assignment_id": assignment_id,
            "event_id": event_id,
            "status": result_type,
            "api_status": status,
            "api_response": response,
            "api_payload": api_payload,
            "original_payload": original_payload,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        producer = self._error_producer if is_error else self._success_producer
        topic = self._error_topic if is_error else self._success_topic

        try:
            await producer.send(
                topic=topic,
                key=event_id.encode("utf-8") if event_id else None,
                value=result_message,
            )
            logger.info(
                "Published API result to %s topic",
                result_type,
                extra={"assignment_id": assignment_id, "api_status": status},
            )
        except Exception:
            logger.exception(
                "Failed to publish API result to %s topic",
                result_type,
                extra={"assignment_id": assignment_id},
            )
            raise

        # Upload result JSON to OneLake (non-fatal)
        if not self.onelake_client:
            logger.warning(
                "OneLake client not initialized — skipping API result upload",
                extra={"assignment_id": assignment_id, "result_type": result_type},
            )
        else:
            try:
                onelake_path = self._build_onelake_path(assignment_id, result_type)
                data = json.dumps(result_message, default=str).encode("utf-8")
                await self.onelake_client.async_upload_bytes(
                    relative_path=onelake_path,
                    data=data,
                    overwrite=True,
                )
                logger.info(
                    "Uploaded API result to OneLake",
                    extra={
                        "assignment_id": assignment_id,
                        "onelake_path": onelake_path,
                        "result_type": result_type,
                    },
                )
            except Exception:
                logger.exception(
                    "Failed to upload API result to OneLake (non-fatal)",
                    extra={"assignment_id": assignment_id},
                )

        return is_error

    def _build_onelake_path(self, assignment_id: str, result_type: str) -> str:
        now = datetime.now(UTC)
        ts = now.strftime("%Y%m%dT%H%M%S")
        return f"{result_type}/{now.year}/{now.month:02d}/{now.day:02d}/{assignment_id}_{ts}.json"

    async def _write_debug_payload(self, api_payload: dict, original_payload: dict) -> None:
        """Write payload to debug directory for post-hoc inspection (non-fatal)."""
        try:
            assignment_id = api_payload.get("integration_test_id", "unknown")
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
            filename = f"itel_payload_{assignment_id}_{timestamp}.json"
            filepath = self.debug_output_dir / filename

            debug_data = {
                "api_payload": api_payload,
                "original_payload": original_payload,
                "written_at": datetime.now(UTC).isoformat(),
            }

            filepath.write_text(json.dumps(debug_data, indent=2, default=str))
            logger.debug(
                "Debug payload written",
                extra={"filepath": str(filepath), "assignment_id": assignment_id},
            )
        except Exception:
            logger.warning("Failed to write debug payload (non-fatal)", exc_info=True)

    async def _write_simulation_payload(self, api_payload: dict, original_payload: dict):
        """Write payload to simulation directory (simulation mode)."""
        assignment_id = api_payload.get("integration_test_id", "unknown")
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        filename = f"itel_submission_{assignment_id}_{timestamp}.json"
        filepath = self.output_dir / filename

        submission_data = {
            **api_payload,
            "submitted_at": datetime.now(UTC).isoformat(),
            "simulation_mode": True,
            "source": "itel_cabinet_tracking_worker",
        }

        filepath.write_text(json.dumps(submission_data, indent=2, default=str))

        logger.info(
            "[SIMULATION MODE] iTel submission written to file (vendor schema)",
            extra={
                "assignment_id": assignment_id,
                "filepath": str(filepath),
                "claim_number": api_payload.get("claim_number"),
                "image_count": len(api_payload.get("images", [])),
                "simulation_mode": True,
            },
        )


async def build_api_sender(
    bootstrap_servers: str,
    connection_manager: ConnectionManager,
) -> ItelApiSender:
    """Build a configured ItelApiSender with all required resources.

    Loads API worker config, creates producers, and initializes the sender.
    The caller is responsible for calling start() and stop().

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        connection_manager: ConnectionManager to add iTel API connections to.

    Returns:
        Configured ItelApiSender instance (call start() before use).
    """
    api_worker_config = load_api_worker_config()

    # Load and register iTel API connections
    api_connections = load_connections(API_CONNECTIONS_CONFIG_PATH)
    for conn in api_connections:
        connection_manager.add_connection(conn)

    # Expand environment variables in API config
    api_config = api_worker_config["api"]
    for key in ("carrier_id", "subscription_id"):
        if key in api_config and isinstance(api_config[key], str):
            expanded = expand_env_var_string(api_config[key])
            if "${" in expanded:
                raise ValueError(
                    f"Environment variable not expanded for api.{key}: {expanded}. "
                    f"Check that all required environment variables are set."
                )
            api_config[key] = expanded

    config = MessageConfig(bootstrap_servers=bootstrap_servers)

    output_config = api_worker_config.get("output", {})
    success_topic_key = output_config.get("success_topic_key", "itel_api_success")
    error_topic_key = output_config.get("error_topic_key", "itel_api_errors")

    success_producer = create_producer(
        config=config,
        domain="plugins",
        worker_name="itel_cabinet_tracking_worker",
        topic_key=success_topic_key,
    )
    await success_producer.start()

    error_producer = create_producer(
        config=config,
        domain="plugins",
        worker_name="itel_cabinet_tracking_worker",
        topic_key=error_topic_key,
    )
    await error_producer.start()

    # Check for simulation mode
    simulation_config = None
    try:
        from pipeline.simulation import get_simulation_config, is_simulation_mode

        if is_simulation_mode():
            simulation_config = get_simulation_config()
            logger.info("Simulation mode detected - API sender will write to local files")
    except ImportError:
        pass

    # Load debug output path
    debug_output_path = None
    raw_debug_path = api_worker_config.get("debug_output_path")
    if raw_debug_path:
        expanded = expand_env_var_string(raw_debug_path).strip()
        debug_output_path = expanded if expanded and "${" not in expanded else None

    # Load OneLake results path
    onelake_config = api_worker_config.get("onelake", {})
    onelake_results_path = None
    raw_path = onelake_config.get("results_path")
    if raw_path:
        expanded = expand_env_var_string(raw_path).strip()
        if "${" in expanded:
            raise ValueError(
                f"Environment variable not expanded for onelake.results_path: {expanded}. "
                f"Check that ITEL_API_ONELAKE_RESULTS_PATH is set."
            )
        onelake_results_path = expanded or None

    sender = ItelApiSender(
        api_config=api_config,
        connection_manager=connection_manager,
        success_producer=success_producer,
        error_producer=error_producer,
        simulation_config=simulation_config,
        onelake_results_path=onelake_results_path,
        output_config=output_config,
        debug_output_path=debug_output_path,
    )

    return sender
