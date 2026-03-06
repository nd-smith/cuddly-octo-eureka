"""Tests for ItelApiSender._handle_api_result success/error/OneLake paths."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from apps.itel_cabinet.api_sender import ItelApiSender


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_sender(*, onelake_client=None):
    """Build a minimal ItelApiSender with mocked producers, bypassing __init__."""
    sender = object.__new__(ItelApiSender)
    sender.onelake_client = onelake_client
    sender._success_topic = "test-success-topic"
    sender._error_topic = "test-error-topic"

    success_producer = AsyncMock()
    error_producer = AsyncMock()
    sender._success_producer = success_producer
    sender._error_producer = error_producer

    return sender


def _make_api_payload(assignment_id: str = "assign-42") -> dict:
    return {"integration_test_id": assignment_id, "claim_number": "CLM-001"}


def _make_original_payload(event_id: str = "evt-99") -> dict:
    return {"event_id": event_id, "assignment_id": "assign-42"}


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


class TestHandleApiResultSuccess:
    async def test_sends_to_success_topic(self):
        sender = _make_sender()
        api_payload = _make_api_payload()
        original = _make_original_payload()

        is_error = await sender._handle_api_result(200, {"ok": True}, api_payload, original)

        assert is_error is False
        sender._success_producer.send.assert_awaited_once()
        call_kwargs = sender._success_producer.send.call_args.kwargs
        assert call_kwargs["topic"] == "test-success-topic"
        assert call_kwargs["value"]["status"] == "success"
        assert call_kwargs["value"]["api_status"] == 200

    async def test_does_not_send_to_error_topic(self):
        sender = _make_sender()
        await sender._handle_api_result(200, {}, _make_api_payload(), _make_original_payload())
        sender._error_producer.send.assert_not_awaited()

    async def test_uploads_to_onelake_with_success_prefix(self):
        mock_client = AsyncMock()
        sender = _make_sender(onelake_client=mock_client)

        await sender._handle_api_result(200, {}, _make_api_payload(), _make_original_payload())

        mock_client.async_upload_bytes.assert_awaited_once()
        path_arg = mock_client.async_upload_bytes.call_args.kwargs["relative_path"]
        assert path_arg.startswith("success/")
        assert "assign-42" in path_arg
        assert path_arg.endswith(".json")


# ---------------------------------------------------------------------------
# Error path
# ---------------------------------------------------------------------------


class TestHandleApiResultError:
    async def test_sends_to_error_topic(self):
        sender = _make_sender()
        api_payload = _make_api_payload()
        original = _make_original_payload()

        is_error = await sender._handle_api_result(500, {"error": "boom"}, api_payload, original)

        assert is_error is True
        sender._error_producer.send.assert_awaited_once()
        call_kwargs = sender._error_producer.send.call_args.kwargs
        assert call_kwargs["topic"] == "test-error-topic"
        assert call_kwargs["value"]["status"] == "error"
        assert call_kwargs["value"]["api_status"] == 500

    async def test_does_not_send_to_success_topic(self):
        sender = _make_sender()
        await sender._handle_api_result(422, {}, _make_api_payload(), _make_original_payload())
        sender._success_producer.send.assert_not_awaited()

    async def test_uploads_to_onelake_with_error_prefix(self):
        mock_client = AsyncMock()
        sender = _make_sender(onelake_client=mock_client)

        await sender._handle_api_result(500, {}, _make_api_payload(), _make_original_payload())

        mock_client.async_upload_bytes.assert_awaited_once()
        path_arg = mock_client.async_upload_bytes.call_args.kwargs["relative_path"]
        assert path_arg.startswith("error/")


# ---------------------------------------------------------------------------
# OneLake client is None — warning logged, no upload attempted
# ---------------------------------------------------------------------------


class TestHandleApiResultNoOneLakeClient:
    async def test_no_upload_when_client_is_none(self):
        sender = _make_sender(onelake_client=None)

        # Should not raise, should not attempt upload
        is_error = await sender._handle_api_result(
            200, {}, _make_api_payload(), _make_original_payload()
        )
        assert is_error is False  # success status

    async def test_warning_logged_when_client_is_none(self, caplog):
        import logging

        sender = _make_sender(onelake_client=None)

        with caplog.at_level(logging.WARNING, logger="apps.itel_cabinet.api_sender"):
            await sender._handle_api_result(
                200, {}, _make_api_payload(), _make_original_payload()
            )

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("OneLake client not initialized" in m for m in warning_messages)


# ---------------------------------------------------------------------------
# OneLake upload exception — non-fatal, exception logged
# ---------------------------------------------------------------------------


class TestHandleApiResultOneLakeException:
    async def test_upload_exception_is_non_fatal(self):
        mock_client = AsyncMock()
        mock_client.async_upload_bytes.side_effect = RuntimeError("connection refused")
        sender = _make_sender(onelake_client=mock_client)

        # Should not raise even though upload failed
        is_error = await sender._handle_api_result(
            200, {}, _make_api_payload(), _make_original_payload()
        )
        assert is_error is False

    async def test_upload_exception_logged(self, caplog):
        import logging

        mock_client = AsyncMock()
        mock_client.async_upload_bytes.side_effect = RuntimeError("network error")
        sender = _make_sender(onelake_client=mock_client)

        with caplog.at_level(logging.ERROR, logger="apps.itel_cabinet.api_sender"):
            await sender._handle_api_result(
                200, {}, _make_api_payload(), _make_original_payload()
            )

        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("Failed to upload API result to OneLake" in m for m in error_messages)

    async def test_producer_result_still_sent_before_upload_failure(self):
        mock_client = AsyncMock()
        mock_client.async_upload_bytes.side_effect = RuntimeError("network error")
        sender = _make_sender(onelake_client=mock_client)

        await sender._handle_api_result(200, {}, _make_api_payload(), _make_original_payload())

        # Producer send should still have been called before the failed upload
        sender._success_producer.send.assert_awaited_once()


# ---------------------------------------------------------------------------
# Duplicate "already exists" treated as idempotent success
# ---------------------------------------------------------------------------


class TestHandleApiResultDuplicate:
    async def test_duplicate_response_is_not_error(self):
        sender = _make_sender()
        response = {
            "integration_test_id": [
                "'5573977-c0d6dd9311fc03acee36b0a4d06b9d0185e2760a29cf2d6efa7d68bc045e9684'"
                " already exists"
            ]
        }

        is_error = await sender._handle_api_result(
            400, response, _make_api_payload(), _make_original_payload()
        )

        assert is_error is False

    async def test_duplicate_response_sent_to_success_topic(self):
        sender = _make_sender()
        response = {"integration_test_id": ["'abc-123' already exists"]}

        await sender._handle_api_result(
            400, response, _make_api_payload(), _make_original_payload()
        )

        sender._success_producer.send.assert_awaited_once()
        call_kwargs = sender._success_producer.send.call_args.kwargs
        assert call_kwargs["topic"] == "test-success-topic"
        assert call_kwargs["value"]["status"] == "success"

    async def test_duplicate_response_not_sent_to_error_topic(self):
        sender = _make_sender()
        response = {"integration_test_id": ["'abc-123' already exists"]}

        await sender._handle_api_result(
            400, response, _make_api_payload(), _make_original_payload()
        )

        sender._error_producer.send.assert_not_awaited()

    async def test_duplicate_logged_as_info(self, caplog):
        import logging

        sender = _make_sender()
        response = {"integration_test_id": ["'abc-123' already exists"]}

        with caplog.at_level(logging.INFO, logger="apps.itel_cabinet.api_sender"):
            await sender._handle_api_result(
                400, response, _make_api_payload(), _make_original_payload()
            )

        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("Duplicate submission detected" in m for m in info_messages)

    async def test_non_duplicate_400_still_error(self):
        sender = _make_sender()
        response = {"detail": "Bad request"}

        is_error = await sender._handle_api_result(
            400, response, _make_api_payload(), _make_original_payload()
        )

        assert is_error is True
        sender._error_producer.send.assert_awaited_once()
