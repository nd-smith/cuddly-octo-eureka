"""Tests for ConnectionManager._execute_request disconnect handling."""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from core.errors.exceptions import PermanentError, TransientError
from pipeline.common.connections import ConnectionManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_response_message(code: int):
    """Create a mock RawResponseMessage with the given status code."""
    msg = MagicMock()
    msg.code = code
    return msg


def _started_manager() -> ConnectionManager:
    """Return a ConnectionManager with a mocked session, ready for requests."""
    mgr = ConnectionManager()
    mgr._started = True
    mgr._session = MagicMock(spec=aiohttp.ClientSession)
    return mgr


# ---------------------------------------------------------------------------
# ServerDisconnectedError with partial 2xx → PermanentError (no retry)
# ---------------------------------------------------------------------------


class TestServerDisconnectWithPartialStatus:
    async def test_disconnect_with_201_raises_permanent_error(self):
        """A disconnect carrying a 201 partial status must NOT be retried."""
        mgr = _started_manager()
        raw_msg = _make_raw_response_message(201)
        exc = aiohttp.ServerDisconnectedError(message=raw_msg)

        mgr._session.request = MagicMock(
            return_value=_async_context_manager_raising(exc)
        )

        with pytest.raises(PermanentError) as exc_info:
            await mgr._execute_request(
                method="POST",
                url="https://api.example.com/v1/requests",
                request_headers={},
                timeout=30,
                json={"test": True},
                data=None,
                params=None,
            )

        assert exc_info.value.context["partial_status"] == 201

    async def test_disconnect_with_400_raises_permanent_error(self):
        """A disconnect carrying a 400 should also be permanent (client error)."""
        mgr = _started_manager()
        raw_msg = _make_raw_response_message(400)
        exc = aiohttp.ServerDisconnectedError(message=raw_msg)

        mgr._session.request = MagicMock(
            return_value=_async_context_manager_raising(exc)
        )

        with pytest.raises(PermanentError) as exc_info:
            await mgr._execute_request(
                method="POST",
                url="https://api.example.com/v1/requests",
                request_headers={},
                timeout=30,
                json={"test": True},
                data=None,
                params=None,
            )

        assert exc_info.value.context["partial_status"] == 400

    async def test_disconnect_with_500_raises_transient_error(self):
        """A disconnect with a 500 partial status is still transient (retry OK)."""
        mgr = _started_manager()
        raw_msg = _make_raw_response_message(500)
        exc = aiohttp.ServerDisconnectedError(message=raw_msg)

        mgr._session.request = MagicMock(
            return_value=_async_context_manager_raising(exc)
        )

        with pytest.raises(TransientError):
            await mgr._execute_request(
                method="POST",
                url="https://api.example.com/v1/requests",
                request_headers={},
                timeout=30,
                json={"test": True},
                data=None,
                params=None,
            )

    async def test_disconnect_without_message_raises_transient_error(self):
        """A disconnect with no partial response is transient (safe to retry)."""
        mgr = _started_manager()
        exc = aiohttp.ServerDisconnectedError()

        mgr._session.request = MagicMock(
            return_value=_async_context_manager_raising(exc)
        )

        with pytest.raises(TransientError):
            await mgr._execute_request(
                method="POST",
                url="https://api.example.com/v1/requests",
                request_headers={},
                timeout=30,
                json={"test": True},
                data=None,
                params=None,
            )

    async def test_disconnect_with_200_includes_partial_status_in_context(self):
        """The partial_status should be available in the exception context."""
        mgr = _started_manager()
        raw_msg = _make_raw_response_message(200)
        exc = aiohttp.ServerDisconnectedError(message=raw_msg)

        mgr._session.request = MagicMock(
            return_value=_async_context_manager_raising(exc)
        )

        with pytest.raises(PermanentError) as exc_info:
            await mgr._execute_request(
                method="POST",
                url="https://api.example.com/v1/requests",
                request_headers={},
                timeout=30,
                json=None,
                data=None,
                params=None,
            )

        assert exc_info.value.context["partial_status"] == 200
        assert "url" in exc_info.value.context
        assert "method" in exc_info.value.context


# ---------------------------------------------------------------------------
# Async context manager helper
# ---------------------------------------------------------------------------


def _async_context_manager_raising(exc: Exception):
    """Return a mock async context manager that raises on __aenter__."""

    class _RaisingCM:
        async def __aenter__(self):
            raise exc

        async def __aexit__(self, *args):
            pass

    return _RaisingCM()
