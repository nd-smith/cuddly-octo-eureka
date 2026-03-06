"""
Tests for __del__ ResourceWarning safety net on async resource holders.

Verifies that classes holding async connections emit ResourceWarning when
garbage-collected without proper cleanup, following the pattern established
in test_delta_resource_cleanup.py.
"""

import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── GenericOAuth2Provider ────────────────────────────────────────────────────


class TestOAuth2ProviderResourceCleanup:
    """Tests for GenericOAuth2Provider.__del__ ResourceWarning."""

    @pytest.mark.asyncio
    async def test_del_warns_if_session_not_closed(self):
        from core.oauth2.provider import GenericOAuth2Provider

        mock_config = MagicMock()
        mock_config.client_id = "id"
        mock_config.client_secret = "secret"
        mock_config.token_url = "https://token"
        mock_config.scope = "scope"
        mock_config.provider_name = "test"

        provider = GenericOAuth2Provider(mock_config)
        # Simulate an open session
        mock_session = MagicMock()
        mock_session.closed = False
        provider._session = mock_session

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            provider.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly closed" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_closed(self):
        from core.oauth2.provider import GenericOAuth2Provider

        mock_config = MagicMock()
        mock_config.client_id = "id"
        mock_config.client_secret = "secret"
        mock_config.token_url = "https://token"
        mock_config.scope = "scope"
        mock_config.provider_name = "test"

        provider = GenericOAuth2Provider(mock_config)
        provider._session = MagicMock()
        provider._session.closed = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            provider.__del__()

            assert len(w) == 0


# ── PrometheusClient ─────────────────────────────────────────────────────────


class TestPrometheusClientResourceCleanup:
    """Tests for PrometheusClient.__del__ ResourceWarning."""

    @pytest.mark.asyncio
    async def test_del_warns_if_session_not_closed(self):
        from pipeline.common.monitoring_service import PrometheusClient

        client = PrometheusClient("http://localhost:9090")
        mock_session = MagicMock()
        mock_session.closed = False
        client._session = mock_session

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            client.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly closed" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_session_closed(self):
        from pipeline.common.monitoring_service import PrometheusClient

        client = PrometheusClient("http://localhost:9090")
        mock_session = MagicMock()
        mock_session.closed = True
        client._session = mock_session

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            client.__del__()

            assert len(w) == 0

    @pytest.mark.asyncio
    async def test_del_no_warning_if_no_session(self):
        from pipeline.common.monitoring_service import PrometheusClient

        client = PrometheusClient("http://localhost:9090")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            client.__del__()

            assert len(w) == 0


# ── ClaimXApiClient ──────────────────────────────────────────────────────────


class TestClaimXApiClientResourceCleanup:
    """Tests for ClaimXApiClient.__del__ ResourceWarning."""

    @pytest.mark.asyncio
    async def test_del_warns_if_not_closed(self):
        from pipeline.claimx.api_client import ClaimXApiClient

        client = ClaimXApiClient(
            base_url="https://api.example.com",
            token="test-token",
        )
        # _closed defaults to False in __init__

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            client.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly closed" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_closed(self):
        from pipeline.claimx.api_client import ClaimXApiClient

        client = ClaimXApiClient(
            base_url="https://api.example.com",
            token="test-token",
        )
        client._closed = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            client.__del__()

            assert len(w) == 0


# ── ConnectionManager ────────────────────────────────────────────────────────


class TestConnectionManagerResourceCleanup:
    """Tests for ConnectionManager.__del__ ResourceWarning."""

    @pytest.mark.asyncio
    async def test_del_warns_if_started_and_not_closed(self):
        from pipeline.common.connections import ConnectionManager

        manager = ConnectionManager()
        manager._started = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            manager.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly closed" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_not_started(self):
        from pipeline.common.connections import ConnectionManager

        manager = ConnectionManager()
        # _started defaults to False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            manager.__del__()

            assert len(w) == 0


# ── EventHubProducer ─────────────────────────────────────────────────────────


class TestEventHubProducerResourceCleanup:
    """Tests for EventHubProducer.__del__ ResourceWarning."""

    @pytest.fixture(autouse=True)
    def mock_azure_modules(self):
        """Mock Azure SDK modules for import."""
        import sys

        mock_eventhub = MagicMock()
        mock_eventhub.TransportType = MagicMock()
        mock_eventhub.TransportType.AmqpOverWebsocket = "AmqpOverWebsocket"
        mock_eventhub.EventData = MagicMock()
        mock_eventhub_aio = MagicMock()

        modules = {
            "azure": MagicMock(),
            "azure.eventhub": mock_eventhub,
            "azure.eventhub.aio": mock_eventhub_aio,
            "azure.eventhub.extensions": MagicMock(),
            "azure.eventhub.extensions.checkpointstoreblobaio": MagicMock(),
            "azure.storage": MagicMock(),
            "azure.storage.blob": MagicMock(),
            "azure.storage.blob.aio": MagicMock(),
        }
        originals = {k: sys.modules.get(k) for k in modules}
        sys.modules.update(modules)

        yield

        for k, v in originals.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

    @pytest.mark.asyncio
    async def test_del_warns_if_started_and_not_stopped(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
            domain="test",
            worker_name="test-worker",
            eventhub_name="test-hub",
        )
        producer._started = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            producer.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly stopped" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_not_started(self):
        from pipeline.common.eventhub.producer import EventHubProducer

        producer = EventHubProducer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
            domain="test",
            worker_name="test-worker",
            eventhub_name="test-hub",
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            producer.__del__()

            assert len(w) == 0


# ── EventHubConsumer ─────────────────────────────────────────────────────────


class TestEventHubConsumerResourceCleanup:
    """Tests for EventHubConsumer.__del__ ResourceWarning."""

    @pytest.fixture(autouse=True)
    def mock_azure_modules(self):
        """Mock Azure SDK modules for import."""
        import sys

        mock_eventhub = MagicMock()
        mock_eventhub.TransportType = MagicMock()
        mock_eventhub.TransportType.AmqpOverWebsocket = "AmqpOverWebsocket"
        mock_eventhub.EventData = MagicMock()
        mock_eventhub_aio = MagicMock()

        modules = {
            "azure": MagicMock(),
            "azure.eventhub": mock_eventhub,
            "azure.eventhub.aio": mock_eventhub_aio,
            "azure.eventhub.extensions": MagicMock(),
            "azure.eventhub.extensions.checkpointstoreblobaio": MagicMock(),
            "azure.storage": MagicMock(),
            "azure.storage.blob": MagicMock(),
            "azure.storage.blob.aio": MagicMock(),
        }
        originals = {k: sys.modules.get(k) for k in modules}
        sys.modules.update(modules)

        yield

        for k, v in originals.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

    @pytest.mark.asyncio
    async def test_del_warns_if_running_and_not_stopped(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
            domain="test",
            worker_name="test-worker",
            eventhub_name="test-hub",
            consumer_group="$Default",
            message_handler=AsyncMock(),
        )
        consumer._running = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            consumer.__del__()

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "was not properly stopped" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_del_no_warning_if_not_running(self):
        from pipeline.common.eventhub.consumer import EventHubConsumer

        consumer = EventHubConsumer(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
            domain="test",
            worker_name="test-worker",
            eventhub_name="test-hub",
            consumer_group="$Default",
            message_handler=AsyncMock(),
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            consumer.__del__()

            assert len(w) == 0
