"""
Pytest fixtures for Kafka pipeline tests.

Provides fixtures for:
- Mock storage classes (available for all tests)
- In-memory Delta table implementations (for E2E testing without external deps)
- Docker-based Kafka test containers (only for integration tests)
- Test Kafka configuration
- Producer and consumer instances
- Topic management

IMPORTANT: Docker/Kafka fixtures only run when tests are marked with
@pytest.mark.integration. Unit tests will not trigger Docker dependencies.
"""

import os
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest

# NOTE: InMemoryDelta classes removed in refactor per OVER_ENGINEERING_REVIEW.md
# Removed entire in-memory Delta Lake simulation (~870 lines)
# Tests now use real Delta Lake or mock objects as needed
# from pipeline.common.storage.inmemory_delta import (
#     InMemoryDeltaTable,
#     InMemoryDeltaTableWriter,
#     InMemoryDeltaRegistry,
# )


# =============================================================================
# Mock Storage Classes - Available for ALL tests (unit and integration)
# These are defined FIRST to ensure they're available without Docker
# =============================================================================


class MockOneLakeClient:
    """Mock OneLake client for testing without Azure dependencies."""

    def __init__(self, base_path: str = ""):
        self.base_path = base_path
        self.uploaded_files: dict[str, bytes] = {}
        self.upload_count = 0
        self.is_open = False

    async def __aenter__(self):
        self.is_open = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.is_open = False
        return False

    async def upload_file(
        self, relative_path: str, local_path: Path, overwrite: bool = True
    ) -> str:
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        content = local_path.read_bytes()
        self.uploaded_files[relative_path] = content
        self.upload_count += 1
        return relative_path

    async def exists(self, blob_path: str) -> bool:
        return blob_path in self.uploaded_files

    async def close(self) -> None:
        self.is_open = False

    def get_uploaded_content(self, blob_path: str) -> bytes | None:
        return self.uploaded_files.get(blob_path)

    def clear(self) -> None:
        self.uploaded_files.clear()
        self.upload_count = 0


# =============================================================================
# Mock Fixtures - Available for ALL tests (no Docker required)
# =============================================================================


@pytest.fixture
def mock_onelake_client() -> MockOneLakeClient:
    """Provide mock OneLake client for testing (no Docker required)."""
    return MockOneLakeClient(base_path="test/attachments")


@pytest.fixture
def mock_storage(
    mock_onelake_client: MockOneLakeClient,
) -> dict[str, object]:
    """Provide all mock storage components as a dict (no Docker required)."""
    return {
        "onelake": mock_onelake_client,
    }

    # =============================================================================
    # In-Memory Delta Tables - DISABLED (removed in refactor)
    # InMemoryDelta classes removed per OVER_ENGINEERING_REVIEW.md (~870 lines)
    # =============================================================================

    # @pytest.fixture
    # def inmemory_delta_registry() -> InMemoryDeltaRegistry:
    #     """
    #     Provide a registry for managing multiple in-memory Delta tables.
    #
    #     The registry allows creating and accessing multiple tables by name,
    #     and provides easy reset between tests.
    #
    #     Usage:
    #         def test_something(inmemory_delta_registry):
    #             events = inmemory_delta_registry.get_table("events")
    #             inventory = inmemory_delta_registry.get_table("inventory")
    #     """
    #     registry = InMemoryDeltaRegistry()
    #     yield registry
    #     registry.reset()

    # @pytest.fixture
    # def inmemory_xact_events() -> InMemoryDeltaTable:
    #     """
    #     Provide in-memory Delta table for XACT events.
    #
    #     Configured to match production xact_events table structure.
    #     """
    #     table = InMemoryDeltaTable(
    #         table_path="inmemory://xact_events",
    #         timestamp_column="created_at",
    #         partition_column="event_date",
    #         z_order_columns=["event_date", "trace_id", "event_id", "type"],
    #     )
    #     yield table
    #     table.clear()
    #
    #
    # @pytest.fixture
    # def inmemory_xact_attachments() -> InMemoryDeltaTable:
    #     """
    #     Provide in-memory Delta table for XACT attachments/inventory.
    #
    #     Configured to match production xact_attachments table structure.
    #     """
    #     table = InMemoryDeltaTable(
    #         table_path="inmemory://xact_attachments",
    #         timestamp_column="created_at",
    #         partition_column="created_date",
    #     )
    #     yield table
    #     table.clear()
    #
    #
    # @pytest.fixture
    # def inmemory_claimx_events() -> InMemoryDeltaTable:
    #     """
    #     Provide in-memory Delta table for ClaimX events.
    #
    #     Configured to match production claimx_events table structure.
    #     """
    #     table = InMemoryDeltaTable(
    #         table_path="inmemory://claimx_events",
    #         timestamp_column="ingested_at",
    #         partition_column="event_date",
    #         z_order_columns=["project_id"],
    #     )
    #     yield table
    #     table.clear()
    #
    #
    # @pytest.fixture
    # def inmemory_delta_storage(
    #     inmemory_xact_events: InMemoryDeltaTable,
    #     inmemory_xact_attachments: InMemoryDeltaTable,
    #     inmemory_claimx_events: InMemoryDeltaTable,
    #     mock_onelake_client: MockOneLakeClient,
    # ) -> Dict[str, object]:
    #     """
    #     Provide all in-memory storage components for E2E testing.
    #
    #     This fixture combines in-memory Delta tables with mock OneLake
    #     for complete pipeline testing without external dependencies.
    #
    #     Usage:
    #         def test_e2e(inmemory_delta_storage):
    #             xact_events = inmemory_delta_storage["xact_events"]
    #             xact_attachments = inmemory_delta_storage["xact_attachments"]
    #             claimx_events = inmemory_delta_storage["claimx_events"]
    #             onelake = inmemory_delta_storage["onelake"]
    #     """
    #     return {
    #         "xact_events": inmemory_xact_events,
    #         "xact_attachments": inmemory_xact_attachments,
    #         "claimx_events": inmemory_claimx_events,
    #         "onelake": mock_onelake_client,
    #     }
    #
    #
    # # =============================================================================
    # # Helper function to check if running integration tests
    # # =============================================================================
    #
    #
    # def _is_integration_test(request) -> bool:
    #     """Check if the current test is marked as an integration test."""
    #     # Check if any item in the session has integration marker
    #     if hasattr(request, "node"):
    #         # Check if the current test has integration marker
    #         if request.node.get_closest_marker("integration"):
    #             return True
    #         # Also check parent for parametrized tests
    #         if hasattr(request.node, "parent") and request.node.parent:
    #             if hasattr(request.node.parent, "get_closest_marker"):
    #                 if request.node.parent.get_closest_marker("integration"):
    #                     return True
    #     return False


def _session_has_integration_tests(session) -> bool:
    """Check if the test session contains any integration tests."""
    return any(item.get_closest_marker("integration") for item in session.items)


# =============================================================================
# Docker/Kafka Fixtures - ONLY run for integration tests
# =============================================================================


@pytest.fixture(scope="session")
def kafka_container(request) -> Generator:
    """
    Provide a Kafka container for integration tests.

    Uses Testcontainers to start a real Kafka instance in Docker.
    The container runs for the entire test session and is shared across tests.

    IMPORTANT: This fixture only starts Docker when tests are marked with
    @pytest.mark.integration. Unit tests will not trigger Docker.

    Yields:
        KafkaContainer: Started Kafka container instance, or None for unit tests
    """
    # Check if the session contains any integration tests
    if not _session_has_integration_tests(request.session):
        # No integration tests - skip Docker startup entirely
        yield None
        return

    # Import testcontainers only when needed (avoids import errors if not installed)
    try:
        from testcontainers.kafka import KafkaContainer
    except ImportError:
        pytest.skip("testcontainers not installed - skipping integration tests")
        return

    # Create and start Kafka container
    # Uses Confluent Kafka image which includes Zookeeper
    kafka = KafkaContainer()
    kafka.start()

    # Set environment variable for MessageConfig.from_env()
    # This allows tests to use the test container's bootstrap server
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka.get_bootstrap_server()
    # Disable auth for local testing
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
    os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"
    # Set allowed domains for URL validation in tests
    os.environ["ALLOWED_ATTACHMENT_DOMAINS"] = (
        "example.com,claimxperience.com,www.claimxperience.com,claimxperience.s3.amazonaws.com,claimxperience.s3.us-east-1.amazonaws.com"
    )

    yield kafka

    # Cleanup: stop container after all tests complete
    kafka.stop()

    # Clean up environment variables
    os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
    os.environ.pop("KAFKA_SECURITY_PROTOCOL", None)
    os.environ.pop("KAFKA_SASL_MECHANISM", None)
    os.environ.pop("ALLOWED_ATTACHMENT_DOMAINS", None)


@pytest.fixture
def kafka_config(kafka_container):
    """
    Provide test Kafka configuration.

    Creates configuration that points to the test container with
    simplified security settings for local testing.

    Args:
        kafka_container: Test Kafka container fixture

    Returns:
        MessageConfig: Configuration for test environment
    """
    if kafka_container is None:
        pytest.skip(
            "Kafka container not available - this fixture requires @pytest.mark.integration"
        )

    from config.config import MessageConfig

    # Create config from environment (which includes container bootstrap server)
    config = MessageConfig.from_env()

    # Override security settings for local testing
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"

    return config



@pytest.fixture
def unique_topic_prefix(request) -> str:
    """
    Generate unique topic prefix for test isolation.

    Creates a unique prefix based on the test name to ensure
    tests don't interfere with each other by using different topics.

    Args:
        request: Pytest request fixture for test metadata

    Returns:
        str: Unique topic prefix (e.g., "test_produce_consume_123")
    """
    # Use test name to create unique prefix
    test_name = request.node.name
    # Remove special characters that aren't allowed in Kafka topic names
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in test_name)
    return safe_name.lower()[:100]  # Limit length


@pytest.fixture
def test_topics(kafka_config, unique_topic_prefix: str) -> dict[str, str]:
    """
    Provide unique test topic names.

    Creates a mapping of logical topic names to actual unique topic names
    for test isolation. Each test gets its own set of topics.

    Args:
        kafka_config: Test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        dict: Mapping of logical name to actual topic name
    """
    return {
        "pending": f"{unique_topic_prefix}.downloads.pending",
        "results": f"{unique_topic_prefix}.downloads.results",
        "retry_5m": f"{unique_topic_prefix}.downloads.pending.retry.5m",
        "retry_10m": f"{unique_topic_prefix}.downloads.pending.retry.10m",
        "retry_20m": f"{unique_topic_prefix}.downloads.pending.retry.20m",
        "retry_40m": f"{unique_topic_prefix}.downloads.pending.retry.40m",
        "dlq": f"{unique_topic_prefix}.downloads.dlq",
    }


@pytest.fixture
def test_kafka_config(kafka_config, unique_topic_prefix: str):
    """
    Provide test-specific Kafka configuration with unique topic names.

    Updates the config to use test-specific topic names for proper
    isolation between tests.

    Args:
        kafka_config: Base test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        MessageConfig: Configuration with test-specific topic names
    """
    # Create a copy and update topic names
    config = kafka_config
    config.downloads_pending_topic = f"{unique_topic_prefix}.downloads.pending"
    config.downloads_results_topic = f"{unique_topic_prefix}.downloads.results"
    config.dlq_topic = f"{unique_topic_prefix}.downloads.dlq"

    return config



@pytest.fixture
async def message_collector() -> callable:
    """
    Provide a message collector for testing.

    Creates a collector that accumulates messages consumed by a consumer.
    Useful for asserting on consumed messages in tests.

    Returns:
        callable: Message handler that collects messages
    """
    messages = []

    async def collect(record):
        """Collect consumed message."""
        messages.append(record)

    # Attach messages list to the function for easy access in tests
    collect.messages = messages

    return collect


@pytest.fixture(scope="session")
def wait_for_kafka_ready(kafka_container):
    """
    Wait for Kafka to be ready before running tests.

    Ensures the Kafka container is fully started and accepting connections.
    NOTE: This is NOT autouse - only runs when explicitly requested or
    when a dependent fixture is used.

    Args:
        kafka_container: Test Kafka container fixture
    """
    if kafka_container is None:
        # Not running integration tests
        return

    # The container's start() method waits for basic readiness
    # No additional wait needed - testcontainers handles this
    pass


# =============================================================================
# Integration Test Worker Fixtures - Require Docker/Kafka
# =============================================================================


@pytest.fixture
def mock_onelake_client_with_config(kafka_config) -> MockOneLakeClient:
    """Provide mock OneLake client with config-based path (requires Kafka)."""
    return MockOneLakeClient(base_path=kafka_config.onelake_base_path)


@pytest.fixture
async def event_ingester_worker(
    test_kafka_config,
) -> AsyncGenerator:
    """Provide event ingester worker."""
    from pipeline.verisk.workers.event_ingester import EventIngesterWorker

    worker = EventIngesterWorker(config=test_kafka_config)

    yield worker

    if worker.consumer and worker.consumer.is_running:
        await worker.stop()


@pytest.fixture
async def download_worker(
    test_kafka_config,
    tmp_path: Path,
) -> AsyncGenerator:
    """Provide download worker for testing.

    Note: DownloadWorker no longer uses OneLakeClient directly.
    It caches files locally and produces CachedDownloadMessage for upload worker.
    """
    from pipeline.verisk.workers.download_worker import DownloadWorker

    worker = DownloadWorker(config=test_kafka_config, temp_dir=tmp_path / "downloads")

    yield worker

    if worker.is_running:
        await worker.stop()


@pytest.fixture
def all_workers(
    event_ingester_worker,
    download_worker,
) -> dict[str, object]:
    """Provide all workers as a dict for E2E tests."""
    return {
        "event_ingester": event_ingester_worker,
        "download_worker": download_worker,
    }
