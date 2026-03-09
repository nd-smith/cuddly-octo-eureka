"""Transport layer for Azure Event Hub.

Provides factory functions to create producer/consumer instances with
EventHub configuration resolution from config.yaml and environment variables.

Event Hub resolution:
- One namespace connection string provides access to all Event Hubs
- Event Hub names are defined per-topic in config.yaml
  under eventhub.{domain}.{topic_key}.eventhub_name
- Consumer groups are defined per-worker under each topic:
  eventhub.{domain}.{topic_key}.consumer_groups.{worker_name}
- The Azure SDK `eventhub_name` parameter is used instead of
  baking EntityPath into the connection string
"""

import datetime
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

from config.config import MessageConfig
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


# Event Hub configuration keys
EVENTHUB_CONFIG_KEY = "eventhub"
NAMESPACE_CONNECTION_STRING_KEY = "namespace_connection_string"
EVENTHUB_NAME_KEY = "eventhub_name"
CONSUMER_GROUPS_KEY = "consumer_groups"
OWNER_LEVELS_KEY = "owner_levels"
DEFAULT_CONSUMER_GROUP_KEY = "default_consumer_group"
SOURCE_CONFIG_KEY = "source"
STARTING_POSITION_KEY = "starting_position"
DEFAULT_STARTING_POSITION_KEY = "default_starting_position"


# =============================================================================
# Event Hub configuration loading
# =============================================================================


def _load_eventhub_config() -> dict[str, Any]:
    """Load the eventhub section from config.yaml.

    Returns the expanded eventhub config dict with Event Hub mappings.
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    if not DEFAULT_CONFIG_FILE.exists():
        return {}

    data = load_yaml(DEFAULT_CONFIG_FILE)
    data = _expand_env_vars(data)
    return data.get(EVENTHUB_CONFIG_KEY, {})


def _get_namespace_connection_string() -> str:
    """Get Event Hub namespace-level connection string.

    Priority:
    1. EVENTHUB_NAMESPACE_CONNECTION_STRING env var
    2. eventhub.namespace_connection_string from config.yaml

    Returns:
        Namespace connection string with EntityPath removed (if present).

    Raises:
        ValueError: If no connection string is configured or if it's empty/invalid.
    """
    # 1. New env var (preferred)
    conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "EVENTHUB_NAMESPACE_CONNECTION_STRING is set but empty or contains only whitespace. "
                "Ensure the environment variable contains a valid Event Hub connection string."
            )
        return stripped

    # 2. Config file
    config = _load_eventhub_config()
    conn = config.get(NAMESPACE_CONNECTION_STRING_KEY, "")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "eventhub.namespace_connection_string in config.yaml is set but empty or contains only whitespace. "
                "Ensure the configuration contains a valid Event Hub connection string."
            )
        return stripped

    raise ValueError(
        "Event Hub namespace connection string is required. "
        "Set EVENTHUB_NAMESPACE_CONNECTION_STRING environment variable."
    )


def get_source_connection_string() -> str:
    """Get source EventHub namespace connection string.

    Source EventHubs (where raw Verisk/ClaimX events originate) may live in a
    different namespace than the internal pipeline EventHubs.

    Priority:
    1. SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING env var
    2. eventhub.source.namespace_connection_string from config.yaml
    3. Falls back to _get_namespace_connection_string() for backward compatibility

    Returns:
        Namespace connection string with EntityPath removed (if present).

    Raises:
        ValueError: If no connection string is configured anywhere.
    """
    # 1. Dedicated env var
    conn = os.getenv("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING is set but empty or contains only whitespace. "
                "Ensure the environment variable contains a valid Event Hub connection string."
            )
        return stripped

    # 2. Config file (eventhub.source.namespace_connection_string)
    config = _load_eventhub_config()
    source_config = config.get(SOURCE_CONFIG_KEY, {})
    conn = source_config.get(NAMESPACE_CONNECTION_STRING_KEY, "")
    if conn:
        stripped = _strip_entity_path(conn)
        if not stripped or not stripped.strip():
            raise ValueError(
                "eventhub.source.namespace_connection_string in config.yaml is set but empty or "
                "contains only whitespace. Ensure the configuration contains a valid Event Hub connection string."
            )
        return stripped

    # 3. Fall back to regular namespace connection string
    return _get_namespace_connection_string()


def _strip_entity_path(connection_string: str) -> str:
    """Remove EntityPath from a connection string if present.

    This normalizes entity-level connection strings to namespace-level
    so the SDK's `eventhub_name` parameter can be used instead.
    """
    parts = [
        part
        for part in connection_string.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)


def _resolve_eventhub_name(
    domain: str,
    topic_key: str | None,
    worker_name: str,
) -> str:
    """Resolve Event Hub name for a given domain/topic.

    Priority:
    1. config.yaml: eventhub.{domain}.{topic_key}.eventhub_name
    2. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}

    Args:
        domain: Pipeline domain (e.g., "verisk", "claimx")
        topic_key: Topic key matching config.yaml (e.g., "events", "downloads_pending")
        worker_name: Worker name for env var lookup

    Returns:
        Event Hub name

    Raises:
        ValueError: If Event Hub name cannot be resolved
    """
    # 1. Config file lookup (preferred) — check source section first
    if topic_key:
        config = _load_eventhub_config()

        # Check eventhub.source.{domain}.{topic_key} first
        source_config = config.get(SOURCE_CONFIG_KEY) or {}
        source_domain = source_config.get(domain) or {}
        source_topic = source_domain.get(topic_key) or {}
        eventhub_name = source_topic.get(EVENTHUB_NAME_KEY)
        if eventhub_name:
            logger.debug(
                f"Resolved Event Hub from source config: "
                f"eventhub.source.{domain}.{topic_key}.eventhub_name={eventhub_name}"
            )
            return eventhub_name

        # Then check eventhub.{domain}.{topic_key}
        domain_config = config.get(domain) or {}
        topic_config = domain_config.get(topic_key) or {}
        eventhub_name = topic_config.get(EVENTHUB_NAME_KEY)
        if eventhub_name:
            logger.debug(
                f"Resolved Event Hub from config: "
                f"eventhub.{domain}.{topic_key}.eventhub_name={eventhub_name}"
            )
            return eventhub_name

    # 2. Worker-specific env var
    worker_env_var = f"EVENTHUB_NAME_{worker_name.upper().replace('-', '_')}"
    eventhub_name = os.getenv(worker_env_var)
    if eventhub_name:
        logger.debug("Using Event Hub name from %s: %s", worker_env_var, eventhub_name)
        return eventhub_name

    raise ValueError(
        f"Event Hub name not configured for domain='{domain}', topic_key='{topic_key}', "
        f"worker='{worker_name}'. Configure in config.yaml under "
        f"eventhub.{domain}.{topic_key}.eventhub_name or set {worker_env_var} environment variable."
    )


def _resolve_eventhub_consumer_group(
    domain: str,
    topic_key: str | None,
    worker_name: str,
    message_config: MessageConfig,
) -> str:
    """Resolve Event Hub consumer group for a given domain/topic/worker.

    Unlike Kafka, Event Hub consumer groups must be explicitly created in Azure
    before use. This function requires the consumer group to be explicitly
    configured — it will NOT generate a name automatically.

    Lookup order:
    1. config.yaml: eventhub.{domain}.{topic_key}.consumer_groups.{worker_name}
    2. config.yaml: eventhub.default_consumer_group

    Raises ValueError if not configured, to prevent silent failures from
    connecting with a consumer group that doesn't exist on the Event Hub.
    """
    config = _load_eventhub_config()

    # 1. Worker-specific consumer group — check source section first
    if topic_key:
        # Check eventhub.source.{domain}.{topic_key}.consumer_groups.{worker_name}
        source_config = config.get(SOURCE_CONFIG_KEY) or {}
        source_domain = source_config.get(domain) or {}
        source_topic = source_domain.get(topic_key) or {}
        source_groups = source_topic.get(CONSUMER_GROUPS_KEY) or {}
        consumer_group = source_groups.get(worker_name)
        if consumer_group:
            logger.debug(
                f"Resolved consumer group from source config: "
                f"eventhub.source.{domain}.{topic_key}.consumer_groups.{worker_name}={consumer_group}"
            )
            return consumer_group

        # Then check eventhub.{domain}.{topic_key}.consumer_groups.{worker_name}
        domain_config = config.get(domain) or {}
        topic_config = domain_config.get(topic_key) or {}
        consumer_groups = topic_config.get(CONSUMER_GROUPS_KEY) or {}
        consumer_group = consumer_groups.get(worker_name)
        if consumer_group:
            logger.debug(
                f"Resolved consumer group from config: "
                f"eventhub.{domain}.{topic_key}.consumer_groups.{worker_name}={consumer_group}"
            )
            return consumer_group

    # 2. Default from config
    default_group = config.get(DEFAULT_CONSUMER_GROUP_KEY)
    if default_group:
        return default_group

    raise ValueError(
        f"Event Hub consumer group not configured for domain='{domain}', "
        f"topic_key='{topic_key}', worker='{worker_name}'. "
        f"Event Hub consumer groups must be pre-created in Azure and explicitly "
        f"configured in config.yaml under "
        f"eventhub.{domain}.{topic_key}.consumer_groups.{worker_name} "
        f"or eventhub.default_consumer_group."
    )


def _resolve_owner_level(
    domain: str,
    topic_key: str | None,
    worker_name: str,
) -> int:
    """Resolve Event Hub owner level (epoch) for a given domain/topic/worker.

    Owner level determines exclusive partition ownership — a consumer with a
    higher owner_level takes ownership from consumers with lower levels.

    Lookup order:
    1. eventhub.source.{domain}.{topic_key}.owner_levels.{worker_name}
    2. eventhub.{domain}.{topic_key}.owner_levels.{worker_name}
    3. Default: 0 (no exclusive ownership)
    """
    config = _load_eventhub_config()

    if topic_key:
        # Check source section first
        source_config = config.get(SOURCE_CONFIG_KEY) or {}
        source_domain = source_config.get(domain) or {}
        source_topic = source_domain.get(topic_key) or {}
        source_levels = source_topic.get(OWNER_LEVELS_KEY) or {}
        level = source_levels.get(worker_name)
        if level is not None:
            return int(level)

        # Then check domain section
        domain_config = config.get(domain) or {}
        topic_config = domain_config.get(topic_key) or {}
        owner_levels = topic_config.get(OWNER_LEVELS_KEY) or {}
        level = owner_levels.get(worker_name)
        if level is not None:
            return int(level)

    return 0


def _parse_starting_position(value: str) -> tuple[str | datetime.datetime, bool]:
    """Parse a starting position config value into SDK-compatible (position, inclusive) tuple.

    Args:
        value: One of "@latest", "-1", or an ISO datetime string

    Returns:
        (position, inclusive) — inclusive is True for datetime positions

    Raises:
        ValueError: If value is not a recognized starting position
    """
    if value == "@latest":
        return ("@latest", False)
    if value == "-1":
        return ("-1", False)

    # Try ISO datetime
    try:
        dt = datetime.datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return (dt, True)
    except (ValueError, TypeError):
        pass

    raise ValueError(
        f"Invalid starting_position '{value}'. "
        f"Must be '@latest', '-1', or an ISO datetime string (e.g., '2025-06-01T00:00:00Z')."
    )


def _resolve_starting_position(
    domain: str,
    topic_key: str | None,
) -> tuple[str | datetime.datetime, bool]:
    """Resolve starting position from config, following the same priority chain as consumer group.

    Lookup order:
    1. eventhub.source.{domain}.{topic_key}.starting_position
    2. eventhub.{domain}.{topic_key}.starting_position
    3. eventhub.default_starting_position
    4. Hardcoded "@latest"

    Returns:
        (position, inclusive) tuple for the Azure SDK
    """
    config = _load_eventhub_config()

    if topic_key:
        # Check eventhub.source.{domain}.{topic_key}.starting_position
        source_config = config.get(SOURCE_CONFIG_KEY) or {}
        source_domain = source_config.get(domain) or {}
        source_topic = source_domain.get(topic_key) or {}
        value = source_topic.get(STARTING_POSITION_KEY)
        if value:
            return _parse_starting_position(str(value))

        # Check eventhub.{domain}.{topic_key}.starting_position
        domain_config = config.get(domain) or {}
        topic_config = domain_config.get(topic_key) or {}
        value = topic_config.get(STARTING_POSITION_KEY)
        if value:
            return _parse_starting_position(str(value))

    # Global default
    value = config.get(DEFAULT_STARTING_POSITION_KEY)
    if value:
        return _parse_starting_position(str(value))

    # Hardcoded fallback
    return ("@latest", False)


# =============================================================================
# Factory functions
# =============================================================================


async def _setup_eventhub_consumer_args(
    domain: str,
    worker_name: str,
    topics: list[str],
    topic_key: str | None,
    config: MessageConfig,
    *,
    connection_string: str | None = None,
    starting_position: str | None = None,
    consumer_label: str = "consumer",
) -> dict:
    """Resolve common Event Hub consumer configuration.

    Returns a dict with: connection_string, eventhub_name, consumer_group,
    checkpoint_store, starting_position, starting_position_inclusive.
    """
    namespace_connection_string = connection_string or _get_namespace_connection_string()

    if topic_key:
        eventhub_name = _resolve_eventhub_name(domain, topic_key, worker_name)
    else:
        eventhub_name = topics[0]

    consumer_group = _resolve_eventhub_consumer_group(domain, topic_key, worker_name, config)
    owner_level = _resolve_owner_level(domain, topic_key, worker_name)

    checkpoint_store = None
    try:
        checkpoint_store = await get_checkpoint_store()
        if checkpoint_store is None:
            logger.info(
                f"Event Hub {consumer_label} will use in-memory checkpointing: "
                f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
                f"Configure checkpoint store in config.yaml for durable offset persistence."
            )
        else:
            logger.info(
                f"Event Hub {consumer_label} initialized with blob checkpoint store: "
                f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}"
            )
    except Exception as e:
        _DEV_ENVS = {"development", "local", "dev", "test"}
        env = os.getenv("ENVIRONMENT", "").lower()
        app_env = os.getenv("APP_ENV", "").lower()
        if env not in _DEV_ENVS and app_env not in _DEV_ENVS:
            raise RuntimeError(
                f"Checkpoint store initialization failed (ENVIRONMENT={env!r}): {e}. "
                f"In-memory fallback is only allowed in dev environments "
                f"({', '.join(sorted(_DEV_ENVS))})."
            ) from e
        logger.error(
            f"Failed to initialize checkpoint store for Event Hub {consumer_label}: "
            f"domain={domain}, worker={worker_name}, eventhub={eventhub_name}. "
            f"Falling back to in-memory checkpointing. Error: {e}"
        )

    if starting_position is not None:
        pos, pos_inclusive = _parse_starting_position(starting_position)
    else:
        pos, pos_inclusive = _resolve_starting_position(domain, topic_key)

    return {
        "connection_string": namespace_connection_string,
        "eventhub_name": eventhub_name,
        "consumer_group": consumer_group,
        "owner_level": owner_level,
        "checkpoint_store": checkpoint_store,
        "starting_position": pos,
        "starting_position_inclusive": pos_inclusive,
    }


def create_producer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    topic: str | None = None,
    topic_key: str | None = None,
):
    """Create an EventHub producer instance.

    Args:
        config: MessageConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging
        topic: Optional explicit Event Hub name (overrides config-based detection)
        topic_key: Optional topic key for Event Hub resolution from config.yaml
                   (e.g., "events", "downloads_pending"). When provided, the
                   Event Hub name is looked up from
                   eventhub.{domain}.{topic_key}.eventhub_name.

    Returns:
        EventHubProducer instance

    Note:
        Name resolution priority:
        1. Explicit 'topic' parameter
        2. config.yaml lookup via topic_key
        3. Worker-specific env var: EVENTHUB_NAME_{WORKER_NAME}
    """
    from pipeline.common.eventhub.producer import EventHubProducer

    logger.debug(
        f"Creating producer: domain={domain}, worker={worker_name}"
    )

    # Get namespace connection string
    logger.debug(
        "Attempting to load Event Hub namespace connection string",
        extra={
            "EVENTHUB_NAMESPACE_CONNECTION_STRING": "***SET***"
            if os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING")
            else "NOT SET",
            "EVENTHUB_CONNECTION_STRING": "***SET***"
            if os.getenv("EVENTHUB_CONNECTION_STRING")
            else "NOT SET",
        },
    )
    namespace_connection_string = _get_namespace_connection_string()

    # Determine Event Hub name
    # When topic_key is provided, always resolve from config.yaml
    # (topic may be a Kafka topic name which differs from the Event Hub entity name)
    if topic_key:
        eventhub_name = _resolve_eventhub_name(domain, topic_key, worker_name)
    else:
        eventhub_name = topic or _resolve_eventhub_name(domain, topic_key, worker_name)

    logger.info(
        f"Creating Event Hub producer: domain={domain}, worker={worker_name}, "
        f"eventhub={eventhub_name}"
    )

    return EventHubProducer(
        connection_string=namespace_connection_string,
        domain=domain,
        worker_name=worker_name,
        eventhub_name=eventhub_name,
    )


async def create_consumer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    message_handler: Callable[[PipelineMessage], Awaitable[None]],
    *,
    topic_key: str | None = None,
    connection_string: str | None = None,
    consumer_config: ConsumerConfig | None = None,
):
    """Create an EventHub consumer instance.

    Args:
        config: MessageConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging
        topics: List of topics to consume from (must be single topic)
        message_handler: Async function to process each message
        topic_key: Optional topic key for Event Hub / consumer group resolution
                   from config.yaml (e.g., "events", "downloads_pending").
        connection_string: Optional Event Hub connection string override.
                           When provided, uses this instead of the default namespace
                           connection string. Use for source topics that live in a
                           different namespace.
        consumer_config: Optional tuning parameters (commit, prefetch, position,
                         checkpoint interval, etc.). Defaults to ``ConsumerConfig()``.

    Returns:
        EventHubConsumer instance
    """
    cc = consumer_config or ConsumerConfig()

    if len(topics) != 1:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic. "
            f"Got {len(topics)} topics: {topics}"
        )

    from pipeline.common.eventhub.consumer import EventHubConsumer

    eh = await _setup_eventhub_consumer_args(
        domain, worker_name, topics, topic_key, config,
        connection_string=connection_string,
        starting_position=cc.starting_position,
        consumer_label="consumer",
    )

    logger.info(
        f"Creating Event Hub consumer: domain={domain}, worker={worker_name}, "
        f"eventhub={eh['eventhub_name']}, group={eh['consumer_group']}, "
        f"starting_position={eh['starting_position']}"
    )

    return EventHubConsumer(
        connection_string=eh["connection_string"],
        domain=domain,
        worker_name=worker_name,
        eventhub_name=eh["eventhub_name"],
        consumer_group=eh["consumer_group"],
        message_handler=message_handler,
        enable_message_commit=cc.enable_message_commit,
        instance_id=cc.instance_id,
        checkpoint_store=eh["checkpoint_store"],
        prefetch=cc.prefetch,
        starting_position=eh["starting_position"],
        starting_position_inclusive=eh["starting_position_inclusive"],
        checkpoint_interval=cc.checkpoint_interval,
        owner_level=eh["owner_level"],
        on_partition_revoked=cc.on_partition_revoked,
    )


async def create_batch_consumer(
    config: MessageConfig,
    domain: str,
    worker_name: str,
    topics: list[str],
    batch_handler: Callable[[list[PipelineMessage]], Awaitable[bool]],
    *,
    topic_key: str | None = None,
    connection_string: str | None = None,
    consumer_config: ConsumerConfig | None = None,
    health_server: Any | None = None,
):
    """Create an EventHub batch consumer for concurrent message processing.

    Args:
        config: MessageConfig with connection details
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for logging and metrics
        topics: List of topics to consume (must be single topic)
        batch_handler: Async function that processes message batches
        topic_key: Optional topic key for EventHub resolution from config.yaml
        connection_string: Optional Event Hub connection string override.
        consumer_config: Optional tuning parameters (batch size, prefetch,
                         commit strategy, etc.). Defaults to ``ConsumerConfig()``.

    Returns:
        EventHubBatchConsumer instance

    Batch Handler Contract:
        The batch_handler receives a list of PipelineMessage objects and must:
        - Return True to commit/checkpoint the batch
        - Return False to skip commit (messages will be redelivered)
        - Raise an exception to skip commit (messages will be redelivered)
    """
    cc = consumer_config or ConsumerConfig()

    if len(topics) != 1:
        raise ValueError(
            f"Event Hub transport only supports consuming from a single topic. "
            f"Got {len(topics)} topics: {topics}"
        )

    from pipeline.common.eventhub.batch_consumer import EventHubBatchConsumer

    eh = await _setup_eventhub_consumer_args(
        domain, worker_name, topics, topic_key, config,
        connection_string=connection_string,
        starting_position=cc.starting_position,
        consumer_label="batch consumer",
    )

    logger.info(
        f"Creating Event Hub batch consumer: domain={domain}, worker={worker_name}, "
        f"eventhub={eh['eventhub_name']}, group={eh['consumer_group']}, "
        f"batch_size={cc.batch_size}, timeout_ms={cc.batch_timeout_ms}, "
        f"starting_position={eh['starting_position']}"
    )

    return EventHubBatchConsumer(
        connection_string=eh["connection_string"],
        domain=domain,
        worker_name=worker_name,
        eventhub_name=eh["eventhub_name"],
        consumer_group=eh["consumer_group"],
        batch_handler=batch_handler,
        batch_size=cc.batch_size,
        max_batch_size=cc.max_batch_size,
        batch_timeout_ms=cc.batch_timeout_ms,
        enable_message_commit=cc.enable_message_commit,
        instance_id=cc.instance_id,
        checkpoint_store=eh["checkpoint_store"],
        prefetch=cc.prefetch,
        starting_position=eh["starting_position"],
        starting_position_inclusive=eh["starting_position_inclusive"],
        owner_level=eh["owner_level"],
        health_server=health_server,
    )


__all__ = [
    "ConsumerConfig",
    "create_producer",
    "create_consumer",
    "create_batch_consumer",
    "get_source_connection_string",
]
