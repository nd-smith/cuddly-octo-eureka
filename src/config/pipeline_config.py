"""
Pipeline configuration for Event Hub pipeline.

Architecture:
    - Event Source: Azure Event Hub
    - Internal communication: EventHub (AMQP transport)

Workers:
    - EventIngesterWorker: Reads from Event Hub, produces enrichment tasks
    - DownloadWorker: Reads/writes via EventHub
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config.config import DEFAULT_CONFIG_FILE, MessageConfig
from config.config import get_config_value as _get_config_value

logger = logging.getLogger(__name__)


def _load_config_data(config_path: Path) -> dict[str, Any]:
    """Load and expand env vars in config.yaml. Raises FileNotFoundError if missing."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\nExpected file: config/config.yaml"
        )

    from config.config import _expand_env_vars, load_yaml

    config_data = load_yaml(config_path)
    return _expand_env_vars(config_data)


@dataclass
class EventHubConfig:
    """Configuration for Azure Event Hub connection.

    Supports two modes:
    1. AMQP transport (default): Namespace connection string + per-entity config from config.yaml
    2. Kafka protocol (legacy): Full connection string with SASL_PLAIN authentication

    For AMQP transport, entity names and consumer groups are defined per-topic
    in config.yaml under eventhub.{domain}.{topic_key}.
    """

    # Namespace-level connection string (no EntityPath)
    namespace_connection_string: str = ""

    # Kafka-compatible settings (legacy, for Kafka protocol fallback)
    bootstrap_servers: str = ""
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = "$ConnectionString"
    sasl_password: str = ""  # Full connection string for Kafka protocol

    # Consumer settings (defaults, can be overridden per-topic in config.yaml)
    events_topic: str = "verisk_events"
    consumer_group: str = "xact-event-ingester"
    auto_offset_reset: str = "earliest"

    @classmethod
    def from_env(cls) -> "EventHubConfig":
        """Load from environment variables.

        Required (at least one):
            EVENTHUB_NAMESPACE_CONNECTION_STRING (preferred)
            EVENTHUB_CONNECTION_STRING (backward compat)
        """
        namespace_conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING", "")
        legacy_conn = os.getenv("EVENTHUB_CONNECTION_STRING", "")
        bootstrap_servers = os.getenv("EVENTHUB_BOOTSTRAP_SERVERS", "")

        if not namespace_conn and not legacy_conn:
            raise ValueError(
                "Event Hub connection string is required. "
                "Set EVENTHUB_NAMESPACE_CONNECTION_STRING (preferred) "
                "or EVENTHUB_CONNECTION_STRING environment variable."
            )

        return cls(
            namespace_connection_string=namespace_conn or legacy_conn,
            bootstrap_servers=bootstrap_servers,
            sasl_password=legacy_conn or namespace_conn,
            events_topic=os.getenv("EVENTHUB_EVENTS_TOPIC", "verisk_events"),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", "xact-event-ingester"),
            auto_offset_reset=os.getenv("EVENTHUB_AUTO_OFFSET_RESET", "earliest"),
        )

    def to_message_config(self) -> MessageConfig:
        """Convert to MessageConfig for Kafka-protocol consumers."""
        verisk_config = {
            "topics": {
                "events": self.events_topic,
            },
            "consumer_group_prefix": (
                self.consumer_group.rsplit("-", 1)[0]
                if "-" in self.consumer_group
                else "verisk"
            ),
            "event_ingester": {
                "consumer": {
                    "group_id": self.consumer_group,
                    "auto_offset_reset": self.auto_offset_reset,
                }
            },
        }

        return MessageConfig(
            bootstrap_servers=self.bootstrap_servers,
            verisk=verisk_config,
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration (EventHub + read-only Delta Lake paths)."""

    eventhub: EventHubConfig | None = None
    domain: str = "verisk"  # OneLake routing domain ("verisk" or "claimx")

    # ClaimX project cache (read-only, used by enrichment worker)
    claimx_projects_table_path: str = ""

    @classmethod
    def load_config(cls, config_path: Path | None = None) -> "PipelineConfig":
        """Load config. Priority: env vars > config.yaml > dataclass defaults."""
        resolved_path = config_path or DEFAULT_CONFIG_FILE
        yaml_data = _load_config_data(resolved_path)

        eventhub_config = EventHubConfig.from_env()

        delta_config = yaml_data.get("delta", {})
        domain = os.getenv("PIPELINE_DOMAIN", "verisk")

        return cls(
            eventhub=eventhub_config,
            domain=domain,
            claimx_projects_table_path=_get_config_value(
                "CLAIMX_PROJECTS_TABLE_PATH",
                delta_config.get("claimx", {}).get("projects_table_path", ""),
            ),
        )


def get_pipeline_config(config_path: Path | None = None) -> PipelineConfig:
    """Load pipeline configuration from config.yaml and environment."""
    return PipelineConfig.load_config(config_path)
