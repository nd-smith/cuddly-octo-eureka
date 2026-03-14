import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from config.pipeline_config import (
    EventHubConfig,
    PipelineConfig,
    _get_config_value,
)


def _write_config(tmp_path, data):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(data))
    return config_file


# =========================================================================
# _get_config_value
# =========================================================================


class TestGetConfigValue:
    def test_prefers_env_var(self):
        with patch.dict(os.environ, {"MY_VAR": "from_env"}):
            assert _get_config_value("MY_VAR", "yaml_val") == "from_env"

    def test_falls_back_to_yaml_value(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _get_config_value("MY_VAR", "yaml_val") == "yaml_val"

    def test_falls_back_to_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _get_config_value("MY_VAR", "", "default") == "default"

    def test_expands_env_vars_in_yaml_value(self):
        with patch.dict(os.environ, {"INNER": "expanded"}, clear=True):
            result = _get_config_value("MISSING", "${INNER}")
            assert result == "expanded"

    def test_warns_on_unexpanded_variable(self):
        with patch.dict(os.environ, {}, clear=True):
            # ${UNDEFINED} stays literal, triggering the warning
            result = _get_config_value("MISSING", "${UNDEFINED}")
            assert "${UNDEFINED}" in result


# =========================================================================
# EventHubConfig.from_env
# =========================================================================


class TestEventHubConfigFromEnv:
    def test_raises_without_connection_string(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="connection string is required"),
        ):
            EventHubConfig.from_env()

    def test_loads_from_namespace_connection_string(self):
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "Endpoint=sb://ns.servicebus.windows.net/"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.namespace_connection_string == "Endpoint=sb://ns.servicebus.windows.net/"

    def test_loads_from_legacy_connection_string(self):
        env = {"EVENTHUB_CONNECTION_STRING": "Endpoint=sb://legacy.servicebus.windows.net/"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert (
                config.namespace_connection_string == "Endpoint=sb://legacy.servicebus.windows.net/"
            )

    def test_prefers_namespace_over_legacy(self):
        env = {
            "EVENTHUB_NAMESPACE_CONNECTION_STRING": "namespace_conn",
            "EVENTHUB_CONNECTION_STRING": "legacy_conn",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.namespace_connection_string == "namespace_conn"

    def test_loads_optional_settings(self):
        env = {
            "EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn",
            "EVENTHUB_BOOTSTRAP_SERVERS": "ns.servicebus.windows.net:9093",
            "EVENTHUB_EVENTS_TOPIC": "custom_topic",
            "EVENTHUB_CONSUMER_GROUP": "custom-group",
            "EVENTHUB_AUTO_OFFSET_RESET": "latest",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.bootstrap_servers == "ns.servicebus.windows.net:9093"
            assert config.events_topic == "custom_topic"
            assert config.consumer_group == "custom-group"
            assert config.auto_offset_reset == "latest"

    def test_defaults_for_optional_settings(self):
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn"}
        with patch.dict(os.environ, env, clear=True):
            config = EventHubConfig.from_env()
            assert config.events_topic == "verisk_events"
            assert config.consumer_group == "xact-event-ingester"
            assert config.auto_offset_reset == "earliest"


# =========================================================================
# PipelineConfig
# =========================================================================


class TestPipelineConfig:
    def test_load_config(self, tmp_path):
        data = {}
        config_file = _write_config(tmp_path, data)
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn_str"}
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.eventhub is not None

    def test_claimx_projects_table_path(self, tmp_path):
        data = {
            "delta": {
                "claimx": {
                    "projects_table_path": "/claimx/projects",
                }
            },
        }
        config_file = _write_config(tmp_path, data)
        env = {"EVENTHUB_NAMESPACE_CONNECTION_STRING": "conn_str"}
        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.load_config(config_file)
        assert config.claimx_projects_table_path == "/claimx/projects"

    def test_missing_config_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PipelineConfig.load_config(Path("/nonexistent/config.yaml"))
