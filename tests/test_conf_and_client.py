import pytest

from hotdata_materialized.conf import Config
from hotdata_materialized.client import get_clients, reset_clients
from hotdata_materialized.exceptions import ConfigurationError


def test_missing_required_keys_rejected():
    with pytest.raises(ConfigurationError, match="API_KEY"):
        Config.from_dict({"WORKSPACE_ID": "w"})


def test_from_django_reads_the_settings_dict():
    config = Config.from_django()  # conftest configures HOTDATA_MATERIALIZED
    assert config.api_key == "hd_test"
    assert config.workspace_id == "ws_test"


def test_clients_are_a_process_singleton():
    reset_clients()
    try:
        config = Config(api_key="k", workspace_id="w")
        bundle = get_clients(config)
        assert get_clients(config) is bundle
        reset_clients()
        assert get_clients(config) is not bundle
    finally:
        reset_clients()
