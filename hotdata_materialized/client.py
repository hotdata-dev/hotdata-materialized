"""Hotdata SDK client construction.

One ApiClient (and its connection pool) per process, shared by the query,
databases, and uploads resource APIs.
"""

from __future__ import annotations

import threading
from typing import Optional

import hotdata
from hotdata.arrow import ResultsApi
from hotdata.query import QueryApi
from hotdata.uploads import UploadsApi

from .conf import Config


class HotdataClients:
    def __init__(self, config: Config):
        sdk_config = hotdata.Configuration(
            host=config.api_url,
            api_key=config.api_key,
            workspace_id=config.workspace_id,
        )
        self.api_client = hotdata.ApiClient(sdk_config)
        self.query = QueryApi(self.api_client)
        self.databases = hotdata.DatabasesApi(self.api_client)
        self.uploads = UploadsApi(self.api_client)
        self.results = ResultsApi(self.api_client)


_lock = threading.Lock()
_clients: Optional[HotdataClients] = None


def get_clients(config: Optional[Config] = None) -> HotdataClients:
    global _clients
    with _lock:
        if _clients is None:
            _clients = HotdataClients(config or Config.from_django())
        return _clients


def reset_clients() -> None:
    global _clients
    with _lock:
        _clients = None
