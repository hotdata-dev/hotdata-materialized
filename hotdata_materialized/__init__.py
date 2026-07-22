"""hotdata-materialized: offload expensive Django query results to Hotdata.

Public surface: the @materialize decorator and MaterializedFrame handle,
built on fingerprinting, the remote registry, and the entry store.
"""

from .conf import Config
from .client import HotdataClients, get_clients, reset_clients
from .decorator import MaterializedFrame, materialize
from .exceptions import (
    ConfigurationError,
    FingerprintError,
    MaterializedError,
    RegistryError,
    StoreError,
)
from .fingerprint import fingerprint_call, fingerprint_queryset
from .registry import Registry, RegistryEntry
from .store import EntryStore

__version__ = "0.1.0"

__all__ = [
    "materialize",
    "MaterializedFrame",
    "Config",
    "HotdataClients",
    "get_clients",
    "reset_clients",
    "MaterializedError",
    "ConfigurationError",
    "FingerprintError",
    "RegistryError",
    "StoreError",
    "fingerprint_call",
    "fingerprint_queryset",
    "Registry",
    "RegistryEntry",
    "EntryStore",
    "__version__",
]
