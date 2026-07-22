"""hotdata-materialized: offload expensive Django query results to Hotdata.

Step-1 surface: fingerprinting, the remote registry, and the entry store.
The @materialize decorator and MaterializedFrame build on these (step 2).
"""

from .conf import Config
from .client import HotdataClients, get_clients, reset_clients
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
