"""Exception hierarchy. Everything raised by this package subclasses
MaterializedError so callers can catch broadly."""


class MaterializedError(Exception):
    """Base class for all hotdata-materialized errors."""


class ConfigurationError(MaterializedError):
    """The HOTDATA_MATERIALIZED setting is missing or incomplete."""


class FingerprintError(MaterializedError):
    """A call could not be fingerprinted deterministically."""


class RegistryError(MaterializedError):
    """A registry read or write failed."""


class StoreError(MaterializedError):
    """Materializing or evicting an entry failed."""
