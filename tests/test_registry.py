import datetime

import pytest

from hotdata_materialized.conf import Config
from hotdata_materialized.registry import STATUS_FAILED, STATUS_READY, Registry
from tests.fakes import FakeBackend


class Clock:
    def __init__(self, start="2026-07-22T12:00:00+00:00"):
        self.moment = datetime.datetime.fromisoformat(start)

    def advance(self, seconds):
        self.moment += datetime.timedelta(seconds=seconds)

    def __call__(self):
        return self.moment.isoformat()


@pytest.fixture
def config():
    return Config(api_key="hd_test", workspace_id="ws_test")


@pytest.fixture
def backend():
    return FakeBackend()


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def registry(backend, config, clock):
    return Registry(backend, config, now_fn=clock)


FP = "a" * 64


def ready_entry(registry, fingerprint=FP, **overrides):
    defaults = dict(database_id="db_9", ttl=3600, row_count=10, byte_size=1234)
    defaults.update(overrides)
    return registry.mark_ready(fingerprint, **defaults)


def test_bootstrap_creates_registry_database_once(backend, registry):
    db_id = registry.database_id()
    assert backend.database_meta[db_id].name == "materialized_registry"
    assert registry.database_id() == db_id
    assert len(backend.database_meta) == 1


def test_bootstrap_finds_existing_database(backend, config, clock):
    first = Registry(backend, config, now_fn=clock)
    existing_id = first.database_id()
    second = Registry(backend, config, now_fn=clock)
    assert second.database_id() == existing_id
    assert len(backend.database_meta) == 1


def test_lookup_miss_returns_none(registry):
    assert registry.lookup(FP) is None


def test_mark_ready_round_trips_all_fields(registry, clock):
    entry = ready_entry(
        registry,
        inline_payload="cGF5bG9hZA==",
        schema_json='[{"name":"n","type":"int64"}]',
        key="daily-signups",
        version=3,
    )
    stored = registry.lookup(FP)
    assert stored.status == STATUS_READY
    assert stored.database_id == "db_9"
    assert stored.expires_at == entry.expires_at == "2026-07-22T13:00:00+00:00"
    assert stored.row_count == 10
    assert stored.inline_payload == "cGF5bG9hZA=="
    assert stored.key == "daily-signups"
    assert stored.version == 3
    assert not stored.is_expired(clock())
    clock.advance(3601)
    assert stored.is_expired(clock())


def test_mark_ready_without_ttl_never_expires(registry, clock):
    ready_entry(registry, ttl=None)
    stored = registry.lookup(FP)
    assert stored.expires_at is None
    clock.advance(10**9)
    assert not stored.is_expired(clock())


def test_mark_ready_again_replaces_the_row(registry):
    ready_entry(registry, database_id="db_1", row_count=1)
    ready_entry(registry, database_id="db_2", row_count=99)
    stored = registry.lookup(FP)
    assert stored.database_id == "db_2"
    assert stored.row_count == 99


def test_mark_failed(registry):
    ready_entry(registry)
    registry.mark_failed(FP)
    assert registry.lookup(FP).status == STATUS_FAILED


def test_mark_failed_without_row_is_a_noop(registry):
    registry.mark_failed(FP)
    assert registry.lookup(FP) is None


def test_delete(registry):
    ready_entry(registry)
    registry.delete(FP)
    assert registry.lookup(FP) is None


def test_list_expired(registry, clock):
    ready_entry(registry, "a" * 64, database_id="db_1", ttl=60)
    ready_entry(registry, "b" * 64, database_id="db_2", ttl=7200)
    ready_entry(registry, "c" * 64, database_id="db_3", ttl=None)
    clock.advance(61)
    expired = registry.list_expired()
    assert [e.fingerprint for e in expired] == ["a" * 64]


def test_values_with_quotes_round_trip(registry):
    ready_entry(registry, key="it's a 'key'")
    assert registry.lookup(FP).key == "it's a 'key'"
