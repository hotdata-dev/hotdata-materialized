import io
import json
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hotdata.exceptions import ApiException

from hotdata_materialized.conf import Config
from hotdata_materialized.exceptions import StoreError
from hotdata_materialized.registry import STATUS_BUILDING, STATUS_READY, Registry
from hotdata_materialized.store import (
    EntryStore,
    decode_inline_payload,
    encode_inline_payload,
)
from tests.fakes import FakeBackend
from tests.test_registry import FP, Clock


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


@pytest.fixture
def store(backend, config, registry, clock):
    return EntryStore(backend, config, registry, now_fn=clock)


@pytest.fixture
def table():
    return pa.table({"channel": ["organic", "paid"], "n": [10, 7]})


def entry_database_ids(backend):
    return [
        db_id
        for db_id, req in backend.database_meta.items()
        if (req.name or "").startswith("mz_")
    ]


# -- write-behind (the default) ---------------------------------------------


def test_materialize_returns_pending_entry_immediately(store, registry, table):
    entry = store.materialize(FP, table, key="daily", ttl=3600)
    assert entry.status == STATUS_BUILDING
    assert entry.database_id is None
    assert store.flush() == []
    stored = registry.lookup(FP)
    assert stored.status == STATUS_READY
    assert stored.database_id


def test_materialize_creates_entry_database(backend, store, registry, table):
    store.materialize(FP, table, key="daily", ttl=3600, version=2)
    store.flush()
    (db_id,) = entry_database_ids(backend)
    assert registry.lookup(FP).database_id == db_id
    request = backend.database_meta[db_id]
    assert request.name == "mz_" + FP[:32]
    # server-side expiry backstop: ttl + grace beyond the registry expiry
    assert request.expires_at == "2026-07-23T13:00:00+00:00"
    assert request.schemas[0].name == "main"
    assert request.schemas[0].tables[0].name == "data"


def test_materialize_uploads_parquet_into_data_table(backend, store, table):
    store.materialize(FP, table, ttl=60)
    store.flush()
    ((db_id, schema, table_name, request),) = [
        load for load in backend.single_loads if load[2] == "data"
    ]
    assert schema == "main"
    assert request.mode == "replace"
    assert request.format == "parquet"
    uploaded = pq.read_table(io.BytesIO(backend.upload_blobs[request.upload_id]))
    assert uploaded.equals(table)


def test_materialize_marks_registry_ready(store, registry, table):
    store.materialize(FP, table, key="daily", ttl=3600, version=2)
    store.flush()
    stored = registry.lookup(FP)
    assert stored.status == STATUS_READY
    assert stored.row_count == 2
    assert stored.key == "daily"
    assert stored.version == 2
    assert json.loads(stored.schema_json) == [
        {"name": "channel", "type": "string"},
        {"name": "n", "type": "int64"},
    ]
    assert stored.inline_payload is not None
    assert decode_inline_payload(stored.inline_payload).equals(table)


def test_background_failure_is_reported_by_flush(backend, store, registry, table):
    backend.fail_upload_once = True
    entry = store.materialize(FP, table, ttl=60)  # does not raise
    assert entry.status == STATUS_BUILDING
    errors = store.flush()
    assert len(errors) == 1
    assert isinstance(errors[0], StoreError)
    assert entry_database_ids(backend) == []
    assert len(backend.deleted_databases) == 1
    assert registry.lookup(FP) is None  # failed persist leaves no row


def test_same_fingerprint_concurrent_persists_are_all_tracked(
    backend, store, registry, table
):
    backend.stall_uploads = threading.Event()
    store.materialize(FP, table, ttl=60)
    store.materialize(FP, table, ttl=120)
    with store._inflight_lock:
        assert len(store._inflight[FP]) == 2
    backend.stall_uploads.set()
    assert store.flush() == []
    assert FP not in store._inflight
    assert registry.lookup(FP).status == STATUS_READY


def test_evict_waits_for_inflight_persist(backend, store, registry, table):
    store.materialize(FP, table, ttl=60)
    store.evict(FP)
    assert registry.lookup(FP) is None
    assert entry_database_ids(backend) == []


def test_registry_publication_failure_deletes_database(backend, store, registry, table):
    # data load succeeds, then the mark_ready registry write fails: the
    # populated database must not leak (publication is in the failure domain)
    registry.database_id()  # bootstrap (and its seed upload) before counting
    calls = {"n": 0}
    original = backend.uploads.upload_file

    def flaky(source, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:  # 1 = entry data upload, 2 = mark_ready upsert
            raise ApiException(status=503, reason="registry unavailable")
        return original(source, **kwargs)

    backend.uploads.upload_file = flaky
    with pytest.raises(StoreError):
        store.materialize(FP, table, ttl=60, background=False)
    assert entry_database_ids(backend) == []
    assert len(backend.deleted_databases) == 1
    assert registry.lookup(FP) is None


# -- write-through (background=False) ----------------------------------------


def test_synchronous_mode_returns_ready_entry(store, registry, table):
    entry = store.materialize(FP, table, ttl=3600, background=False)
    assert entry.status == STATUS_READY
    assert entry.database_id
    assert registry.lookup(FP).database_id == entry.database_id


def test_synchronous_failed_upload_raises_and_cleans_up(
    backend, store, registry, table
):
    backend.fail_upload_once = True
    with pytest.raises(StoreError):
        store.materialize(FP, table, ttl=60, background=False)
    assert entry_database_ids(backend) == []
    assert len(backend.deleted_databases) == 1


# -- reads ---------------------------------------------------------------------


def test_read_table_serves_inline_entries_locally(backend, store, registry, table):
    store.materialize(FP, table, ttl=60)
    store.flush()
    entry = registry.lookup(FP)
    assert entry.inline_payload is not None
    assert store.read_table(entry).equals(table)
    assert backend.arrow_fetches == []  # decoded locally, no fetch


def test_read_table_fetches_remote_entries_via_stored_result(
    backend, registry, clock, table
):
    tiny = Config(api_key="k", workspace_id="w", inline_threshold_bytes=8)
    store = EntryStore(backend, tiny, registry, now_fn=clock)
    store.materialize(FP, table, ttl=60)
    store.flush()
    entry = registry.lookup(FP)
    assert entry.inline_payload is None
    assert entry.result_id  # minted at persist time
    result = store.read_table(entry)
    assert result.to_pylist() == table.to_pylist()
    assert backend.arrow_fetches == [entry.result_id]  # one call, no re-query


def test_read_table_requeries_when_stored_result_expired(
    backend, registry, clock, table
):
    tiny = Config(api_key="k", workspace_id="w", inline_threshold_bytes=8)
    store = EntryStore(backend, tiny, registry, now_fn=clock)
    store.materialize(FP, table, ttl=60)
    store.flush()
    entry = registry.lookup(FP)
    del backend.result_tables[entry.result_id]  # server expired the result
    result = store.read_table(entry)
    assert result.to_pylist() == table.to_pylist()


# -- shared behavior ----------------------------------------------------------


def test_large_result_skips_inline_payload(backend, registry, clock, table):
    tiny = Config(api_key="k", workspace_id="w", inline_threshold_bytes=8)
    store = EntryStore(backend, tiny, registry, now_fn=clock)
    store.materialize(FP, table, ttl=60)
    store.flush()
    assert registry.lookup(FP).inline_payload is None


def test_no_ttl_means_no_database_expiry(backend, store, table):
    store.materialize(FP, table, ttl=None)
    store.flush()
    (db_id,) = entry_database_ids(backend)
    assert backend.database_meta[db_id].expires_at is None


def test_evict_deletes_database_and_row(backend, store, registry, table):
    store.materialize(FP, table, ttl=60)
    store.flush()
    (db_id,) = entry_database_ids(backend)
    store.evict(FP)
    assert db_id in backend.deleted_databases
    assert registry.lookup(FP) is None


def test_evict_tolerates_already_deleted_database(backend, store, registry, table):
    store.materialize(FP, table, ttl=60)
    store.flush()
    (db_id,) = entry_database_ids(backend)
    backend._delete_database(db_id)
    store.evict(FP)
    assert registry.lookup(FP) is None


def test_evict_of_unknown_fingerprint_is_a_noop(store, registry):
    store.evict("f" * 64)
    assert registry.lookup("f" * 64) is None


def test_inline_payload_round_trip(table):
    payload = encode_inline_payload(table, threshold_bytes=10**6)
    assert payload is not None
    assert decode_inline_payload(payload).equals(table)
    assert encode_inline_payload(table, threshold_bytes=1) is None
