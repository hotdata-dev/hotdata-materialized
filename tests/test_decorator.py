import pyarrow as pa
import pytest

import hotdata_materialized.decorator as decorator_module
from hotdata_materialized import materialize
from hotdata_materialized.conf import Config
from hotdata_materialized.decorator import to_arrow
from hotdata_materialized.exceptions import RegistryError
from hotdata_materialized.registry import Registry
from hotdata_materialized.store import EntryStore
from tests.fakes import FakeBackend
from tests.test_registry import Clock


class Runtime:
    def __init__(self):
        self.backend = FakeBackend()
        self.config = Config(api_key="hd_test", workspace_id="ws_test")
        self.clock = Clock()
        self.registry = Registry(self.backend, self.config, now_fn=self.clock)
        self.store = EntryStore(
            self.backend, self.config, self.registry, now_fn=self.clock
        )


@pytest.fixture
def runtime(monkeypatch):
    rt = Runtime()
    monkeypatch.setattr(
        decorator_module, "get_runtime", lambda: (rt.registry, rt.store)
    )
    return rt


ROWS = [{"channel": "organic", "n": 10}, {"channel": "paid", "n": 7}]


def make_report(calls):
    @materialize(ttl=3600)
    def report(day):
        calls["n"] += 1
        return [dict(row, day=day) for row in ROWS]

    return report


def test_miss_runs_function_and_serves_result(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    frame = report("2026-07-22")
    assert frame.cached is False
    assert calls["n"] == 1
    assert frame.to_pylist()[0]["channel"] == "organic"
    assert len(frame) == 2


def test_hit_skips_the_function(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    first = report("2026-07-22")
    runtime.store.flush()
    second = report("2026-07-22")
    assert second.cached is True
    assert calls["n"] == 1
    assert second.to_pylist() == first.to_pylist()


def test_different_arguments_are_different_entries(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-21")
    runtime.store.flush()
    frame = report("2026-07-22")
    assert frame.cached is False
    assert calls["n"] == 2


def test_expired_entry_reruns_the_function(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-22")
    runtime.store.flush()
    runtime.clock.advance(3601)
    frame = report("2026-07-22")
    assert frame.cached is False
    assert calls["n"] == 2


def test_version_busts_the_cache(runtime):
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return list(ROWS)

    report_v1 = materialize(ttl=3600, version=1)(compute)
    report_v2 = materialize(ttl=3600, version=2)(compute)
    report_v1()
    runtime.store.flush()
    frame = report_v2()
    assert frame.cached is False
    assert calls["n"] == 2


def test_fail_open_when_registry_is_unreachable(runtime, monkeypatch):
    calls = {"n": 0}
    report = make_report(calls)

    def down(*args, **kwargs):
        raise RegistryError("registry unavailable")

    monkeypatch.setattr(runtime.registry, "lookup", down)
    frame = report("2026-07-22")
    assert frame.cached is False
    assert calls["n"] == 1
    assert len(frame.to_pylist()) == 2  # the caller still gets data


def test_sql_runs_server_side_against_the_entry(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-22")
    runtime.store.flush()
    frame = report("2026-07-22")
    assert frame.cached is True
    top = frame.sql("SELECT channel FROM this ORDER BY n DESC LIMIT 1")
    assert top.to_pylist() == [{"channel": "organic"}]


def test_sql_leaves_string_literals_alone(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    report("this")  # the day column literally contains "this"
    runtime.store.flush()
    frame = report("this")
    rows = frame.sql("SELECT n FROM this WHERE day = 'this' AND n > 8")
    assert rows.to_pylist() == [{"n": 10}]


def test_to_arrow_accepts_table_and_rows():
    table = pa.table({"n": [1, 2]})
    assert to_arrow(table) is table
    assert to_arrow(iter([{"n": 1}])).to_pylist() == [{"n": 1}]


def test_to_arrow_rejects_non_dict_rows():
    with pytest.raises(TypeError, match="iterable of dicts"):
        to_arrow([1, 2, 3])


def test_index_declarations_create_indexes_at_persist(runtime):
    from hotdata_materialized import BM25, Vector

    @materialize(ttl=60, index=BM25("notes"))
    def notes():
        return [{"notes": "server outage in eu-west", "n": 1}]

    @materialize(ttl=60, index=Vector("notes", provider="emb_1", metric="cosine"))
    def embedded_notes():
        return [{"notes": "payment gateway timeout", "n": 2}]

    notes()
    embedded_notes()
    runtime.store.flush()
    assert len(runtime.backend.index_creates) == 2
    # two background persists race; order by type before asserting
    (conn_id, schema, table, bm25_req), (_, _, _, vec_req) = sorted(
        runtime.backend.index_creates, key=lambda c: c[3].index_type
    )
    assert conn_id.startswith("conn_db_")
    assert (schema, table) == ("main", "data")
    assert bm25_req.index_type == "bm25"
    assert bm25_req.columns == ["notes"]
    assert bm25_req.var_async is True
    assert vec_req.index_type == "vector"
    assert vec_req.metric == "cosine"
    assert vec_req.embedding_provider_id == "emb_1"


def test_no_index_declared_creates_none(runtime):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-22")
    runtime.store.flush()
    assert runtime.backend.index_creates == []


def test_search_builds_bm25_sql(runtime, monkeypatch):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-22")
    runtime.store.flush()
    frame = report("2026-07-22")
    captured: dict[str, str] = {}
    monkeypatch.setattr(
        runtime.store, "query_table", lambda entry, sql: captured.update(sql=sql)
    )
    frame.search("outage in eu-west", column="day", limit=5)
    assert captured["sql"] == (
        "SELECT * FROM bm25_search('default.main.data', 'day', "
        "'outage in eu-west') ORDER BY score DESC LIMIT 5"
    )


def test_vector_search_builds_sql_and_validates_column(runtime, monkeypatch):
    calls = {"n": 0}
    report = make_report(calls)
    report("2026-07-22")
    runtime.store.flush()
    frame = report("2026-07-22")
    captured: dict[str, str] = {}
    monkeypatch.setattr(
        runtime.store, "query_table", lambda entry, sql: captured.update(sql=sql)
    )
    frame.vector_search("churn risk", column="notes", limit=3)
    assert captured["sql"] == (
        "SELECT *, vector_distance(notes, 'churn risk') AS distance "
        "FROM data ORDER BY distance LIMIT 3"
    )
    with pytest.raises(ValueError, match="identifier"):
        frame.vector_search("x", column="notes; DROP TABLE data")


def test_embedded_vector_cannot_combine_with_other_indexes():
    from hotdata_materialized import BM25, Vector

    with pytest.raises(ValueError, match="cannot be combined"):
        materialize(index=[BM25("notes"), Vector("notes", provider="emb_1")])


def test_index_failure_does_not_discard_the_cached_entry(runtime, monkeypatch):
    from hotdata_materialized import BM25
    from hotdata_materialized.registry import STATUS_READY

    def broken(*args, **kwargs):
        raise RuntimeError("bad embedding provider")

    monkeypatch.setattr(runtime.backend.indexes, "create_index", broken)

    @materialize(ttl=60, index=BM25("notes"))
    def notes():
        return [{"notes": "outage", "n": 1}]

    notes()
    errors = runtime.store.flush()
    assert len(errors) == 1 and "entry cached" in str(errors[0])
    frame = notes()  # next call is a hit despite the failed index
    assert frame.cached is True
    assert frame.entry.status == STATUS_READY
