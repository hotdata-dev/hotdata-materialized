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
