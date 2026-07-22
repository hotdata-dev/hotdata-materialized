"""A registry database found by name but never seeded (crashed bootstrap)
must publish the entries schema on first read instead of erroring."""

from hotdata.models.create_database_request import CreateDatabaseRequest
from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl

from hotdata_materialized.conf import Config
from hotdata_materialized.registry import Registry
from tests.fakes import FakeBackend
from tests.test_registry import FP, Clock, ready_entry


def test_unseeded_registry_database_heals_on_first_lookup():
    backend = FakeBackend()
    config = Config(api_key="hd_test", workspace_id="ws_test")
    # simulate a bootstrap that created the database but crashed before seeding
    backend._create_database(
        CreateDatabaseRequest(
            name=config.registry_database,
            schemas=[
                DatabaseDefaultSchemaDecl(
                    name="main",
                    tables=[DatabaseDefaultTableDecl(name="entries", key=["fingerprint"])],
                )
            ],
        )
    )
    registry = Registry(backend, config, now_fn=Clock())
    assert registry.lookup(FP) is None
    ready_entry(registry)
    healed = registry.lookup(FP)
    assert healed is not None and healed.status == "ready"
    assert len(backend.database_meta) == 1
