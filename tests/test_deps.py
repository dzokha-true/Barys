import importlib
import sys
import types
from types import SimpleNamespace

import pytest


def load_deps_with_stubbed_modules(monkeypatch: pytest.MonkeyPatch):
    stub_defs = {
        "schema_manager": "SchemaManager",
        "query_executor": "QueryExecutor",
        "query_service": "QueryService",
        "llm_adapter": "LLMAdapter",
    }

    for module_name, class_name in stub_defs.items():
        module = types.ModuleType(module_name)
        setattr(module, class_name, type(class_name, (), {}))
        monkeypatch.setitem(sys.modules, module_name, module)

    csv_ingestor_module = types.ModuleType("csv_ingestor")

    def ingest_csv(_filepath, _schema_manager, _query_executor, table_name=None):
        return 0

    csv_ingestor_module.ingest_csv = ingest_csv
    monkeypatch.setitem(sys.modules, "csv_ingestor", csv_ingestor_module)

    sys.modules.pop("deps", None)
    deps = importlib.import_module("deps")
    return importlib.reload(deps)


def test_sqlite_connection_pool_initializes_and_reuses_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    connections = []

    def fake_connect(db_path: str):
        conn = object()
        connections.append((db_path, conn))
        return conn

    monkeypatch.setattr(deps.sqlite3, "connect", fake_connect)

    pool = deps.SQLiteConnectionPool("/tmp/test.db", pool_size=2)

    assert len(connections) == 2
    assert all(path == "/tmp/test.db" for path, _ in connections)

    conn = pool.get_connection()
    pool.return_connection(conn)
    assert pool.pool.qsize() == 2


def test_build_instance_handles_regular_and_typeerror_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    class NeedsArg:
        def __init__(self, value):
            self.value = value

    class NoArg:
        def __init__(self):
            self.value = "ok"

    assert deps._build_instance(NeedsArg, 7).value == 7
    assert deps._build_instance(NoArg, "extra").value == "ok"


def test_load_class_success(monkeypatch: pytest.MonkeyPatch) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    fake_module = types.SimpleNamespace(TargetClass=object)
    monkeypatch.setattr(deps, "import_module", lambda _name: fake_module)

    loaded = deps._load_class("any_module", "TargetClass")

    assert loaded is object


def test_load_class_raises_import_error_when_module_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    def raise_import(_name: str):
        raise RuntimeError("boom")

    monkeypatch.setattr(deps, "import_module", raise_import)

    with pytest.raises(ImportError, match="Failed to import module 'bad_module'"):
        deps._load_class("bad_module", "Anything")


def test_load_class_raises_import_error_when_class_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    monkeypatch.setattr(deps, "import_module", lambda _name: SimpleNamespace())

    with pytest.raises(ImportError, match="does not define class 'MissingClass'"):
        deps._load_class("good_module", "MissingClass")


def test_init_services_wires_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    deps = load_deps_with_stubbed_modules(monkeypatch)

    class FakePool:
        def __init__(self, db_path: str, pool_size: int) -> None:
            self.db_path = db_path
            self.pool_size = pool_size

    class FakeSchemaManager:
        def __init__(self, db_pool):
            self.db_pool = db_pool

    class FakeQueryExecutor:
        def __init__(self, db_pool):
            self.db_pool = db_pool

    class FakeLLMAdapter:
        def __init__(self, config_obj):
            self.config_obj = config_obj

    class FakeQueryService:
        def __init__(self, schema_manager, query_executor, llm_adapter, config_obj):
            self.schema_manager = schema_manager
            self.query_executor = query_executor
            self.llm_adapter = llm_adapter
            self.config_obj = config_obj

    class_map = {
        "SchemaManager": FakeSchemaManager,
        "QueryExecutor": FakeQueryExecutor,
        "LLMAdapter": FakeLLMAdapter,
        "QueryService": FakeQueryService,
    }

    captured = {}

    csv_ingestor_module = types.ModuleType("csv_ingestor")

    def fake_ingest_csv(filepath, schema_manager, query_executor, table_name=None):
        captured["args"] = (filepath, schema_manager, query_executor, table_name)
        return 11

    csv_ingestor_module.ingest_csv = fake_ingest_csv
    monkeypatch.setitem(sys.modules, "csv_ingestor", csv_ingestor_module)

    monkeypatch.setattr(deps, "SQLiteConnectionPool", FakePool)
    monkeypatch.setattr(deps, "_load_class", lambda _module, class_name: class_map[class_name])

    cfg = SimpleNamespace(db_path="/tmp/test.db", db_url="sqlite:///tmp.db", sqlite_conn_pool_size=9)

    services = deps.init_services(cfg)

    assert isinstance(services.db_pool, FakePool)
    assert services.db_pool.db_path == "/tmp/test.db"
    assert services.db_pool.pool_size == 9

    assert services.schema_manager.db_pool is services.db_pool
    assert services.query_executor.db_pool is services.db_pool
    assert services.llm_adapter.config_obj is cfg

    assert services.query_service.schema_manager is services.schema_manager
    assert services.query_service.query_executor is services.query_executor
    assert services.query_service.llm_adapter is services.llm_adapter
    assert services.query_service.config_obj is cfg

    inserted = services.csv_ingestor("/tmp/sample.csv")
    assert inserted == 11
    assert captured["args"] == (
        "/tmp/sample.csv",
        services.schema_manager,
        services.query_executor,
        None,
    )

    inserted_named = services.csv_ingestor("/tmp/sample.csv", "test")
    assert inserted_named == 11
    assert captured["args"] == (
        "/tmp/sample.csv",
        services.schema_manager,
        services.query_executor,
        "test",
    )

