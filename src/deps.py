import sqlite3
from dataclasses import dataclass
from importlib import import_module
from queue import Empty, LifoQueue
from typing import Any, Type
from config import Config
from schema_manager import SchemaManager
from query_executor import QueryExecutor
from query_service import QueryService
from llm_adapter import LLMAdapter


class SQLiteConnectionPool:
    def __init__(self, db_url: str, pool_size: int = 6) -> None:
        self.db_path = db_url
        self.pool = LifoQueue(maxsize=pool_size) #LIFO, because we want to make use of the cache locality

        for _ in range(pool_size):
            try:
                self.pool.put(self.get_new_connection())
            except Empty: #TODO: read about things that can break this, like reaching Max connections?
                break

    def get_new_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def get_connection(self) -> sqlite3.Connection:
        return self.pool.get()

    def return_connection(self, connection: sqlite3.Connection) -> None:
        return self.pool.put(connection)


@dataclass(frozen=True)
class Services:
    db_pool: SQLiteConnectionPool
    schema_manager: SchemaManager
    query_executor: QueryExecutor
    llm_adapter: LLMAdapter
    query_service: QueryService


def _build_instance(class_type: Type[Any], *args: Any) -> Any:
    """Try constructor injection first, then fallback for legacy zero-arg classes."""
    try:
        return class_type(*args)
    except TypeError:
        return class_type()


def _load_class(module_name: str, class_name: str) -> Type[Any]:
    try:
        module = import_module(module_name)
    except Exception as exc:
        raise ImportError(f"Failed to import module '{module_name}'") from exc

    try:
        return getattr(module, class_name)
    except AttributeError as exc:
        raise ImportError(
            f"Module '{module_name}' does not define class '{class_name}'"
        ) from exc


def init_services(config: Config) -> Services:
    db_pool = SQLiteConnectionPool(config.db_url)

    # Imported lazily so this module stays importable while other modules evolve.
    SchemaManager = _load_class("schema_manager", "SchemaManager")
    QueryExecutor = _load_class("query_executor", "QueryExecutor")
    LLMAdapter = _load_class("llm_adapter", "LLMAdapter")
    QueryService = _load_class("query_service", "QueryService")

    schema_manager = _build_instance(SchemaManager, db_pool)
    query_executor = _build_instance(QueryExecutor, db_pool)
    llm_adapter = _build_instance(LLMAdapter, config)
    query_service = _build_instance(
        QueryService,
        schema_manager,
        query_executor,
        llm_adapter,
        config,
    )

    return Services(
        db_pool=db_pool,
        schema_manager=schema_manager,
        query_executor=query_executor,
        llm_adapter=llm_adapter,
        query_service=query_service,
    )
