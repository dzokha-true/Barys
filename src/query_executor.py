from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Sequence

import config as conf
from sql_sanitizer import SecurityError, strip_markdown
from sql_validator import is_safe_read_only


class QueryExecutor:
    """Isolated database execution engine for read and write operations."""

    def __init__(self, db_pool: Any | None = None) -> None:
        self._db_pool = db_pool
        self._owns_connection = db_pool is None

        if db_pool is not None and hasattr(db_pool, "get_connection"):
            self.conn = db_pool.get_connection()
        else:
            config = conf.Config()
            db_path = self._resolve_db_path(config)
            self.conn = sqlite3.connect(db_path)

        self.cursor = self.conn.cursor()

    def _resolve_db_path(self, config: conf.Config) -> str:
        get_db_path = getattr(config, "get_db_path", None)
        if callable(get_db_path):
            return str(get_db_path())

        db_url = str(getattr(config, "db_url", ""))
        sqlite_prefix = "sqlite:///"
        if db_url.startswith(sqlite_prefix):
            return db_url[len(sqlite_prefix):]

        return "data/database.db"

    def execute_read(self, sql_string: str) -> list[tuple[Any, ...]]:
        normalized_sql = strip_markdown(sql_string).strip()
        if not is_safe_read_only(normalized_sql):
            raise SecurityError("Read query is not in the approved read-only allowlist.")

        self.cursor.execute(normalized_sql)
        return list(self.cursor.fetchall())

    def execute_write(self, sql_string: str, parameters: Sequence[Any]) -> int:
        normalized_sql = strip_markdown(sql_string).strip()
        if not normalized_sql.lower().startswith("insert"):
            raise SecurityError("Write operations are restricted to parameterized INSERT statements.")

        self.cursor.execute(normalized_sql, tuple(parameters))
        self.conn.commit()
        return int(self.cursor.rowcount)

    def execute(self, sql_string: str, params: Sequence[Any] | None = None) -> Any:
        """Compatibility API used by existing services and ingestors."""
        normalized_sql = strip_markdown(sql_string).strip()

        if params is not None:
            return self.execute_write(normalized_sql, params)

        if is_safe_read_only(normalized_sql):
            return self.execute_read(normalized_sql)

        self.cursor.execute(normalized_sql)
        self.conn.commit()
        return []

    def execute_query(self, sql_string: str, params: Sequence[Any] | None = None) -> Any:
        return self.execute(sql_string, params)

    def run_query(self, sql_string: str, params: Sequence[Any] | None = None) -> Any:
        return self.execute(sql_string, params)

    def executemany(self, sql_string: str, rows: Iterable[Sequence[Any]]) -> int:
        normalized_sql = strip_markdown(sql_string).strip()
        if not normalized_sql.lower().startswith("insert"):
            raise SecurityError("Batch writes are restricted to parameterized INSERT statements.")

        row_list = [tuple(row) for row in rows]
        if not row_list:
            return 0

        self.cursor.executemany(normalized_sql, row_list)
        self.conn.commit()
        return len(row_list)

    def close(self) -> None:
        try:
            self.cursor.close()
        finally:
            if self._owns_connection:
                self.conn.close()
            elif self._db_pool is not None and hasattr(self._db_pool, "return_connection"):
                self._db_pool.return_connection(self.conn)
