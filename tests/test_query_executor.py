import sqlite3

import pytest

import query_executor
import sql_sanitizer as sanitizer


class FakePool:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self.returned = False

    def get_connection(self) -> sqlite3.Connection:
        return self._conn

    def return_connection(self, conn: sqlite3.Connection) -> None:
        if conn is self._conn:
            self.returned = True


def test_execute_read_returns_rows_for_select() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ann')")
    conn.commit()

    executor = query_executor.QueryExecutor(db_pool=FakePool(conn))

    rows = executor.execute_read("SELECT id, name FROM users")

    assert rows == [(1, "Ann")]


def test_execute_read_rejects_non_read_only_query() -> None:
    conn = sqlite3.connect(":memory:")
    executor = query_executor.QueryExecutor(db_pool=FakePool(conn))

    with pytest.raises(sanitizer.SecurityError):
        executor.execute_read("DROP TABLE users")


def test_execute_write_inserts_rows_with_parameters() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE logs (id INTEGER, message TEXT)")
    conn.commit()

    executor = query_executor.QueryExecutor(db_pool=FakePool(conn))

    written = executor.execute_write(
        "INSERT INTO logs (id, message) VALUES (?, ?)",
        (1, "ok"),
    )

    assert written == 1
    assert conn.execute("SELECT id, message FROM logs").fetchall() == [(1, "ok")]


def test_executemany_inserts_all_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE events (id INTEGER, label TEXT)")
    conn.commit()

    executor = query_executor.QueryExecutor(db_pool=FakePool(conn))

    inserted = executor.executemany(
        "INSERT INTO events (id, label) VALUES (?, ?)",
        [(1, "a"), (2, "b")],
    )

    assert inserted == 2
    assert conn.execute("SELECT id, label FROM events ORDER BY id").fetchall() == [(1, "a"), (2, "b")]


def test_execute_runs_ddl_for_compatibility() -> None:
    conn = sqlite3.connect(":memory:")
    executor = query_executor.QueryExecutor(db_pool=FakePool(conn))

    result = executor.execute("CREATE TABLE t (id INTEGER)")

    assert result == []
    assert conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='t'").fetchone() is not None

