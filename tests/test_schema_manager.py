import sqlite3

import pytest

import schema_manager
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


def test_create_table_and_get_schema() -> None:
	conn = sqlite3.connect(":memory:")
	manager = schema_manager.SchemaManager(db_pool=FakePool(conn))

	manager.create_table("users", {"id": "INTEGER", "name": "TEXT"})

	assert manager.get_table_schema("users") == {"id": "INTEGER", "name": "TEXT"}
	assert "users(id INTEGER, name TEXT)" in manager.get_formatted_schema()


def test_build_create_table_sql_rejects_unsafe_identifier() -> None:
	conn = sqlite3.connect(":memory:")
	manager = schema_manager.SchemaManager(db_pool=FakePool(conn))

	with pytest.raises(sanitizer.SecurityError):
		manager.build_create_table_sql("users", {"bad-name": "TEXT"})


def test_refresh_cache_calls_identifier_whitelist(monkeypatch: pytest.MonkeyPatch) -> None:
	conn = sqlite3.connect(":memory:")
	conn.execute("CREATE TABLE events (id INTEGER)")
	pool = FakePool(conn)

	seen = []
	original = sanitizer.enforce_identifier_whitelist

	def spy(identifier: str) -> str:
		seen.append(identifier)
		return original(identifier)

	monkeypatch.setattr(schema_manager.sanitizer, "enforce_identifier_whitelist", spy)

	manager = schema_manager.SchemaManager(db_pool=pool)
	manager.refresh_cache()

	assert "events" in seen

