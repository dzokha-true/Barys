from __future__ import annotations

from typing import Any

import pytest

import query_service
import sql_sanitizer as sanitizer


class FakeSchemaManager:
    def __init__(self, schema: str) -> None:
        self.schema = schema
        self.calls = 0

    def get_formatted_schema(self) -> str:
        self.calls += 1
        return self.schema


class FakeExecutor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.last_sql: str | None = None

    def execute_read(self, sql: str) -> list[tuple[Any, ...]]:
        self.last_sql = sql
        return self.rows


class FakeLLMAdapter:
    def __init__(self, generated_sql: str, summary: str = "ok") -> None:
        self.generated_sql = generated_sql
        self.summary = summary
        self.sql_calls: list[tuple[str, str]] = []
        self.summary_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def generate_sql(self, prompt: str, schema: str) -> str:
        self.sql_calls.append((prompt, schema))
        return self.generated_sql

    def generate_nl_summary(self, prompt: str, data_rows: list[tuple[Any, ...]]) -> str:
        self.summary_calls.append((prompt, data_rows))
        return self.summary


def test_process_nl_query_runs_two_pass_flow_with_hard_limit() -> None:
    rows = [(i, f"v{i}") for i in range(20)]
    schema = "users(id INTEGER, name TEXT)"
    adapter = FakeLLMAdapter("```sql\nSELECT id, name FROM users\n```")
    executor = FakeExecutor(rows)
    service = query_service.QueryService(FakeSchemaManager(schema), executor, adapter)

    result = service.process_nl_query("show users")

    assert executor.last_sql == "SELECT id, name FROM users LIMIT 10"
    assert result == {
        "sql": "SELECT id, name FROM users LIMIT 10",
        "summary": "ok",
    }
    assert adapter.sql_calls == [("show users", schema)]
    assert adapter.summary_calls[0][1] == rows[:10]
    assert "User question: show users" in adapter.summary_calls[0][0]
    assert "Executed SQL: SELECT id, name FROM users LIMIT 10" in adapter.summary_calls[0][0]
    assert f"SQL output rows: {rows[:10]}" in adapter.summary_calls[0][0]


def test_process_nl_query_preserves_existing_limit() -> None:
    adapter = FakeLLMAdapter("SELECT id FROM users LIMIT 3;")
    executor = FakeExecutor([(1,), (2,), (3,)])
    service = query_service.QueryService(FakeSchemaManager("users(id INTEGER)"), executor, adapter)

    result = service.process_nl_query("top users")

    assert executor.last_sql == "SELECT id FROM users LIMIT 3;"
    assert result["sql"] == "SELECT id FROM users LIMIT 3;"


def test_process_nl_query_raises_for_unsafe_sql() -> None:
    adapter = FakeLLMAdapter("DROP TABLE users")
    executor = FakeExecutor([])
    service = query_service.QueryService(FakeSchemaManager("users(id INTEGER)"), executor, adapter)

    with pytest.raises(sanitizer.SecurityError):
        service.process_nl_query("delete users")

    assert executor.last_sql is None
    assert adapter.summary_calls == []


def test_process_nl_query_does_not_append_limit_to_pragma() -> None:
    adapter = FakeLLMAdapter("PRAGMA table_info(users)")
    executor = FakeExecutor([(0, "id", "INTEGER", 0, None, 0)])
    service = query_service.QueryService(FakeSchemaManager("users(id INTEGER)"), executor, adapter)

    result = service.process_nl_query("what columns are in users")

    assert executor.last_sql == "PRAGMA table_info(users)"
    assert result["sql"] == "PRAGMA table_info(users)"


