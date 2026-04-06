from types import SimpleNamespace

import pytest

import llm_adapter


class FakeModels:
    def __init__(self, result=None, should_raise: Exception | None = None) -> None:
        self.result = result
        self.should_raise = should_raise
        self.calls = []

    def generate_content(self, *, model, contents):
        self.calls.append({"model": model, "contents": contents})
        if self.should_raise is not None:
            raise self.should_raise
        return self.result


class FakeClient:
    def __init__(self, models: FakeModels) -> None:
        self.models = models


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text


def test_generate_sql_calls_gemini_with_combined_prompt() -> None:
    models = FakeModels(result=FakeResponse("SELECT id FROM users"))
    cfg = SimpleNamespace(
        gemini_api_key="key-123",
        llm_timeout_seconds=7,
        llm_model="test-model",
        max_rows_context=100,
    )
    adapter = llm_adapter.LLMAdapter(config=cfg, client=FakeClient(models))

    sql = adapter.generate_sql("Show all user ids", {"users": ["id", "name"]})

    assert sql == "SELECT id FROM users"
    call = models.calls[0]
    assert call["model"] == "test-model"
    assert "System instruction:" in call["contents"]
    assert "Show all user ids" in call["contents"]
    assert "users" in call["contents"]
    assert "sqlite_master" in call["contents"]
    assert "PRAGMA table_info" in call["contents"]


def test_generate_nl_summary_uses_row_limit() -> None:
    models = FakeModels(result=FakeResponse("Found 2 matching users."))
    cfg = SimpleNamespace(gemini_api_key="key-123", max_rows_context=2)
    adapter = llm_adapter.LLMAdapter(config=cfg, client=FakeClient(models))

    summary = adapter.generate_nl_summary("Summarize", [(1, "Ann"), (2, "Bo"), (3, "Cy")])

    assert summary == "Found 2 matching users."
    rows_context = adapter._build_summary_system_prompt([(1, "Ann"), (2, "Bo"), (3, "Cy")])
    assert "[1, \"Ann\"], [2, \"Bo\"]" in rows_context
    assert "Cy" not in rows_context


def test_generate_sql_raises_for_timeout() -> None:
    class NeverFinishes:
        def generate_content(self, *, model, contents):
            import time

            time.sleep(0.05)
            return FakeResponse("SELECT 1")

    cfg = SimpleNamespace(gemini_api_key="key-123", llm_timeout_seconds=0.001)
    adapter = llm_adapter.LLMAdapter(config=cfg, client=SimpleNamespace(models=NeverFinishes()))

    with pytest.raises(llm_adapter.LLMAdapterError, match="timed out"):
        adapter.generate_sql("Show users", "users(id)")


def test_generate_sql_raises_for_unsupported_response_shape() -> None:
    models = FakeModels(result=object())
    cfg = SimpleNamespace(gemini_api_key="key-123")
    adapter = llm_adapter.LLMAdapter(config=cfg, client=FakeClient(models))

    with pytest.raises(llm_adapter.LLMAdapterError, match="unsupported"):
        adapter.generate_sql("Show users", "users(id)")


def test_generate_sql_raises_for_client_error() -> None:
    models = FakeModels(should_raise=RuntimeError("quota exceeded"))
    cfg = SimpleNamespace(gemini_api_key="key-123")
    adapter = llm_adapter.LLMAdapter(config=cfg, client=FakeClient(models))

    with pytest.raises(llm_adapter.LLMAdapterError, match="request failed"):
        adapter.generate_sql("Show users", "users(id)")


def test_generate_sql_requires_api_key() -> None:
    adapter = llm_adapter.LLMAdapter(config=SimpleNamespace(gemini_api_key=""), client=FakeClient(FakeModels()))

    with pytest.raises(llm_adapter.LLMAdapterError, match="GEMINI_API_KEY is not configured"):
        adapter.generate_sql("Show users", "users(id)")

