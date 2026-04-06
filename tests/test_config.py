import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config


def test_parse_max_rows_context_valid_integer() -> None:
    assert config.parse_max_rows_context("250") == 250


def test_parse_max_rows_context_invalid_integer_raises_value_error() -> None:
    with pytest.raises(ValueError, match="MAX_ROWS_CONTEXT must be an integer"):
        config.parse_max_rows_context("two-hundred")


def test_parse_max_import_size_bytes_valid_integer() -> None:
    assert config.parse_max_import_size_bytes("1024") == 1024


def test_parse_max_import_size_bytes_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="MAX_IMPORT_SIZE_BYTES must be an integer"):
        config.parse_max_import_size_bytes("five-gb")

    with pytest.raises(ValueError, match="MAX_IMPORT_SIZE_BYTES must be positive"):
        config.parse_max_import_size_bytes("0")


def test_config_uses_defaults_and_loads_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "DB_PATH",
        "DB_URL",
        "GEMINI_API_KEY",
        "LLM_API_KEY",
        "LLM_MODEL",
        "LLM_TIMEOUT_SECONDS",
        "MAX_ROWS_CONTEXT",
        "SQL_POOL_SIZE",
        "MAX_IMPORT_SIZE_BYTES",
    ):
        monkeypatch.delenv(key, raising=False)

    dotenv_calls = {}

    def fake_find_dotenv() -> str:
        return "/tmp/.env"

    def fake_load_dotenv(path: str) -> bool:
        dotenv_calls["path"] = path
        return True

    monkeypatch.setattr(config, "find_dotenv", fake_find_dotenv)
    monkeypatch.setattr(config, "load_dotenv", fake_load_dotenv)

    cfg = config.Config()

    assert dotenv_calls["path"] == "/tmp/.env"
    assert cfg.db_path == "data/database.db"
    assert cfg.db_url == "sqlite:///data/database.db"
    assert cfg.gemini_api_key == ""
    assert cfg.llm_api_key == ""
    assert cfg.llm_model == "gemini-3.1-flash-lite-preview"
    assert cfg.llm_timeout_seconds == 15.0
    assert cfg.max_rows_context == 100
    assert cfg.sqlite_conn_pool_size == 3
    assert cfg.max_import_size_bytes == 5 * 1024 * 1024 * 1024


def test_config_prefers_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "find_dotenv", lambda: "")
    monkeypatch.setattr(config, "load_dotenv", lambda _path: True)

    monkeypatch.setenv("DB_PATH", "data/local.db")
    monkeypatch.setenv("DB_URL", "postgresql://db.example.edu:5432/barys")
    monkeypatch.setenv("GEMINI_API_KEY", "gem-key-123")
    monkeypatch.setenv("LLM_API_KEY", "legacy-key")
    monkeypatch.setenv("LLM_MODEL", "gemini-3.1-flash-lite-preview")
    monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "9")
    monkeypatch.setenv("MAX_ROWS_CONTEXT", "42")
    monkeypatch.setenv("SQL_POOL_SIZE", "7")
    monkeypatch.setenv("MAX_IMPORT_SIZE_BYTES", "123456")

    cfg = config.Config()

    assert cfg.db_path == "data/local.db"
    assert cfg.db_url == "postgresql://db.example.edu:5432/barys"
    assert cfg.gemini_api_key == "gem-key-123"
    assert cfg.llm_api_key == "gem-key-123"
    assert cfg.llm_model == "gemini-3.1-flash-lite-preview"
    assert cfg.llm_timeout_seconds == 9.0
    assert cfg.max_rows_context == 42
    assert cfg.sqlite_conn_pool_size == 7
    assert cfg.max_import_size_bytes == 123456


def test_config_uses_legacy_llm_api_key_when_gemini_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "find_dotenv", lambda: "")
    monkeypatch.setattr(config, "load_dotenv", lambda _path: True)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("LLM_API_KEY", "legacy-only-key")

    cfg = config.Config()

    assert cfg.gemini_api_key == "legacy-only-key"
    assert cfg.llm_api_key == "legacy-only-key"


def test_config_raises_for_invalid_max_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "find_dotenv", lambda: "")
    monkeypatch.setattr(config, "load_dotenv", lambda _path: True)
    monkeypatch.setenv("MAX_ROWS_CONTEXT", "not-an-int")

    with pytest.raises(ValueError, match="MAX_ROWS_CONTEXT must be an integer"):
        config.Config()


def test_config_raises_for_invalid_max_import_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "find_dotenv", lambda: "")
    monkeypatch.setattr(config, "load_dotenv", lambda _path: True)
    monkeypatch.setenv("MAX_IMPORT_SIZE_BYTES", "-1")

    with pytest.raises(ValueError, match="MAX_IMPORT_SIZE_BYTES must be positive"):
        config.Config()


