import cmd
import os
import stat
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cli import CLI


class FakeQueryService:
    def __init__(self, table_names: list[str] | None = None) -> None:
        self.calls: list[str] = []
        self.schema_manager = SimpleNamespace(get_existing_tables=lambda: list(table_names or []))

    def process_nl_query(self, text: str) -> str:
        self.calls.append(text)
        return f"answer:{text}"


class DictQueryService:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def process_nl_query(self, text: str) -> dict[str, str]:
        self.calls.append(text)
        return {
            "sql": "SELECT 1;",
            "summary": "Found one row.",
        }


class FailingQueryService:
    def process_nl_query(self, _text: str) -> str:
        raise RuntimeError("query failure")


class QueryServiceWithoutSchemaManager:
    def process_nl_query(self, _text: str) -> str:
        return "ok"


class RemovableSchemaManager:
    def __init__(self, table_names: list[str] | None = None) -> None:
        self.table_names = list(table_names or [])
        self.removed: list[str] = []

    def get_existing_tables(self) -> list[str]:
        return list(self.table_names)

    def drop_table(self, table_name: str) -> bool:
        if table_name not in self.table_names:
            return False
        self.table_names.remove(table_name)
        self.removed.append(table_name)
        return True


class FakeIngestor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def ingest_file(self, path: str) -> None:
        self.calls.append(path)


class TableAwareIngestor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def ingest_file(self, path: str, table_name: str | None = None) -> None:
        self.calls.append((path, table_name))


class FailingIngestor:
    def ingest_file(self, _path: str) -> None:
        raise RuntimeError("ingestion failure")


def test_do_import_prints_usage_when_filepath_missing(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(FakeQueryService(), FakeIngestor())

    cli.do_import("   ")

    captured = capsys.readouterr()
    assert "Usage: import <filepath>" in captured.out


def test_do_import_handles_missing_file(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    cli = CLI(FakeQueryService(), FakeIngestor(), import_root=tmp_path)

    cli.do_import(str(tmp_path / "missing.csv"))

    captured = capsys.readouterr()
    assert "Could not access file" in captured.out


def test_do_import_cancels_when_user_declines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    ingestor = FakeIngestor()
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")

    cli = CLI(FakeQueryService(), ingestor, import_root=tmp_path)

    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert "Import cancelled." in captured.out
    assert ingestor.calls == []


def test_do_import_ingests_file_after_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    ingestor = FakeIngestor()
    csv_file = tmp_path / "my data.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    cli = CLI(FakeQueryService(), ingestor, import_root=tmp_path)

    prompts: list[str] = []

    def fake_input(prompt: str) -> str:
        prompts.append(prompt)
        if "Are you sure" in prompt:
            return "y"
        return "test_table"

    monkeypatch.setattr("builtins.input", fake_input)

    cli.do_import(f'"{csv_file}"')

    captured = capsys.readouterr()
    assert any("approximately" in prompt for prompt in prompts)
    assert any("Destination table name" in prompt for prompt in prompts)
    assert len(ingestor.calls) == 1
    assert Path(ingestor.calls[0]).name.startswith("barys_import_")
    assert not os.path.exists(ingestor.calls[0])
    assert f"Import completed for '{csv_file.resolve()}' into table 'test_table'." in captured.out


def test_do_import_passes_requested_table_name_to_ingestor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    ingestor = TableAwareIngestor()
    csv_file = tmp_path / "my-data.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    cli = CLI(FakeQueryService(), ingestor, import_root=tmp_path)

    def fake_input(prompt: str) -> str:
        if "Are you sure" in prompt:
            return "y"
        return "test"

    monkeypatch.setattr("builtins.input", fake_input)

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert len(ingestor.calls) == 1
    _, table_name = ingestor.calls[0]
    assert table_name == "test"
    assert "into table 'test'" in captured.out


def test_do_import_supports_callable_ingestor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def ingest_callable(path: str) -> None:
        calls.append(path)

    csv_file = tmp_path / "callable.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    cli = CLI(FakeQueryService(), ingest_callable, import_root=tmp_path)

    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert len(calls) == 1
    assert "Import completed" in captured.out


def test_do_import_reports_ingestion_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    cli = CLI(FakeQueryService(), FailingIngestor(), import_root=tmp_path)

    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert "Import failed: ingestion failure" in captured.out


def test_do_import_blocks_path_traversal_outside_allowed_root(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    import_root = tmp_path / "allowed"
    import_root.mkdir()
    outside_file = tmp_path / "outside.csv"
    outside_file.write_text("a,b\n1,2\n", encoding="utf-8")

    cli = CLI(FakeQueryService(), FakeIngestor(), import_root=import_root)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    cli.do_import(str(outside_file))

    captured = capsys.readouterr()
    assert "Import blocked: file must be inside" in captured.out


def test_do_import_blocks_special_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    cli = CLI(FakeQueryService(), FakeIngestor(), import_root=tmp_path)

    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    monkeypatch.setattr(
        "cli.os.fstat",
        lambda _fd: SimpleNamespace(st_mode=stat.S_IFCHR, st_size=0),
    )

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert "Import blocked: only regular files can be imported." in captured.out


def test_do_import_blocks_files_over_5gb(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "too_big.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    ingestor = FakeIngestor()
    max_bytes = 5 * 1024 * 1024 * 1024
    cli = CLI(FakeQueryService(), ingestor, import_root=tmp_path, max_import_size_bytes=max_bytes)

    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    monkeypatch.setattr(
        "cli.os.fstat",
        lambda _fd: SimpleNamespace(st_mode=stat.S_IFREG, st_size=max_bytes + 1),
    )

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert "exceeds the 5.00 GB limit" in captured.out
    assert ingestor.calls == []


def test_do_import_allows_exact_5gb_boundary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "edge.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    ingestor = FakeIngestor()
    max_bytes = 5 * 1024 * 1024 * 1024
    cli = CLI(FakeQueryService(), ingestor, import_root=tmp_path, max_import_size_bytes=max_bytes)

    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    monkeypatch.setattr(
        "cli.os.fstat",
        lambda _fd: SimpleNamespace(st_mode=stat.S_IFREG, st_size=max_bytes),
    )

    cli.do_import(str(csv_file))

    captured = capsys.readouterr()
    assert len(ingestor.calls) == 1
    assert "Import completed" in captured.out


def test_default_ignores_blank_lines(capsys: pytest.CaptureFixture[str]) -> None:
    query_service = FakeQueryService()
    cli = CLI(query_service, FakeIngestor())

    cli.default("   ")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert query_service.calls == []


def test_default_routes_nl_query_and_prints_result(capsys: pytest.CaptureFixture[str]) -> None:
    query_service = FakeQueryService()
    cli = CLI(query_service, FakeIngestor())

    cli.default("show all tables")

    captured = capsys.readouterr()
    assert query_service.calls == ["show all tables"]
    assert "answer:show all tables" in captured.out


def test_default_formats_sql_and_summary_when_query_service_returns_dict(
    capsys: pytest.CaptureFixture[str],
) -> None:
    query_service = DictQueryService()
    cli = CLI(query_service, FakeIngestor())

    cli.default("who is latest")

    captured = capsys.readouterr()
    assert query_service.calls == ["who is latest"]
    assert "SQL:\nSELECT 1;" in captured.out
    assert "Summary:\nFound one row." in captured.out


def test_default_reports_query_failures(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(FailingQueryService(), FakeIngestor())

    cli.default("anything")

    captured = capsys.readouterr()
    assert "Query failed: query failure" in captured.out


def test_help_without_topic_prints_overview(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(FakeQueryService(), FakeIngestor())

    cli.do_help("")

    captured = capsys.readouterr()
    assert "Available commands:" in captured.out
    assert "tables [rm <name>]" in captured.out
    assert "Any non-command text is treated as a natural-language query." in captured.out


def test_help_with_topic_delegates_to_base_class(monkeypatch: pytest.MonkeyPatch) -> None:
    cli = CLI(FakeQueryService(), FakeIngestor())
    seen = {}

    def fake_super_help(self: cmd.Cmd, topic: str) -> None:
        seen["topic"] = topic

    monkeypatch.setattr(cmd.Cmd, "do_help", fake_super_help)

    cli.do_help("import")

    assert seen["topic"] == "import"


def test_tables_lists_available_tables(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(FakeQueryService(table_names=["users", "orders"]), FakeIngestor())

    cli.do_tables("")

    captured = capsys.readouterr()
    assert "Available tables:" in captured.out
    assert "  - orders" in captured.out
    assert "  - users" in captured.out


def test_tables_reports_when_schema_manager_is_unavailable(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(QueryServiceWithoutSchemaManager(), FakeIngestor())

    cli.do_tables("")

    captured = capsys.readouterr()
    assert "schema manager is unavailable" in captured.out


def test_tables_rm_removes_existing_table(capsys: pytest.CaptureFixture[str]) -> None:
    query_service = FakeQueryService(table_names=[])
    schema_manager = RemovableSchemaManager(["users", "orders"])
    query_service.schema_manager = schema_manager
    cli = CLI(query_service, FakeIngestor())

    cli.do_tables('rm "users"')

    captured = capsys.readouterr()
    assert "Removed table 'users'." in captured.out
    assert schema_manager.removed == ["users"]


def test_tables_rm_reports_missing_table(capsys: pytest.CaptureFixture[str]) -> None:
    query_service = FakeQueryService(table_names=[])
    query_service.schema_manager = RemovableSchemaManager(["orders"])
    cli = CLI(query_service, FakeIngestor())

    cli.do_tables('rm "users"')

    captured = capsys.readouterr()
    assert "table does not exist" in captured.out


def test_tables_rm_prints_usage_when_table_name_missing(capsys: pytest.CaptureFixture[str]) -> None:
    query_service = FakeQueryService(table_names=[])
    query_service.schema_manager = RemovableSchemaManager(["users"])
    cli = CLI(query_service, FakeIngestor())

    cli.do_tables("rm")

    captured = capsys.readouterr()
    assert "Usage: tables rm <table_name>" in captured.out


def test_exit_quit_and_eof_exit_shell(capsys: pytest.CaptureFixture[str]) -> None:
    cli = CLI(FakeQueryService(), FakeIngestor())

    assert cli.do_exit("") is True
    assert cli.do_quit("") is True
    assert cli.do_EOF("") is True
    assert capsys.readouterr().out == "\n"

