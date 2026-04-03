from pathlib import Path
import sys

import pandas as pd
import pytest
from pandas.errors import EmptyDataError

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from csv_ingestor import ingest_csv, infer_sql_types


class FakeSchemaManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []

    def build_create_table_sql(self, table_name: str, schema: dict[str, str]) -> str:
        self.calls.append((table_name, schema))
        return f'CREATE TABLE IF NOT EXISTS "{table_name}" ("id" BIGINT);'


class FakeQueryExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.execute_calls.append((sql, params))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self.executemany_calls.append((sql, rows))


def test_infer_sql_types_maps_common_pandas_dtypes() -> None:
    dataframe = pd.DataFrame(
        {
            "int_col": [1, 2],
            "float_col": [1.2, 3.4],
            "bool_col": [True, False],
            "date_col": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "text_col": ["a", "b"],
        }
    )

    inferred = infer_sql_types(dataframe)

    assert inferred == {
        "int_col": "BIGINT",
        "float_col": "DOUBLE PRECISION",
        "bool_col": "BOOLEAN",
        "date_col": "TIMESTAMP",
        "text_col": "TEXT",
    }


def test_ingest_csv_builds_schema_and_inserts_in_chunks(tmp_path: Path) -> None:
    csv_file = tmp_path / "scores.csv"
    csv_file.write_text("id,name,score\n1,Ann,3.5\n2,Bob,4.0\n3,Cat,4.2\n", encoding="utf-8")

    schema_manager = FakeSchemaManager()
    query_executor = FakeQueryExecutor()
    inserted = ingest_csv(
        str(csv_file),
        schema_manager=schema_manager,
        query_executor=query_executor,
        table_name="scores",
        chunksize=2,
    )

    assert inserted == 3
    assert schema_manager.calls == [
        (
            "scores",
            {
                "id": "BIGINT",
                "name": "TEXT",
                "score": "DOUBLE PRECISION",
            },
        )
    ]
    assert len(query_executor.execute_calls) == 1
    assert "CREATE TABLE IF NOT EXISTS" in query_executor.execute_calls[0][0]
    assert len(query_executor.executemany_calls) == 2
    assert len(query_executor.executemany_calls[0][1]) == 2
    assert len(query_executor.executemany_calls[1][1]) == 1


def test_ingest_csv_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="was not found"):
        ingest_csv(
            str(tmp_path / "does_not_exist.csv"),
            schema_manager=FakeSchemaManager(),
            query_executor=FakeQueryExecutor(),
        )


def test_ingest_csv_raises_for_empty_csv(tmp_path: Path) -> None:
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("a,b\n", encoding="utf-8")

    with pytest.raises(EmptyDataError, match="was empty"):
        ingest_csv(
            str(csv_file),
            schema_manager=FakeSchemaManager(),
            query_executor=FakeQueryExecutor(),
            table_name="empty_table",
        )


