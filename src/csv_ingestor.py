from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_timedelta64_dtype,
)
from pandas.errors import EmptyDataError


class CSVIngestor:
    def __init__(
        self,
        schema_manager: Any | None = None,
        query_executor: Any | None = None,
        table_name: str | None = None,
        chunksize: int = 1_000,
    ) -> None:
        self.schema_manager = schema_manager
        self.query_executor = query_executor
        self.table_name = table_name
        self.chunksize = chunksize

    def infer_sql_types(self, dataframe_chunk: pd.DataFrame) -> dict[str, str]:
        type_map: dict[str, str] = {}

        for column_name, column in dataframe_chunk.items():
            normalized_name = str(column_name)
            if is_bool_dtype(column):
                type_map[normalized_name] = "BOOLEAN"
            elif is_integer_dtype(column):
                type_map[normalized_name] = "BIGINT"
            elif is_float_dtype(column):
                type_map[normalized_name] = "DOUBLE PRECISION"
            elif is_datetime64_any_dtype(column) or is_timedelta64_dtype(column):
                type_map[normalized_name] = "TIMESTAMP"
            else:
                type_map[normalized_name] = "TEXT"

        return type_map

    def ingest_csv(
        self,
        filepath: str,
        schema_manager: Any,
        query_executor: Any,
        table_name: str | None = None,
    ) -> int:
        resolved_path = Path(filepath).expanduser()

        try:
            chunks = pd.read_csv(resolved_path, chunksize=self.chunksize) # iterator ingestor – avoid RAM overhead
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"File '{resolved_path}' was not found.") from exc

        try:
            first_chunk = next(chunks)
        except StopIteration as exc:
            raise EmptyDataError(f"During ingestion, csv file at '{resolved_path}' was empty.") from exc

        if first_chunk.empty:
            raise EmptyDataError(f"During ingestion, csv file at '{resolved_path}' was empty.")

        target_table = table_name or self.table_name or self._derive_table_name(resolved_path)
        schema = self.infer_sql_types(first_chunk)

        create_table_sql = self._build_create_table_sql(schema_manager, target_table, schema)
        self._execute_statement(query_executor, create_table_sql)

        rows_inserted = self._insert_chunk(query_executor, target_table, first_chunk)
        for chunk in chunks:
            if chunk.empty:
                continue
            rows_inserted += self._insert_chunk(query_executor, target_table, chunk)

        return rows_inserted

    def ingest_file(self, csv_path: str) -> int:
        if self.schema_manager is None or self.query_executor is None:
            raise ValueError(
                "CSVIngestor requires schema_manager and query_executor for ingest_file()."
            )

        return self.ingest_csv(
            csv_path,
            schema_manager=self.schema_manager,
            query_executor=self.query_executor,
            table_name=self.table_name,
        )

    def _build_create_table_sql(
        self, schema_manager: Any, table_name: str, schema: dict[str, str]
    ) -> str:
        for method_name in ("build_create_table_sql", "build_create_table", "create_table_sql"):
            method = getattr(schema_manager, method_name, None)
            if callable(method):
                return str(method(table_name, schema))

        columns = ", ".join(
            f"{self._quote_identifier(column_name)} {column_type}"
            for column_name, column_type in schema.items()
        )
        return f"CREATE TABLE IF NOT EXISTS {self._quote_identifier(table_name)} ({columns});"

    def _insert_chunk(self, query_executor: Any, table_name: str, chunk: pd.DataFrame) -> int:
        column_names = [str(column_name) for column_name in chunk.columns]
        quoted_columns = ", ".join(self._quote_identifier(column) for column in column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = (
            f"INSERT INTO {self._quote_identifier(table_name)} ({quoted_columns}) "
            f"VALUES ({placeholders});"
        )

        rows = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in chunk.itertuples(index=False, name=None)
        ]
        if not rows:
            return 0

        executemany = getattr(query_executor, "executemany", None)
        if callable(executemany):
            executemany(insert_sql, rows)
            return len(rows)

        insert_rows = getattr(query_executor, "insert_rows", None)
        if callable(insert_rows):
            insert_rows(table_name, column_names, rows)
            return len(rows)

        for row in rows:
            self._execute_statement(query_executor, insert_sql, row)

        return len(rows)

    def _execute_statement(self, query_executor: Any, sql: str, params: tuple[Any, ...] | None = None) -> None:
        for method_name in ("execute", "execute_query", "run_query"):
            method = getattr(query_executor, method_name, None)
            if not callable(method):
                continue

            if params is None:
                method(sql)
            else:
                try:
                    method(sql, params)
                except TypeError:
                    method(sql)
            return

        raise AttributeError(
            "query_executor must implement one of: execute, execute_query, run_query, executemany, insert_rows."
        )

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def _derive_table_name(filepath: Path) -> str:
        cleaned = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in filepath.stem)
        cleaned = cleaned.strip("_") or "imported_data"
        if cleaned[0].isdigit():
            cleaned = f"t_{cleaned}"
        return cleaned
