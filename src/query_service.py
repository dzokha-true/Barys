from __future__ import annotations

import re
from typing import Any

import sql_sanitizer as sanitizer
import sql_validator as validator


class QueryService:
    """Coordinates NL-to-SQL generation, guarded execution, and answer summarization."""

    def __init__(
        self,
        schema_manager: Any,
        query_executor: Any,
        llm_adapter: Any,
        _config: Any | None = None,
    ) -> None:
        self.schema_manager = schema_manager
        self.query_executor = query_executor
        self.llm_adapter = llm_adapter
        self._hard_row_limit = 10

    def process_nl_query(self, user_question: str) -> dict[str, str]:
        schema = self.schema_manager.get_formatted_schema()
        raw_sql = self.llm_adapter.generate_sql(user_question, schema)
        cleaned_sql = sanitizer.strip_markdown(raw_sql).strip()

        if not validator.is_safe_read_only(cleaned_sql):
            raise sanitizer.SecurityError("Generated SQL failed read-only safety checks.")

        sql_for_execution = self._ensure_limit(cleaned_sql, self._hard_row_limit)
        rows = self.query_executor.execute_read(sql_for_execution)
        top_rows = rows[: self._hard_row_limit]

        summary_prompt = (
            "Use this query context to answer the user request clearly and concisely.\n"
            f"User question: {user_question}\n"
            f"Executed SQL: {sql_for_execution}\n"
            f"SQL output rows: {top_rows}\n"
            "Do not speculate. If the rows are empty, state that no matching records were found."
        )
        summary = self.llm_adapter.generate_nl_summary(summary_prompt, top_rows)

        return {
            "sql": sql_for_execution,
            "summary": summary,
        }

    def _ensure_limit(self, sql: str, limit: int) -> str:
        statement = sql.strip()
        if not statement.lower().startswith("select"):
            return statement

        if re.search(r"\blimit\b", statement, flags=re.IGNORECASE):
            return statement

        has_semicolon = statement.endswith(";")
        if has_semicolon:
            statement = statement[:-1].rstrip()

        limited_statement = f"{statement} LIMIT {int(limit)}"
        return f"{limited_statement};" if has_semicolon else limited_statement
