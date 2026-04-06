from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import json
from typing import Any, Sequence

import config as conf

try:
    from google import genai
except Exception:  # pragma: no cover - exercised via missing dependency path
    genai = None


class LLMAdapterError(RuntimeError):
    """Raised when the Gemini gateway cannot complete a request."""


class LLMAdapter:
    """Gemini API Gateway for SQL generation and result summarization."""

    def __init__(
        self,
        config: conf.Config | None = None,
        client: Any | None = None,
    ) -> None:
        self._config = config or conf.Config()
        self.model = str(getattr(self._config, "llm_model", "gemini-3.1-flash-lite-preview"))
        self.timeout_seconds = float(getattr(self._config, "llm_timeout_seconds", 15.0))
        self.max_rows_context = int(getattr(self._config, "max_rows_context", 100))
        self.api_key = str(
            getattr(
                self._config,
                "gemini_api_key",
                getattr(self._config, "llm_api_key", ""),
            )
        )

        if client is not None:
            self._client = client
        else:
            if genai is None:
                raise LLMAdapterError("google-genai is required. Install with: pip install google-genai")
            self._client = genai.Client(api_key=self.api_key or None)

    def generate_sql(self, prompt: str, schema: Any) -> str:
        system_prompt = self._build_sql_system_prompt(schema)
        return self._call_gateway(system_prompt=system_prompt, user_prompt=prompt, temperature=0.0)

    def generate_nl_summary(self, prompt: str, data_rows: Sequence[Sequence[Any]]) -> str:
        system_prompt = self._build_summary_system_prompt(data_rows)
        return self._call_gateway(system_prompt=system_prompt, user_prompt=prompt, temperature=0.2)

    def _call_gateway(self, system_prompt: str, user_prompt: str, temperature: float) -> str:
        if not self.api_key:
            raise LLMAdapterError("GEMINI_API_KEY is not configured.")

        combined_prompt = (
            f"System instruction:\n{system_prompt}\n\n"
            f"User request:\n{user_prompt.strip()}\n\n"
            f"Generation temperature hint: {temperature}"
        )

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self._client.models.generate_content,
                    model=self.model,
                    contents=combined_prompt,
                )
                response = future.result(timeout=self.timeout_seconds)
        except FuturesTimeoutError as exc:
            raise LLMAdapterError("Gemini request timed out.") from exc
        except Exception as exc:
            raise LLMAdapterError(f"Gemini request failed: {exc}") from exc

        content = self._extract_content(response)
        if not content.strip():
            raise LLMAdapterError("Gemini returned an empty response.")
        return content.strip()

    def _build_sql_system_prompt(self, schema: Any) -> str:
        return (
            "You are a SQL generator. Return exactly one read-only SQLite SQL query and no markdown. "
            "Do not explain the query. Use only tables and columns in this schema. "
            "If the user asks to list tables, use SELECT against sqlite_master. "
            "If the user asks for table columns, you may use PRAGMA table_info(<table_name>).\n"
            f"{self._format_schema(schema)}"
        )

    def _build_summary_system_prompt(self, data_rows: Sequence[Sequence[Any]]) -> str:
        return (
            "You are a data analyst. Summarize the result rows in plain language. "
            "Be concise, avoid speculation, and mention if the dataset is empty. "
            "Rows context:\n"
            f"{self._format_rows(data_rows)}"
        )

    def _format_schema(self, schema: Any) -> str:
        if isinstance(schema, str):
            return schema.strip()

        try:
            return json.dumps(schema, ensure_ascii=True, default=str, indent=2)
        except TypeError:
            return str(schema)

    def _format_rows(self, data_rows: Sequence[Sequence[Any]]) -> str:
        if not data_rows:
            return "[]"

        limited_rows = [list(row) for row in data_rows[: self.max_rows_context]]
        return json.dumps(limited_rows, ensure_ascii=True, default=str)

    def _extract_content(self, parsed: Any) -> str:
        text = getattr(parsed, "text", None)
        if isinstance(text, str):
            return text

        if isinstance(parsed, dict) and isinstance(parsed.get("text"), str):
            return parsed["text"]

        if isinstance(parsed, dict) and isinstance(parsed.get("content"), str):
            return parsed["content"]

        raise LLMAdapterError("Gemini response format is unsupported.")
