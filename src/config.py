import os
from dotenv import load_dotenv, find_dotenv


def parse_max_rows_context(raw_value: str) -> int:
    try:
        max_rows = int(raw_value)
    except ValueError as exc:
        raise ValueError("MAX_ROWS_CONTEXT must be an integer") from exc

    return max_rows


def parse_max_import_size_bytes(raw_value: str) -> int:
    try:
        max_size = int(raw_value)
    except ValueError as exc:
        raise ValueError("MAX_IMPORT_SIZE_BYTES must be an integer") from exc

    if max_size <= 0:
        raise ValueError("MAX_IMPORT_SIZE_BYTES must be positive")

    return max_size


class Config:
    def __init__(self) -> None:
        load_dotenv(find_dotenv()) #TODO: this is probably bad practice?

        self.db_path = os.getenv("DB_PATH", "data/database.db")
        self.db_url = os.getenv("DB_URL", f"sqlite:///{self.db_path}")
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", os.getenv("LLM_API_KEY", ""))
        self.llm_api_key = self.gemini_api_key
        self.llm_model = os.getenv("LLM_MODEL", "gemini-3.1-flash-lite-preview")
        self.llm_timeout_seconds = float(os.getenv("LLM_TIMEOUT_SECONDS", "15"))
        self.max_rows_context = int(
                parse_max_rows_context(
                os.getenv("MAX_ROWS_CONTEXT", "100")
            )
        )
        self.sqlite_conn_pool_size = int(os.getenv("SQL_POOL_SIZE", 3))
        self.max_import_size_bytes = parse_max_import_size_bytes(
            os.getenv("MAX_IMPORT_SIZE_BYTES", str(5 * 1024 * 1024 * 1024))
        )

    def get_db_path(self) -> str:
        return self.db_path

