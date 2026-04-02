import os
from dotenv import load_dotenv, find_dotenv


def parse_max_rows_context(raw_value: str) -> int:
    try:
        max_rows = int(raw_value)
    except ValueError as exc:
        raise ValueError("MAX_ROWS_CONTEXT must be an integer") from exc

    return max_rows


class Config:
    def __init__(self) -> None:
        load_dotenv(find_dotenv()) #TODO: this is probably bad practice?

        db_path = os.getenv("DB_PATH", "data/database.db")
        self.db_url = os.getenv("DB_URL", f"sqlite:///{db_path}")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.max_rows_context = parse_max_rows_context(
            os.getenv("MAX_ROWS_CONTEXT", "100")
        )
        self.sqlite_conn_pool_size = os.getenv("SQL_POOL_SIZE", 3)