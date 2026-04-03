import sqlite3
import re
import config as conf
import sql_sanitizer as sanitizer


_ALLOWED_COLUMN_TYPE_PATTERN = r"^[A-Z][A-Z0-9_]*(?:\s+[A-Z][A-Z0-9_]*)*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?$"


class SchemaManager:
    def __init__(self, db_pool=None):
        self._db_pool = db_pool
        self._owns_connection = db_pool is None

        if db_pool is not None and hasattr(db_pool, "get_connection"):
            self.conn = db_pool.get_connection()
        else:
            config = conf.Config()
            db_path = self._resolve_db_path(config)
            self.conn = sqlite3.connect(db_path)

        self.cursor = self.conn.cursor()  # TODO: maybe separate this concern?
        self._schema_cache: dict[str, dict[str, object]] = {}
        self.refresh_cache()

    def _resolve_db_path(self, config: conf.Config) -> str:
        get_db_path = getattr(config, "get_db_path", None)
        if callable(get_db_path):
            return str(get_db_path())

        db_url = str(getattr(config, "db_url", ""))
        sqlite_prefix = "sqlite:///"
        if db_url.startswith(sqlite_prefix):
            return db_url[len(sqlite_prefix):]

        return "data/database.db"

    def get_existing_tables(self) -> list:
        getTablesQueries = """SELECT name
                       FROM sqlite_master
                       WHERE type = 'table'
                       AND name NOT LIKE 'sqlite_%';"""

        self.cursor.execute(getTablesQueries)
        tables = [row[0] for row in self.cursor.fetchall()]
        return tables

    def get_table_schema(self, table_name: str) -> dict:
        safe_table_name = sanitizer.enforce_identifier_whitelist(table_name)
        if safe_table_name not in self._schema_cache:
            self.refresh_cache()

        table_meta = self._schema_cache.get(safe_table_name, {})
        return dict(table_meta.get("columns", {}))

    def build_create_table_sql(self, table_name: str, schema: dict[str, str]) -> str:
        safe_table_name = sanitizer.enforce_identifier_whitelist(table_name)
        if not schema:
            raise ValueError("Cannot create table without columns.")

        column_parts = []
        for column_name, column_type in schema.items():
            safe_column_name = sanitizer.enforce_identifier_whitelist(column_name)
            normalized_type = str(column_type).strip().upper()
            if re.fullmatch(_ALLOWED_COLUMN_TYPE_PATTERN, normalized_type) is None:
                raise ValueError(f"Unsupported SQL column type '{column_type}' for '{column_name}'.")
            column_parts.append(f'"{safe_column_name}" {normalized_type}')

        columns_sql = ", ".join(column_parts)
        return f'CREATE TABLE IF NOT EXISTS "{safe_table_name}" ({columns_sql});'

    def build_create_table(self, table_name: str, schema: dict[str, str]) -> str:
        return self.build_create_table_sql(table_name, schema)

    def create_table_sql(self, table_name: str, schema: dict[str, str]) -> str:
        return self.build_create_table_sql(table_name, schema)

    def create_table(self, table_name: str, schema: dict[str, str]) -> None:
        ddl = self.build_create_table_sql(table_name, schema)
        self.cursor.execute(ddl)
        self.conn.commit()
        self.refresh_cache()

    def refresh_cache(self) -> None:
        cache: dict[str, dict[str, object]] = {}
        table_names = self.get_existing_tables()

        for table_name in table_names:
            safe_table_name = sanitizer.enforce_identifier_whitelist(table_name)

            # PRAGMA table_info does not support standard DB-API bind parameters.
            self.cursor.execute(f'PRAGMA table_info("{safe_table_name}")')
            columns = {
                row[1]: row[2] or "TEXT"
                for row in self.cursor.fetchall()
            }

            self.cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (safe_table_name,),
            )
            ddl_row = self.cursor.fetchone()
            cache[safe_table_name] = {
                "columns": columns,
                "ddl": ddl_row[0] if ddl_row else "",
            }

        self._schema_cache = cache

    def get_formatted_schema(self) -> str:
        if not self._schema_cache:
            return "No tables available."

        lines = []
        for table_name in sorted(self._schema_cache):
            columns = self._schema_cache[table_name]["columns"]
            formatted_columns = ", ".join(
                f"{column_name} {column_type}"
                for column_name, column_type in columns.items()
            )
            lines.append(f"{table_name}({formatted_columns})")

        return "\n".join(lines)

    def close(self) -> None:
        try:
            self.cursor.close()
        finally:
            if self._owns_connection:
                self.conn.close()
            elif self._db_pool is not None and hasattr(self._db_pool, "return_connection"):
                self._db_pool.return_connection(self.conn)
