import sqlite3
import config as conf
import sql_sanitizer as sanitizer


class SchemaManager:
    def __init__(self):
        config = conf.Config()
        db_path = config.get_db_path()
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def get_existing_tables(self) -> list:
        getTablesQueries = """SELECT name \
                       FROM sqlite_master
                       WHERE type = 'table';"""

        self.cursor.execute(getTablesQueries)
        tables = [row[0] for row in self.cursor.fetchall()]

        return tables

    def get_table_schema(self, table_name: str) -> dict:
        # the sqlite3 automatically sanitizes the cursor '?' placeholders according to:
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor
        self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))

        schema = self.cursor.fetchone()

        if schema is None:
            schema = {}

        return schema

    def evaluate_compatibility(self, csv_schema, table_name) -> str:


    def build_create_table_sql(self, table_name, schema) -> str: