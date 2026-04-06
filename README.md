# Barys

Barys is a CLI tool that lets you query SQLite data in natural language and import CSV files safely.

It combines:
- a local SQLite database,
- a guarded NL-to-SQL flow,
- and a friendly interactive shell.

## What You Can Do

- Ask questions in plain English (or your preferred language) and get:
  - generated SQL,
  - and a concise summary of results.
- Import CSV files into SQLite with safety checks (path restrictions, file type checks, size limit, confirmation).
- List available tables and remove tables from the CLI.

## Project Layout

```text
Barys/
  src/
	main.py            # App entrypoint
	cli.py             # Interactive shell commands
	query_service.py   # NL-to-SQL + summarization orchestration
	query_executor.py  # DB execution layer
	schema_manager.py  # Schema introspection and table operations
	csv_ingestor.py    # CSV ingestion pipeline
  data/
	db/                # SQLite database location
	csv/               # Example CSV input location
  tests/               # Unit tests
```

## Requirements

- Python 3.11+ (recommended)
- SQLite (bundled with standard Python builds)
- Dependencies from `requirements.txt`

## Quick Start

```zsh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir -p data/db
touch data/db/database.db
python3 src/main.py
```

## Optional Environment Variables

Create a `.env` file in the project root if needed:

```env
DB_PATH=data/db/database.db
DB_URL=sqlite:///data/db/database.db
GEMINI_API_KEY=your_key_here
LLM_MODEL=gemini-3.1-flash-lite-preview
LLM_TIMEOUT_SECONDS=15
MAX_ROWS_CONTEXT=100
SQL_POOL_SIZE=3
MAX_IMPORT_SIZE_BYTES=5368709120
```

Notes:
- `GEMINI_API_KEY` is required for LLM-based SQL generation/summarization.
- `MAX_IMPORT_SIZE_BYTES` defaults to 5 GB.

## CLI Usage

Once the shell starts (`barys>`), you can run:

```text
help
import <filepath>
tables
tables rm "table_name"
exit
```

### Example Session

```text
barys> import data/csv/test.csv
barys> who registered most recently?
SQL:
SELECT ...

Summary:
...
barys> tables
barys> tables rm "old_table"
```

`tables rm` asks for confirmation before deleting.

## Run Tests

```zsh
pytest -q
```

---

Built for EC530 with a focus on practical safety and developer ergonomics.
