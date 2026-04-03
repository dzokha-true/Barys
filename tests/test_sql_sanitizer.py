import pytest

import sql_sanitizer as sanitizer


def test_enforce_identifier_whitelist_allows_alnum_and_underscore() -> None:
	assert sanitizer.enforce_identifier_whitelist("table_2026") == "table_2026"


def test_enforce_identifier_whitelist_rejects_unsafe_identifier() -> None:
	with pytest.raises(sanitizer.SecurityError):
		sanitizer.enforce_identifier_whitelist('users; DROP TABLE users;--')


def test_strip_markdown_extracts_sql_block() -> None:
	raw = """Here is your query:\n```sql\nSELECT * FROM users;\n```"""
	assert sanitizer.strip_markdown(raw) == "SELECT * FROM users;"


def test_strip_markdown_removes_plain_prefix() -> None:
	assert sanitizer.strip_markdown("Query: SELECT 1;") == "SELECT 1;"

