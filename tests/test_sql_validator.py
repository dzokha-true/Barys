import sql_validator


def test_is_safe_read_only_accepts_simple_select() -> None:
	assert sql_validator.is_safe_read_only("SELECT id, name FROM users") is True


def test_is_safe_read_only_rejects_destructive_keyword() -> None:
	assert sql_validator.is_safe_read_only("SELECT * FROM users; DELETE FROM users;") is False


def test_is_safe_read_only_rejects_non_select_statement() -> None:
	assert sql_validator.is_safe_read_only("PRAGMA table_info(users)") is False


def test_is_safe_read_only_handles_markdown_and_comments() -> None:
	query = """```sql
	-- read only
	SELECT * FROM users;
	```"""
	assert sql_validator.is_safe_read_only(query) is True


def test_is_safe_read_only_allows_words_like_drop_inside_string_literal() -> None:
	assert sql_validator.is_safe_read_only("SELECT 'drop table users' AS note") is True


