import re

from sql_sanitizer import strip_markdown


_ALLOWED_STATEMENT_PREFIXES = (
    "select",
)


def _remove_comments(sql: str) -> str:
    without_inline = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return re.sub(r"/\*.*?\*/", "", without_inline, flags=re.DOTALL)


def is_safe_read_only(sql_string: str) -> bool:
    """Return True only when the statement prefix is in the read-only allowlist."""
    normalized = strip_markdown(sql_string)
    normalized = _remove_comments(normalized).strip()
    if not normalized:
        return False

    statements = [part.strip() for part in normalized.split(";") if part.strip()]
    if len(statements) != 1:
        return False

    statement = statements[0]
    lowered = statement.lower().lstrip()
    return any(lowered.startswith(f"{prefix} ") or lowered == prefix for prefix in _ALLOWED_STATEMENT_PREFIXES)
