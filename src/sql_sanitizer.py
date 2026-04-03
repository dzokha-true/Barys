import re


class SecurityError(ValueError):
    """Raised when user-controlled SQL fragments fail security checks."""


_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")
_MARKDOWN_SQL_BLOCK_PATTERN = re.compile(r"```(?:sql)?\s*(.*?)\s*```", re.IGNORECASE | re.DOTALL)


def enforce_identifier_whitelist(identifier: str) -> str:
    """Allow only SQL identifiers made of letters, numbers, and underscores."""
    normalized = str(identifier).strip()
    if not normalized or _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise SecurityError("Identifier contains unsafe characters.")
    return normalized


def strip_markdown(raw_llm_output: str) -> str:
    """Extract SQL text from markdown fences and remove common conversational prefixes."""
    text = str(raw_llm_output or "").strip()
    if not text:
        return ""

    blocks = _MARKDOWN_SQL_BLOCK_PATTERN.findall(text)
    if blocks:
        return "\n".join(block.strip() for block in blocks if block.strip())

    normalized = text.replace("```sql", "").replace("```", "")
    for prefix in (
        "Here is the SQL:",
        "Here is your SQL:",
        "SQL query:",
        "Query:",
    ):
        if normalized.lower().startswith(prefix.lower()):
            normalized = normalized[len(prefix):].lstrip()
            break

    return normalized.strip()


def sanitize(identifier: str) -> str:
    """Backward-compatible alias used by older callers."""
    return enforce_identifier_whitelist(identifier)
