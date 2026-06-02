"""Unified TSV serialization for all query output paths.

This is the single source of truth for how a Snowflake result row becomes text,
used by both the inline ``execute_query`` payload and every on-disk file
(``execute_query`` auto-spill and ``execute_query_to_file``).

One format, one NULL sentinel, one escaping scheme. A ``\\N`` learned from the
inline payload means exactly the same thing in a file on disk, and an empty
field is always an actual zero-length value (never NULL). Every row is
guaranteed to be exactly one physical line, so the agent can rely on
``grep`` / ``awk -F'\\t'`` / ``cut -f`` and on ``wc -l`` for counting.
"""

from datetime import datetime
from typing import Any

# Sentinel rendered for SQL NULL. Distinguishable from the empty string (an
# actual zero-length value), which renders as a literal empty field. Used
# identically inline and on disk so the sentinel is portable between them.
TSV_NULL = "\\N"

# File extension for on-disk TSV exports.
TSV_EXTENSION = ".tsv"


def format_value(value: Any) -> Any:
    """Normalize a raw cell value before TSV escaping.

    Datetimes become ISO strings and bytes are decoded; everything else is
    passed through. ``None`` is preserved so :func:`tsv_escape` can render the
    NULL sentinel.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return value


def tsv_escape(value: Any) -> str:
    """Render a single cell value as a TSV-safe field.

    Guarantees exactly one logical line per row and clean tab-delimited fields.
    Tabs, newlines, carriage returns and backslashes are escaped reversibly.
    SQL NULL becomes :data:`TSV_NULL` so it is distinguishable from an empty
    string.
    """
    if value is None:
        return TSV_NULL
    if not isinstance(value, str):
        value = str(value)
    # Backslash first so we don't double-escape the escapes we add next.
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def column_names(columns: Any, sample_row: Any = None) -> list[str]:
    """Extract ordered column names from a result's ``columns`` field.

    ``columns`` is normally a list of ``{"name", "type", "nullable"}`` dicts
    (see ``SnowflakeConnection.execute_query``). Some callers/tests pass a plain
    list of name strings. Fall back to the keys of a sample row dict.
    """
    if columns:
        first = columns[0]
        if isinstance(first, dict):
            return [str(c.get("name", "")) for c in columns]
        return [str(c) for c in columns]
    if isinstance(sample_row, dict):
        return list(sample_row.keys())
    return []


def tsv_header_line(names: list[str]) -> str:
    """Render the header line: tab-joined, escaped column names."""
    return "\t".join(tsv_escape(name) for name in names)


def column_index_map(names: list[str]) -> str:
    """Render a compact 1-based column index map: ``1=ID 2=NAME 3=TYPE ...``.

    This exists to remove the single most error-prone step when an agent parses
    a wide TSV result with ``awk``/``cut``: translating a column *name* into its
    *positional* index. Counting to e.g. column 63 by eye over a 92-name header
    is exactly where models (including strong ones) miscount, and it only gets
    worse as column count grows.

    The map is 1-based to match ``awk`` (``$1``..``$N``) and ``cut -f`` directly,
    so the agent can read ``63=TYPE`` and immediately write ``$63`` with no
    counting. It is rendered on a single physical line (space-separated pairs)
    so it survives the one-record-per-line response contract, yet stays
    pattern-matchable: the agent can scan for ``=TYPE`` or ``63=`` to resolve a
    name<->index pairing without reading the whole map.

    Column names are not escaped here because this is a human/agent-facing
    lookup hint, not a parseable data line; the authoritative, escaped names
    still live in the file's TSV header line.
    """
    return " ".join(f"{i}={name}" for i, name in enumerate(names, start=1))


def tsv_row_line(row: dict, names: list[str]) -> str:
    """Render one data row, pulling values positionally by column name.

    Field order matches the header exactly so the output is cleanly columnar.
    """
    return "\t".join(tsv_escape(format_value(row.get(name))) for name in names)


def build_tsv(rows: list[dict], names: list[str]) -> str:
    """Build a full TSV block: header line + one line per row.

    Designed for direct piping into grep/awk/cut by the agent.
    """
    lines = [tsv_header_line(names)]
    lines.extend(tsv_row_line(row, names) for row in rows)
    return "\n".join(lines)


def write_tsv_file(file_path: str, rows: list[dict], names: list[str]) -> int:
    """Write rows to ``file_path`` as TSV (header + one line per row).

    Returns the number of data rows written. Uses the same escaping/NULL
    semantics as the inline payload so the file is byte-for-byte the format the
    agent already knows how to parse.
    """
    written = 0
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(tsv_header_line(names))
        f.write("\n")
        for row in rows:
            f.write(tsv_row_line(row, names))
            f.write("\n")
            written += 1
    return written
