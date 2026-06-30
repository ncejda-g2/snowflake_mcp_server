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

import csv
from datetime import datetime
from typing import IO, Any

# Sentinel rendered for SQL NULL. Distinguishable from the empty string (an
# actual zero-length value), which renders as a literal empty field. Used
# identically inline and on disk so the sentinel is portable between them.
TSV_NULL = "\\N"

# File extensions for on-disk exports.
TSV_EXTENSION = ".tsv"
CSV_EXTENSION = ".csv"


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


def build_tsv_rows(rows: list[dict], names: list[str]) -> str:
    """Build a header-less TSV block: one line per row, no column-name line.

    Used for the auto-spill preview, where the column names are already carried
    (with positions) by the ``column_index`` map. Emitting a header line there
    too would duplicate every column name in the inline payload for no benefit:
    the agent resolves names via the map and reads the real data from the file.
    The preview's job is only to show value formatting/shape, so it is pure
    data lines.
    """
    return "\n".join(tsv_row_line(row, names) for row in rows)


def build_labeled_record(row: dict, names: list[str]) -> str:
    """Render a SINGLE result row as aligned ``NAME  value`` lines (one per col).

    This is the inline format for a one-row result, used in place of positional
    TSV regardless of column count. A lone row is read by *label*, never by tab
    position, so the wide-result miscount hazard (counting tab fields by eye to
    pair a value with its column) simply does not apply -- which is why the
    column-count gate is ignored for a single row. The labels ARE the column
    names, so no separate ``column_index`` map is needed.

    Names are left-padded to a common width so values line up in a readable
    column. Values reuse the shared TSV escaping (``format_value`` +
    :func:`tsv_escape`): SQL NULL becomes :data:`TSV_NULL`, and tabs/newlines are
    backslash-escaped so each field stays on exactly one physical line and never
    spills into an unlabeled continuation line.

    Names are not escaped (they are display labels, consistent with
    :func:`column_index_map`). Callers must size-check the returned string
    against the inline char budget: a row carrying a giant cell still has to
    spill to a file.
    """
    width = max((len(name) for name in names), default=0)
    return "\n".join(
        f"{name.ljust(width)}  {tsv_escape(format_value(row.get(name)))}"
        for name in names
    )


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


# --- CSV export ----------------------------------------------------------
#
# CSV is a convenience export for humans/spreadsheets, NOT the agent's native
# parsing format (that is TSV, shared with the inline payload). The semantics
# deliberately differ from TSV because standard CSV consumers expect them:
#   * Quoting, not backslash-escaping: values containing commas, quotes, or
#     newlines are wrapped per RFC 4180 (handled by the stdlib ``csv`` module).
#   * NULL renders as an EMPTY field, not ``\\N`` -- CSV has no portable NULL
#     sentinel, and an empty cell is the near-universal convention. (This means
#     CSV cannot distinguish SQL NULL from an empty string; TSV still can.)


def csv_cell(value: Any) -> str:
    """Normalize a raw cell value for a CSV field.

    Reuses :func:`format_value` for datetime/bytes handling, then renders SQL
    NULL as an empty string (the CSV convention). The stdlib ``csv`` writer
    handles all quoting/escaping, so no manual escaping is done here.
    """
    formatted = format_value(value)
    if formatted is None:
        return ""
    if not isinstance(formatted, str):
        formatted = str(formatted)
    return formatted


class _TsvStreamWriter:
    """Streaming TSV writer: header then one escaped line per row."""

    extension = TSV_EXTENSION

    def __init__(self, handle: IO[str], names: list[str]):
        self._handle = handle
        self._names = names
        handle.write(tsv_header_line(names))
        handle.write("\n")

    def write_row(self, row: dict) -> None:
        self._handle.write(tsv_row_line(row, self._names))
        self._handle.write("\n")


class _CsvStreamWriter:
    """Streaming CSV writer (RFC 4180 quoting via the stdlib ``csv`` module)."""

    extension = CSV_EXTENSION

    def __init__(self, handle: IO[str], names: list[str]):
        self._names = names
        # newline="" is required by the csv module so it controls line endings.
        self._writer = csv.writer(handle)
        self._writer.writerow(names)

    def write_row(self, row: dict) -> None:
        self._writer.writerow([csv_cell(row.get(name)) for name in self._names])


# Map of recognized export extensions -> streaming writer class. Resolution is
# case-insensitive and based solely on the path the caller chose.
_STREAM_WRITERS = {
    TSV_EXTENSION: _TsvStreamWriter,
    CSV_EXTENSION: _CsvStreamWriter,
}


def resolve_export_extension(file_path: str) -> str:
    """Pick the export extension implied by ``file_path``.

    Returns the recognized extension (``.csv`` or ``.tsv``) the path already
    ends with (case-insensitively), or :data:`TSV_EXTENSION` as the default when
    the path has no recognized export extension. TSV is the default because it
    is the agent's native format, identical to the inline payload.
    """
    lowered = file_path.lower()
    for ext in _STREAM_WRITERS:
        if lowered.endswith(ext):
            return ext
    return TSV_EXTENSION


def open_export_writer(handle: IO[str], names: list[str], extension: str):
    """Create a streaming writer for ``extension`` bound to an open file handle.

    The returned object writes the header immediately and exposes
    ``write_row(row)`` for incremental, memory-bounded streaming. ``extension``
    must be one of the keys in :data:`_STREAM_WRITERS` (use
    :func:`resolve_export_extension` to derive it).
    """
    writer_cls = _STREAM_WRITERS[extension]
    return writer_cls(handle, names)
