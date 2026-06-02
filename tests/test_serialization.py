"""Tests for the unified TSV serialization layer.

These lock in the single-format contract: one NULL sentinel (``\\N``), one
escaping scheme, and one row-per-line guarantee shared by the inline payload
and every on-disk file.
"""

from datetime import datetime

from server.serialization import (
    TSV_EXTENSION,
    TSV_NULL,
    build_tsv,
    build_tsv_rows,
    column_index_map,
    column_names,
    format_value,
    tsv_escape,
    write_tsv_file,
)


def test_null_renders_as_sentinel_not_empty():
    """SQL NULL must be distinguishable from an empty string."""
    assert tsv_escape(None) == TSV_NULL
    assert tsv_escape("") == ""  # actual empty value stays empty
    assert TSV_NULL != ""


def test_escaping_keeps_one_row_per_line():
    """Tabs/newlines/backslashes are escaped reversibly, no raw line breaks."""
    value = "a\tb\nc\rd\\e"
    escaped = tsv_escape(value)
    assert "\t" not in escaped
    assert "\n" not in escaped
    assert "\r" not in escaped
    # Backslash is escaped first so escapes aren't doubled ambiguously.
    assert escaped == "a\\tb\\nc\\rd\\\\e"


def test_format_value_datetime_and_bytes():
    assert format_value(datetime(2024, 1, 1, 12, 0, 0)) == "2024-01-01T12:00:00"
    assert format_value(b"hello") == "hello"
    assert format_value(None) is None
    assert format_value(42) == 42


def test_column_names_from_dicts_and_strings_and_sample():
    assert column_names([{"name": "a"}, {"name": "b"}]) == ["a", "b"]
    assert column_names(["a", "b"]) == ["a", "b"]
    assert column_names(None, {"x": 1, "y": 2}) == ["x", "y"]


def test_build_tsv_header_and_rows_positional():
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": None}]
    tsv = build_tsv(rows, ["id", "name"])
    lines = tsv.split("\n")
    assert lines[0] == "id\tname"
    assert lines[1] == "1\tAlice"
    assert lines[2] == f"2\t{TSV_NULL}"  # NULL sentinel on disk == inline


def test_build_tsv_rows_has_no_header_line():
    """The header-less variant emits data lines only (for the spill preview).

    Column names are carried by the column_index map in that path, so the
    preview must NOT repeat them as a TSV header line.
    """
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": None}]
    names = ["id", "name"]
    block = build_tsv_rows(rows, names)
    lines = block.split("\n")
    # No header line: first line is already a data row.
    assert lines[0] == "1\tAlice"
    assert lines[1] == f"2\t{TSV_NULL}"
    assert len(lines) == len(rows)
    # And it is exactly build_tsv minus its first (header) line.
    assert block == "\n".join(build_tsv(rows, names).split("\n")[1:])


def test_build_tsv_rows_empty():
    assert build_tsv_rows([], ["id", "name"]) == ""


def test_column_index_map_is_1_based_and_matches_awk():
    """The map must be 1-based so ``63=TYPE`` maps directly to awk ``$63``."""
    names = ["ID", "NAME", "TYPE"]
    m = column_index_map(names)
    assert m == "1=ID 2=NAME 3=TYPE"
    # Resolving a name to its awk field number is a plain pattern match.
    assert "3=TYPE" in m
    # Index N corresponds to the Nth header field (1-based), so build_tsv's
    # header field at that position is the named column.
    header = build_tsv([], names).split("\n")[0].split("\t")
    assert header[3 - 1] == "TYPE"


def test_column_index_map_empty():
    assert column_index_map([]) == ""


def test_write_tsv_file_roundtrip(tmp_path):
    rows = [{"id": 1, "v": "x"}, {"id": 2, "v": None}, {"id": 3, "v": ""}]
    path = tmp_path / "out.tsv"
    written = write_tsv_file(str(path), rows, ["id", "v"])
    assert written == 3
    content = path.read_text(encoding="utf-8").splitlines()
    assert content[0] == "id\tv"
    assert content[1] == "1\tx"
    assert content[2] == f"2\t{TSV_NULL}"  # NULL
    assert content[3] == "3\t"  # empty string, distinct from NULL


def test_tsv_extension_constant():
    assert TSV_EXTENSION == ".tsv"
