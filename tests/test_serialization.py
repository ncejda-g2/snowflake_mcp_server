"""Tests for the unified TSV serialization layer.

These lock in the single-format contract: one NULL sentinel (``\\N``), one
escaping scheme, and one row-per-line guarantee shared by the inline payload
and every on-disk file.
"""

from datetime import datetime

from server.serialization import (
    CSV_EXTENSION,
    TSV_EXTENSION,
    TSV_NULL,
    build_labeled_record,
    build_tsv,
    build_tsv_rows,
    column_index_map,
    column_names,
    csv_cell,
    format_value,
    open_export_writer,
    resolve_export_extension,
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


def test_build_labeled_record_aligns_and_reuses_tsv_escaping():
    """A single row renders as aligned ``NAME  value`` lines, one per column.

    Names are left-padded to a common width; values reuse the shared escaping
    (NULL -> sentinel, newlines/tabs escaped so each field is one line).
    """
    row = {"id": 1, "long_name": "Alice", "note": None}
    names = ["id", "long_name", "note"]
    out = build_labeled_record(row, names)
    width = len("long_name")  # widest name drives the column
    assert out.split("\n") == [
        f"{'id'.ljust(width)}  1",
        f"{'long_name'.ljust(width)}  Alice",
        f"{'note'.ljust(width)}  {TSV_NULL}",  # NULL sentinel, not empty
    ]


def test_build_labeled_record_keeps_one_line_per_field():
    """A value with embedded tabs/newlines stays on its label's single line."""
    row = {"blob": "a\tb\nc"}
    out = build_labeled_record(row, ["blob"])
    assert "\n" not in out.split("  ", 1)[1]  # value half has no raw newline
    assert out == "blob  a\\tb\\nc"


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


# --- CSV export ----------------------------------------------------------


def test_csv_extension_constant():
    assert CSV_EXTENSION == ".csv"


def test_csv_cell_null_and_empty_both_render_empty():
    # CSV has no NULL sentinel: NULL and "" are indistinguishable (empty field).
    assert csv_cell(None) == ""
    assert csv_cell("") == ""


def test_csv_cell_normalizes_datetime_and_bytes():
    assert csv_cell(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert csv_cell(b"hi") == "hi"
    assert csv_cell(42) == "42"


def test_resolve_export_extension_defaults_to_tsv():
    assert resolve_export_extension("/tmp/out") == ".tsv"
    assert resolve_export_extension("/tmp/out.json") == ".tsv"  # unrecognized
    assert resolve_export_extension("/tmp/out.tsv") == ".tsv"


def test_resolve_export_extension_detects_csv_case_insensitively():
    assert resolve_export_extension("/tmp/out.csv") == ".csv"
    assert resolve_export_extension("/tmp/out.CSV") == ".csv"


def test_open_export_writer_csv_quotes_and_roundtrips(tmp_path):
    import csv

    path = tmp_path / "x.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = open_export_writer(f, ["id", "name"], ".csv")
        writer.write_row({"id": 1, "name": "Smith, Bob"})  # comma -> quoted
        writer.write_row({"id": 2, "name": None})  # NULL -> empty
    rows = list(csv.reader(path.read_text(encoding="utf-8").splitlines()))
    assert rows == [["id", "name"], ["1", "Smith, Bob"], ["2", ""]]


def test_open_export_writer_tsv_matches_inline_format(tmp_path):
    path = tmp_path / "x.tsv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = open_export_writer(f, ["id", "v"], ".tsv")
        writer.write_row({"id": 1, "v": None})
        writer.write_row({"id": 2, "v": ""})
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines == ["id\tv", f"1\t{TSV_NULL}", "2\t"]
