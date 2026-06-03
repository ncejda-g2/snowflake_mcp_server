"""Tests for server/tools/query_executor.py module."""

import os
import time
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from server.constants import INLINE_RESULT_CHAR_BUDGET
from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection
from server.tools import query_executor
from server.tools.query_executor import (
    _format_value,
    execute_query,
)


def parse_text_response(text: str) -> dict:
    """Parse the execute_query text payload into a dict for assertions.

    The payload is a ``key: value`` header, an optional ``---`` separator, then
    a TSV block (header line + row lines). Returns a dict with the header keys
    plus ``columns`` (list of names) and ``rows`` (list of lists of str cells).
    """
    assert isinstance(text, str)
    header_part, sep, tsv_part = text.partition("\n---\n")
    parsed: dict = {}
    for line in header_part.splitlines():
        if not line.strip():
            continue
        key, _, value = line.partition(": ")
        parsed[key] = value

    columns: list[str] = []
    data_rows: list[list[str]] = []
    if sep:
        tsv_lines = tsv_part.split("\n")
        if tsv_lines:
            columns = tsv_lines[0].split("\t") if tsv_lines[0] else []
            for line in tsv_lines[1:]:
                data_rows.append(line.split("\t"))
    parsed["columns"] = columns
    parsed["data_rows"] = data_rows
    return parsed


class TestHelperFunctions:
    """Test helper functions."""

    def test_format_value_none(self):
        """Test _format_value with None."""
        assert _format_value(None) is None

    def test_format_value_datetime(self):
        """Test _format_value with datetime."""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = _format_value(dt)
        assert isinstance(result, str)
        assert "2024-01-01" in result

    def test_format_value_bytes(self):
        """Test _format_value with bytes."""
        data = b"test data"
        result = _format_value(data)
        assert result == "test data"

    def test_format_value_bytes_with_errors(self):
        """Test _format_value with bytes containing decode errors."""
        data = b"\xff\xfe"
        result = _format_value(data)
        assert isinstance(result, str)

    def test_format_value_regular(self):
        """Test _format_value with regular types."""
        assert _format_value("string") == "string"
        assert _format_value(123) == 123
        assert _format_value(45.67) == 45.67


class TestExecuteQuery:
    """Test execute_query function."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SnowflakeConnection."""
        conn = Mock(spec=SnowflakeConnection)
        return conn

    @pytest.fixture
    def mock_cache(self):
        """Create a mock SchemaCache."""
        cache = Mock(spec=SchemaCache)
        cache.is_empty.return_value = False
        cache.is_expired.return_value = False
        return cache

    @pytest.fixture
    def mock_query_result(self):
        """Create a mock query result."""
        result = Mock()
        result.data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        result.columns = ["id", "name"]
        result.execution_time = 0.123
        result.query_id = "test-query-id-123"
        return result

    @pytest.mark.asyncio
    async def test_execute_query_invalid_sql(self, mock_connection, mock_cache):
        """Test execute_query with invalid SQL."""
        with patch.object(
            QueryValidator,
            "validate",
            return_value=(False, "Invalid query", QueryType.UNKNOWN),
        ):
            result = await execute_query(mock_connection, mock_cache, "DROP TABLE test")

            parsed = parse_text_response(result)
            assert parsed["status"] == "error"
            assert "Invalid query" in parsed["message"]
            assert parsed["query_type"] == "QueryType.UNKNOWN"

    @pytest.mark.asyncio
    async def test_execute_query_empty_cache(
        self, mock_connection, mock_cache, mock_query_result
    ):
        """Test execute_query with empty cache triggers auto-refresh."""
        mock_cache.is_empty.return_value = True
        mock_connection.execute_query.return_value = mock_query_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch(
                "server.tools.query_executor.refresh_catalog"
            ) as mock_refresh_catalog,
        ):
            # Mock successful refresh
            mock_refresh_catalog.return_value = {"status": "success"}

            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            # Verify refresh was called
            mock_refresh_catalog.assert_called_once_with(
                mock_connection, mock_cache, force=True
            )
            # Verify query succeeded after refresh
            assert parse_text_response(result)["status"] == "success"

    @pytest.mark.asyncio
    async def test_execute_query_empty_cache_refresh_fails(
        self, mock_connection, mock_cache
    ):
        """Test execute_query with empty cache when refresh fails."""
        mock_cache.is_empty.return_value = True

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch(
                "server.tools.query_executor.refresh_catalog"
            ) as mock_refresh_catalog,
        ):
            # Mock failed refresh
            mock_refresh_catalog.return_value = {
                "status": "error",
                "message": "Connection failed",
            }

            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            # Verify error is returned when refresh fails
            parsed = parse_text_response(result)
            assert parsed["status"] == "error"
            assert "Failed to refresh catalog" in parsed["message"]
            assert parsed["error"] == "Connection failed"

    @pytest.mark.asyncio
    async def test_execute_query_success(
        self, mock_connection, mock_cache, mock_query_result
    ):
        """Test successful query execution."""
        mock_connection.execute_query.return_value = mock_query_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(
                mock_connection,
                mock_cache,
                "SELECT * FROM test",
                database="db1",
                schema="schema1",
            )

            parsed = parse_text_response(result)
            assert parsed["status"] == "success"
            assert parsed["rows"] == "2"
            assert len(parsed["data_rows"]) == 2
            assert parsed["columns"] == ["id", "name"]
            assert parsed["execution_time"] == "0.123"
            assert parsed["query_id"] == "test-query-id-123"
            # The chatty export fields are gone now.
            assert "export_available" not in parsed
            assert "export_message" not in parsed
            # Row data is positional and matches the header order.
            assert parsed["data_rows"] == [["1", "Alice"], ["2", "Bob"]]

    @pytest.mark.asyncio
    async def test_execute_query_no_results(self, mock_connection, mock_cache):
        """Test query with no results."""
        mock_result = Mock()
        mock_result.data = []
        mock_result.columns = ["id", "name"]
        mock_result.execution_time = 0.05
        mock_connection.execute_query.return_value = mock_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test WHERE 1=0"
            )

            parsed = parse_text_response(result)
            assert parsed["status"] == "success"
            assert parsed["data_rows"] == []
            assert "no results" in parsed["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_query_expired_cache_triggers_refresh(
        self, mock_connection, mock_cache, mock_query_result
    ):
        """Test query with expired cache triggers auto-refresh."""
        mock_cache.is_expired.return_value = True
        mock_cache.is_empty.return_value = False
        mock_connection.execute_query.return_value = mock_query_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch(
                "server.tools.query_executor.refresh_catalog"
            ) as mock_refresh_catalog,
        ):
            # Mock successful refresh
            mock_refresh_catalog.return_value = {"status": "success"}

            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            # Verify refresh was called
            mock_refresh_catalog.assert_called_once_with(
                mock_connection, mock_cache, force=True
            )
            # Verify query succeeded after refresh
            assert parse_text_response(result)["status"] == "success"

    @pytest.mark.asyncio
    async def test_execute_query_auto_spills_large_result(
        self, mock_connection, mock_cache, tmp_path
    ):
        """Large results spill to a temp .tsv file with a preview + path.

        The tool must NOT dump the full wall of text inline and must NOT
        truncate silently: it writes the COMPLETE result to disk and returns a
        short preview plus the file path. The on-disk file is the same TSV
        format as the inline payload.
        """
        # Create a result set whose TSV size exceeds the inline budget.
        cell_len = 200
        num_rows = (INLINE_RESULT_CHAR_BUDGET // cell_len) + 50
        medium_data = [{"id": i, "data": "x" * cell_len} for i in range(num_rows)]
        mock_result = Mock()
        mock_result.data = medium_data
        mock_result.columns = ["id", "data"]
        mock_result.execution_time = 0.8
        mock_result.query_id = "medium-query-id"
        mock_connection.execute_query.return_value = mock_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM medium_table"
            )

            # Header-part fields are still a clean key:value block.
            header_part, _, tsv_part = result.partition("\n---\n")
            parsed: dict = {}
            for line in header_part.splitlines():
                if not line.strip():
                    continue
                key, _, value = line.partition(": ")
                parsed[key] = value

            assert parsed["status"] == "success"
            # True total row count is still reported.
            assert parsed["rows"] == str(num_rows)
            # A spill file path is returned and the file exists.
            assert "results_file" in parsed
            spill_path = parsed["results_file"]
            assert spill_path.endswith(".tsv")
            assert os.path.exists(spill_path)
            # The inline preview is DATA ONLY (no header line): the column names
            # live in the column_index map, so re-emitting them as a TSV header
            # would duplicate every name for no benefit. Every preview line must
            # therefore be a real data row, not the header.
            preview_lines = tsv_part.split("\n") if tsv_part else []
            assert 0 < len(preview_lines) <= query_executor.SPILL_PREVIEW_ROWS
            # First preview line is a data row, not the column-name header.
            assert preview_lines[0].split("\t") != ["id", "data"]
            # The first preview row matches the first data row positionally.
            assert preview_lines[0].split("\t") == ["0", "x" * cell_len]
            # The 1-based column index map is the SOLE column reference inline,
            # so the agent never has to count columns by eye to write awk/cut.
            assert parsed["column_index"] == "1=id 2=data"
            # The on-disk file still carries its own header line + every row.
            with open(spill_path, encoding="utf-8") as f:
                file_lines = f.read().splitlines()
            assert len(file_lines) == num_rows + 1  # header + all rows
            assert file_lines[0].split("\t") == ["id", "data"]

    @pytest.mark.asyncio
    async def test_narrow_inline_result_keeps_positional_tsv(
        self, mock_connection, mock_cache
    ):
        """A few-column inline result stays as compact positional TSV.

        Narrow results are trivially readable, so positional (header + rows) is
        the cheaper format and no labeling is applied.
        """
        cols = ["id", "name", "city"]  # <= MAX_INLINE_COLUMNS
        data = [{"id": 1, "name": "Alice", "city": "NYC"}]
        mock_result = Mock()
        mock_result.data = data
        mock_result.columns = cols
        mock_result.execution_time = 0.1
        mock_result.query_id = "narrow-id"
        mock_connection.execute_query.return_value = mock_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(mock_connection, mock_cache, "SELECT * FROM t")

        header_part, _, tsv_part = result.partition("\n---\n")
        # Inline, positional: header line present, no spill fields.
        assert "results_file" not in header_part
        assert "spilled" not in header_part
        lines = tsv_part.split("\n")
        assert lines[0] == "id\tname\tcity"  # header line of column names
        assert lines[1] == "1\tAlice\tNYC"  # bare positional values

    @pytest.mark.asyncio
    async def test_wide_result_always_spills_even_when_tiny(
        self, mock_connection, mock_cache, tmp_path
    ):
        """A result with > MAX_INLINE_COLUMNS columns ALWAYS spills to a file,
        regardless of how few rows or characters it is.

        There is no inline labeled (name=value) format anymore: if a result is
        wide enough to be miscount-prone read by eye, it does not belong in
        context at all. It spills so the agent reads it by column INDEX via the
        column_index map. This case is deliberately tiny (one short row) to prove
        column count alone -- not size -- triggers the spill.
        """
        n = query_executor.MAX_INLINE_COLUMNS + 1  # one past the inline limit
        cols = [f"c{i}" for i in range(n)]
        row = {c: i for i, c in enumerate(cols)}
        mock_result = Mock()
        mock_result.data = [row]
        mock_result.columns = cols
        mock_result.execution_time = 0.1
        mock_result.query_id = "wide-id"
        mock_connection.execute_query.return_value = mock_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
        ):
            result = await execute_query(mock_connection, mock_cache, "SELECT * FROM t")

        header_part, _, tsv_part = result.partition("\n---\n")
        # It spilled despite being a single tiny row: width alone forced it.
        assert "results_file" in header_part
        assert "spilled" in header_part
        # column_index is the sole column reference, 1-based for awk.
        assert f"column_index: {query_executor.column_index_map(cols)}" in header_part
        # Preview is a single DATA-ONLY positional row (no header, not labeled).
        preview_lines = tsv_part.split("\n")
        assert len(preview_lines) == 1
        assert preview_lines[0] == "\t".join(str(i) for i in range(n))
        assert "c0=" not in tsv_part  # no leftover labeled format

    @pytest.mark.asyncio
    async def test_narrow_tall_result_spills_on_row_count(
        self, mock_connection, mock_cache, tmp_path
    ):
        """A NARROW result spills once it has more than MAX_INLINE_ROWS rows --
        the row gate is independent of width and of total chars.

        Two columns (LLM-friendly shape) with tiny cells, so the char budget is
        nowhere near tripped: this proves ROW COUNT alone forces the spill. A
        tall wall of rows is a reasoning hazard (the model miscounts aggregations
        in context), so it belongs in a file the agent counts with awk/wc.
        """
        num_rows = query_executor.MAX_INLINE_ROWS + 1  # one past the row limit
        data = [{"id": i, "data": "x"} for i in range(num_rows)]  # tiny cells
        mock_result = Mock()
        mock_result.data = data
        mock_result.columns = ["id", "data"]  # narrow: 2 cols
        mock_result.execution_time = 0.2
        mock_result.query_id = "narrow-tall-id"
        mock_connection.execute_query.return_value = mock_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
        ):
            result = await execute_query(mock_connection, mock_cache, "SELECT * FROM t")

        header_part, _, _ = result.partition("\n---\n")
        # Total chars are trivially small; only the row count forced the spill.
        assert "results_file" in header_part
        assert "spilled" in header_part

    @pytest.mark.asyncio
    async def test_narrow_short_giant_cell_spills_on_char_backstop(
        self, mock_connection, mock_cache, tmp_path
    ):
        """The char budget is a BACKSTOP for a narrow + short result that still
        busts the size ceiling via one giant cell (e.g. a big JSON blob).

        Shape gates pass (1 col, 1 row), so only INLINE_RESULT_CHAR_BUDGET can
        catch it. This proves the char gate fires independently of row/column
        count -- exactly the pathological case it exists for.
        """
        giant = "x" * (INLINE_RESULT_CHAR_BUDGET + 100)
        data = [{"blob": giant}]  # 1 row, 1 col, but one huge value
        mock_result = Mock()
        mock_result.data = data
        mock_result.columns = ["blob"]
        mock_result.execution_time = 0.1
        mock_result.query_id = "giant-cell-id"
        mock_connection.execute_query.return_value = mock_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
        ):
            result = await execute_query(mock_connection, mock_cache, "SELECT * FROM t")

        header_part, _, _ = result.partition("\n---\n")
        assert "results_file" in header_part
        assert "spilled" in header_part

    @pytest.mark.asyncio
    async def test_execute_query_formats_values(self, mock_connection, mock_cache):
        """Test query formats special values correctly."""
        mock_result = Mock()
        mock_result.data = [
            {
                "dt": datetime(2024, 1, 1, 10, 30, 0),
                "binary": b"test",
                "null": None,
                "regular": "value",
            }
        ]
        mock_result.columns = ["dt", "binary", "null", "regular"]
        mock_result.execution_time = 0.1
        mock_result.query_id = "format-test-id"
        mock_connection.execute_query.return_value = mock_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            parsed = parse_text_response(result)
            assert parsed["status"] == "success"
            assert parsed["columns"] == ["dt", "binary", "null", "regular"]
            cells = dict(zip(parsed["columns"], parsed["data_rows"][0], strict=False))
            assert "2024-01-01" in cells["dt"]
            assert cells["binary"] == "test"
            assert cells["null"] == "\\N"  # SQL NULL sentinel
            assert cells["regular"] == "value"

    @pytest.mark.asyncio
    async def test_execute_query_value_error(self, mock_connection, mock_cache):
        """Test query execution with ValueError."""
        mock_connection.execute_query.side_effect = ValueError("Invalid parameter")

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            parsed = parse_text_response(result)
            assert parsed["status"] == "error"
            assert "Invalid parameter" in parsed["message"]
            assert parsed["error_type"] == "validation_error"

    @pytest.mark.asyncio
    async def test_execute_query_general_exception(self, mock_connection, mock_cache):
        """Test query execution with general exception."""
        mock_connection.execute_query.side_effect = Exception(
            "Database connection failed"
        )

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch("server.tools.query_executor.logger") as mock_logger,
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            parsed = parse_text_response(result)
            assert parsed["status"] == "error"
            assert "Database connection failed" in parsed["message"]
            assert parsed["error_type"] == "execution_error"
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_no_sql_in_metadata(self, mock_connection, mock_cache):
        """Test that SQL is not included in query metadata (token usage optimization)."""
        long_sql = "SELECT * FROM test WHERE " + " AND ".join(
            [f"col{i} = {i}" for i in range(100)]
        )
        assert len(long_sql) > 500

        mock_result = Mock()
        mock_result.data = [{"id": 1}]
        mock_result.columns = ["id"]
        mock_result.execution_time = 0.1
        mock_result.query_id = "long-sql-id"
        mock_connection.execute_query.return_value = mock_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(mock_connection, mock_cache, long_sql)

            parsed = parse_text_response(result)
            assert parsed["status"] == "success"
            # SQL should not be echoed back (agent already has it in context).
            assert long_sql not in result
            assert "sql:" not in result
            assert parsed["query_id"] == "long-sql-id"


class TestSweepSpillDir:
    """Retention sweep for spilled result files (bounds SPILL_DIR by age+count)."""

    def _make_spill(
        self, dir_path, name: str, age_seconds: float = 0.0, size_bytes: int = 0
    ) -> str:
        """Create a query_*.tsv file with a given mtime-age and (optional) size."""
        path = os.path.join(str(dir_path), name)
        with open(path, "w", encoding="utf-8") as f:
            if size_bytes:
                f.write("x" * size_bytes)
            else:
                f.write("ID\tV\n1\tx\n")
        if age_seconds:
            past = time.time() - age_seconds
            os.utime(path, (past, past))
        return path

    def test_age_pass_deletes_only_expired(self, tmp_path):
        """Files older than the TTL are removed; fresh ones are kept."""
        fresh = self._make_spill(tmp_path, "query_fresh.tsv", age_seconds=10)
        stale = self._make_spill(tmp_path, "query_stale.tsv", age_seconds=9999)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 100),
            patch.object(query_executor, "SPILL_MAX_FILES", 1000),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 1
        assert os.path.exists(fresh)
        assert not os.path.exists(stale)

    def test_count_pass_is_fifo_oldest_first(self, tmp_path):
        """When over the count cap, the OLDEST surviving files go first."""
        # 5 fresh files (none expired), ages 5..1s; cap of 2 should keep the 2
        # newest (ages 1s, 2s) and delete the 3 oldest (5s, 4s, 3s).
        paths = {
            age: self._make_spill(tmp_path, f"query_{age}.tsv", age_seconds=age)
            for age in (1, 2, 3, 4, 5)
        }

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 2),
            # This test is about FIFO ordering, not the grace window: disable the
            # min-age guard so the second-old files are all eligible to evict.
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 0),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 3
        assert os.path.exists(paths[1])  # newest kept
        assert os.path.exists(paths[2])
        assert not os.path.exists(paths[3])  # oldest evicted
        assert not os.path.exists(paths[4])
        assert not os.path.exists(paths[5])

    def test_age_then_count_combined(self, tmp_path):
        """Age pass runs first, then FIFO count on the survivors."""
        expired = self._make_spill(tmp_path, "query_old.tsv", age_seconds=9999)
        # 3 fresh; cap 2 -> after age removes `expired`, drop the oldest fresh.
        f3 = self._make_spill(tmp_path, "query_f3.tsv", age_seconds=3)
        f2 = self._make_spill(tmp_path, "query_f2.tsv", age_seconds=2)
        f1 = self._make_spill(tmp_path, "query_f1.tsv", age_seconds=1)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 100),
            patch.object(query_executor, "SPILL_MAX_FILES", 2),
            # Ordering test, not a grace test: let the second-old files evict.
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 0),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 2  # expired + oldest-fresh
        assert not os.path.exists(expired)
        assert not os.path.exists(f3)  # oldest fresh, over the cap
        assert os.path.exists(f2)
        assert os.path.exists(f1)

    def test_sweep_ignores_non_spill_files(self, tmp_path):
        """The sweep only touches query_*.tsv -- never unrelated files."""
        keep = os.path.join(str(tmp_path), "important.txt")
        with open(keep, "w", encoding="utf-8") as f:
            f.write("do not delete")
        os.utime(keep, (time.time() - 9999, time.time() - 9999))  # very old

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 1),
            patch.object(query_executor, "SPILL_MAX_FILES", 1),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 0
        assert os.path.exists(keep)  # untouched despite being old

    def test_sweep_missing_dir_is_noop(self, tmp_path):
        """A non-existent SPILL_DIR is handled gracefully (no error, 0 deleted)."""
        missing = os.path.join(str(tmp_path), "does_not_exist")
        with patch.object(query_executor, "SPILL_DIR", missing):
            assert query_executor.sweep_spill_dir() == 0

    @pytest.mark.asyncio
    async def test_spill_sweeps_before_writing_and_keeps_new_file(self, tmp_path):
        """A real spill prunes the dir first, and the new file survives the sweep.

        With the cap at 1 and a pre-existing stale file, spilling must evict the
        old file but always keep the just-written result.
        """
        stale = self._make_spill(tmp_path, "query_stale.tsv", age_seconds=9999)
        data = [{"id": i, "data": "x"} for i in range(query_executor.MAX_INLINE_ROWS + 1)]
        mock_result = Mock()
        mock_result.data = data
        mock_result.columns = ["id", "data"]
        mock_result.execution_time = 0.1
        mock_result.query_id = "sweep-on-spill-id"
        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.execute_query.return_value = mock_result
        mock_cache = Mock(spec=SchemaCache)
        mock_cache.is_expired.return_value = False
        mock_cache.is_empty.return_value = False

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 100),
            patch.object(query_executor, "SPILL_MAX_FILES", 1),
        ):
            result = await execute_query(mock_connection, mock_cache, "SELECT * FROM t")

        header_part, _, _ = result.partition("\n---\n")
        new_path = next(
            line.split(": ", 1)[1]
            for line in header_part.splitlines()
            if line.startswith("results_file: ")
        )
        assert not os.path.exists(stale)  # stale evicted by the pre-write sweep
        assert os.path.exists(new_path)  # the new spill file survives

    def test_byte_pass_is_fifo_until_under_budget(self, tmp_path):
        """Over the byte budget, evict OLDEST first until combined size fits."""
        # 4 files of 100 bytes each = 400 total; budget 250 -> must drop the 2
        # oldest (ages 4s, 3s) to bring 4 survivors -> 2 (200 bytes, under 250).
        old2 = self._make_spill(tmp_path, "query_o4.tsv", age_seconds=4, size_bytes=100)
        old1 = self._make_spill(tmp_path, "query_o3.tsv", age_seconds=3, size_bytes=100)
        new2 = self._make_spill(tmp_path, "query_n2.tsv", age_seconds=2, size_bytes=100)
        new1 = self._make_spill(tmp_path, "query_n1.tsv", age_seconds=1, size_bytes=100)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 1000),  # count inert
            patch.object(query_executor, "SPILL_MAX_TOTAL_BYTES", 250),
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 0),  # grace inert
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 2
        assert not os.path.exists(old2)  # oldest evicted first
        assert not os.path.exists(old1)
        assert os.path.exists(new2)  # newest kept once under budget
        assert os.path.exists(new1)

    def test_byte_pass_respects_min_age_grace(self, tmp_path):
        """A fresh file is never byte-evicted, even when over budget."""
        # Both files are 100 bytes (200 > 150 budget) but younger than the 60s
        # grace, so neither may be evicted -- we tolerate the overage rather than
        # reclaim a just-returned result.
        f1 = self._make_spill(tmp_path, "query_a.tsv", age_seconds=2, size_bytes=100)
        f2 = self._make_spill(tmp_path, "query_b.tsv", age_seconds=1, size_bytes=100)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 1000),
            patch.object(query_executor, "SPILL_MAX_TOTAL_BYTES", 150),
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 60),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 0  # both protected by grace despite being over budget
        assert os.path.exists(f1)
        assert os.path.exists(f2)

    def test_count_pass_respects_min_age_grace(self, tmp_path):
        """A fresh file is never count-evicted, even over the count cap."""
        # 3 fresh files, cap 1; all younger than the grace -> none evictable.
        f3 = self._make_spill(tmp_path, "query_c.tsv", age_seconds=3)
        f2 = self._make_spill(tmp_path, "query_b.tsv", age_seconds=2)
        f1 = self._make_spill(tmp_path, "query_a.tsv", age_seconds=1)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 1),
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 60),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 0  # over cap, but all within grace
        assert os.path.exists(f1)
        assert os.path.exists(f2)
        assert os.path.exists(f3)

    def test_grace_protects_recent_but_evicts_aged_over_budget(self, tmp_path):
        """Byte pass evicts only the grace-aged file, keeps the fresh one."""
        # aged (90s, evictable) + fresh (5s, protected), each 100 bytes, budget
        # 150. Only the aged file may go -> survivors = fresh (100 <= 150).
        aged = self._make_spill(tmp_path, "query_aged.tsv", age_seconds=90, size_bytes=100)
        fresh = self._make_spill(tmp_path, "query_fresh.tsv", age_seconds=5, size_bytes=100)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 1000),
            patch.object(query_executor, "SPILL_MAX_TOTAL_BYTES", 150),
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 60),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 1
        assert not os.path.exists(aged)  # past grace, over budget -> evicted
        assert os.path.exists(fresh)  # within grace -> protected

    def test_pass_order_age_then_count_then_bytes(self, tmp_path):
        """All three passes compose: age first, then count, then bytes."""
        # expired (TTL) + 3 aged files of 100 bytes. cap 2 drops 1 (oldest aged),
        # then bytes (budget 150) drops 1 more -> 1 survivor under budget.
        expired = self._make_spill(
            tmp_path, "query_exp.tsv", age_seconds=99999, size_bytes=100
        )
        a3 = self._make_spill(tmp_path, "query_a3.tsv", age_seconds=300, size_bytes=100)
        a2 = self._make_spill(tmp_path, "query_a2.tsv", age_seconds=200, size_bytes=100)
        a1 = self._make_spill(tmp_path, "query_a1.tsv", age_seconds=100, size_bytes=100)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(query_executor, "SPILL_FILE_TTL_SECONDS", 10_000),
            patch.object(query_executor, "SPILL_MAX_FILES", 2),
            patch.object(query_executor, "SPILL_MAX_TOTAL_BYTES", 150),
            patch.object(query_executor, "SPILL_MIN_AGE_SECONDS", 60),
        ):
            deleted = query_executor.sweep_spill_dir()

        assert deleted == 3  # expired(age) + a3(count) + a2(bytes)
        assert not os.path.exists(expired)
        assert not os.path.exists(a3)  # oldest survivor, dropped by count cap
        assert not os.path.exists(a2)  # next oldest, dropped by byte budget
        assert os.path.exists(a1)  # newest, survives (100 <= 150)
