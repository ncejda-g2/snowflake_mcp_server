"""Tests for server/tools/query_executor.py module."""

import os
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from server.constants import MCP_CHAR_WARNING_THRESHOLD
from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection
from server.tools import query_executor
from server.tools.query_executor import (
    _format_value,
    execute_query,
    get_query_history,
    validate_query_without_execution,
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
        # Create a result set whose TSV size exceeds the spill threshold.
        cell_len = 200
        num_rows = (MCP_CHAR_WARNING_THRESHOLD // cell_len) + 50
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


class TestValidateQueryWithoutExecution:
    """Test validate_query_without_execution function."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SnowflakeConnection."""
        return Mock(spec=SnowflakeConnection)

    @pytest.fixture
    def mock_cache(self):
        """Create a mock SchemaCache."""
        cache = Mock(spec=SchemaCache)
        cache.is_empty.return_value = False
        cache.is_expired.return_value = False
        return cache

    @pytest.mark.asyncio
    async def test_validate_read_only_query(self, mock_connection, mock_cache):
        """Test validation of read-only query."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            assert result["status"] == "success"
            assert result["validation"]["is_read_only"] is True
            assert result["validation"]["execution_allowed"] is True
            assert result["validation"]["query_type"] == "QueryType.SELECT"
            assert "can be executed" in result["note"]

    @pytest.mark.asyncio
    async def test_validate_write_query(self, mock_connection, mock_cache):
        """Test validation of write query."""
        with patch.object(
            QueryValidator,
            "validate",
            return_value=(False, "Write operation", QueryType.WRITE),
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "INSERT INTO test VALUES (1)"
            )

            assert result["status"] == "success"
            assert result["validation"]["is_read_only"] is False
            assert result["validation"]["execution_allowed"] is False
            assert result["validation"]["query_type"] == "QueryType.WRITE"
            assert "CANNOT be executed" in result["note"]

    @pytest.mark.asyncio
    async def test_validate_with_empty_cache(self, mock_connection, mock_cache):
        """Test validation with empty cache."""
        mock_cache.is_empty.return_value = True

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            assert result["status"] == "success"
            assert result["cache_status"]["is_populated"] is False
            assert "warning" in result["cache_status"]
            assert "refresh_catalog" in result["cache_status"]["warning"]

    @pytest.mark.asyncio
    async def test_validate_with_expired_cache(self, mock_connection, mock_cache):
        """Test validation with expired cache."""
        mock_cache.is_expired.return_value = True

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM test"
            )

            assert result["status"] == "success"
            assert result["cache_status"]["is_expired"] is True

    @pytest.mark.asyncio
    async def test_validate_with_database_schema_context(
        self, mock_connection, mock_cache
    ):
        """Test validation with database and schema context."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection,
                mock_cache,
                "SELECT * FROM test",
                database="db1",
                schema="schema1",
            )

            assert result["status"] == "success"
            assert "-- Context: Database: db1, Schema: schema1" in result["query"]
            assert result["metadata"]["database_context"] == "db1"
            assert result["metadata"]["schema_context"] == "schema1"

    @pytest.mark.asyncio
    async def test_validate_extracts_table_references(
        self, mock_connection, mock_cache
    ):
        """Test validation extracts table references."""
        sql = """
        SELECT a.*, b.name
        FROM table1 a
        JOIN table2 b ON a.id = b.id
        LEFT JOIN table3 c ON b.id = c.id
        """

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, sql
            )

            assert result["status"] == "success"
            assert len(result["metadata"]["table_references"]) == 3
            assert result["metadata"]["estimated_complexity"] == "moderate"

    @pytest.mark.asyncio
    async def test_validate_complexity_levels(self, mock_connection, mock_cache):
        """Test query complexity estimation."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            # Simple query
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM table1"
            )
            assert result["metadata"]["estimated_complexity"] == "simple"

            # Complex query
            sql_complex = "SELECT * FROM t1 JOIN t2 ON t1.id=t2.id JOIN t3 ON t2.id=t3.id JOIN t4 ON t3.id=t4.id"
            result = await validate_query_without_execution(
                mock_connection, mock_cache, sql_complex
            )
            assert result["metadata"]["estimated_complexity"] == "complex"

    @pytest.mark.asyncio
    async def test_validate_strips_semicolon(self, mock_connection, mock_cache):
        """Test validation strips trailing semicolon."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM test;"
            )

            assert result["status"] == "success"
            assert not result["query"].strip().endswith(";")

    @pytest.mark.asyncio
    async def test_validate_provides_hints(self, mock_connection, mock_cache):
        """Test validation provides helpful hints."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            # Should suggest fully qualified names
            result = await validate_query_without_execution(
                mock_connection,
                mock_cache,
                "SELECT * FROM test",
                database="db1",
                schema="schema1",
            )
            assert "hints" in result
            assert any("fully qualified" in hint for hint in result["hints"])

            # Should suggest LIMIT clause
            assert any("LIMIT" in hint for hint in result["hints"])

    @pytest.mark.asyncio
    async def test_validate_no_hints_with_limit(self, mock_connection, mock_cache):
        """Test validation doesn't suggest LIMIT when already present."""
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await validate_query_without_execution(
                mock_connection, mock_cache, "SELECT * FROM test LIMIT 100"
            )

            # Should not suggest LIMIT if already present
            if "hints" in result:
                assert not any("LIMIT" in hint for hint in result["hints"])


class TestGetQueryHistory:
    """Test get_query_history function."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SnowflakeConnection."""
        return Mock(spec=SnowflakeConnection)

    @pytest.mark.asyncio
    async def test_get_query_history_success(self, mock_connection):
        """Test getting query history successfully."""
        mock_history = [
            {
                "timestamp": 1704110400.0,  # 2024-01-01 12:00:00
                "sql": "SELECT * FROM test",
                "status": "success",
                "execution_time": 0.123,
                "row_count": 10,
                "database": "db1",
                "schema": "schema1",
            }
        ]
        mock_connection.get_query_history.return_value = mock_history

        result = await get_query_history(
            mock_connection, limit=10, only_successful=True
        )

        assert result["status"] == "success"
        assert len(result["history"]) == 1
        assert result["count"] == 1
        assert result["limit"] == 10
        assert result["filter"] == "successful_only"

        entry = result["history"][0]
        assert "2024-01-01" in entry["timestamp"]
        assert entry["sql"] == "SELECT * FROM test"
        assert entry["status"] == "success"

    @pytest.mark.asyncio
    async def test_get_query_history_empty(self, mock_connection):
        """Test getting query history when empty."""
        mock_connection.get_query_history.return_value = []

        result = await get_query_history(mock_connection)

        assert result["status"] == "success"
        assert result["message"] == "No query history available"
        assert result["history"] == []

    @pytest.mark.asyncio
    async def test_get_query_history_with_errors(self, mock_connection):
        """Test getting query history including errors."""
        mock_history = [
            {
                "timestamp": 1704110400.0,
                "sql": "SELECT * FROM test",
                "status": "error",
                "error": "Table not found",
            }
        ]
        mock_connection.get_query_history.return_value = mock_history

        result = await get_query_history(
            mock_connection, limit=20, only_successful=False
        )

        assert result["status"] == "success"
        assert result["filter"] == "all"
        assert result["history"][0]["error"] == "Table not found"

    @pytest.mark.asyncio
    async def test_get_query_history_exception(self, mock_connection):
        """Test getting query history with exception."""
        mock_connection.get_query_history.side_effect = Exception("Database error")

        with patch("server.tools.query_executor.logger") as mock_logger:
            result = await get_query_history(mock_connection)

            assert result["status"] == "error"
            assert "Database error" in result["message"]
            mock_logger.error.assert_called_once()
