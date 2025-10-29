"""Tests for server/tools/query_executor.py module."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from server.constants import MAX_CACHE_SIZE_BYTES, MCP_CHAR_WARNING_THRESHOLD
from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection
from server.tools import query_executor
from server.tools.query_executor import (
    _estimate_size,
    _format_value,
    execute_query,
    get_last_query_cache,
    get_query_history,
    validate_query_without_execution,
)


class TestHelperFunctions:
    """Test helper functions."""

    def test_estimate_size(self):
        """Test _estimate_size function."""
        obj = {"key": "value", "number": 123}
        size = _estimate_size(obj)
        assert size > 0
        assert isinstance(size, int)

    def test_estimate_size_complex_object(self):
        """Test _estimate_size with complex nested object."""
        obj = {
            "list": [1, 2, 3],
            "nested": {"inner": "value"},
            "data": "x" * 1000,
        }
        size = _estimate_size(obj)
        assert size > 1000

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

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear the query cache before each test."""
        query_executor.last_query_cache = None
        yield
        query_executor.last_query_cache = None

    @pytest.mark.asyncio
    async def test_execute_query_invalid_sql(self, mock_connection, mock_cache):
        """Test execute_query with invalid SQL."""
        with patch.object(
            QueryValidator,
            "validate",
            return_value=(False, "Invalid query", QueryType.UNKNOWN),
        ):
            result = await execute_query(mock_connection, mock_cache, "DROP TABLE test")

            assert result["status"] == "error"
            assert "Invalid query" in result["message"]
            assert result["query_type"] == "QueryType.UNKNOWN"

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
            assert result["status"] == "success"

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
            assert result["status"] == "error"
            assert "Failed to refresh catalog" in result["message"]
            assert result["error"] == "Connection failed"

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

            assert result["status"] == "success"
            assert len(result["data"]) == 2
            assert result["row_count"] == 2
            assert result["columns"] == ["id", "name"]
            assert result["execution_time"] == 0.123
            assert result["csv_export"]["available"] is True
            assert result["query_metadata"]["query_id"] == "test-query-id-123"

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

            assert result["status"] == "success"
            assert result["data"] == []
            assert "no results" in result["message"].lower()
            assert get_last_query_cache() is None

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
            assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_execute_query_large_cache_exceeded(
        self, mock_connection, mock_cache
    ):
        """Test query with results exceeding cache limit."""
        # Create large result set
        large_data = [{"id": i, "data": "x" * 10000} for i in range(1000)]
        mock_result = Mock()
        mock_result.data = large_data
        mock_result.columns = ["id", "data"]
        mock_result.execution_time = 1.5
        mock_result.query_id = "large-query-id"
        mock_connection.execute_query.return_value = mock_result

        with (
            patch.object(
                QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
            ),
            patch("server.tools.query_executor._estimate_size") as mock_estimate,
        ):
            # Mock estimate_size to return a value larger than MAX_CACHE_SIZE_BYTES
            mock_estimate.return_value = MAX_CACHE_SIZE_BYTES + 1000000

            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM large_table"
            )

            assert result["status"] == "success"
            assert result["csv_export"]["available"] is False
            assert "too large for CSV export" in result["csv_export"]["message"]

            # Check cache only contains metadata
            cache = get_last_query_cache()
            assert cache is not None
            assert cache["status"] == "size_exceeded"
            assert "all_results" not in cache

    @pytest.mark.asyncio
    async def test_execute_query_token_warning(self, mock_connection, mock_cache):
        """Test query with results approaching token limits."""
        # Create medium-large result set that triggers token warning
        data_size = MCP_CHAR_WARNING_THRESHOLD + 1000
        medium_data = [{"id": i, "data": "x" * 100} for i in range(data_size // 120)]
        mock_result = Mock()
        mock_result.data = medium_data
        mock_result.columns = ["id", "data"]
        mock_result.execution_time = 0.8
        mock_result.query_id = "medium-query-id"
        mock_connection.execute_query.return_value = mock_result

        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query(
                mock_connection, mock_cache, "SELECT * FROM medium_table"
            )

            assert result["status"] == "success"
            assert "token_limit_warning" in result
            assert (
                "approaching typical MCP token limits" in result["token_limit_warning"]
            )

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

            assert result["status"] == "success"
            row = result["data"][0]
            assert "2024-01-01" in row["dt"]
            assert row["binary"] == "test"
            assert row["null"] is None
            assert row["regular"] == "value"

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

            assert result["status"] == "error"
            assert "Invalid parameter" in result["message"]
            assert result["error_type"] == "validation_error"

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

            assert result["status"] == "error"
            assert "Database connection failed" in result["message"]
            assert result["error_type"] == "execution_error"
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

            assert result["status"] == "success"
            # SQL should not be in metadata (agent already has it in context)
            assert "sql" not in result["query_metadata"]
            assert result["query_metadata"]["query_id"] == "long-sql-id"


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


class TestGetLastQueryCache:
    """Test get_last_query_cache function."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear the query cache before each test."""
        query_executor.last_query_cache = None
        yield
        query_executor.last_query_cache = None

    def test_get_last_query_cache_empty(self):
        """Test getting cache when it's empty."""
        assert get_last_query_cache() is None

    def test_get_last_query_cache_with_data(self):
        """Test getting cache with data."""
        test_cache = {"sql": "SELECT * FROM test", "data": [{"id": 1}]}
        query_executor.last_query_cache = test_cache

        result = get_last_query_cache()
        assert result == test_cache
        assert result["sql"] == "SELECT * FROM test"


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
