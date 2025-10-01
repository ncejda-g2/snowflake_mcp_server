"""Fixed tests for server/snowflake_connection.py module."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from server.config import Config
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection


class TestQueryValidator:
    """Test QueryValidator class."""

    def test_validate_select(self):
        """Test validating SELECT queries."""
        queries = [
            "SELECT * FROM table",
            "SELECT col1, col2 FROM schema.table",
            "  SELECT  *  FROM  table  ",
            "select * from table",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True
            assert qtype == QueryType.SELECT

    def test_validate_show(self):
        """Test validating SHOW queries."""
        queries = [
            "SHOW TABLES",
            "SHOW DATABASES",
            "SHOW SCHEMAS IN DATABASE test",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True
            assert qtype == QueryType.SHOW

    def test_validate_describe(self):
        """Test validating DESCRIBE queries."""
        queries = [
            "DESCRIBE TABLE test_table",
            "DESC test_table",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True
            assert qtype == QueryType.DESCRIBE

    def test_validate_with_cte(self):
        """Test validating WITH (CTE) queries."""
        query = """
        WITH cte AS (
            SELECT * FROM table1
        )
        SELECT * FROM cte
        """
        is_valid, error, qtype = QueryValidator.validate(query)
        assert is_valid is True
        assert qtype == QueryType.WITH

    def test_validate_rejects_write_operations(self):
        """Test that validator rejects write operations."""
        write_queries = [
            "INSERT INTO table VALUES (1, 2)",
            "UPDATE table SET col = 1",
            "DELETE FROM table",
            "CREATE TABLE test (id INT)",
            "DROP TABLE test",
            "ALTER TABLE test ADD COLUMN col2 VARCHAR",
            "TRUNCATE TABLE test",
        ]

        for query in write_queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is False
            assert "write operations" in error.lower()
            assert "only read operations are allowed" in error.lower()

    def test_validate_rejects_semicolon(self):
        """Test that validator rejects queries with semicolons."""
        query = "SELECT * FROM table1; DELETE FROM table2"
        is_valid, error, qtype = QueryValidator.validate(query)
        assert is_valid is False
        assert "Multiple statements" in error

    def test_validate_allows_write_keywords_in_string_literals(self):
        """Test that validator allows write keywords inside string literals (regression test)."""
        # This is the exact query from the bug report
        query = """
        SELECT
            a.ID as answer_id,
            a.SURVEY_RESPONSE_ID,
            a.PRODUCT_ID,
            a.QUESTION_ID,
            a.COMMENT as answer_text
        FROM GDC.STAGING.ADMIN__ANSWERS a
        WHERE a.PRODUCT_ID = 40484
            AND (
                LOWER(a.COMMENT) LIKE '%reassign%'
                OR LOWER(a.COMMENT) LIKE '%pdf%'
                OR LOWER(a.COMMENT) LIKE '%photo%'
                OR LOWER(a.COMMENT) LIKE '%repeat%task%'
                OR LOWER(a.COMMENT) LIKE '%time limit%'
                OR LOWER(a.COMMENT) LIKE '%questions%set%'
                OR LOWER(a.COMMENT) LIKE '%locked%format%'
                OR LOWER(a.COMMENT) LIKE '%redo%'
                OR LOWER(a.COMMENT) LIKE '%lapsed%audit%'
                OR LOWER(a.COMMENT) LIKE '%missed%audit%'
            )
        LIMIT 30
        """
        is_valid, error, qtype = QueryValidator.validate(query)
        assert is_valid is True, f"Query should be valid but got error: {error}"
        assert qtype == QueryType.SELECT

    def test_validate_allows_write_keywords_in_column_names(self):
        """Test that validator allows write keywords as column names."""
        queries = [
            "SELECT comment, insert_date, update_time FROM table",
            "SELECT a.COMMENT, a.DELETE_FLAG FROM table a",
            "SELECT * FROM table WHERE comment LIKE '%test%'",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True, (
                f"Query '{query}' should be valid but got error: {error}"
            )
            assert qtype == QueryType.SELECT

    def test_validate_rejects_actual_set_command(self):
        """Test that validator correctly rejects actual SET commands."""
        queries = [
            "SET SQL_MODE = 'TRADITIONAL'",
            "SET @var = 1",
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is False, f"Query '{query}' should be rejected"
            assert "write operations" in error.lower()
            assert "only read operations are allowed" in error.lower()

    def test_validate_provides_detailed_error_for_write_operations(self):
        """Test that validator provides detailed error messages with position info."""
        query = "SELECT * FROM table; INSERT INTO table VALUES (1)"
        is_valid, error, qtype = QueryValidator.validate(query)
        assert is_valid is False
        assert "Multiple statements" in error

        # Test write operation detection with position
        query2 = "UPDATE table SET col = 1"
        is_valid, error, qtype = QueryValidator.validate(query2)
        assert is_valid is False
        assert "UPDATE" in error
        assert "line 1, column 1" in error
        assert "write operations" in error.lower()
        assert "only read operations are allowed" in error.lower()

    def test_validate_complex_queries_with_string_literals(self):
        """Test validator with complex queries containing various string patterns."""
        queries = [
            # Query with 'INSERT' in string
            "SELECT * FROM logs WHERE action = 'INSERT'",
            # Query with 'DELETE' in string
            "SELECT user_id FROM audit WHERE message LIKE '%DELETE%operation%'",
            # Query with multiple write keywords in strings
            "SELECT * FROM events WHERE type IN ('CREATE', 'UPDATE', 'DELETE')",
            # Query with 'SET' in LIKE pattern
            "SELECT * FROM data WHERE description LIKE '%data%set%'",
            # Query with 'DROP' in column comparison
            "SELECT * FROM items WHERE status != 'DROP'",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True, (
                f"Query '{query}' should be valid but got error: {error}"
            )
            assert qtype == QueryType.SELECT

    def test_validate_cte_with_write_operation_rejected(self):
        """Test that CTEs with write operations are rejected."""
        query = """
        WITH cte AS (
            SELECT * FROM table1
        )
        INSERT INTO table2 SELECT * FROM cte
        """
        is_valid, error, qtype = QueryValidator.validate(query)
        assert is_valid is False
        assert "INSERT" in error

    def test_validate_empty_and_invalid_queries(self):
        """Test validator with empty and invalid queries."""
        queries = [
            "",
            "   ",
            "\n\n",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is False
            assert "Empty" in error

    def test_validate_case_insensitive(self):
        """Test that validation works regardless of case."""
        queries = [
            "select * from table",
            "SELECT * FROM TABLE",
            "SeLeCt * FrOm TaBlE",
        ]

        for query in queries:
            is_valid, error, qtype = QueryValidator.validate(query)
            assert is_valid is True
            assert qtype == QueryType.SELECT


class TestSnowflakeConnection:
    """Test SnowflakeConnection class."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config object."""
        config = Mock(spec=Config)
        config.account = "test123.us-east-1"
        config.username = "testuser"
        config.warehouse = "TEST_WH"
        config.role = "TEST_ROLE"
        config.debug = False
        return config

    @pytest.fixture
    def connection(self, mock_config):
        """Create a SnowflakeConnection instance."""
        with patch("server.snowflake_connection.snowflake.connector"):
            return SnowflakeConnection(mock_config)

    def test_initialization(self, mock_config):
        """Test SnowflakeConnection initialization."""
        with patch("server.snowflake_connection.snowflake.connector"):
            conn = SnowflakeConnection(mock_config)

            assert conn.config == mock_config
            assert conn.connection is None
            assert conn.query_log == []

    @patch("server.snowflake_connection.snowflake.connector.connect")
    def test_connect_success(self, mock_connect, connection):
        """Test successful connection to Snowflake."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connection.connect()

        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]
        assert call_args["account"] == "test123.us-east-1"
        assert call_args["user"] == "testuser"
        assert call_args["warehouse"] == "TEST_WH"
        assert call_args["role"] == "TEST_ROLE"
        assert call_args["authenticator"] == "externalbrowser"

        assert connection.connection == mock_conn

    def test_disconnect(self, connection):
        """Test disconnecting from Snowflake."""
        mock_conn = MagicMock()
        connection.connection = mock_conn

        connection.disconnect()

        mock_conn.close.assert_called_once()
        assert connection.connection is None

    def test_disconnect_no_connection(self, connection):
        """Test disconnecting when not connected."""
        connection.disconnect()  # Should not raise

    def test_execute_query_success(self, connection):
        """Test successful query execution."""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "data")]
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]
        mock_cursor.sfqid = "query-123"
        # The actual code calls connection.cursor(DictCursor) directly
        mock_conn.cursor.return_value = mock_cursor
        connection.connection = mock_conn

        query = "SELECT * FROM test_table"
        result = connection.execute_query(query)

        assert result.data is not None
        assert result.row_count == 2
        assert len(result.data) == 2
        assert result.columns[0]["name"] == "id"
        assert result.columns[1]["name"] == "name"

    def test_execute_query_validation_failure(self, connection):
        """Test query execution with validation failure."""
        connection.connection = MagicMock()

        query = "DELETE FROM test_table"
        with pytest.raises(ValueError, match="write operations"):
            connection.execute_query(query)

    def test_execute_query_not_connected(self, connection):
        """Test query execution when not connected."""
        query = "SELECT 1"
        with pytest.raises(RuntimeError, match="Not connected"):
            connection.execute_query(query)

    def test_get_query_history(self, connection):
        """Test getting query history."""
        # Add some history to query_log
        connection.query_log = [
            {
                "query": "SELECT 1",
                "status": "success",
                "timestamp": 1704067200,
            },  # Unix timestamp
            {"query": "SELECT 2", "status": "success", "timestamp": 1704153600},
            {"query": "INVALID", "status": "failed", "timestamp": 1704240000},
        ]

        # Get all history
        history = connection.get_query_history()
        assert len(history) == 3

        # Get only successful queries
        history = connection.get_query_history(only_successful=True)
        assert len(history) == 2

        # Get limited history
        history = connection.get_query_history(limit=1)
        assert len(history) == 1

    def test_test_connection(self, connection):
        """Test the test_connection method."""
        from server.snowflake_connection import QueryResult

        # Not connected - test_connection calls execute_query which needs connection
        assert connection.test_connection() is False

        # Mock successful connection and query result
        with patch.object(connection, "execute_query") as mock_execute:
            # Set up successful result
            mock_result = QueryResult(
                data=[{"TEST": 1}],
                columns=[{"name": "TEST"}],
                row_count=1,
                execution_time=0.1,
            )
            mock_execute.return_value = mock_result

            assert connection.test_connection() is True

        # Test connection failure
        with patch.object(connection, "execute_query") as mock_execute:
            mock_execute.side_effect = Exception("Connection error")
            assert connection.test_connection() is False

    def test_context_manager(self, mock_config):
        """Test using SnowflakeConnection as a context manager."""
        with patch(
            "server.snowflake_connection.snowflake.connector.connect"
        ) as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            with SnowflakeConnection(mock_config) as conn:
                assert conn.connection is not None

            mock_conn.close.assert_called_once()
