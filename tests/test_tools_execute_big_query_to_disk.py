"""Tests for execute_big_query_to_disk tool."""

import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from server.tools.execute_big_query_to_disk import (
    _cleanup_partial_files,
    _write_sql_file,
    execute_big_query_to_disk,
)


class TestWriteSqlFile:
    """Test the _write_sql_file function."""

    def test_write_sql_file_success(self, tmp_path):
        """Test successful SQL file write."""
        csv_path = str(tmp_path / "results.csv")
        sql = "SELECT * FROM table WHERE id = 1"

        result = _write_sql_file(sql, csv_path)

        expected_sql_path = str(tmp_path / "results.sql")
        assert result["status"] == "success"
        assert result["sql_file_path"] == expected_sql_path
        assert os.path.exists(expected_sql_path)

        with open(expected_sql_path, "r") as f:
            content = f.read()
            assert "SELECT" in content.upper()
            assert content.strip().endswith(";")

    def test_write_sql_file_non_csv_extension(self, tmp_path):
        """Test SQL file creation with non-CSV extension."""
        txt_path = str(tmp_path / "results.txt")
        sql = "SELECT 1"

        result = _write_sql_file(sql, txt_path)

        expected_sql_path = txt_path + ".sql"
        assert result["status"] == "success"
        assert result["sql_file_path"] == expected_sql_path
        assert os.path.exists(expected_sql_path)

    @patch("builtins.open", side_effect=PermissionError("No write access"))
    def test_write_sql_file_permission_error(self, mock_open):
        """Test handling of permission errors."""
        result = _write_sql_file("SELECT 1", "/tmp/test.csv")

        assert result["status"] == "warning"
        assert "Failed to export SQL file" in result["message"]

    def test_write_sql_file_formats_complex_query(self, tmp_path):
        """Test that complex queries are properly formatted."""
        csv_path = str(tmp_path / "results.csv")
        sql = "select col1,col2 from table where id=1 and status='active' order by col1"

        result = _write_sql_file(sql, csv_path)

        sql_path = result["sql_file_path"]
        with open(sql_path, "r") as f:
            content = f.read()
            # Check formatting was applied
            assert "SELECT" in content  # Uppercased
            assert "\n" in content  # Has line breaks
            assert content.strip().endswith(";")


class TestCleanupPartialFiles:
    """Test the _cleanup_partial_files function."""

    def test_cleanup_existing_files(self, tmp_path):
        """Test cleanup of existing files."""
        csv_path = tmp_path / "test.csv"
        sql_path = tmp_path / "test.sql"

        # Create files
        csv_path.write_text("partial data")
        sql_path.write_text("SELECT 1")

        _cleanup_partial_files(str(csv_path), str(sql_path))

        assert not csv_path.exists()
        assert not sql_path.exists()

    def test_cleanup_non_existing_files(self, tmp_path):
        """Test cleanup with non-existing files (should not error)."""
        csv_path = tmp_path / "non_existing.csv"
        sql_path = tmp_path / "non_existing.sql"

        # Should not raise any errors
        _cleanup_partial_files(str(csv_path), str(sql_path))

    def test_cleanup_csv_only(self, tmp_path):
        """Test cleanup with only CSV file."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("data")

        _cleanup_partial_files(str(csv_path), None)

        assert not csv_path.exists()

    @patch("os.remove", side_effect=PermissionError("No delete access"))
    @patch("server.tools.execute_big_query_to_disk.logger")
    def test_cleanup_permission_error(self, mock_logger, mock_remove, tmp_path):
        """Test handling of permission errors during cleanup."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("data")

        _cleanup_partial_files(str(csv_path))

        # Should log warning but not raise
        assert mock_logger.warning.called


@pytest.mark.asyncio
class TestExecuteBigQueryToDisk:
    """Test the main execute_big_query_to_disk function."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Snowflake connection."""
        conn = MagicMock()
        conn.connection = MagicMock()

        # Mock streaming results
        def mock_stream(sql, database=None, schema=None, batch_size=10000):
            # Yield two batches of data
            yield [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ]
            yield [
                {"id": 3, "name": "Charlie", "age": 35},
                {"id": 4, "name": "David", "age": 28},
            ]

        conn.execute_query_stream = mock_stream
        return conn

    @pytest.fixture
    def mock_cache(self):
        """Create a mock schema cache."""
        cache = MagicMock()
        cache.is_empty.return_value = False
        cache.is_expired.return_value = False
        return cache

    @pytest.fixture
    def mock_validator(self):
        """Create a mock query validator."""
        with patch("server.tools.execute_big_query_to_disk.QueryValidator") as mock:
            validator = mock.return_value
            validator.validate.return_value = (True, None, "SELECT")
            yield validator

    async def test_successful_query_export(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test successful query execution and CSV export."""
        csv_path = str(tmp_path / "results.csv")

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            csv_path,
        )

        assert result["status"] == "success"
        assert result["row_count"] == 4
        assert result["column_count"] == 3
        assert os.path.exists(csv_path)

        # Check CSV content
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 4
            assert rows[0]["name"] == "Alice"
            assert rows[3]["name"] == "David"

        # Check SQL file was created
        sql_path = csv_path[:-4] + ".sql"
        assert os.path.exists(sql_path)

    async def test_invalid_timeout(self, mock_connection, mock_cache):
        """Test rejection of invalid timeout values."""
        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT 1",
            "/tmp/test.csv",
            timeout_seconds=0,
        )

        assert result["status"] == "error"
        assert "at least 1 second" in result["message"]

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT 1",
            "/tmp/test.csv",
            timeout_seconds=3601,
        )

        assert result["status"] == "error"
        assert "cannot exceed 3600" in result["message"]

    async def test_invalid_query(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test rejection of invalid queries."""
        mock_validator.validate.return_value = (
            False,
            "INSERT queries are not allowed",
            "INSERT",
        )

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "INSERT INTO table VALUES (1)",
            str(tmp_path / "test.csv"),
        )

        assert result["status"] == "error"
        assert "INSERT queries are not allowed" in result["message"]

    async def test_empty_cache_error(self, mock_connection, mock_cache, mock_validator):
        """Test error when cache is empty."""
        mock_cache.is_empty.return_value = True

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT 1",
            "/tmp/test.csv",
        )

        assert result["status"] == "error"
        assert "refresh_catalog" in result["message"]

    async def test_existing_file_error(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test error when file already exists."""
        csv_path = tmp_path / "exists.csv"
        csv_path.write_text("existing data")

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT 1",
            str(csv_path),
        )

        assert result["status"] == "error"
        assert "already exists" in result["message"]

    async def test_create_directory(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test creation of non-existing directory."""
        csv_path = tmp_path / "new_dir" / "results.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            str(csv_path),
        )

        assert result["status"] == "success"
        assert csv_path.exists()
        assert csv_path.parent.exists()

    async def test_query_execution_error(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test handling of query execution errors."""
        # Mock connection to raise error
        mock_connection.execute_query_stream = Mock(
            side_effect=Exception("Connection lost")
        )

        csv_path = tmp_path / "error.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            str(csv_path),
        )

        assert result["status"] == "error"
        assert "Connection lost" in result["message"]
        # Check that partial file was cleaned up
        assert not csv_path.exists()

    async def test_empty_result_set(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test handling of empty result set."""
        # Mock empty results
        mock_connection.execute_query_stream = lambda **kwargs: iter([[]])

        csv_path = tmp_path / "empty.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM empty_table",
            str(csv_path),
        )

        assert result["status"] == "success"
        assert result["row_count"] == 0
        assert csv_path.exists()

    async def test_datetime_conversion(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test proper conversion of datetime values."""
        from datetime import datetime

        # Mock results with datetime
        def mock_stream(**kwargs):
            yield [
                {"id": 1, "created_at": datetime(2024, 1, 15, 10, 30, 45)},
            ]

        mock_connection.execute_query_stream = mock_stream

        csv_path = tmp_path / "dates.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            str(csv_path),
        )

        assert result["status"] == "success"

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["created_at"] == "2024-01-15T10:30:45"

    async def test_null_value_handling(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test proper handling of NULL values."""

        # Mock results with None values
        def mock_stream(**kwargs):
            yield [
                {"id": 1, "name": None, "age": 30},
            ]

        mock_connection.execute_query_stream = mock_stream

        csv_path = tmp_path / "nulls.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            str(csv_path),
        )

        assert result["status"] == "success"

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["name"] == ""  # CSV_NULL_VALUE is ""

    async def test_large_result_progress_logging(
        self, mock_connection, mock_cache, mock_validator, tmp_path
    ):
        """Test progress logging for large result sets."""

        # Mock large results (200k rows)
        def mock_stream(**kwargs):
            batch = [{"id": i} for i in range(10000)]
            for _ in range(20):  # 20 batches of 10k = 200k rows
                yield batch

        mock_connection.execute_query_stream = mock_stream

        csv_path = tmp_path / "large.csv"

        with patch("server.tools.execute_big_query_to_disk.logger") as mock_logger:
            result = await execute_big_query_to_disk(
                mock_connection,
                mock_cache,
                "SELECT * FROM large_table",
                str(csv_path),
            )

            assert result["status"] == "success"
            assert result["row_count"] == 200000
            # Check that progress was logged
            progress_logs = [
                call
                for call in mock_logger.info.call_args_list
                if "Streamed" in str(call)
            ]
            assert len(progress_logs) >= 2  # At 100k and 200k

    async def test_path_expansion(
        self, mock_connection, mock_cache, mock_validator, monkeypatch, tmp_path
    ):
        """Test expansion of ~ and environment variables in path."""
        # Set HOME to tmp_path for testing
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("TEST_DIR", "mydir")

        csv_path = "~/$TEST_DIR/results.csv"

        result = await execute_big_query_to_disk(
            mock_connection,
            mock_cache,
            "SELECT * FROM table",
            csv_path,
        )

        assert result["status"] == "success"
        expected_path = tmp_path / "mydir" / "results.csv"
        assert expected_path.exists()
        assert result["file_path"] == str(expected_path)
