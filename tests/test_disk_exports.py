"""Tests for the on-disk export tool (execute_query_to_file).

These verify the unification: the tool writes the SAME TSV format as the inline
payload (``.tsv`` extension, ``\\N`` for NULL, tab-delimited, one row per line),
so a sentinel learned inline is valid in the file. They also lock in that no
``.sql`` sidecar is written -- the data file stands alone.
"""

from unittest.mock import Mock, patch

import pytest

from server.schema_cache import SchemaCache
from server.serialization import TSV_NULL
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection
from server.tools.execute_query_to_file import execute_query_to_file


class TestExecuteQueryToFile:
    @pytest.fixture
    def mock_cache(self):
        cache = Mock(spec=SchemaCache)
        cache.is_empty.return_value = False
        cache.is_expired.return_value = False
        return cache

    @pytest.mark.asyncio
    async def test_streams_tsv_with_null_sentinel(self, tmp_path, mock_cache):
        conn = Mock(spec=SnowflakeConnection)
        conn.execute_query_stream.return_value = iter(
            [
                [{"id": 1, "name": "Alice"}],
                [{"id": 2, "name": None}, {"id": 3, "name": ""}],
            ]
        )

        out = tmp_path / "data.tsv"
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query_to_file(
                conn, mock_cache, sql="SELECT * FROM t", file_path=str(out)
            )

        assert result["status"] == "success"
        assert result["row_count"] == 3
        assert result["column_count"] == 2
        assert result["file_path"].endswith(".tsv")

        lines = out.read_text(encoding="utf-8").splitlines()
        assert lines[0] == "id\tname"
        assert lines[1] == "1\tAlice"
        assert lines[2] == f"2\t{TSV_NULL}"  # NULL distinct from empty
        assert lines[3] == "3\t"  # empty string

    @pytest.mark.asyncio
    async def test_appends_tsv_extension(self, tmp_path, mock_cache):
        conn = Mock(spec=SnowflakeConnection)
        conn.execute_query_stream.return_value = iter([[{"id": 1}]])
        out = tmp_path / "noext"
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query_to_file(
                conn, mock_cache, sql="SELECT 1", file_path=str(out)
            )
        assert result["status"] == "success"
        assert (tmp_path / "noext.tsv").exists()

    @pytest.mark.asyncio
    async def test_no_sql_sidecar_written(self, tmp_path, mock_cache):
        """The data file stands alone -- we no longer emit a .sql sidecar."""
        conn = Mock(spec=SnowflakeConnection)
        conn.execute_query_stream.return_value = iter([[{"id": 1}]])
        out = tmp_path / "q.tsv"
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query_to_file(
                conn, mock_cache, sql="SELECT 1", file_path=str(out)
            )
        assert result["status"] == "success"
        assert "sql_export" not in result
        assert not (tmp_path / "q.sql").exists()

    @pytest.mark.asyncio
    async def test_small_result_is_fine(self, tmp_path, mock_cache):
        """Size-agnostic: a two-row 'share this' export works identically."""
        conn = Mock(spec=SnowflakeConnection)
        conn.execute_query_stream.return_value = iter(
            [[{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]]
        )
        out = tmp_path / "share.tsv"
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query_to_file(
                conn, mock_cache, sql="SELECT a, b FROM t", file_path=str(out)
            )
        assert result["status"] == "success"
        assert result["row_count"] == 2
        lines = out.read_text(encoding="utf-8").splitlines()
        assert lines == ["a\tb", "1\tx", "2\ty"]

    @pytest.mark.asyncio
    async def test_rejects_existing_file(self, tmp_path, mock_cache):
        out = tmp_path / "exists.tsv"
        out.write_text("already here")
        conn = Mock(spec=SnowflakeConnection)
        with patch.object(
            QueryValidator, "validate", return_value=(True, None, QueryType.SELECT)
        ):
            result = await execute_query_to_file(
                conn, mock_cache, sql="SELECT 1", file_path=str(out)
            )
        assert result["status"] == "error"
        assert "already exists" in result["message"]
