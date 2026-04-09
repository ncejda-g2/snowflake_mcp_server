"""Tests for server/tools/table_inspector.py module."""

import tempfile
from pathlib import Path

import pytest

from server.schema_cache import ColumnInfo, SchemaCache, TableInfo
from server.tools.table_inspector import describe_table


class TestDescribeTable:
    """Test describe_table function - two-tier cache behavior."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a SchemaCache instance with temporary directory."""
        return SchemaCache(ttl_days=5, cache_dir=temp_cache_dir)

    @pytest.fixture
    def sample_table_with_columns(self):
        """Create sample table info with columns already loaded."""
        columns = [
            ColumnInfo(
                name="ID",
                data_type="NUMBER",
                is_nullable=False,
                ordinal_position=1,
                default_value=None,
                comment="Primary key",
                is_primary_key=True,
            ),
            ColumnInfo(
                name="NAME",
                data_type="VARCHAR",
                is_nullable=True,
                ordinal_position=2,
                default_value=None,
                comment="Name field",
                is_primary_key=False,
            ),
        ]

        return TableInfo(
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table_name="TEST_TABLE",
            table_type="BASE TABLE",
            columns=columns,
            column_count=2,
            comment="Test table",
        )

    @pytest.fixture
    def sample_table_no_columns(self):
        """Create sample table info without columns (catalog-only)."""
        return TableInfo(
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table_name="NO_COLS_TABLE",
            table_type="BASE TABLE",
            columns=[],
            column_count=5,
            comment="Table without loaded columns",
        )

    @pytest.mark.asyncio
    async def test_describe_table_from_cache(self, cache, sample_table_with_columns):
        """Test that describe_table returns data from cache when columns loaded."""
        cache.add_table(sample_table_with_columns)

        result = await describe_table(
            cache=cache,
            connection=None,
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table="TEST_TABLE",
        )

        assert result["status"] == "success"
        assert result["database"] == "TEST_DB"
        assert result["schema"] == "TEST_SCHEMA"
        assert result["table"] == "TEST_TABLE"
        assert result["source"] == "cache"
        assert result["column_count"] == 2
        assert len(result["columns"]) == 2

    @pytest.mark.asyncio
    async def test_describe_table_not_in_cache(self, cache):
        """Test that describe_table returns not_found when table not in cache."""
        result = await describe_table(
            cache=cache,
            connection=None,
            database="MISSING_DB",
            schema="MISSING_SCHEMA",
            table="MISSING_TABLE",
        )

        assert result["status"] == "not_found"
        assert "not found in cache" in result["message"]

    @pytest.mark.asyncio
    async def test_describe_table_column_details(
        self, cache, sample_table_with_columns
    ):
        """Test that describe_table returns correct column details from cache."""
        cache.add_table(sample_table_with_columns)

        result = await describe_table(
            cache=cache,
            connection=None,
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table="TEST_TABLE",
        )

        assert result["status"] == "success"
        columns = result["columns"]

        assert columns[0]["name"] == "ID"
        assert columns[0]["type"] == "NUMBER"
        assert columns[0]["nullable"] is False
        assert columns[0]["position"] == 1
        assert columns[0]["is_primary_key"] is True
        assert columns[0]["comment"] == "Primary key"

        assert columns[1]["name"] == "NAME"
        assert columns[1]["type"] == "VARCHAR"
        assert columns[1]["nullable"] is True
        assert columns[1]["position"] == 2
        assert columns[1]["is_primary_key"] is False
        assert columns[1]["comment"] == "Name field"

    @pytest.mark.asyncio
    async def test_describe_table_no_columns_no_connection(
        self, cache, sample_table_no_columns
    ):
        """Test describe_table with no columns and no connection returns metadata only."""
        cache.add_table(sample_table_no_columns)

        result = await describe_table(
            cache=cache,
            connection=None,
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table="NO_COLS_TABLE",
        )

        assert result["status"] == "success"
        assert result["column_count"] == 5
        assert result["columns"] == []
        assert result["source"] == "table_metadata_only"
