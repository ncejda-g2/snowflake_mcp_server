"""Tests for server/tools/table_inspector.py module."""

import tempfile
from pathlib import Path

import pytest

from server.schema_cache import ColumnInfo, SchemaCache, TableInfo
from server.tools.table_inspector import get_table_schema


class TestGetTableSchema:
    """Test get_table_schema function - cache-only behavior."""

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
    def sample_table_info(self):
        """Create sample table info for testing."""
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
            comment="Test table",
        )

    @pytest.mark.asyncio
    async def test_get_table_schema_from_cache(self, cache, sample_table_info):
        """Test that get_table_schema returns data from cache."""
        # Add table to cache
        cache.add_table(sample_table_info)

        # Get table schema
        result = await get_table_schema(
            cache=cache,
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table="TEST_TABLE",
        )

        # Verify result
        assert result["status"] == "success"
        assert result["database"] == "TEST_DB"
        assert result["schema"] == "TEST_SCHEMA"
        assert result["table"] == "TEST_TABLE"
        assert result["source"] == "cache"
        assert result["column_count"] == 2
        assert len(result["columns"]) == 2

    @pytest.mark.asyncio
    async def test_get_table_schema_not_in_cache(self, cache):
        """Test that get_table_schema returns not_found when table not in cache."""
        # Get table schema for non-existent table
        result = await get_table_schema(
            cache=cache,
            database="MISSING_DB",
            schema="MISSING_SCHEMA",
            table="MISSING_TABLE",
        )

        # Verify result shows not found
        assert result["status"] == "not_found"
        assert "not found in cache" in result["message"]
        assert result["database"] == "MISSING_DB"
        assert result["schema"] == "MISSING_SCHEMA"
        assert result["table"] == "MISSING_TABLE"

    @pytest.mark.asyncio
    async def test_get_table_schema_column_details(self, cache, sample_table_info):
        """Test that get_table_schema returns correct column details from cache."""
        # Add table to cache
        cache.add_table(sample_table_info)

        # Get table schema
        result = await get_table_schema(
            cache=cache,
            database="TEST_DB",
            schema="TEST_SCHEMA",
            table="TEST_TABLE",
        )

        # Verify column details
        assert result["status"] == "success"
        columns = result["columns"]

        # Check first column
        assert columns[0]["name"] == "ID"
        assert columns[0]["type"] == "NUMBER"
        assert columns[0]["nullable"] is False
        assert columns[0]["position"] == 1
        assert columns[0]["is_primary_key"] is True
        assert columns[0]["comment"] == "Primary key"

        # Check second column
        assert columns[1]["name"] == "NAME"
        assert columns[1]["type"] == "VARCHAR"
        assert columns[1]["nullable"] is True
        assert columns[1]["position"] == 2
        assert columns[1]["is_primary_key"] is False
        assert columns[1]["comment"] == "Name field"
