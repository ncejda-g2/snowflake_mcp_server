"""Fixed tests for server/schema_cache.py module."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from server.schema_cache import ColumnInfo, SchemaCache, TableInfo


class TestSchemaCache:
    """Test SchemaCache class - fixed version."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a SchemaCache instance with temporary directory."""
        return SchemaCache(ttl_days=5, cache_dir=temp_cache_dir)

    def test_schema_cache_initialization(self, temp_cache_dir):
        """Test SchemaCache initialization."""
        cache = SchemaCache(ttl_days=7, cache_dir=temp_cache_dir)

        assert cache.ttl_days == 7
        assert cache.cache_dir == temp_cache_dir
        assert cache.cache_file == temp_cache_dir / "schema_cache.json"
        assert cache.checkpoint_dir == temp_cache_dir / "checkpoints"
        assert cache.error_log_file == temp_cache_dir / "refresh_errors.json"
        assert cache.tables == {}
        assert cache.databases == set()
        assert cache.last_refresh is None
        assert cache.refresh_in_progress is False

    def test_is_expired_no_refresh(self, cache):
        """Test is_expired when no refresh has occurred."""
        assert cache.is_expired() is True

    def test_is_expired_recent_refresh(self, cache):
        """Test is_expired with recent refresh."""
        cache.last_refresh = datetime.now()
        assert cache.is_expired() is False

    def test_is_expired_old_refresh(self, cache):
        """Test is_expired with old refresh."""
        cache.last_refresh = datetime.now() - timedelta(days=10)
        assert cache.is_expired() is True

    def test_is_empty(self, cache):
        """Test is_empty method."""
        assert cache.is_empty() is True

        table = TableInfo("DB", "SCH", "TAB", "TABLE", [])
        cache.add_table(table)
        assert cache.is_empty() is False

    def test_add_table(self, cache):
        """Test adding a table to cache."""
        table = TableInfo(
            database="TEST_DB",
            schema="PUBLIC",
            table_name="USERS",
            table_type="TABLE",
            columns=[],
        )

        cache.add_table(table)

        assert "TEST_DB.PUBLIC.USERS" in cache.tables
        assert cache.tables["TEST_DB.PUBLIC.USERS"] == table
        assert "TEST_DB" in cache.databases

    def test_get_table(self, cache):
        """Test getting a table from cache."""
        table = TableInfo(
            database="TEST_DB",
            schema="PUBLIC",
            table_name="USERS",
            table_type="TABLE",
            columns=[],
        )
        cache.add_table(table)

        result = cache.get_table("TEST_DB", "PUBLIC", "USERS")
        assert result == table

    def test_get_tables_in_schema(self, cache):
        """Test getting all tables in a schema."""
        table1 = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [])
        table2 = TableInfo("DB1", "SCH1", "TAB2", "TABLE", [])
        table3 = TableInfo("DB1", "SCH2", "TAB3", "TABLE", [])

        for table in [table1, table2, table3]:
            cache.add_table(table)

        result = cache.get_tables_in_schema("DB1", "SCH1")
        assert len(result) == 2
        assert table1 in result
        assert table2 in result

    def test_get_databases(self, cache):
        """Test getting list of databases."""
        table1 = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [])
        table2 = TableInfo("DB2", "SCH1", "TAB2", "TABLE", [])

        cache.add_table(table1)
        cache.add_table(table2)

        databases = cache.get_databases()
        assert set(databases) == {"DB1", "DB2"}

    def test_get_schemas(self, cache):
        """Test getting schemas in a database."""
        table1 = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [])
        table2 = TableInfo("DB1", "SCH2", "TAB2", "TABLE", [])
        table3 = TableInfo("DB1", "SCH1", "TAB3", "TABLE", [])

        for table in [table1, table2, table3]:
            cache.add_table(table)

        schemas = cache.get_schemas("DB1")
        assert set(schemas) == {"SCH1", "SCH2"}

    def test_search_tables(self, cache):
        """Test searching tables by name."""
        table1 = TableInfo("DB1", "SCH1", "USERS", "TABLE", [], comment="User data")
        table2 = TableInfo("DB1", "SCH1", "USER_LOGS", "TABLE", [])
        table3 = TableInfo("DB1", "SCH1", "PRODUCTS", "TABLE", [])

        for table in [table1, table2, table3]:
            cache.add_table(table)

        # Search in table names
        result = cache.search_tables("USER")
        assert len(result) == 2

        # Case insensitive search
        result = cache.search_tables("user")
        assert len(result) == 2

    def test_save_and_load_cache(self, cache):
        """Test saving and loading cache to/from disk."""
        # Add some data
        table = TableInfo(
            database="TEST_DB",
            schema="PUBLIC",
            table_name="USERS",
            table_type="TABLE",
            columns=[
                ColumnInfo("id", "NUMBER", False, 1),
            ],
        )
        cache.add_table(table)
        cache.last_refresh = datetime.now()

        # Save cache
        cache.save()
        assert cache.cache_file.exists()

        # Create new cache instance and load
        cache2 = SchemaCache(cache_dir=cache.cache_dir)
        result = cache2.load()
        assert result is True

        assert "TEST_DB.PUBLIC.USERS" in cache2.tables
        assert "TEST_DB" in cache2.databases

    def test_clear_cache(self, cache):
        """Test clearing the cache."""
        # Add some data
        table = TableInfo("DB", "SCH", "TAB", "TABLE", [])
        cache.add_table(table)
        cache.last_refresh = datetime.now()

        # Clear cache
        cache.clear()

        assert cache.tables == {}
        assert cache.databases == set()
        assert cache.last_refresh is None

    def test_get_statistics(self, cache):
        """Test getting cache statistics."""
        # Add some tables
        table1 = TableInfo(
            "DB1",
            "SCH1",
            "TAB1",
            "TABLE",
            [ColumnInfo("col1", "VARCHAR", True, 1)],
            column_count=1,
        )
        table2 = TableInfo("DB1", "SCH2", "TAB2", "VIEW", [], column_count=3)

        for table in [table1, table2]:
            cache.add_table(table)

        cache.last_refresh = datetime.now()

        stats = cache.get_statistics()

        assert stats["total_tables"] == 2
        assert stats["total_databases"] == 1
        assert stats["is_expired"] is False

    def test_save_and_load_checkpoints(self, cache):
        """Test checkpoint save/load functionality."""
        # Save checkpoint
        checkpoint_data = [{"TABLE_NAME": "TEST_TABLE", "DATABASE_NAME": "TEST_DB"}]
        cache.save_checkpoint("TEST_DB", "PUBLIC", checkpoint_data)

        # Load checkpoints
        results, processed = cache.load_checkpoints()
        assert len(results) > 0
        assert "TEST_DB.PUBLIC" in processed

    def test_clear_checkpoints(self, cache):
        """Test clearing checkpoints."""
        # Create checkpoint
        cache.save_checkpoint("DB1", "PUBLIC", [{"data": "test"}])

        # Clear
        cache.clear_checkpoints()

        # Verify cleared
        results, processed = cache.load_checkpoints()
        assert len(results) == 0

    def test_merge_schema_results(self, cache):
        """Test merging table-level results into cache."""
        # Add existing data for two schemas
        table1 = TableInfo("DB1", "SCH1", "OLD_TAB", "TABLE", [])
        table2 = TableInfo("DB1", "SCH2", "KEEP_ME", "TABLE", [])
        cache.add_table(table1)
        cache.add_table(table2)

        # Merge new table-level results for SCH1 only
        table_results = [
            {
                "TABLE_CATALOG": "DB1",
                "TABLE_SCHEMA": "SCH1",
                "TABLE_NAME": "NEW_TAB",
                "TABLE_TYPE": "TABLE",
                "ROW_COUNT": 100,
                "BYTES": 5000,
                "COMMENT": "A new table",
            },
        ]
        column_counts = {"NEW_TAB": 5}

        count = cache.merge_schema_results(
            "DB1",
            "SCH1",
            table_results,
            column_counts=column_counts,
            max_last_altered="2026-01-01",
        )

        assert count == 1
        # Old table in SCH1 should be gone
        assert cache.get_table("DB1", "SCH1", "OLD_TAB") is None
        # New table in SCH1 should exist with column_count but no columns
        new_tab = cache.get_table("DB1", "SCH1", "NEW_TAB")
        assert new_tab is not None
        assert new_tab.column_count == 5
        assert new_tab.columns == []
        # Table in SCH2 should be untouched
        assert cache.get_table("DB1", "SCH2", "KEEP_ME") is not None
        # LAST_ALTERED should be stored
        assert cache.get_schema_last_altered("DB1", "SCH1") == "2026-01-01"

    def test_merge_preserves_loaded_columns(self, cache):
        """Test that merge preserves previously-loaded column details."""
        col = ColumnInfo("ID", "NUMBER", False, 1)
        table = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [col], column_count=1)
        cache.add_table(table)

        # Re-merge the schema with table-level data
        table_results = [
            {
                "TABLE_CATALOG": "DB1",
                "TABLE_SCHEMA": "SCH1",
                "TABLE_NAME": "TAB1",
                "TABLE_TYPE": "TABLE",
                "ROW_COUNT": 200,
            },
        ]

        cache.merge_schema_results("DB1", "SCH1", table_results)

        # Columns should be preserved
        result = cache.get_table("DB1", "SCH1", "TAB1")
        assert len(result.columns) == 1
        assert result.columns[0].name == "ID"

    def test_remove_schema(self, cache):
        """Test removing all entries for a schema."""
        table1 = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [])
        table2 = TableInfo("DB1", "SCH1", "TAB2", "TABLE", [])
        table3 = TableInfo("DB1", "SCH2", "TAB3", "TABLE", [])
        for t in [table1, table2, table3]:
            cache.add_table(t)
        cache.schema_last_altered["DB1.SCH1"] = "2026-01-01"

        removed = cache.remove_schema("DB1", "SCH1")

        assert removed == 2
        assert cache.get_table("DB1", "SCH1", "TAB1") is None
        assert cache.get_table("DB1", "SCH1", "TAB2") is None
        assert cache.get_table("DB1", "SCH2", "TAB3") is not None
        assert cache.get_schema_last_altered("DB1", "SCH1") is None

    def test_get_schema_last_altered(self, cache):
        """Test getting schema last altered timestamp."""
        assert cache.get_schema_last_altered("DB1", "SCH1") is None

        cache.schema_last_altered["DB1.SCH1"] = "2026-04-08T12:00:00"
        assert cache.get_schema_last_altered("DB1", "SCH1") == "2026-04-08T12:00:00"

    def test_save_load_schema_last_altered(self, cache):
        """Test that schema_last_altered persists through save/load."""
        table = TableInfo("DB1", "SCH1", "TAB1", "TABLE", [])
        cache.add_table(table)
        cache.last_refresh = datetime.now()
        cache.schema_last_altered["DB1.SCH1"] = "2026-04-08T12:00:00"

        cache.save()

        cache2 = SchemaCache(cache_dir=cache.cache_dir)
        cache2.load()

        assert cache2.schema_last_altered.get("DB1.SCH1") == "2026-04-08T12:00:00"

    def test_load_v1_cache_migration(self, temp_cache_dir):
        """Test loading a v1.0 cache file gracefully."""
        import json

        # v1.0 has no schema_last_altered or column_count
        cache_file = temp_cache_dir / "schema_cache.json"
        cache_data = {
            "version": "1.0",
            "last_refresh": datetime.now().isoformat(),
            "ttl_days": 5,
            "tables": {
                "DB1.SCH1.TAB1": {
                    "database": "DB1",
                    "schema": "SCH1",
                    "table_name": "TAB1",
                    "table_type": "TABLE",
                    "columns": [
                        {
                            "name": "ID",
                            "data_type": "NUMBER",
                            "is_nullable": False,
                            "ordinal_position": 1,
                        },
                    ],
                }
            },
            "databases": ["DB1"],
        }
        with open(cache_file, "w") as f:
            json.dump(cache_data, f)

        cache = SchemaCache(cache_dir=temp_cache_dir)
        assert cache.schema_last_altered == {}
        # column_count should be computed from len(columns)
        tab = cache.get_table("DB1", "SCH1", "TAB1")
        assert tab is not None
        assert tab.column_count == 1

    def test_error_log_operations(self, cache):
        """Test error log save/load/clear."""
        # Save errors
        errors = {"DB1": "Error 1", "DB2": "Error 2"}
        cache.save_error_log(errors)

        # Load errors
        loaded = cache.load_error_log()
        assert loaded == errors

        # Clear errors
        cache.clear_error_log()
        loaded = cache.load_error_log()
        assert loaded == {}

    def test_update_from_information_schema(self, cache):
        """Test updating cache from information schema results."""
        results = [
            {
                "TABLE_CATALOG": "DB1",
                "TABLE_SCHEMA": "SCH1",
                "TABLE_NAME": "TAB1",
                "TABLE_TYPE": "BASE TABLE",
                "ROW_COUNT": 100,
                "BYTES": 5000,
                "COMMENT": "Test table",
                "CREATED": "2024-01-01",
                "LAST_ALTERED": "2024-01-02",
            }
        ]

        count = cache.update_from_information_schema(results)
        assert count == 1

        table = cache.get_table("DB1", "SCH1", "TAB1")
        assert table is not None
        assert table.table_name == "TAB1"
