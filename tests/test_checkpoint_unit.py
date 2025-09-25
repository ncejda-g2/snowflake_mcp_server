#!/usr/bin/env python3
"""Unit test for checkpoint functionality without requiring Snowflake connection."""

import shutil
from pathlib import Path

from server.schema_cache import SchemaCache


def test_checkpoint_functionality():
    """Test checkpoint save, load, and cleanup functionality."""

    print("=" * 60)
    print("Testing Checkpoint Functionality")
    print("=" * 60)

    # Create a temporary cache directory for testing
    test_cache_dir = Path("./test_cache")
    if test_cache_dir.exists():
        shutil.rmtree(test_cache_dir)

    # Initialize cache with test directory
    cache = SchemaCache(ttl_days=5, cache_dir=test_cache_dir)

    print(f"\nTest cache directory: {cache.cache_dir}")
    print(f"Checkpoint directory: {cache.checkpoint_dir}")

    # Test 1: Save checkpoint
    print("\n--- Test 1: Save Checkpoint ---")

    test_results = [
        {
            "TABLE_CATALOG": "TEST_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "USERS",
            "TABLE_TYPE": "TABLE",
            "COLUMN_NAME": "ID",
            "DATA_TYPE": "NUMBER",
            "IS_NULLABLE": "NO",
            "ORDINAL_POSITION": 1,
            "ROW_COUNT": 1000,
            "BYTES": 4096,
        },
        {
            "TABLE_CATALOG": "TEST_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "USERS",
            "TABLE_TYPE": "TABLE",
            "COLUMN_NAME": "NAME",
            "DATA_TYPE": "VARCHAR",
            "IS_NULLABLE": "YES",
            "ORDINAL_POSITION": 2,
            "ROW_COUNT": 1000,
            "BYTES": 4096,
        },
    ]

    cache.save_checkpoint("TEST_DB", test_results)

    checkpoint_file = cache.checkpoint_dir / "checkpoint_TEST_DB.json"
    assert checkpoint_file.exists(), "Checkpoint file should exist"
    print(f"✓ Checkpoint saved: {checkpoint_file}")

    # Test 2: Load checkpoints
    print("\n--- Test 2: Load Checkpoints ---")

    loaded_results, processed_databases = cache.load_checkpoints()

    assert len(loaded_results) == 2, f"Should load 2 results, got {len(loaded_results)}"
    assert "TEST_DB" in processed_databases, "TEST_DB should be in processed databases"
    print(f"✓ Loaded {len(loaded_results)} results from {processed_databases}")

    # Test 3: Multiple checkpoints
    print("\n--- Test 3: Multiple Checkpoints ---")

    test_results_2 = [
        {
            "TABLE_CATALOG": "ANALYTICS_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "EVENTS",
            "TABLE_TYPE": "TABLE",
            "COLUMN_NAME": "EVENT_ID",
            "DATA_TYPE": "VARCHAR",
            "IS_NULLABLE": "NO",
            "ORDINAL_POSITION": 1,
        }
    ]

    cache.save_checkpoint("ANALYTICS_DB", test_results_2)

    loaded_results, processed_databases = cache.load_checkpoints()

    assert len(loaded_results) == 3, (
        f"Should load 3 total results, got {len(loaded_results)}"
    )
    assert len(processed_databases) == 2, (
        f"Should have 2 databases, got {len(processed_databases)}"
    )
    print(
        f"✓ Loaded {len(loaded_results)} results from {len(processed_databases)} databases"
    )

    # Test 4: Error log
    print("\n--- Test 4: Error Log ---")

    test_errors = {"FAILED_DB": "Connection timeout", "RESTRICTED_DB": "Access denied"}

    cache.save_error_log(test_errors)
    assert cache.error_log_file.exists(), "Error log file should exist"
    print(f"✓ Error log saved: {cache.error_log_file}")

    loaded_errors = cache.load_error_log()
    assert len(loaded_errors) == 2, f"Should load 2 errors, got {len(loaded_errors)}"
    assert loaded_errors["FAILED_DB"] == "Connection timeout", (
        "Error message should match"
    )
    print(f"✓ Loaded {len(loaded_errors)} errors")

    # Test 5: Update from information schema (with checkpoint data)
    print("\n--- Test 5: Process Checkpoint Data ---")

    # This simulates what happens after loading checkpoints
    loaded_results, _ = cache.load_checkpoints()
    table_count = cache.update_from_information_schema(loaded_results)

    assert table_count == 2, f"Should have 2 tables, got {table_count}"
    assert len(cache.databases) == 2, (
        f"Should have 2 databases, got {len(cache.databases)}"
    )
    print(f"✓ Processed {table_count} tables from {len(cache.databases)} databases")

    # Test 6: Clear checkpoints
    print("\n--- Test 6: Clear Checkpoints ---")

    cache.clear_checkpoints()

    checkpoint_files = list(cache.checkpoint_dir.glob("checkpoint_*.json"))
    assert len(checkpoint_files) == 0, (
        f"Should have 0 checkpoint files, got {len(checkpoint_files)}"
    )
    print("✓ All checkpoints cleared")

    # Test 7: Clear error log
    print("\n--- Test 7: Clear Error Log ---")

    cache.clear_error_log()
    assert not cache.error_log_file.exists(), "Error log should be deleted"
    print("✓ Error log cleared")

    # Cleanup test directory
    shutil.rmtree(test_cache_dir)
    print("\n✓ Test directory cleaned up")

    print("\n" + "=" * 60)
    print("All Checkpoint Tests Passed!")
    print("=" * 60)


if __name__ == "__main__":
    test_checkpoint_functionality()
