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

    # Test 1: Save checkpoint (schema-level)
    print("\n--- Test 1: Save Checkpoint ---")

    test_results = [
        {
            "TABLE_CATALOG": "TEST_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "USERS",
            "TABLE_TYPE": "TABLE",
            "ROW_COUNT": 1000,
            "BYTES": 4096,
            "COMMENT": "Users table",
        },
    ]

    cache.save_checkpoint("TEST_DB", "PUBLIC", test_results)

    checkpoint_file = cache.checkpoint_dir / "checkpoint_TEST_DB__PUBLIC.json"
    assert checkpoint_file.exists(), "Checkpoint file should exist"
    print(f"✓ Checkpoint saved: {checkpoint_file}")

    # Test 2: Load checkpoints
    print("\n--- Test 2: Load Checkpoints ---")

    loaded_results, processed_schemas = cache.load_checkpoints()

    assert len(loaded_results) == 1, f"Should load 1 result, got {len(loaded_results)}"
    assert "TEST_DB.PUBLIC" in processed_schemas, (
        "TEST_DB.PUBLIC should be in processed schemas"
    )
    print(f"✓ Loaded {len(loaded_results)} results from {processed_schemas}")

    # Test 3: Multiple checkpoints
    print("\n--- Test 3: Multiple Checkpoints ---")

    test_results_2 = [
        {
            "TABLE_CATALOG": "ANALYTICS_DB",
            "TABLE_SCHEMA": "STAGING",
            "TABLE_NAME": "EVENTS",
            "TABLE_TYPE": "TABLE",
            "ROW_COUNT": 500,
            "COMMENT": "Events table",
        }
    ]

    cache.save_checkpoint("ANALYTICS_DB", "STAGING", test_results_2)

    loaded_results, processed_schemas = cache.load_checkpoints()

    assert len(loaded_results) == 2, (
        f"Should load 2 total results, got {len(loaded_results)}"
    )
    assert len(processed_schemas) == 2, (
        f"Should have 2 schemas, got {len(processed_schemas)}"
    )
    print(
        f"✓ Loaded {len(loaded_results)} results from {len(processed_schemas)} schemas"
    )

    # Test 4: Error log
    print("\n--- Test 4: Error Log ---")

    test_errors = {"DB1.SCH1": "Connection timeout", "DB2.PUBLIC": "Access denied"}

    cache.save_error_log(test_errors)
    assert cache.error_log_file.exists(), "Error log file should exist"
    print(f"✓ Error log saved: {cache.error_log_file}")

    loaded_errors = cache.load_error_log()
    assert len(loaded_errors) == 2, f"Should load 2 errors, got {len(loaded_errors)}"
    assert loaded_errors["DB1.SCH1"] == "Connection timeout", (
        "Error message should match"
    )
    print(f"✓ Loaded {len(loaded_errors)} errors")

    # Test 5: Merge schema results (with checkpoint data)
    print("\n--- Test 5: Process Checkpoint Data via merge ---")

    loaded_results, _ = cache.load_checkpoints()

    # Group by schema and merge
    schema_groups: dict[str, list[dict]] = {}
    for row in loaded_results:
        db = row.get("TABLE_CATALOG", "")
        sch = row.get("TABLE_SCHEMA", "")
        key = f"{db}.{sch}"
        schema_groups.setdefault(key, []).append(row)

    total_tables = 0
    for schema_key, rows in schema_groups.items():
        db, sch = schema_key.split(".", 1)
        total_tables += cache.merge_schema_results(db, sch, rows)

    assert total_tables == 2, f"Should have 2 tables, got {total_tables}"
    assert cache.get_table("TEST_DB", "PUBLIC", "USERS") is not None
    assert cache.get_table("ANALYTICS_DB", "STAGING", "EVENTS") is not None
    assert len(cache.databases) == 2, (
        f"Should have 2 databases, got {len(cache.databases)}"
    )
    print(f"✓ Processed {total_tables} tables from {len(cache.databases)} databases")

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
