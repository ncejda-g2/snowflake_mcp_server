#!/usr/bin/env python3
"""Test script for checkpoint functionality in schema cache."""

import asyncio
import json
import os
from pathlib import Path
import shutil
from datetime import datetime
from dotenv import load_dotenv

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection
from server.config import Config
from server.tools.catalog_refresh import refresh_catalog

# Load environment variables from .env file
load_dotenv()


async def test_checkpointing():
    """Test the checkpoint and resume functionality."""
    
    print("=" * 60)
    print("Testing Schema Cache Checkpointing")
    print("=" * 60)
    
    # Initialize connection and cache
    config = Config.from_env()
    connection = SnowflakeConnection(config)
    connection.connect()  # Connect to Snowflake
    cache = SchemaCache(ttl_days=5)
    
    # Clear any existing checkpoints for clean test
    cache_dir = cache.cache_dir
    checkpoint_dir = cache.checkpoint_dir
    
    print(f"\nCache directory: {cache_dir}")
    print(f"Checkpoint directory: {checkpoint_dir}")
    
    # Test 1: Normal refresh with checkpointing
    print("\n--- Test 1: Full Refresh with Checkpointing ---")
    
    result = await refresh_catalog(connection, cache, force=True, resume=False)
    
    print(f"Status: {result['status']}")
    print(f"Tables found: {result.get('tables_found', 0)}")
    print(f"Databases scanned: {result.get('databases_scanned', 0)}")
    print(f"Databases failed: {result.get('databases_failed', 0)}")
    
    if result.get('errors'):
        print(f"Errors: {len(result['errors'])} databases failed")
        for error in result['errors'][:3]:  # Show first 3 errors
            print(f"  - {error[:100]}...")
    
    # Check if checkpoints were created and cleaned up
    checkpoint_files = list(checkpoint_dir.glob("checkpoint_*.json"))
    print(f"\nCheckpoint files after completion: {len(checkpoint_files)}")
    
    if checkpoint_files:
        print("WARNING: Checkpoint files should be cleaned up after successful completion!")
    
    # Test 2: Simulate interrupted refresh
    print("\n--- Test 2: Simulating Interrupted Refresh ---")
    
    # Clear the cache to start fresh
    cache.clear()
    
    # Manually create some checkpoint files to simulate interruption
    test_checkpoint_data = {
        'database': 'TEST_DB',
        'timestamp': datetime.now().isoformat(),
        'results': [
            {
                'TABLE_CATALOG': 'TEST_DB',
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'TEST_TABLE',
                'TABLE_TYPE': 'TABLE',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'IS_NULLABLE': 'NO',
                'ORDINAL_POSITION': 1,
                'COLUMN_DEFAULT': None,
                'COLUMN_COMMENT': None,
                'TABLE_COMMENT': 'Test table',
                'ROW_COUNT': 100,
                'BYTES': 1024
            }
        ]
    }
    
    checkpoint_file = checkpoint_dir / "checkpoint_TEST_DB.json"
    with open(checkpoint_file, 'w') as f:
        json.dump(test_checkpoint_data, f)
    
    print(f"Created test checkpoint: {checkpoint_file}")
    
    # Test loading checkpoints
    loaded_results, loaded_databases = cache.load_checkpoints()
    print(f"Loaded {len(loaded_results)} results from {len(loaded_databases)} databases")
    print(f"Processed databases: {loaded_databases}")
    
    # Test 3: Resume from checkpoint
    print("\n--- Test 3: Resume from Checkpoint ---")
    
    result = await refresh_catalog(connection, cache, force=True, resume=True)
    
    print(f"Status: {result['status']}")
    print(f"Tables found: {result.get('tables_found', 0)}")
    print(f"Databases scanned: {result.get('databases_scanned', 0)}")
    
    # Test 4: Error log functionality
    print("\n--- Test 4: Error Log Functionality ---")
    
    # Create a test error log
    test_errors = {
        'FAILED_DB_1': 'Connection timeout',
        'FAILED_DB_2': 'Permission denied'
    }
    
    cache.save_error_log(test_errors)
    print(f"Saved error log with {len(test_errors)} errors")
    
    # Load error log
    loaded_errors = cache.load_error_log()
    print(f"Loaded {len(loaded_errors)} errors from log")
    for db, error in loaded_errors.items():
        print(f"  - {db}: {error}")
    
    # Clear error log
    cache.clear_error_log()
    print("Error log cleared")
    
    # Verify cleanup
    print("\n--- Cleanup Verification ---")
    
    checkpoint_files = list(checkpoint_dir.glob("checkpoint_*.json"))
    print(f"Remaining checkpoint files: {len(checkpoint_files)}")
    
    if cache.error_log_file.exists():
        print("Error log file still exists")
    else:
        print("Error log file cleaned up")
    
    print("\n" + "=" * 60)
    print("Checkpoint Testing Complete")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_checkpointing())