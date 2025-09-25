#!/usr/bin/env python3
"""Test script to verify the catalog refresh fix."""

import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the server directory to path
sys.path.insert(0, str(Path(__file__).parent))

import logging

from server.config import Config
from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def test_catalog_refresh():
    """Test the catalog refresh with our fix."""

    # Initialize components using from_env
    config = Config.from_env()
    connection = SnowflakeConnection(config)
    cache = SchemaCache()

    # Connect to Snowflake
    print("Connecting to Snowflake...")
    connection.connect()

    # Test a single database first - GDC which should have 1,249 tables
    print("\nTesting GDC database specifically...")
    query = """
    SELECT COUNT(DISTINCT TABLE_NAME) as table_count,
           COUNT(*) as total_columns
    FROM GDC.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
    """

    result = connection.execute_query(query, max_rows=None)
    if result.data:
        print(f"GDC actual counts: {result.data[0]}")

    # Now test the catalog refresh
    print("\n" + "=" * 60)
    print("Running catalog refresh with force=True...")
    print("=" * 60)

    result = await refresh_catalog(connection, cache, force=True, resume=False)

    print(f"\nRefresh result: {result['status']}")
    print(f"Tables found: {result.get('tables_found', 0)}")
    print(f"Databases scanned: {result.get('databases_scanned', 0)}")

    if result.get("statistics"):
        stats = result["statistics"]
        print("\nDatabase table counts:")
        for db, info in stats.get("databases", {}).items():
            print(f"  {db}: {info['table_count']} tables")

    # Disconnect
    connection.disconnect()

    return result


if __name__ == "__main__":
    result = asyncio.run(test_catalog_refresh())

    # Check if we got the expected number of tables
    expected_counts = {
        "GDC": 1249,
        "GDC_TESTING": 1184,
        "ML_DEV": 113,
        "ML_PROD": 61,
        "GONG_DB": 22,
        "AGGREGATES_DB": 5,
        "AGGREGATES_DB_TESTING": 4,
        "AI_DRIVEN": 4,
    }

    print("\n" + "=" * 60)
    print("Validation Results:")
    print("=" * 60)

    stats = result.get("statistics", {})
    databases = stats.get("databases", {})

    all_match = True
    for db, expected in expected_counts.items():
        actual = databases.get(db, {}).get("table_count", 0)
        match = "✓" if actual == expected else "✗"
        print(f"{match} {db}: Expected {expected}, Got {actual}")
        if actual != expected:
            all_match = False

    if all_match:
        print("\n✅ SUCCESS: All table counts match!")
    else:
        print("\n❌ FAILURE: Table counts don't match. The fix may not be working.")
