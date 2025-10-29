#!/usr/bin/env python3
"""Snowflake MCP Server - Main application."""

import logging
from typing import Any

from fastmcp import FastMCP

from server.config import Config
from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection
from server.tools import (
    catalog_refresh,
    execute_big_query_to_disk,
    query_executor,
    save_to_csv,
    schema_inspector,
    table_inspector,
)

# Initialize configuration and logging
config = Config.from_env()
logging.basicConfig(
    level=logging.DEBUG if config.debug else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("Snowflake Read-Only MCP")

# Global instances (initialized on startup)
connection: SnowflakeConnection | None = None
cache: SchemaCache | None = None


# Initialize resources on first use
def initialize_resources(require_connection: bool = True):
    """Initialize connection and cache if not already done.

    Args:
        require_connection: If True, initializes the Snowflake connection.
                          If False, only initializes the cache.
    """
    global connection, cache

    if require_connection and connection is None:
        logger.info("Initializing Snowflake connection...")
        connection = SnowflakeConnection(config)
        connection.connect()
        logger.info("Snowflake connection established")

        # Test connection
        if not connection.test_connection():
            raise RuntimeError("Connection test failed")

    if cache is None:
        logger.info(f"Initializing schema cache (TTL: {config.cache_ttl_days} days)")
        cache = SchemaCache(ttl_days=config.cache_ttl_days)

        # Log cache status
        if cache.is_expired() or cache.is_empty():
            logger.info("Cache is expired or empty - run refresh_catalog to populate")
        else:
            stats = cache.get_statistics()
            logger.info(
                f"Using existing cache: {stats['total_tables']} tables from {stats['total_databases']} databases"
            )


# Tool: Refresh Catalog
@mcp.tool(
    name="refresh_catalog",
    description="""Refresh the schema catalog by scanning all accessible Snowflake databases.

    This tool queries INFORMATION_SCHEMA across all databases to build a comprehensive
    index of tables, schemas, and columns. The cache has a 5-day TTL.

    Use this tool when:
    - First connecting to Snowflake
    - Schema changes have been made
    - Cache has expired (after 5 days)

    Parameters:
    - force: Force refresh even if cache is not expired (default: false)
    - resume: Resume from checkpoints if they exist (default: true)
    """,
)
async def refresh_catalog_tool(
    force: bool = False, resume: bool = True
) -> dict[str, Any]:
    """Refresh the schema catalog cache."""
    try:
        # First, initialize only the cache to check if refresh is needed
        initialize_resources(require_connection=False)
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize cache: {str(e)}"}

    if cache is None:
        raise RuntimeError("Cache initialization failed")

    # Check if cache is valid before connecting to Snowflake
    if not force and not cache.is_expired() and not cache.is_empty():
        stats = cache.get_statistics()
        return {
            "status": "cache_valid",
            "message": "Cache is still valid and not expired",
            "statistics": stats,
        }

    # Only connect to Snowflake if we actually need to refresh
    try:
        initialize_resources(require_connection=True)
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to connect to Snowflake: {str(e)}",
        }

    if connection is None:
        raise RuntimeError("Connection initialization failed")

    return await catalog_refresh.refresh_catalog(
        connection, cache, force=force, resume=resume
    )


# Tool: Inspect Schemas
@mcp.tool(
    name="inspect_schemas",
    description="""Browse the hierarchical structure of databases, schemas, and tables.

    This tool provides a structured view of available database objects with optional
    filtering. Results are retrieved from the cache for fast access.

    NOTE: When filtering by database_pattern, column counts are omitted to reduce
    response size and avoid token limits.

    Parameters:
    - database_pattern: Filter databases by pattern (case-insensitive substring match)
    - schema_pattern: Filter schemas by pattern
    - table_pattern: Filter tables by pattern

    Examples:
    - inspect_schemas() - Show all databases and their structure
    - inspect_schemas(database_pattern="SALES") - Show only SALES-related databases
    - inspect_schemas(table_pattern="CUSTOMER") - Find all customer tables
    """,
)
async def inspect_schemas_tool(
    database_pattern: str | None = None,
    schema_pattern: str | None = None,
    table_pattern: str | None = None,
) -> dict[str, Any]:
    """List databases, schemas, and tables with optional filtering."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await schema_inspector.inspect_schemas(
        connection,
        cache,
        database_pattern=database_pattern,
        schema_pattern=schema_pattern,
        table_pattern=table_pattern,
    )


# Tool: Search Tables
@mcp.tool(
    name="search_tables",
    description="""Search for tables across all databases.

    This tool searches table names and comments for the specified term.
    Useful for finding tables when you don't know the exact location.

    Parameters:
    - search_term: Term to search for (case-insensitive)

    Examples:
    - search_tables("customer") - Find all customer-related tables
    - search_tables("revenue") - Find revenue tables
    """,
)
async def search_tables_tool(search_term: str) -> dict[str, Any]:
    """Search for tables by name or comment."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await schema_inspector.search_tables(connection, cache, search_term)


# Tool: Get Table Schema
@mcp.tool(
    name="get_table_schema",
    description="""Get detailed schema information for a specific table from cache only.

    This tool provides comprehensive column information including names, types,
    and constraints retrieved from the schema cache. It does NOT query Snowflake directly.

    Note: To get sample data from the table, use the execute_query tool separately.

    Parameters:
    - database: Database name
    - schema: Schema name
    - table: Table name

    Examples:
    - get_table_schema("SALES_DB", "PUBLIC", "CUSTOMERS")
    - get_table_schema("SALES_DB", "PUBLIC", "ORDERS")
    """,
)
async def get_table_schema_tool(
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any]:
    """Get detailed table schema information from cache."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if cache is None:
        raise RuntimeError("Cache initialization failed")
    return await table_inspector.get_table_schema(
        cache,
        database=database,
        schema=schema,
        table=table,
    )


# Tool: Execute Query
@mcp.tool(
    name="execute_query",
    description="""Execute a read-only SQL query on Snowflake.

    This tool validates queries for safety, executes them, and returns all results.
    Only SELECT, SHOW, DESCRIBE, and WITH queries are allowed.

    IMPORTANT: The schema cache must be populated before executing queries.
    Run refresh_catalog first if this is your first query.

    Parameters:
    - sql: SQL query to execute (SELECT, SHOW, DESCRIBE, or WITH)
    - database: Optional database context
    - schema: Optional schema context

    Returns:
    - All query results (respects LIMIT clause if present in SQL)
    - Results are cached for CSV export if under 5GB
    - Use save_last_query_to_csv to export results

    Note:
    - If you encounter token limit issues with large result sets, consider using
      execute_big_query_to_disk instead, which streams results directly to a file
      without returning the data in the response, or consider adding a stricter LIMIT clause.

    Examples:
    - execute_query("SELECT * FROM SALES_DB.PUBLIC.CUSTOMERS LIMIT 10")
    - execute_query("SELECT COUNT(*) FROM orders", database="SALES_DB", schema="PUBLIC")
    - execute_query("SELECT * FROM large_table LIMIT 1000")
    """,
)
async def execute_query_tool(
    sql: str, database: str | None = None, schema: str | None = None
) -> dict[str, Any]:
    """Execute a read-only SQL query."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await query_executor.execute_query(
        connection, cache, sql=sql, database=database, schema=schema
    )


# Tool: Validate Query Without Execution
@mcp.tool(
    name="validate_query_without_execution",
    description="""Generate and validate a SQL query without executing it.

    This tool can generate ANY type of SQL query including both read and write operations
    (SELECT, INSERT, UPDATE, DELETE, etc.) but does NOT execute them. Useful for generating
    queries that users want to review and execute elsewhere after manual verification.

    IMPORTANT: Write queries (INSERT, UPDATE, DELETE, etc.) can be generated here but
    CANNOT be executed through the execute_query tool for safety reasons. Users must
    execute write queries directly in Snowflake after manual review.

    Parameters:
    - sql: SQL query to generate (read or write operations allowed)
    - database: Optional database context
    - schema: Optional schema context

    The tool will:
    - Accept both read and write queries
    - Check query type (SELECT, INSERT, UPDATE, DELETE, etc.)
    - Extract table references
    - Provide hints for improvement
    - Return the formatted query ready for manual review
    - Indicate whether the query can be executed via execute_query (read-only) or not (write)

    Examples:
    - validate_query_without_execution("SELECT * FROM customers")
    - validate_query_without_execution("INSERT INTO orders (id, amount) VALUES (1, 100.00)")
    - validate_query_without_execution("UPDATE customers SET status = 'active' WHERE id = 123")
    - validate_query_without_execution("DELETE FROM temp_data WHERE created < '2024-01-01'")
    """,
)
async def validate_query_without_execution_tool(
    sql: str, database: str | None = None, schema: str | None = None
) -> dict[str, Any]:
    """Generate and prepare a SQL query (read or write) without executing it."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await query_executor.validate_query_without_execution(
        connection, cache, sql=sql, database=database, schema=schema
    )


# Tool: Get Query History
@mcp.tool(
    name="get_query_history",
    description="""Get the history of executed queries in this session.

    This tool returns a list of previously executed queries with their
    status, execution time, and results.

    Parameters:
    - limit: Maximum number of queries to return (default: 10)
    - only_successful: Only show successful queries (default: true)

    Examples:
    - get_query_history() - Get last 10 successful queries
    - get_query_history(limit=50, only_successful=false) - Get last 50 queries including errors
    """,
)
async def get_query_history_tool(
    limit: int = 10, only_successful: bool = True
) -> dict[str, Any]:
    """Get query execution history."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None:
        raise RuntimeError("Connection initialization failed")
    return await query_executor.get_query_history(
        connection, limit=limit, only_successful=only_successful
    )


# Tool: Save Last Query to CSV
@mcp.tool(
    name="save_last_query_to_csv",
    description="""Save the last executed query results to a CSV file.

    This tool exports the complete results from the most recently executed query
    to a CSV file at the specified path. The query must have been executed
    successfully and its results must be within the 5GB cache size limit.

    Features:
    - Exports ALL rows from the last query
    - Includes column headers
    - Uses comma delimiter
    - Handles NULL values as empty strings
    - Formats datetime values in ISO format
    - Optionally exports the SQL query to a .sql file (enabled by default)

    Parameters:
    - file_path: Path where the CSV file should be saved (absolute paths recommended)
                 Note: Relative paths are resolved from the MCP server's working directory
    - export_sql: Whether to also export the SQL query to a .sql file (default: true)

    Requirements:
    - A query must have been executed successfully using execute_query
    - Query results must be under 5GB (cache limit)

    Examples:
    - save_last_query_to_csv("~/Downloads/customers.csv")
    - save_last_query_to_csv("/tmp/query_results.csv")
    - save_last_query_to_csv("./data/export.csv", export_sql=false)

    Notes:
    - When export_sql is true, the SQL file will be saved with the same name as the CSV file
      but with a .sql extension (e.g., customers.csv → customers.sql)
    - The SQL file will be formatted for readability with proper indentation
    """,
)
async def save_last_query_to_csv_tool(
    file_path: str, export_sql: bool = True
) -> dict[str, Any]:
    """Save the last query results to a CSV file."""
    return await save_to_csv.save_last_query_to_csv(file_path, export_sql)


# Tool: Execute Big Query to Disk
@mcp.tool(
    name="execute_big_query_to_disk",
    description="""Execute a large read-only SQL query and save results directly to a CSV file.

    This tool is designed for queries that return large result sets that would exceed
    token limits. It streams results directly to disk without returning the data in
    the response, avoiding token limit issues.

    Features:
    - Streams results directly to disk (doesn't return data in response)
    - Handles arbitrarily large result sets using streaming
    - Returns only execution status, row count, and file size
    - Exports SQL query to a .sql file alongside the CSV
    - Configurable timeout for long-running queries

    Parameters:
    - sql: The SQL query to execute (must be read-only)
    - file_path: Path where the CSV file should be saved (absolute paths recommended)
                 Note: Relative paths are resolved from the MCP server's working directory
    - database: Optional database context
    - schema: Optional schema context
    - timeout_seconds: Query timeout in seconds (default: 300, max: 3600)

    Requirements:
    - Schema cache must be populated (run refresh_catalog first)
    - Query must be read-only (SELECT, SHOW, DESCRIBE, WITH)
    - Files must not already exist (will not overwrite)

    Examples:
    - execute_big_query_to_disk("SELECT * FROM large_table", "~/Downloads/large_data.csv")
    - execute_big_query_to_disk("SELECT * FROM sales_data", "/tmp/sales.csv", timeout_seconds=600)

    Notes:
    - CSV file uses comma delimiter, includes headers, empty string for NULLs
    - SQL file is created only after successful CSV export
    - Partial files are cleaned up on error
    """,
)
async def execute_big_query_to_disk_tool(
    sql: str,
    file_path: str,
    database: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Execute a large query and stream results to disk."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await execute_big_query_to_disk.execute_big_query_to_disk(
        connection,
        cache,
        sql=sql,
        file_path=file_path,
        database=database,
        schema=schema,
        timeout_seconds=timeout_seconds,
    )


def main():
    """Main entry point for the application."""
    try:
        # Run the MCP server
        if config.transport == "stdio":
            logger.info("Starting MCP server in STDIO mode")
            mcp.run(transport="stdio")
        else:
            logger.info(
                f"Starting MCP server in HTTP mode on {config.host}:{config.port}"
            )
            mcp.run(transport="http", host=config.host, port=config.port)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise


if __name__ == "__main__":
    main()
