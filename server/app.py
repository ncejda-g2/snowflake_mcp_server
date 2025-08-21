#!/usr/bin/env python3
"""Snowflake MCP Server - Main application."""

import logging
from typing import Dict, Optional, Any

from fastmcp import FastMCP

from server.config import Config
from server.snowflake_connection import SnowflakeConnection
from server.schema_cache import SchemaCache
from server.tools import (
    catalog_refresh,
    schema_inspector,
    table_inspector,
    query_executor,
    save_to_csv
)

# Initialize configuration and logging
config = Config.from_env()
logging.basicConfig(
    level=logging.DEBUG if config.debug else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("Snowflake Read-Only MCP")

# Global instances (initialized on startup)
connection: Optional[SnowflakeConnection] = None
cache: Optional[SchemaCache] = None


# Initialize resources on first use
def initialize_resources():
    """Initialize connection and cache if not already done."""
    global connection, cache
    
    if connection is None:
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
            logger.info(f"Using existing cache: {stats['total_tables']} tables from {stats['total_databases']} databases")


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
    """
)
async def refresh_catalog_tool(force: bool = False, resume: bool = True) -> Dict[str, Any]:
    """Refresh the schema catalog cache."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await catalog_refresh.refresh_catalog(connection, cache, force=force, resume=resume)


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
    """
)
async def inspect_schemas_tool(
    database_pattern: Optional[str] = None,
    schema_pattern: Optional[str] = None,
    table_pattern: Optional[str] = None
) -> Dict[str, Any]:
    """List databases, schemas, and tables with optional filtering."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await schema_inspector.inspect_schemas(
        connection, cache,
        database_pattern=database_pattern,
        schema_pattern=schema_pattern,
        table_pattern=table_pattern
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
    """
)
async def search_tables_tool(search_term: str) -> Dict[str, Any]:
    """Search for tables by name or comment."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await schema_inspector.search_tables(connection, cache, search_term)


# Tool: Get Table Schema
@mcp.tool(
    name="get_table_schema",
    description="""Get detailed schema information for a specific table.
    
    This tool provides comprehensive column information including names, types,
    constraints, and optionally sample data.
    
    Parameters:
    - database: Database name
    - schema: Schema name
    - table: Table name
    - include_sample: Include sample data rows (default: false)
    - sample_rows: Number of sample rows to include (default: 5)
    
    Examples:
    - get_table_schema("SALES_DB", "PUBLIC", "CUSTOMERS")
    - get_table_schema("SALES_DB", "PUBLIC", "ORDERS", include_sample=true)
    """
)
async def get_table_schema_tool(
    database: str,
    schema: str,
    table: str,
    include_sample: bool = False,
    sample_rows: int = 5
) -> Dict[str, Any]:
    """Get detailed table schema information."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await table_inspector.get_table_schema(
        connection, cache,
        database=database,
        schema=schema,
        table=table,
        include_sample=include_sample,
        sample_rows=sample_rows
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
    
    Examples:
    - execute_query("SELECT * FROM SALES_DB.PUBLIC.CUSTOMERS LIMIT 10")
    - execute_query("SELECT COUNT(*) FROM orders", database="SALES_DB", schema="PUBLIC")
    - execute_query("SELECT * FROM large_table LIMIT 1000")
    """
)
async def execute_query_tool(
    sql: str,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """Execute a read-only SQL query."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await query_executor.execute_query(
        connection, cache,
        sql=sql,
        database=database,
        schema=schema,
        format_results=True
    )


# Tool: Validate Query Without Execution
@mcp.tool(
    name="validate_query_without_execution",
    description="""Generate and validate a SQL query without executing it.
    
    This tool validates a query for safety (read-only), syntax, and provides
    the prepared query text without running it. Useful when users want to
    review queries before execution or run them elsewhere.
    
    Parameters:
    - sql: SQL query to validate
    - database: Optional database context
    - schema: Optional schema context
    
    The tool will:
    - Validate the query is read-only
    - Check query type (SELECT, SHOW, etc.)
    - Extract table references
    - Provide hints for improvement
    - Return the formatted query ready for use
    
    Examples:
    - validate_query_without_execution("SELECT * FROM customers")
    - validate_query_without_execution("SELECT COUNT(*) FROM orders", database="SALES_DB", schema="PUBLIC")
    """
)
async def validate_query_without_execution_tool(
    sql: str,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """Validate and prepare a SQL query without executing it."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None and cache is not None
    return await query_executor.validate_query_without_execution(
        connection, cache,
        sql=sql,
        database=database,
        schema=schema
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
    """
)
async def get_query_history_tool(
    limit: int = 10,
    only_successful: bool = True
) -> Dict[str, Any]:
    """Get query execution history."""
    try:
        initialize_resources()
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize: {str(e)}"
        }
    
    assert connection is not None
    return await query_executor.get_query_history(
        connection,
        limit=limit,
        only_successful=only_successful
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
    
    Parameters:
    - file_path: The absolute or relative path where the CSV file should be saved
    
    Requirements:
    - A query must have been executed successfully using execute_query
    - Query results must be under 5GB (cache limit)
    
    Examples:
    - save_last_query_to_csv("~/Downloads/customers.csv")
    - save_last_query_to_csv("/tmp/query_results.csv")
    - save_last_query_to_csv("./data/export.csv")
    """
)
async def save_last_query_to_csv_tool(
    file_path: str
) -> Dict[str, Any]:
    """Save the last query results to a CSV file."""
    return await save_to_csv.save_last_query_to_csv(file_path)


def main():
    """Main entry point for the application."""
    try:
        # Run the MCP server
        if config.transport == "stdio":
            logger.info("Starting MCP server in STDIO mode")
            mcp.run(transport="stdio")
        else:
            logger.info(f"Starting MCP server in HTTP mode on {config.host}:{config.port}")
            mcp.run(transport="http", host=config.host, port=config.port)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise


if __name__ == "__main__":
    main()