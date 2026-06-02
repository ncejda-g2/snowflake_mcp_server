#!/usr/bin/env python3
"""Snowflake MCP Server - Main application."""

import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult

from server.config import Config
from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection
from server.tools import (
    catalog_refresh,
    execute_query_to_file,
    query_executor,
    schema_inspector,
    table_inspector,
)

# Initialize configuration (logging is configured by server.log_utils on import)
config = Config.from_env()
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


# Tool: Show Tables
@mcp.tool(
    name="show_tables",
    description="""Browse databases, schemas, and tables using pattern-based filtering.

    USE THIS WHEN: You want to explore what databases/schemas exist, or need to filter by exact patterns.
    Like SQL's: SHOW TABLES IN database LIKE 'pattern'

    RETURNS: Hierarchical tree structure
    - database → schema → list of tables (with column counts)

    HOW IT WORKS:
    - Auto-refreshes cache if expired/empty (requires Snowflake auth on first use)
    - Uses cached data if available (no auth needed)
    - Pattern matching is case-insensitive substring search

    Parameters:
    - database_pattern: Filter databases (e.g., "SALES" matches "SALES_DB", "SALES_PROD")
    - schema_pattern: Filter schemas (e.g., "PUBLIC")
    - table_pattern: Filter tables (e.g., "CUSTOMER" matches "CUSTOMERS", "CUSTOMER_ORDERS")

    Examples:
    - show_tables() - Browse all databases
    - show_tables(database_pattern="SALES") - Only SALES databases
    - show_tables(schema_pattern="PUBLIC") - All PUBLIC schemas across databases
    """,
)
async def show_tables_tool(
    database_pattern: str | None = None,
    schema_pattern: str | None = None,
    table_pattern: str | None = None,
) -> dict[str, Any]:
    """Browse databases, schemas, and tables hierarchically with pattern filtering."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await schema_inspector.show_tables(
        connection,
        cache,
        database_pattern=database_pattern,
        schema_pattern=schema_pattern,
        table_pattern=table_pattern,
    )


# Tool: Find Tables
@mcp.tool(
    name="find_tables",
    description="""Search for tables by keyword across ALL databases.

    USE THIS WHEN: You don't know where a table is, but know part of its name or purpose.
    Searches both table names AND table comments.

    RETURNS: Flat list of matching tables
    - [{database, schema, table, type, full_name, columns, comment}, ...]

    HOW IT WORKS:
    - Auto-refreshes cache if expired/empty (requires Snowflake auth on first use)
    - Uses cached data if available (no auth needed)
    - Searches table names and comments for the keyword (case-insensitive)

    Parameters:
    - search_term: Keyword to search for (case-insensitive)

    Examples:
    - find_tables("customer") - Find all customer-related tables across all databases
    - find_tables("revenue") - Find revenue tables anywhere
    - find_tables("staging") - Find tables with "staging" in name or comment
    """,
)
async def find_tables_tool(search_term: str) -> dict[str, Any]:
    """Search for tables by keyword in names and comments across all databases."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await schema_inspector.find_tables(connection, cache, search_term)


# Tool: Describe Table
@mcp.tool(
    name="describe_table",
    description="""Get detailed column information for a specific table.

    USE THIS WHEN: You need column names, types, and constraints to write a query.
    Like SQL's: DESCRIBE TABLE database.schema.table

    RETURNS: Detailed column information
    - For each column: name, data_type, nullable, position, default, comment, is_primary_key

    HOW IT WORKS:
    - Looks up table in cache; fetches column details on-demand if not yet loaded
    - First call for a table queries Snowflake live (~200ms), subsequent calls use cache
    - If table not in cache at all, returns error (use show_tables or find_tables first)

    Note: To get sample data rows, use execute_query tool separately.

    Parameters:
    - database: Database name
    - schema: Schema name
    - table: Table name

    Examples:
    - describe_table("SALES_DB", "PUBLIC", "CUSTOMERS")
    - describe_table("GDC", "STAGING", "ADMIN__CATEGORIES")
    """,
)
async def describe_table_tool(
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any]:
    """Get detailed column information for a specific table."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if cache is None:
        raise RuntimeError("Cache initialization failed")
    return await table_inspector.describe_table(
        cache,
        connection=connection,
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

    Returns a compact TEXT payload (not JSON), shaped for efficient parsing:
    - A `key: value` metadata header (status, rows, cols, execution_time,
      query_id).
    - A `---` separator.
    - A TSV block: line 1 is the tab-separated column names, each following line
      is one row. Parse it directly with grep/awk/cut, e.g.
      `awk -F'\\t' 'NR>1 && $3=="ACTIVE"'`.
      NULLs render as `\\N`; tabs/newlines inside values are backslash-escaped,
      so every row stays on exactly one line.

    Auto-spill for large results:
    - If the full TSV is too large to return inline, the tool does NOT truncate
      silently or dump a wall of text. It writes the COMPLETE result to a temp
      `.tsv` file (identical format) and returns a one-row proof-of-shape
      preview plus a `results_file: /path` field. Read/grep/awk that file for
      all rows. A `column_index` map (1=NAME 2=...) is included so you can target
      columns by name without counting.
    - The preview is one row only; `rows:` still reports the true total.

    Notes:
    - Respects the LIMIT clause if present in SQL.
    - To write results to a specific file you choose (to share or persist, any
      size), use execute_query_to_file(sql, file_path) instead. This tool only
      returns data inline or auto-spills to a temp file.

    Examples:
    - execute_query("SELECT * FROM SALES_DB.PUBLIC.CUSTOMERS LIMIT 10")
    - execute_query("SELECT COUNT(*) FROM orders", database="SALES_DB", schema="PUBLIC")
    - execute_query("SELECT * FROM large_table LIMIT 1000")
    """,
)
async def execute_query_tool(
    sql: str, database: str | None = None, schema: str | None = None
) -> ToolResult:
    """Execute a read-only SQL query.

    Wraps the text payload in a ToolResult with ONLY content set so FastMCP
    emits a single TextContent block and never duplicates it as
    structuredContent. This is version-safe across FastMCP 2.x and 3.x (which
    disagree on how `output_schema` disables structured output), because a
    ToolResult is passed through untouched by both.
    """
    try:
        initialize_resources()
    except Exception as e:
        text = query_executor.build_text_response(
            status="error", fields={"message": f"Failed to initialize: {str(e)}"}
        )
        return ToolResult(content=text)

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    text = await query_executor.execute_query(
        connection, cache, sql=sql, database=database, schema=schema
    )
    return ToolResult(content=text)


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


# Tool: Execute Query to File
@mcp.tool(
    name="execute_query_to_file",
    description="""Execute a read-only SQL query and write its results to a TSV file you choose.

    Use this when you want the result as a file at a specific path -- to share
    with someone or persist it -- regardless of size. A two-row lookup someone
    wants sent on, or a multi-million-row export too big to return inline: same
    tool. It streams results straight to disk (no data is returned in the
    response and none is held in memory beyond a batch).

    The file uses the SAME format as the inline execute_query payload:
    tab-delimited, NULL rendered as `\\N` (distinct from an empty field),
    tabs/newlines/backslashes escaped, one row per line -- so you can
    grep/awk/wc it identically.

    When to use which tool:
    - execute_query: you want to SEE the data (returns inline, or auto-spills a
      large result to a temp file the server picks).
    - execute_query_to_file: you want the data AS A FILE at a path you specify.

    Parameters:
    - sql: The SQL query to execute (must be read-only)
    - file_path: Path where the TSV file should be saved (absolute paths recommended).
                 A `.tsv` extension is appended if missing.
                 Note: Relative paths are resolved from the MCP server's working directory
    - database: Optional database context
    - schema: Optional schema context
    - timeout_seconds: Query timeout in seconds (default: 300, max: 3600)

    Requirements:
    - Schema cache must be populated (run refresh_catalog first)
    - Query must be read-only (SELECT, SHOW, DESCRIBE, WITH)
    - The file must not already exist (will not overwrite)

    Examples:
    - execute_query_to_file("SELECT * FROM customers LIMIT 2", "~/Downloads/sample.tsv")
    - execute_query_to_file("SELECT * FROM large_table", "/tmp/export.tsv", timeout_seconds=600)

    Notes:
    - TSV file is tab-delimited, includes a header line, NULL = `\\N`
    - Partial files are cleaned up on error
    """,
)
async def execute_query_to_file_tool(
    sql: str,
    file_path: str,
    database: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Execute a query and stream results to a file at a chosen path."""
    try:
        initialize_resources()
    except Exception as e:
        return {"status": "error", "message": f"Failed to initialize: {str(e)}"}

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return await execute_query_to_file.execute_query_to_file(
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
