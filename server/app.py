#!/usr/bin/env python3
"""Snowflake MCP Server - Main application."""

import json
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


def _json_result(payload: dict[str, Any]) -> ToolResult:
    """Return a dict payload as a single JSON TextContent block.

    FastMCP, given a plain dict, emits the data TWICE: once as a JSON text
    block and again as a ``structuredContent`` object. For an LLM client both
    arrive as text, so the structured copy is pure token waste. Serializing the
    dict ourselves and wrapping it in ``ToolResult(content=...)`` makes FastMCP
    emit exactly one ``TextContent`` and leave ``structured_content`` unset --
    the same single-payload contract ``execute_query`` already follows. The
    agent still receives the full JSON and can read any field (e.g.
    ``file_path``) straight out of it.
    """
    return ToolResult(content=json.dumps(payload, default=str))


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
async def refresh_catalog_tool(force: bool = False, resume: bool = True) -> ToolResult:
    """Refresh the schema catalog cache."""
    try:
        # First, initialize only the cache to check if refresh is needed
        initialize_resources(require_connection=False)
    except Exception as e:
        return _json_result(
            {"status": "error", "message": f"Failed to initialize cache: {str(e)}"}
        )

    if cache is None:
        raise RuntimeError("Cache initialization failed")

    # Check if cache is valid before connecting to Snowflake
    if not force and not cache.is_expired() and not cache.is_empty():
        stats = cache.get_statistics()
        return _json_result(
            {
                "status": "cache_valid",
                "message": "Cache is still valid and not expired",
                "statistics": stats,
            }
        )

    # Only connect to Snowflake if we actually need to refresh
    try:
        initialize_resources(require_connection=True)
    except Exception as e:
        return _json_result(
            {
                "status": "error",
                "message": f"Failed to connect to Snowflake: {str(e)}",
            }
        )

    if connection is None:
        raise RuntimeError("Connection initialization failed")

    return _json_result(
        await catalog_refresh.refresh_catalog(
            connection, cache, force=force, resume=resume
        )
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
) -> ToolResult:
    """Browse databases, schemas, and tables hierarchically with pattern filtering."""
    try:
        initialize_resources()
    except Exception as e:
        return _json_result(
            {"status": "error", "message": f"Failed to initialize: {str(e)}"}
        )

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return _json_result(
        await schema_inspector.show_tables(
            connection,
            cache,
            database_pattern=database_pattern,
            schema_pattern=schema_pattern,
            table_pattern=table_pattern,
        )
    )


# Tool: Find Tables
@mcp.tool(
    name="find_tables",
    description="""Search for tables by keyword across ALL databases.

    USE THIS WHEN: You don't know where a table is, but know part of its name or purpose.
    Matches against both table names AND table comments (so a cryptically-named
    table is still found when its comment mentions the term).

    RETURNS (small result): flat list of matches
    - [{database, schema, table, type, full_name, columns}, ...]
      Note: the comment is NOT returned (it is the one unbounded field and can be
      a multi-KB doc-block). To read a specific table's comment, use describe_table.

    RETURNS (broad result): when too many tables match to return inline, the
    COMPLETE result is written to a temp `.tsv` file and the response is instead a
    compact summary built to help you NARROW: `total_hits`, `results_file`, a
    bounded `top_groups` breakdown of the top database.schema clusters (with a
    `(+X more groups, Y hits)` tail marker), and a `spilled` hint. To narrow, call
    show_tables with database_pattern/schema_pattern from top_groups and/or a more
    specific table_pattern -- don't blindly re-search.

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
async def find_tables_tool(search_term: str) -> ToolResult:
    """Search for tables by keyword in names and comments across all databases."""
    try:
        initialize_resources()
    except Exception as e:
        return _json_result(
            {"status": "error", "message": f"Failed to initialize: {str(e)}"}
        )

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return _json_result(
        await schema_inspector.find_tables(connection, cache, search_term)
    )


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
) -> ToolResult:
    """Get detailed column information for a specific table."""
    try:
        initialize_resources()
    except Exception as e:
        return _json_result(
            {"status": "error", "message": f"Failed to initialize: {str(e)}"}
        )

    if cache is None:
        raise RuntimeError("Cache initialization failed")
    return _json_result(
        await table_inspector.describe_table(
            cache,
            connection=connection,
            database=database,
            schema=schema,
            table=table,
        )
    )


# Tool: Execute Query
@mcp.tool(
    name="execute_query",
    description="""Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, WITH) and return results.

    Requires a populated schema cache; auto-refreshes on first use if empty.

    Parameters:
    - sql: read-only SQL query
    - database: optional database context
    - schema: optional schema context

    Returns a compact TEXT payload (not JSON): a `key: value` header
    (status, rows, cols, execution_time, query_id), a `---` separator, then a
    positional TSV block. TSV rules: line 1 = tab-separated column names, one row
    per line after; NULL = `\\N`; tabs/newlines escaped so each row is one line.
    Parse with awk/cut, e.g. `awk -F'\\t' 'NR>1 && $3=="X"'`.

    Large/wide/tall results auto-spill the COMPLETE result to a temp `.tsv` file;
    the payload then carries `results_file`, `column_index` (name->position), and a
    `spilled` marker in place of inline rows. Read/grep/awk the file; `rows:` is
    always the true total.

    Example: execute_query("SELECT * FROM SALES_DB.PUBLIC.CUSTOMERS LIMIT 10")
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


# Tool: Execute Query to File
@mcp.tool(
    name="execute_query_to_file",
    description="""Writes read-only query results to a file at a path you choose.

    Use when the result needs to land at a specific path -- to share or persist.
    Format follows the extension: `.csv` writes CSV (NULL = empty field);
    anything else writes TSV (same as execute_query: tab-delimited, NULL = `\\N`).

    Parameters:
    - sql: read-only SQL (SELECT, SHOW, DESCRIBE, WITH)
    - file_path: output path (absolute recommended; end with `.csv` for CSV,
      else `.tsv` is used/appended)
    - database: optional database context
    - schema: optional schema context
    - timeout_seconds: query timeout (default 300, max 3600)

    Requires a populated schema cache. Will not overwrite an existing file.

    Example: execute_query_to_file("SELECT * FROM t", "/tmp/export.csv")
    """,
)
async def execute_query_to_file_tool(
    sql: str,
    file_path: str,
    database: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 300,
) -> ToolResult:
    """Execute a query and stream results to a file at a chosen path."""
    try:
        initialize_resources()
    except Exception as e:
        return _json_result(
            {"status": "error", "message": f"Failed to initialize: {str(e)}"}
        )

    if connection is None or cache is None:
        raise RuntimeError("Connection or cache initialization failed")
    return _json_result(
        await execute_query_to_file.execute_query_to_file(
            connection,
            cache,
            sql=sql,
            file_path=file_path,
            database=database,
            schema=schema,
            timeout_seconds=timeout_seconds,
        )
    )


def main():
    """Main entry point for the application."""
    try:
        # Clear stale spill files left by a previous run before serving. Uses the
        # same TTL + FIFO policy as the per-spill sweep (it does NOT blindly wipe
        # the dir), so a just-restarted client whose task is mid-read keeps any
        # still-fresh file while genuinely old/over-cap leftovers are reclaimed.
        removed = query_executor.sweep_spill_dir()
        if removed:
            logger.info(f"Startup spill cleanup removed {removed} stale file(s)")

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
