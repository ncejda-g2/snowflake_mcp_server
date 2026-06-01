"""Query execution tool for running read-only SQL queries."""

import json
import logging
import re
import sys
from datetime import datetime
from typing import Any

from server.constants import MAX_CACHE_SIZE_BYTES, MCP_CHAR_WARNING_THRESHOLD
from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryValidator, SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logger = logging.getLogger(__name__)

# In-memory cache for the last query results (for CSV export)
last_query_cache: dict[str, Any] | None = None


def _estimate_size(obj: Any) -> int:
    """Estimate the memory size of an object in bytes."""
    return sys.getsizeof(json.dumps(obj, default=str))


def _format_value(value: Any) -> Any:
    """Format a value for JSON serialization."""
    if value is None:
        return None
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    else:
        return value


# Sentinel rendered for SQL NULL in TSV output. Distinguishable from the empty
# string (an actual zero-length value), which renders as a literal empty field.
TSV_NULL = "\\N"


def _column_names(columns: Any, sample_row: Any = None) -> list[str]:
    """Extract ordered column names from result.columns.

    result.columns is normally a list of {"name", "type", "nullable"} dicts
    (see SnowflakeConnection.execute_query). Some callers/tests pass a plain
    list of name strings. Fall back to the keys of a sample row dict.
    """
    if columns:
        first = columns[0]
        if isinstance(first, dict):
            return [str(c.get("name", "")) for c in columns]
        return [str(c) for c in columns]
    if isinstance(sample_row, dict):
        return list(sample_row.keys())
    return []


def _tsv_escape(value: Any) -> str:
    """Render a single cell value as a TSV-safe field.

    Guarantees exactly one logical line per row and clean tab-delimited fields
    so the LLM can rely on awk -F'\\t' / cut -f / grep. Tabs, newlines, carriage
    returns and backslashes are escaped reversibly. SQL NULL becomes TSV_NULL so
    it is distinguishable from an empty string.
    """
    if value is None:
        return TSV_NULL
    if not isinstance(value, str):
        value = str(value)
    # Backslash first so we don't double-escape the escapes we add next.
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def _build_tsv(rows: list[dict], column_names: list[str]) -> str:
    """Build a TSV block: header line of column names + one line per row.

    Values are pulled positionally by column name so the field order matches the
    header exactly. Designed for direct piping into grep/awk/cut by the agent.
    """
    lines = ["\t".join(_tsv_escape(name) for name in column_names)]
    for row in rows:
        lines.append(
            "\t".join(
                _tsv_escape(_format_value(row.get(name))) for name in column_names
            )
        )
    return "\n".join(lines)


def build_text_response(
    status: str,
    fields: dict[str, Any],
    tsv: str | None = None,
) -> str:
    """Assemble the final text payload returned to the agent.

    Format::

        status: success
        rows: 50
        cols: 92
        ...
        ---
        <TSV header line>
        <TSV data lines...>

    The header is a flat, one-per-line ``key: value`` block (trivially greppable
    and human-skimmable). When ``tsv`` is provided it follows a ``---`` separator.
    Multi-line field values (e.g. a warning) are kept on their key's line by
    collapsing internal newlines, preserving the one-record-per-line guarantee.
    """
    header_lines = [f"status: {status}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", " ").strip()
        header_lines.append(f"{key}: {text}")

    if tsv is None:
        return "\n".join(header_lines)
    return "\n".join(header_lines) + "\n---\n" + tsv


async def execute_query(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    database: str | None = None,
    schema: str | None = None,
) -> str:
    """
    Execute a read-only SQL query with safety checks.

    This tool validates queries for read-only operations, executes them,
    and returns all results as a compact, line-oriented text payload.

    The response is a small ``key: value`` metadata header followed by a
    ``---`` separator and a TSV block (header line of column names + one line
    per row). Returning text (rather than a dict) both compresses the payload
    and suppresses FastMCP's duplicate ``structuredContent`` serialization.

    The TSV block is designed to be parsed directly with grep/awk/cut, e.g.
    ``awk -F'\\t' 'NR>1 && $3=="X"'``. NULLs render as ``\\N``; tabs/newlines in
    values are backslash-escaped so each row stays on exactly one line.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        sql: SQL query to execute
        database: Optional database context
        schema: Optional schema context

    Returns:
        A text payload: metadata header + ``---`` + TSV result block.
    """
    # First validate the query for safety (before checking cache)
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    if not is_valid:
        return build_text_response(
            status="error",
            fields={"message": error_msg, "query_type": str(query_type)},
        )

    # Check cache and auto-refresh if needed
    if cache.is_expired() or cache.is_empty():
        logger.info("Cache is expired or empty, refreshing catalog...")
        refresh_result = await refresh_catalog(connection, cache, force=True)
        if refresh_result["status"] != "success":
            return build_text_response(
                status="error",
                fields={
                    "message": "Failed to refresh catalog",
                    "error": refresh_result.get("message"),
                },
            )

    try:
        # Execute query
        result = connection.execute_query(sql=sql, database=database, schema=schema)

        if not result.data:
            # No results, but still clear the cache for consistency
            global last_query_cache
            last_query_cache = None

            return build_text_response(
                status="success",
                fields={
                    "rows": 0,
                    "execution_time": round(result.execution_time, 4),
                    "message": "Query executed successfully but returned no results",
                },
                tsv=_build_tsv([], _column_names(result.columns)),
            )

        # Check if results exceed cache size limit for CSV export
        cache_data = {
            "all_results": result.data,
            "columns": result.columns,
            "sql": sql,
            "database": database,
            "schema": schema,
            "cached_at": datetime.now().isoformat(),
            "execution_time": result.execution_time,
            "query_id": result.query_id,
        }

        estimated_size = _estimate_size(cache_data)
        cache_exceeded = estimated_size > MAX_CACHE_SIZE_BYTES

        # Update last_query_cache for CSV export
        if cache_exceeded:
            # Store metadata only in last_query_cache with warning
            last_query_cache = {
                "status": "size_exceeded",
                "message": f"Query results ({estimated_size / (1024**3):.2f}GB) exceed cache limit ({MAX_CACHE_SIZE_BYTES / (1024**3):.2f}GB)",
                "sql": sql,
                "database": database,
                "schema": schema,
                "cached_at": datetime.now().isoformat(),
                "row_count": len(result.data),
            }
            logger.warning(
                f"Query results too large for caching: {estimated_size / (1024**3):.2f}GB"
            )
            csv_available = False
            csv_message = (
                f"Results too large for CSV export ({estimated_size / (1024**3):.2f}GB exceeds "
                f"{MAX_CACHE_SIZE_BYTES / (1024**3):.2f}GB limit). Consider using execute_big_query_to_disk "
                f"to stream large results directly to a file."
            )
        else:
            # Store full results for CSV export
            last_query_cache = cache_data
            logger.debug(
                f"Cached query results for CSV export: {estimated_size / (1024**2):.2f}MB"
            )
            csv_available = True
            csv_message = (
                "Results cached and ready for CSV export using save_last_query_to_csv"
            )

        # Serialize the row data as TSV (header line + one line per row).
        # TSV is the payload the agent parses with grep/awk/cut, and is far more
        # token-efficient than the previous array-of-dicts (no per-row key repeat).
        column_names = _column_names(result.columns, result.data[0])
        tsv = _build_tsv(result.data, column_names)

        # Check if response might exceed MCP token limits. We measure the TSV
        # block since that is the bulk of what actually gets serialized now.
        response_size_estimate = len(tsv)

        # Add warning if approaching estimated token limits (at 80% threshold)
        token_warning = None
        if response_size_estimate > MCP_CHAR_WARNING_THRESHOLD:
            token_warning = (
                f"Response size ({response_size_estimate:,} chars) is approaching typical MCP token limits. "
                f"For larger result sets, consider using execute_big_query_to_disk to stream results directly to a file, "
                f"or add a LIMIT clause to reduce the result set size."
            )
            logger.warning(
                f"Query response approaching token limits: {response_size_estimate:,} chars"
            )

        fields: dict[str, Any] = {
            "rows": len(result.data),
            "cols": len(column_names),
            "execution_time": round(result.execution_time, 4),
            "query_id": result.query_id,
            "csv_export": "available" if csv_available else "unavailable",
            "csv_message": csv_message,
        }
        if token_warning:
            fields["token_limit_warning"] = token_warning

        return build_text_response(status="success", fields=fields, tsv=tsv)

    except ValueError as e:
        # Query validation errors
        return build_text_response(
            status="error",
            fields={"error_type": "validation_error", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        return build_text_response(
            status="error",
            fields={
                "error_type": "execution_error",
                "message": f"Query execution failed: {str(e)}",
            },
        )


async def validate_query_without_execution(
    connection: SnowflakeConnection,  # noqa: ARG001
    cache: SchemaCache,
    sql: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """
    Generate and prepare a SQL query without executing it.

    This tool can generate ANY type of SQL query (including write operations like INSERT,
    UPDATE, DELETE) but does not execute it. Useful for generating queries that users
    want to review and execute elsewhere after manual review.

    Note: While this tool can generate write queries, the execute_query tool will still
    block them from actual execution for safety.

    Args:
        connection: Active Snowflake connection (for context, not execution)
        cache: Schema cache instance
        sql: SQL query to validate and prepare
        database: Optional database context
        schema: Optional schema context

    Returns:
        Dictionary with validation results and the prepared query
    """
    # Check query type but don't block write operations in this tool
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)

    # For this tool, we allow all query types but note whether it's read-only
    validation_result = {
        "is_read_only": is_valid,
        "query_type": str(query_type),
        "execution_allowed": is_valid,  # Only read-only queries can be executed via execute_query
        "message": "Query generated successfully. Write queries cannot be executed through the MCP server."
        if not is_valid
        else "Query is read-only and can be executed via execute_query",
    }

    # Check if cache is populated (recommended but not required for validation)
    cache_status: dict[str, Any] = {
        "is_populated": not cache.is_empty(),
        "is_expired": cache.is_expired() if not cache.is_empty() else None,
    }

    if cache.is_empty():
        cache_status["warning"] = (
            "Schema cache is empty. Consider running refresh_catalog for better validation."
        )

    # Prepare the final query with context if provided
    final_query = sql.strip()
    if final_query.endswith(";"):
        final_query = final_query[:-1]

    # Add database/schema context comment if provided
    context_info = []
    if database:
        context_info.append(f"Database: {database}")
    if schema:
        context_info.append(f"Schema: {schema}")

    if context_info:
        final_query = f"-- Context: {', '.join(context_info)}\n{final_query}"

    # Try to extract table references from the query (basic parsing)
    table_references = []
    try:
        # Simple regex to find potential table names after FROM/JOIN
        from_pattern = r"\b(?:FROM|JOIN)\s+([^\s,()]+)"
        matches = re.findall(from_pattern, sql.upper())
        for match in matches:
            # Clean up and add to references
            table_ref = match.strip().replace('"', "").replace("`", "")
            if table_ref and not table_ref.startswith("("):
                table_references.append(table_ref)
    except Exception:
        pass  # Ignore parsing errors

    # Build response
    response = {
        "status": "success",
        "query": final_query,
        "validation": validation_result,
        "cache_status": cache_status,
        "metadata": {
            "database_context": database,
            "schema_context": schema,
            "table_references": list(set(table_references)) if table_references else [],
            "query_length": len(sql),
            "estimated_complexity": "simple"
            if len(table_references) <= 1
            else "moderate"
            if len(table_references) <= 3
            else "complex",
        },
    }

    # Add syntax hints if query might need adjustment
    hints = []
    if (
        database
        and schema
        and not any(
            x in sql.upper()
            for x in [
                f"{database.upper()}.{schema.upper()}",
                "USE DATABASE",
                "USE SCHEMA",
            ]
        )
    ):
        hints.append(
            f"Consider using fully qualified table names: {database}.{schema}.table_name"
        )

    if "LIMIT" not in sql.upper() and str(query_type) == "QueryType.SELECT":
        hints.append("Consider adding a LIMIT clause to control result size")

    if hints:
        response["hints"] = hints

    # Add appropriate note about execution based on query type
    if is_valid:
        response["note"] = (
            "This read-only query has been generated and can be executed using execute_query."
        )
    else:
        response["note"] = (
            "This write query has been generated but CANNOT be executed through the MCP server. Please review and execute it directly in Snowflake after manual verification."
        )

    return response


def get_last_query_cache() -> dict[str, Any] | None:
    """
    Get the cached results from the last executed query.

    Returns:
        The cached query data or None if no cache exists
    """
    return last_query_cache


async def get_query_history(
    connection: SnowflakeConnection, limit: int = 10, only_successful: bool = True
) -> dict[str, Any]:
    """
    Get the history of executed queries.

    Args:
        connection: Active Snowflake connection
        limit: Maximum number of queries to return
        only_successful: Only return successful queries

    Returns:
        Dictionary with query history
    """
    try:
        history = connection.get_query_history(
            limit=limit, only_successful=only_successful
        )

        if not history:
            return {
                "status": "success",
                "message": "No query history available",
                "history": [],
            }

        # Format history for response
        formatted_history = []
        for entry in history:
            formatted_entry = {
                "timestamp": datetime.fromtimestamp(entry["timestamp"]).isoformat(),
                "sql": entry["sql"],
                "status": entry.get("status", "unknown"),
                "execution_time": entry.get("execution_time"),
                "row_count": entry.get("row_count"),
                "database": entry.get("database"),
                "schema": entry.get("schema"),
                "error": entry.get("error"),
            }
            formatted_history.append(formatted_entry)

        return {
            "status": "success",
            "history": formatted_history,
            "count": len(formatted_history),
            "limit": limit,
            "filter": "successful_only" if only_successful else "all",
        }

    except Exception as e:
        logger.error(f"Failed to get query history: {str(e)}")
        return {"status": "error", "message": f"Failed to get query history: {str(e)}"}
