"""Query execution tool for running read-only SQL queries."""

import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any

from server.constants import (
    MCP_CHAR_WARNING_THRESHOLD,
    SPILL_DIR,
    SPILL_PREVIEW_ROWS,
)
from server.schema_cache import SchemaCache
from server.serialization import (
    TSV_NULL,
    build_tsv,
    column_index_map,
    write_tsv_file,
)
from server.serialization import (
    column_names as _column_names,
)
from server.serialization import (
    format_value as _format_value,
)
from server.snowflake_connection import QueryValidator, SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logger = logging.getLogger(__name__)

# Re-exported for backward compatibility with callers/tests that imported these
# from query_executor before the serialization module existed.
__all__ = [
    "TSV_NULL",
    "_build_tsv",
    "_column_names",
    "_format_value",
    "execute_query",
]

# Backward-compatible aliases (the canonical implementations live in
# server.serialization now).
_build_tsv = build_tsv


def _spill_to_disk(
    rows: list[dict], names: list[str]
) -> tuple[str, int]:
    """Write the full result to a temp TSV file and return (path, rows_written).

    Same TSV format/escaping/NULL sentinel as the inline payload, so the agent
    can grep/awk/wc the file exactly as it would the inline block.
    """
    os.makedirs(SPILL_DIR, exist_ok=True)
    file_path = os.path.join(SPILL_DIR, f"query_{uuid.uuid4().hex}.tsv")
    written = write_tsv_file(file_path, rows, names)
    return file_path, written


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

    Auto-spill: when the full TSV would exceed the inline size threshold, the
    tool does NOT dump a wall of text or silently truncate. It writes the
    *complete* result to a temp ``.tsv`` file (identical format) and returns a
    one-row proof-of-shape preview plus ``results_file: /path``. The preview is
    deliberately a single row: for a spilled result the preview is never the
    answer, so it only needs to show column names and value formatting so the
    agent can write a correct grep/awk against the file. The agent reads/greps
    that file for the full data. NULL/empty semantics are identical on disk and
    inline.

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
            return build_text_response(
                status="success",
                fields={
                    "rows": 0,
                    "execution_time": round(result.execution_time, 4),
                    "message": "Query executed successfully but returned no results",
                },
                tsv=_build_tsv([], _column_names(result.columns)),
            )

        # Serialize the row data as TSV (header line + one line per row).
        # TSV is the payload the agent parses with grep/awk/cut, and is far more
        # token-efficient than an array-of-dicts (no per-row key repeat).
        column_names = _column_names(result.columns, result.data[0])
        tsv = _build_tsv(result.data, column_names)

        fields: dict[str, Any] = {
            "rows": len(result.data),
            "cols": len(column_names),
            "execution_time": round(result.execution_time, 4),
            "query_id": result.query_id,
        }

        # Auto-spill: if the full TSV is too large to return inline, write the
        # COMPLETE result to a temp .tsv file and return only a one-row
        # proof-of-shape preview plus the file path. We never dump a wall of
        # text and never silently truncate without telling the agent. The
        # preview is not a data sample: for a spilled result it can never be the
        # answer, so a single row (column shape + value formatting) is enough
        # for the agent to write a correct grep/awk and avoids wasting context.
        if len(tsv) > MCP_CHAR_WARNING_THRESHOLD:
            spill_path, written = _spill_to_disk(result.data, column_names)
            preview_count = min(SPILL_PREVIEW_ROWS, written)
            preview_tsv = _build_tsv(
                result.data[:SPILL_PREVIEW_ROWS], column_names
            )
            row_word = "row" if preview_count == 1 else "rows"
            fields["results_file"] = spill_path
            fields["preview_rows"] = preview_count
            # 1-based name->position map so the agent never has to count columns
            # by eye to write awk/cut (the most error-prone step on wide results).
            # Spill-only: small/mid inline results are narrow enough to eyeball.
            fields["column_index"] = column_index_map(column_names)
            fields["results_truncated_inline"] = (
                f"Full result ({written:,} rows, {len(tsv):,} chars) exceeds the inline "
                f"limit and was written to {spill_path}. The preview below is a "
                f"proof-of-shape sample ({preview_count} {row_word}), not the answer: "
                f"read/grep/awk the file for all rows "
                f"(same TSV format: \\t-delimited, NULL = \\N, one row per line). "
                f"To target a column by name, use the 1-based column_index map above "
                f'(e.g. 63=TYPE means awk -F\'\\t\' \'$63=="..."\'); do not count '
                f"columns by eye."
            )
            logger.warning(
                f"Query result spilled to disk: {written:,} rows, "
                f"{len(tsv):,} chars -> {spill_path}"
            )
            return build_text_response(
                status="success", fields=fields, tsv=preview_tsv
            )

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
