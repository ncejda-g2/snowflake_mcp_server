"""Tool for executing a query and writing its results to a file on disk.

Use this when the caller wants the result as a shareable/persisted file at a
specific path, regardless of size -- a two-row lookup someone wants to send on,
or a multi-million-row export too big to return inline. ``execute_query`` itself
returns data to the agent (inline, or auto-spilled to a temp file); this tool is
the explicit "write the result to *this* path" intent.

The output format is chosen from the path's extension:

* ``.tsv`` (the default for any unrecognized/absent extension) -- the SAME
  format as the inline ``execute_query`` payload: tab-delimited, NULL rendered
  as ``\\N`` (distinct from an empty field), tabs/newlines/backslashes escaped,
  one row per line. A ``\\N`` learned inline means the same thing here and the
  agent can grep/awk/wc it identically.
* ``.csv`` -- a convenience format for humans/spreadsheets: RFC 4180
  comma-delimited with proper quoting, and SQL NULL rendered as an empty field
  (CSV has no portable NULL sentinel, so NULL and empty string are
  indistinguishable in CSV). Pick ``.tsv`` when that distinction matters.
"""

import logging
import os
import time
from typing import Any

from server.schema_cache import SchemaCache
from server.serialization import (
    column_names as _column_names,
)
from server.serialization import (
    open_export_writer,
    resolve_export_extension,
)
from server.snowflake_connection import QueryValidator, SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logger = logging.getLogger(__name__)

# Batch size for streaming result sets. Streaming is size-agnostic: a tiny
# result is written in a single batch, a huge one in many. No special "big" path.
STREAMING_BATCH_SIZE = 10000


def _cleanup_partial_file(data_path: str) -> None:
    """Remove a partially-written data file after an error."""
    try:
        if os.path.exists(data_path):
            os.remove(data_path)
            logger.info(f"Cleaned up partial TSV file: {data_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up TSV file {data_path}: {str(e)}")


async def execute_query_to_file(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    file_path: str,
    database: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """
    Execute a read-only SQL query and write its results to a file.

    Streams results directly to disk (no result data is held in memory beyond a
    batch, and none is returned in the response), so it works for any result
    size -- from a two-row file you want to share to a multi-GB export.

    The format follows the path's extension: ``.csv`` writes comma-delimited
    CSV (NULL = empty field); anything else writes ``.tsv`` -- the same format
    as the inline ``execute_query`` payload (tab-delimited, NULL = ``\\N``, one
    row per line).

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        sql: SQL query to execute
        file_path: The absolute or relative path where the file should be saved.
            A ``.csv`` extension selects CSV; otherwise ``.tsv`` is used and
            appended if the path has no recognized export extension.
        database: Optional database context
        schema: Optional schema context
        timeout_seconds: Query timeout in seconds (default: 300, max: 3600)

    Returns:
        Dictionary with execution status, row count, column count, and file size
    """
    # Validate timeout
    if timeout_seconds < 1:
        return {"status": "error", "message": "Timeout must be at least 1 second"}
    if timeout_seconds > 3600:
        return {
            "status": "error",
            "message": "Timeout cannot exceed 3600 seconds (1 hour)",
        }

    # First validate the query for safety (reuse existing validator)
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    if not is_valid:
        return {"status": "error", "message": error_msg, "query_type": str(query_type)}

    # Check cache and auto-refresh if needed
    if cache.is_expired() or cache.is_empty():
        logger.info("Cache is expired or empty, refreshing catalog...")
        refresh_result = await refresh_catalog(connection, cache, force=True)
        if refresh_result["status"] != "success":
            return {
                "status": "error",
                "message": "Failed to refresh catalog",
                "error": refresh_result.get("message"),
            }

    # Expand the path if it contains ~ or environment variables
    # Convert relative paths to absolute paths based on current working directory
    expanded_path = os.path.expanduser(os.path.expandvars(file_path))
    if not os.path.isabs(expanded_path):
        expanded_path = os.path.abspath(expanded_path)

    # Pick the export format from the extension the caller chose: `.csv` exports
    # comma-delimited CSV, anything else defaults to `.tsv` (the agent's native
    # format). The resolved extension is appended only when the path does not
    # already end with a recognized one, so the on-disk format stays
    # self-describing without clobbering a deliberate choice.
    export_extension = resolve_export_extension(expanded_path)
    if not expanded_path.lower().endswith(export_extension):
        expanded_path = expanded_path + export_extension

    # Check if file already exists (before executing query to avoid costs)
    if os.path.exists(expanded_path):
        return {
            "status": "error",
            "message": f"File already exists: {os.path.abspath(expanded_path)}. Please use a different filename.",
        }

    # Create directory if it doesn't exist
    directory = os.path.dirname(expanded_path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to create directory {directory}: {str(e)}",
            }

    # Track execution
    start_time = time.time()
    row_count = 0
    column_names = []

    try:
        # newline="" is required so the CSV writer controls line endings; it is
        # harmless for TSV (which writes explicit "\n").
        with open(expanded_path, "w", encoding="utf-8", newline="") as outfile:
            logger.info(f"Streaming query results to {os.path.abspath(expanded_path)}")

            writer = None

            for batch in connection.execute_query_stream(
                sql=sql,
                database=database,
                schema=schema,
                batch_size=STREAMING_BATCH_SIZE,
            ):
                if not batch:
                    continue

                if writer is None:
                    # Establish column order from the first non-empty batch and
                    # write the header in the resolved format.
                    column_names = _column_names(None, batch[0])
                    writer = open_export_writer(
                        outfile, column_names, export_extension
                    )

                # Write batch rows in the established column order.
                for row in batch:
                    writer.write_row(row)
                    row_count += 1

                # Log progress every 100k rows
                if row_count > 0 and row_count % 100000 == 0:
                    logger.info(f"Streamed {row_count:,} rows to disk...")

        # File is now closed by context manager
        file_size = os.path.getsize(expanded_path)
        file_size_mb = file_size / (1024 * 1024)
        execution_time = time.time() - start_time

        response = {
            "status": "success",
            "message": f"Successfully executed query and wrote {row_count:,} rows to {os.path.abspath(expanded_path)}",
            "file_path": os.path.abspath(expanded_path),  # Always return absolute path
            "row_count": row_count,
            "column_count": len(column_names) if column_names else 0,
            "file_size_mb": round(file_size_mb, 2),
            "execution_time_seconds": round(execution_time, 2),
        }

        logger.info(
            f"Completed: wrote {row_count:,} rows to {os.path.abspath(expanded_path)} ({file_size_mb:.2f}MB) in {execution_time:.2f}s"
        )

        return response

    except ValueError as e:
        # Query validation errors
        logger.error(f"Query validation failed: {str(e)}")
        _cleanup_partial_file(expanded_path)
        return {
            "status": "error",
            "message": str(e),
            "error_type": "validation_error",
        }

    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        _cleanup_partial_file(expanded_path)
        return {
            "status": "error",
            "message": f"Query execution failed: {str(e)}",
            "error_type": "execution_error",
        }
