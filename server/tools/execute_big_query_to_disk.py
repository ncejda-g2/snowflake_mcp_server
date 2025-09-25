"""Tool for executing large queries and streaming results directly to disk."""

import csv
import logging
import os
import time
from datetime import datetime
from typing import Any

import sqlparse

from server.constants import CSV_DELIMITER, CSV_INCLUDE_HEADERS, CSV_NULL_VALUE
from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryValidator, SnowflakeConnection

logger = logging.getLogger(__name__)

# Batch size for streaming large result sets
STREAMING_BATCH_SIZE = 10000


def _write_sql_file(sql: str, csv_path: str) -> dict[str, Any]:
    """
    Write the SQL query to a .sql file alongside the CSV file.

    Args:
        sql: The SQL query to write
        csv_path: The path of the CSV file (used to derive SQL file path)

    Returns:
        Dictionary with status of SQL file write operation
    """
    try:
        # Derive SQL file path from CSV path
        if csv_path.lower().endswith('.csv'):
            sql_path = csv_path[:-4] + '.sql'
        else:
            sql_path = csv_path + '.sql'

        # Format SQL for readability
        formatted_sql = sqlparse.format(
            sql,
            reindent=True,
            keyword_case='upper',
            strip_comments=False,
            use_space_around_operators=True,
            indent_width=2
        )

        # Ensure SQL ends with semicolon
        if not formatted_sql.rstrip().endswith(';'):
            formatted_sql = formatted_sql.rstrip() + ';'

        # Write SQL file
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(formatted_sql)
            f.write('\n')  # Add newline at end of file

        return {
            "status": "success",
            "sql_file_path": sql_path,
            "message": f"SQL query exported to {sql_path}"
        }

    except Exception as e:
        logger.warning(f"Failed to write SQL file: {str(e)}")
        return {
            "status": "warning",
            "message": f"Failed to export SQL file: {str(e)}"
        }


def _cleanup_partial_files(csv_path: str, sql_path: str | None = None) -> None:
    """
    Clean up partial files in case of error.

    Args:
        csv_path: Path to CSV file to clean up
        sql_path: Optional path to SQL file to clean up
    """
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logger.info(f"Cleaned up partial CSV file: {csv_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up CSV file {csv_path}: {str(e)}")

    if sql_path:
        try:
            if os.path.exists(sql_path):
                os.remove(sql_path)
                logger.info(f"Cleaned up partial SQL file: {sql_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up SQL file {sql_path}: {str(e)}")


async def execute_big_query_to_disk(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    file_path: str,
    database: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 300
) -> dict[str, Any]:
    """
    Execute a large read-only SQL query and stream results directly to a CSV file.

    This tool is designed for queries that return large result sets that would
    exceed token limits. It streams results directly to disk without returning
    the data in the response.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        sql: SQL query to execute
        file_path: The absolute or relative path where the CSV file should be saved
        database: Optional database context
        schema: Optional schema context
        timeout_seconds: Query timeout in seconds (default: 300, max: 3600)

    Returns:
        Dictionary with execution status, row count, and file size
    """
    # Validate timeout
    if timeout_seconds < 1:
        return {
            "status": "error",
            "message": "Timeout must be at least 1 second"
        }
    if timeout_seconds > 3600:
        return {
            "status": "error",
            "message": "Timeout cannot exceed 3600 seconds (1 hour)"
        }

    # First validate the query for safety (reuse existing validator)
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    if not is_valid:
        return {
            "status": "error",
            "message": error_msg,
            "query_type": str(query_type)
        }

    # Check cache is populated (required before queries)
    if cache.is_empty():
        return {
            "status": "error",
            "message": "Schema cache is empty. Please run refresh_catalog first.",
            "action_required": "refresh_catalog"
        }

    if cache.is_expired():
        logger.warning("Schema cache is expired, consider refreshing")

    # Expand the path if it contains ~ or environment variables
    expanded_path = os.path.expanduser(os.path.expandvars(file_path))

    # Derive SQL file path
    if expanded_path.lower().endswith('.csv'):
        sql_file_path = expanded_path[:-4] + '.sql'
    else:
        sql_file_path = expanded_path + '.sql'

    # Check if files already exist (before executing query to avoid costs)
    if os.path.exists(expanded_path):
        return {
            "status": "error",
            "message": f"CSV file already exists: {expanded_path}. Please use a different filename."
        }

    if os.path.exists(sql_file_path):
        return {
            "status": "error",
            "message": f"SQL file already exists: {sql_file_path}. Please use a different filename."
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
                "message": f"Failed to create directory {directory}: {str(e)}"
            }

    # Track execution
    start_time = time.time()
    row_count = 0
    column_names = []

    try:
        # Use context manager for CSV file
        with open(expanded_path, 'w', newline='', encoding='utf-8') as csvfile:
            logger.info(f"Executing query with {timeout_seconds}s timeout and streaming to {expanded_path}")

            # Set timeout for this query
            if connection.connection:
                with connection.connection.cursor() as cursor:
                    cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_seconds}")

            # Execute query using streaming
            try:
                first_batch = True
                csv_writer = None

                for batch in connection.execute_query_stream(
                    sql=sql,
                    database=database,
                    schema=schema,
                    batch_size=STREAMING_BATCH_SIZE
                ):
                    if first_batch:
                        # Get column names from first batch
                        if batch:
                            column_names = list(batch[0].keys())
                            csv_writer = csv.DictWriter(
                                csvfile,
                                fieldnames=column_names,
                                delimiter=CSV_DELIMITER,
                                restval=CSV_NULL_VALUE
                            )

                            # Write headers if configured
                            if CSV_INCLUDE_HEADERS:
                                csv_writer.writeheader()
                        first_batch = False

                    # Write batch to CSV
                    if csv_writer:
                        for row in batch:
                            # Convert values to strings, handling None and datetime
                            csv_row = {}
                            for col_name in column_names:
                                value = row.get(col_name)
                                if value is None:
                                    csv_row[col_name] = CSV_NULL_VALUE
                                elif isinstance(value, datetime):
                                    csv_row[col_name] = value.isoformat()
                                else:
                                    csv_row[col_name] = str(value)

                            csv_writer.writerow(csv_row)
                            row_count += 1

                    # Log progress every 100k rows
                    if row_count > 0 and row_count % 100000 == 0:
                        logger.info(f"Streamed {row_count:,} rows to disk...")

            finally:
                # Restore to default timeout (300 seconds)
                if connection.connection:
                    with connection.connection.cursor() as cursor:
                        cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300")

        # File is now closed by context manager
        # Get file size
        file_size = os.path.getsize(expanded_path)
        file_size_mb = file_size / (1024 * 1024)
        execution_time = time.time() - start_time

        # Write SQL file AFTER successful CSV completion
        sql_export_result = _write_sql_file(sql, expanded_path)

        # Prepare response
        response = {
            "status": "success",
            "message": f"Successfully executed query and exported {row_count:,} rows to CSV",
            "file_path": expanded_path,
            "row_count": row_count,
            "column_count": len(column_names) if column_names else 0,
            "file_size_mb": round(file_size_mb, 2),
            "execution_time_seconds": round(execution_time, 2),
        }

        # Add SQL export info to response
        if sql_export_result:
            response["sql_export"] = sql_export_result
            if sql_export_result.get("status") == "success":
                response["message"] += f" and SQL query to {sql_export_result.get('sql_file_path')}"

        logger.info(f"Completed: Exported {row_count:,} rows to {expanded_path} ({file_size_mb:.2f}MB) in {execution_time:.2f}s")

        return response

    except ValueError as e:
        # Query validation errors
        logger.error(f"Query validation failed: {str(e)}")
        _cleanup_partial_files(expanded_path, None)  # Don't clean up SQL file since we didn't create it
        return {
            "status": "error",
            "message": str(e),
            "error_type": "validation_error",
        }

    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        _cleanup_partial_files(expanded_path, None)  # Don't clean up SQL file since we didn't create it
        return {
            "status": "error",
            "message": f"Query execution failed: {str(e)}",
            "error_type": "execution_error",
        }
