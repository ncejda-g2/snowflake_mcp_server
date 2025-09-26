"""Tool for saving query results to CSV files."""

import csv
import logging
import os
from datetime import datetime
from typing import Any

import sqlparse

from server.constants import CSV_DELIMITER, CSV_INCLUDE_HEADERS, CSV_NULL_VALUE
from server.tools.query_executor import get_last_query_cache

logger = logging.getLogger(__name__)


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
        # Replace .csv extension with .sql, or append .sql if no .csv extension
        if csv_path.lower().endswith(".csv"):
            sql_path = csv_path[:-4] + ".sql"
        else:
            sql_path = csv_path + ".sql"

        # Format SQL for readability
        formatted_sql = sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            strip_comments=False,
            use_space_around_operators=True,
            indent_width=2,
        )

        # Ensure SQL ends with semicolon
        if not formatted_sql.rstrip().endswith(";"):
            formatted_sql = formatted_sql.rstrip() + ";"

        # Write SQL file
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(formatted_sql)
            f.write("\n")  # Add newline at end of file

        return {
            "status": "success",
            "sql_file_path": os.path.abspath(sql_path),  # Always return absolute path
            "message": f"SQL query exported to {os.path.abspath(sql_path)}",
        }

    except Exception as e:
        logger.warning(f"Failed to write SQL file: {str(e)}")
        return {"status": "warning", "message": f"Failed to export SQL file: {str(e)}"}


async def save_last_query_to_csv(
    file_path: str, export_sql: bool = True
) -> dict[str, Any]:
    """
    Save the last executed query results to a CSV file.

    This tool exports the complete results from the most recently executed query
    to a CSV file at the specified path. The query must have been executed
    successfully and its results must be within the cache size limit.

    Args:
        file_path: The absolute or relative path where the CSV file should be saved
        export_sql: Whether to also export the SQL query to a .sql file (default: True)

    Returns:
        Dictionary with status and information about the export
    """
    try:
        # Get the cached query results
        cache = get_last_query_cache()

        if cache is None:
            return {
                "status": "error",
                "message": "No query has been executed yet. Please execute a query first using execute_query.",
            }

        # Check if cache indicates size exceeded
        if cache.get("status") == "size_exceeded":
            return {
                "status": "error",
                "message": cache.get(
                    "message", "Query results exceeded cache size limit"
                ),
                "row_count": cache.get("row_count", "unknown"),
            }

        # Extract data and columns
        results = cache.get("all_results", [])
        columns = cache.get("columns", [])

        if not results:
            return {
                "status": "warning",
                "message": "Last query returned no results to export",
                "sql": cache.get("sql", "")[:200] + "..."
                if len(cache.get("sql", "")) > 200
                else cache.get("sql", ""),
            }

        if not columns:
            return {
                "status": "error",
                "message": "No column information available for the last query",
            }

        # Get column names
        column_names = [col.get("name", f"column_{i}") for i, col in enumerate(columns)]

        # Expand the path if it contains ~ or environment variables
        # Convert relative paths to absolute paths based on current working directory
        expanded_path = os.path.expanduser(os.path.expandvars(file_path))
        if not os.path.isabs(expanded_path):
            expanded_path = os.path.abspath(expanded_path)

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

        # Write to CSV file
        try:
            with open(expanded_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=column_names,
                    delimiter=CSV_DELIMITER,
                    restval=CSV_NULL_VALUE,
                )

                # Write headers if configured
                if CSV_INCLUDE_HEADERS:
                    writer.writeheader()

                # Write data rows
                row_count = 0
                for row in results:
                    # Convert row to use column names as keys
                    csv_row = {}
                    for col_name in column_names:
                        value = row.get(col_name)
                        if value is None:
                            csv_row[col_name] = CSV_NULL_VALUE
                        elif isinstance(value, datetime):
                            csv_row[col_name] = value.isoformat()
                        else:
                            csv_row[col_name] = str(value)

                    writer.writerow(csv_row)
                    row_count += 1

            # Get file size
            file_size = os.path.getsize(expanded_path)
            file_size_mb = file_size / (1024 * 1024)

            # Export SQL file if requested
            sql_export_result = None
            sql_query = cache.get("sql")
            if export_sql and sql_query:
                sql_export_result = _write_sql_file(sql_query, expanded_path)

            # Prepare response
            response = {
                "status": "success",
                "message": f"Successfully exported {row_count} rows to CSV",
                "file_path": os.path.abspath(expanded_path),  # Always return absolute path
                "row_count": row_count,
                "column_count": len(column_names),
                "file_size_mb": round(file_size_mb, 2),
                "query_info": {
                    "sql": cache.get("sql", "")[:200] + "..."
                    if len(cache.get("sql", "")) > 200
                    else cache.get("sql", ""),
                    "database": cache.get("database"),
                    "schema": cache.get("schema"),
                    "cached_at": cache.get("cached_at"),
                    "execution_time": cache.get("execution_time"),
                },
            }

            # Add SQL export info to response
            if sql_export_result:
                response["sql_export"] = sql_export_result
                if sql_export_result.get("status") == "success":
                    current_message = str(response["message"])
                    response["message"] = current_message + (
                        f" and SQL query to {sql_export_result.get('sql_file_path')}"
                    )
                elif sql_export_result.get("status") == "warning":
                    response["sql_export_warning"] = sql_export_result.get("message")

            logger.info(
                f"Exported {row_count} rows to {os.path.abspath(expanded_path)} ({file_size_mb:.2f}MB)"
            )
            if sql_export_result and sql_export_result.get("status") == "success":
                logger.info(
                    f"Exported SQL query to {sql_export_result.get('sql_file_path')}"
                )

            return response

        except PermissionError:
            return {
                "status": "error",
                "message": f"Permission denied: Cannot write to {os.path.abspath(expanded_path)}",
            }
        except OSError as e:
            return {"status": "error", "message": f"Failed to write CSV file: {str(e)}"}

    except Exception as e:
        logger.error(f"Unexpected error during CSV export: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error during CSV export: {str(e)}",
        }
