"""Table inspection tool for detailed table and column information."""

import logging
from typing import Any

from server.schema_cache import SchemaCache

logger = logging.getLogger(__name__)


async def describe_table(
    cache: SchemaCache,
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any]:
    """
    Get detailed column information for a specific table from cache only.

    This tool provides comprehensive schema information including
    column names, types, and constraints from the cache only.
    Never queries Snowflake directly for table details.

    Note: To get sample data, use the execute_query tool separately.

    Args:
        cache: Schema cache instance
        database: Database name
        schema: Schema name
        table: Table name

    Returns:
        Dictionary with detailed table schema from cache
    """
    try:
        # Get from cache only - never query Snowflake directly
        table_info = cache.get_table(database, schema, table)

        if not table_info:
            return {
                "status": "not_found",
                "message": f"Table {database}.{schema}.{table} not found in cache. Use refresh_catalog to update the cache. If you just refreshed cache, it's likely the table does not exist.",
                "database": database,
                "schema": schema,
                "table": table,
            }

        # Build response from cache
        if table_info:
            # Build response from cache
            columns = []
            for col in table_info.columns:
                columns.append(
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": col.is_nullable,
                        "position": col.ordinal_position,
                        "default": col.default_value,
                        "comment": col.comment,
                        "is_primary_key": col.is_primary_key,
                    }
                )

            result = {
                "status": "success",
                "database": table_info.database,
                "schema": table_info.schema,
                "table": table_info.table_name,
                "table_type": table_info.table_type,
                "columns": columns,
                "column_count": len(columns),
                "comment": table_info.comment,
                "source": "cache",
            }

        return result

    except Exception as e:
        logger.error(f"Failed to get table schema: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to get table schema: {str(e)}",
            "database": database,
            "schema": schema,
            "table": table,
        }
