"""Table inspection tool for detailed table and column information."""

import logging
from typing import Any

from server.schema_cache import ColumnInfo, SchemaCache
from server.snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)


async def describe_table(
    cache: SchemaCache,
    connection: SnowflakeConnection | None,
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any]:
    """
    Get detailed column information for a specific table.

    Uses a two-tier approach: if column details are already cached,
    returns them immediately. Otherwise, queries Snowflake live via
    DESCRIBE TABLE and caches the result.

    Args:
        cache: Schema cache instance
        connection: Snowflake connection (for on-demand column fetch)
        database: Database name
        schema: Schema name
        table: Table name

    Returns:
        Dictionary with detailed table schema
    """
    try:
        table_info = cache.get_table(database, schema, table)

        if not table_info:
            return {
                "status": "not_found",
                "message": f"Table {database}.{schema}.{table} not found in cache. Use refresh_catalog to update the cache. If you just refreshed cache, it's likely the table does not exist.",
                "database": database,
                "schema": schema,
                "table": table,
            }

        # On-demand column loading: fetch if columns are empty
        if not table_info.columns and connection is not None:
            logger.info(f"Fetching columns on-demand for {database}.{schema}.{table}")
            try:
                raw_columns = connection.get_table_columns(database, schema, table)
                if raw_columns:
                    table_info.columns = [
                        ColumnInfo(
                            name=col["name"],
                            data_type=col["type"],
                            is_nullable=col.get("nullable", True),
                            ordinal_position=i + 1,
                            comment=col.get("comment"),
                            default_value=col.get("default"),
                            is_primary_key=col.get("primary_key", False),
                        )
                        for i, col in enumerate(raw_columns)
                    ]
                    table_info.column_count = len(table_info.columns)
                    cache.save()
                    logger.info(
                        f"Cached {len(table_info.columns)} columns for "
                        f"{database}.{schema}.{table}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch columns for {database}.{schema}.{table}: {e}"
                )

        # Build response
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

        return {
            "status": "success",
            "database": table_info.database,
            "schema": table_info.schema,
            "table": table_info.table_name,
            "table_type": table_info.table_type,
            "columns": columns,
            "column_count": table_info.column_count,
            "comment": table_info.comment,
            "source": "cache" if columns else "table_metadata_only",
        }

    except Exception as e:
        logger.error(f"Failed to get table schema: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to get table schema: {str(e)}",
            "database": database,
            "schema": schema,
            "table": table,
        }
