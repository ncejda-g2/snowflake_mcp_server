"""Table inspection tool for detailed table and column information."""

import logging

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)


async def get_table_schema(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    database: str,
    schema: str,
    table: str,
    include_sample: bool = False,
    sample_rows: int = 5
) -> dict:
    """
    Get detailed column information for a specific table.
    
    This tool provides comprehensive schema information including
    column names, types, constraints, and optionally sample data.
    
    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        database: Database name
        schema: Schema name
        table: Table name
        include_sample: Whether to include sample data
        sample_rows: Number of sample rows to include (default 5)
        
    Returns:
        Dictionary with detailed table schema and optionally sample data
    """
    try:
        # First try to get from cache
        table_info = cache.get_table(database, schema, table)

        if not table_info:
            # If not in cache, try direct query
            logger.info(f"Table {database}.{schema}.{table} not in cache, querying directly")

            # Get column information
            columns = connection.get_table_columns(database, schema, table)

            if not columns:
                return {
                    "status": "not_found",
                    "message": f"Table {database}.{schema}.{table} not found or not accessible",
                    "database": database,
                    "schema": schema,
                    "table": table
                }

            # Build response from direct query
            result = {
                "status": "success",
                "database": database,
                "schema": schema,
                "table": table,
                "columns": columns,
                "source": "direct_query"
            }
        else:
            # Build response from cache
            columns = []
            for col in table_info.columns:
                columns.append({
                    "name": col.name,
                    "type": col.data_type,
                    "nullable": col.is_nullable,
                    "position": col.ordinal_position,
                    "default": col.default_value,
                    "comment": col.comment,
                    "is_primary_key": col.is_primary_key
                })

            result = {
                "status": "success",
                "database": table_info.database,
                "schema": table_info.schema,
                "table": table_info.table_name,
                "table_type": table_info.table_type,
                "columns": columns,
                "column_count": len(columns),
                "comment": table_info.comment,
                "source": "cache"
            }

        # Add sample data if requested
        if include_sample:
            try:
                sample_query = f"""
                SELECT *
                FROM {database}.{schema}.{table}
                LIMIT {sample_rows}
                """

                sample_result = connection.execute_query(sample_query)

                if sample_result.data:
                    result["sample_data"] = {
                        "rows": sample_result.data,
                        "row_count": len(sample_result.data),
                        "columns": [col['name'] for col in sample_result.columns]
                    }
                else:
                    result["sample_data"] = {
                        "message": "Table is empty or no data accessible",
                        "row_count": 0
                    }

            except Exception as e:
                result["sample_data"] = {
                    "error": f"Failed to retrieve sample data: {str(e)}"
                }

        return result

    except Exception as e:
        logger.error(f"Failed to get table schema: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to get table schema: {str(e)}",
            "database": database,
            "schema": schema,
            "table": table
        }


async def describe_table(
    connection: SnowflakeConnection,
    full_table_name: str,
    include_stats: bool = False
) -> dict:
    """
    Describe a table using its fully qualified name.
    
    Args:
        connection: Active Snowflake connection
        full_table_name: Fully qualified table name (database.schema.table)
        include_stats: Whether to include table statistics
        
    Returns:
        Dictionary with table description
    """
    try:
        # Parse the full table name
        parts = full_table_name.split('.')
        if len(parts) != 3:
            return {
                "status": "error",
                "message": "Invalid table name format. Expected: database.schema.table",
                "provided": full_table_name
            }

        database, schema, table = parts

        # Use DESCRIBE command
        describe_query = f"DESCRIBE TABLE {database}.{schema}.{table}"
        result = connection.execute_query(describe_query)

        if not result.data:
            return {
                "status": "not_found",
                "message": f"Table {full_table_name} not found",
                "table": full_table_name
            }

        # Format column information
        columns = []
        for row in result.data:
            columns.append({
                "name": row.get('name'),
                "type": row.get('type'),
                "kind": row.get('kind', 'COLUMN'),
                "nullable": row.get('null?') == 'Y',
                "default": row.get('default'),
                "primary_key": row.get('primary key') == 'Y',
                "unique_key": row.get('unique key') == 'Y',
                "check": row.get('check'),
                "expression": row.get('expression'),
                "comment": row.get('comment')
            })

        response = {
            "status": "success",
            "table": full_table_name,
            "database": database,
            "schema": schema,
            "table_name": table,
            "columns": columns,
            "column_count": len(columns)
        }

        # Add table statistics if requested
        if include_stats:
            try:
                stats_query = f"""
                SELECT 
                    ROW_COUNT,
                    BYTES,
                    LAST_ALTERED,
                    CREATED,
                    COMMENT
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
                """

                stats_result = connection.execute_query(stats_query)

                if stats_result.data and len(stats_result.data) > 0:
                    stats = stats_result.data[0]
                    response["statistics"] = {
                        "row_count": stats.get('ROW_COUNT'),
                        "size_bytes": stats.get('BYTES'),
                        "last_altered": str(stats.get('LAST_ALTERED')) if stats.get('LAST_ALTERED') else None,
                        "created": str(stats.get('CREATED')) if stats.get('CREATED') else None,
                        "comment": stats.get('COMMENT')
                    }

            except Exception as e:
                response["statistics"] = {
                    "error": f"Failed to retrieve statistics: {str(e)}"
                }

        return response

    except Exception as e:
        logger.error(f"Failed to describe table: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to describe table: {str(e)}",
            "table": full_table_name
        }
