"""Schema inspection tool for exploring Snowflake database structures."""

import logging
from typing import Any

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logger = logging.getLogger(__name__)


async def inspect_schemas(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    database_pattern: str | None = None,
    schema_pattern: str | None = None,
    table_pattern: str | None = None,
) -> dict[str, Any]:
    """
    List available databases, schemas, and tables from cache.

    This tool provides a hierarchical view of the database structure,
    with optional filtering by patterns.

    NOTE: When database_pattern is used, column information is omitted
    to reduce response size and avoid token limits.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        database_pattern: Filter databases by pattern (case-insensitive)
        schema_pattern: Filter schemas by pattern
        table_pattern: Filter tables by pattern

    Returns:
        Dictionary with hierarchical structure of matching database objects
    """
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

    try:
        # Build hierarchical structure
        hierarchy: dict[str, Any] = {}
        total_tables = 0
        total_columns = 0

        # When filtering by database, omit column info to reduce response size
        include_columns = database_pattern is None

        # Get all databases from cache
        databases = cache.get_databases()

        for database in databases:
            # Apply database filter
            if database_pattern:
                if database_pattern.upper() not in database.upper():
                    continue

            db_schemas: dict[str, Any] = {}
            schemas = cache.get_schemas(database)

            for schema in schemas:
                # Apply schema filter
                if schema_pattern:
                    if schema_pattern.upper() not in schema.upper():
                        continue

                tables = cache.get_tables_in_schema(database, schema)
                schema_tables: list[Any] = []

                for table in tables:
                    # Apply table filter
                    if table_pattern:
                        if table_pattern.upper() not in table.table_name.upper():
                            continue

                    # When filtering by database, just use table names (most compact)
                    if not include_columns:
                        schema_tables.append(table.table_name)
                    else:
                        # Include full details when not filtering by database
                        table_info = {
                            "name": table.table_name,
                            "columns": len(table.columns),
                        }
                        total_columns += len(table.columns)

                        # Only include comment if it exists
                        if table.comment:
                            table_info["comment"] = table.comment

                        schema_tables.append(table_info)

                    total_tables += 1

                if schema_tables:
                    # When filtering by database, use most compact format
                    if not include_columns:
                        db_schemas[schema] = (
                            schema_tables  # Just the list of table names
                        )
                    else:
                        db_schemas[schema] = {
                            "tables": schema_tables,
                            "table_count": len(schema_tables),
                        }

            if db_schemas:
                # When filtering by database, use ultra-compact format
                if not include_columns:
                    hierarchy[database] = db_schemas  # Just the schemas dict
                else:
                    hierarchy[database] = {
                        "schemas": db_schemas,
                        "schema_count": len(db_schemas),
                        "total_tables": sum(
                            s["table_count"] for s in db_schemas.values()
                        ),
                    }

        # Format results
        if not hierarchy:
            return {
                "status": "no_results",
                "message": "No database objects found matching the specified filters",
                "filters": {
                    "database_pattern": database_pattern,
                    "schema_pattern": schema_pattern,
                    "table_pattern": table_pattern,
                },
            }

        # Simpler response when filtering by database
        if not include_columns:
            return {
                "status": "success",
                "data": hierarchy,
                "total_tables": total_tables,
            }

        # Full response when not filtering
        summary = {
            "databases": len(hierarchy),
            "total_schemas": sum(db["schema_count"] for db in hierarchy.values()),
            "total_tables": total_tables,
            "total_columns": total_columns,
        }

        return {
            "status": "success",
            "hierarchy": hierarchy,
            "summary": summary,
            "filters_applied": {
                "database_pattern": database_pattern,
                "schema_pattern": schema_pattern,
                "table_pattern": table_pattern,
            },
        }

    except Exception as e:
        logger.error(f"Schema inspection failed: {str(e)}")
        return {"status": "error", "message": f"Failed to inspect schemas: {str(e)}"}


async def search_tables(
    connection: SnowflakeConnection, cache: SchemaCache, search_term: str
) -> dict[str, Any]:
    """
    Search for tables across all databases.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        search_term: Term to search for in table names and comments

    Returns:
        Dictionary with search results
    """
    # Check cache
    if cache.is_expired() or cache.is_empty():
        logger.info("Cache is expired or empty, refreshing catalog...")
        refresh_result = await refresh_catalog(connection, cache, force=True)
        if refresh_result["status"] != "success":
            return {
                "status": "error",
                "message": "Failed to refresh catalog",
                "error": refresh_result.get("message"),
            }

    try:
        # Search for matching tables
        matching_tables = cache.search_tables(search_term)

        if not matching_tables:
            return {
                "status": "no_results",
                "message": f"No tables found matching '{search_term}'",
                "search_term": search_term,
            }

        # Format results
        results = []
        for table in matching_tables:
            results.append(
                {
                    "database": table.database,
                    "schema": table.schema,
                    "table": table.table_name,
                    "type": table.table_type,
                    "full_name": table.full_name,
                    "columns": len(table.columns),
                    "comment": table.comment,
                }
            )

        return {
            "status": "success",
            "search_term": search_term,
            "results": results,
            "count": len(results),
        }

    except Exception as e:
        logger.error(f"Table search failed: {str(e)}")
        return {"status": "error", "message": f"Search failed: {str(e)}"}
