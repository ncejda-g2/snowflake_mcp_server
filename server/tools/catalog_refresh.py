"""Catalog refresh tool for Snowflake schema discovery."""

import logging

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)


async def refresh_catalog(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    force: bool = False,
    resume: bool = True,
) -> dict:
    """
    Refresh the schema catalog cache by scanning all accessible databases.

    This tool queries INFORMATION_SCHEMA across all databases to build
    a comprehensive index of tables, schemas, and columns. Supports
    checkpointing for reliability and resume capability.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        force: Force refresh even if cache is not expired
        resume: Whether to resume from checkpoints if they exist

    Returns:
        Dictionary with refresh results and statistics
    """
    # Check if refresh is needed
    if not force and not cache.is_expired() and not cache.is_empty():
        stats = cache.get_statistics()
        return {
            "status": "cache_valid",
            "message": "Cache is still valid and not expired",
            "statistics": stats,
        }

    # Check if refresh is already in progress
    if cache.refresh_in_progress:
        return {
            "status": "in_progress",
            "message": "Catalog refresh is already in progress",
        }

    try:
        cache.refresh_in_progress = True

        # Check for existing checkpoints to resume from
        checkpoint_results: list[dict] = []
        processed_databases: set[str] = set()
        failed_databases = {}

        if resume:
            checkpoint_results, processed_databases = cache.load_checkpoints()
            failed_databases = cache.load_error_log()

            if checkpoint_results:
                logger.info(
                    f"Resuming from {len(processed_databases)} checkpointed databases"
                )
                logger.info(
                    f"Found {len(failed_databases)} previously failed databases to retry"
                )

        logger.info("Starting catalog refresh...")

        # Get list of all databases
        databases = connection.get_databases()
        logger.info(f"Found {len(databases)} accessible databases")

        all_results = checkpoint_results.copy()  # Start with checkpoint results
        errors = {}

        # Query each database's INFORMATION_SCHEMA
        for database in databases:
            try:
                # Skip system databases
                if database.upper() in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                    logger.debug(f"Skipping system database: {database}")
                    continue

                # Skip if already processed (from checkpoint)
                if database in processed_databases and database not in failed_databases:
                    logger.debug(f"Skipping already processed database: {database}")
                    continue

                logger.info(f"Querying INFORMATION_SCHEMA for database: {database}")

                # Query to get all tables and columns in this database
                # Join TABLES and COLUMNS to get complete metadata
                query = f"""
                SELECT
                    c.TABLE_CATALOG,
                    c.TABLE_SCHEMA,
                    c.TABLE_NAME,
                    t.TABLE_TYPE,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    c.ORDINAL_POSITION,
                    c.COLUMN_DEFAULT,
                    c.COMMENT as COLUMN_COMMENT,
                    t.COMMENT as TABLE_COMMENT,
                    t.ROW_COUNT,
                    t.BYTES
                FROM {database}.INFORMATION_SCHEMA.COLUMNS c
                JOIN {database}.INFORMATION_SCHEMA.TABLES t
                    ON c.TABLE_CATALOG = t.TABLE_CATALOG
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                    AND c.TABLE_NAME = t.TABLE_NAME
                WHERE c.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
                ORDER BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
                """

                logger.info(f"Executing query for {database}")
                result = connection.execute_query(query)

                if result.data:
                    # Save checkpoint immediately after successful query
                    cache.save_checkpoint(database, result.data)

                    all_results.extend(result.data)
                    processed_databases.add(database)

                    # Remove from failed databases if it was there
                    if database in failed_databases:
                        del failed_databases[database]

                    logger.info(
                        f"Retrieved {len(result.data)} column definitions from {database}"
                    )

            except Exception as e:
                error_msg = f"Failed to query database {database}: {str(e)}"
                logger.warning(error_msg)
                errors[database] = error_msg
                continue

        # Save error log if there were any errors
        if errors:
            cache.save_error_log(errors)

        # Update cache with results
        if all_results:
            table_count = cache.update_from_information_schema(all_results)

            # Clear checkpoints and error log on successful completion
            cache.clear_checkpoints()
            if not errors:  # Only clear error log if no new errors
                cache.clear_error_log()

            stats = cache.get_statistics()

            return {
                "status": "success" if not errors else "partial_success",
                "message": f"Catalog refreshed {'successfully' if not errors else 'with some errors'}",
                "tables_found": table_count,
                "databases_scanned": len(processed_databases),
                "databases_failed": len(errors),
                "errors": list(errors.values()) if errors else None,
                "failed_databases": list(errors.keys()) if errors else None,
                "statistics": stats,
            }
        else:
            return {
                "status": "error",
                "message": "No schema information retrieved",
                "errors": list(errors.values())
                if errors
                else ["No databases could be accessed"],
                "failed_databases": list(errors.keys()) if errors else None,
            }

    except Exception as e:
        logger.error(f"Catalog refresh failed: {str(e)}")
        cache.refresh_in_progress = False
        return {"status": "error", "message": f"Catalog refresh failed: {str(e)}"}
    finally:
        cache.refresh_in_progress = False
