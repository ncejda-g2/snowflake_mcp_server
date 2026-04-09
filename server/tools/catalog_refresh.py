"""Catalog refresh tool for Snowflake schema discovery."""

import logging
from datetime import datetime

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

    Uses a two-tier approach: only table-level metadata is loaded eagerly
    (from INFORMATION_SCHEMA.TABLES + column counts via GROUP BY).
    Column details are fetched on-demand by describe_table.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        force: Force refresh even if cache is not expired (bypasses LAST_ALTERED check)
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
        processed_schemas: set[str] = set()
        failed_schemas: dict[str, str] = {}

        if resume:
            checkpoint_results, processed_schemas = cache.load_checkpoints()
            failed_schemas = cache.load_error_log()

            if checkpoint_results:
                logger.info(
                    f"Resuming from {len(processed_schemas)} checkpointed schemas"
                )
                # Group checkpoint results by schema and merge
                schema_groups: dict[str, list[dict]] = {}
                for row in checkpoint_results:
                    db = row.get("TABLE_CATALOG", row.get("table_catalog", ""))
                    sch = row.get("TABLE_SCHEMA", row.get("table_schema", ""))
                    key = f"{db}.{sch}"
                    schema_groups.setdefault(key, []).append(row)

                for schema_key, rows in schema_groups.items():
                    db, sch = schema_key.split(".", 1)
                    max_la = _compute_max_last_altered(rows)
                    cache.merge_schema_results(db, sch, rows, max_last_altered=max_la)

        logger.info("Starting catalog refresh...")

        # Get list of all databases
        databases = connection.get_databases()
        logger.info(f"Found {len(databases)} accessible databases")

        errors: dict[str, str] = {}
        schemas_scanned = 0
        schemas_skipped = 0
        total_tables = 0

        for database in databases:
            # Skip system databases
            if database.upper() in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                logger.debug(f"Skipping system database: {database}")
                continue

            try:
                schemas = connection.get_schemas(database)
            except Exception as e:
                error_msg = f"Failed to list schemas in {database}: {str(e)}"
                logger.warning(error_msg)
                errors[database] = error_msg
                continue

            # Filter out system schemas
            schemas = [
                s
                for s in schemas
                if s.upper() not in ("INFORMATION_SCHEMA",)
            ]

            for schema_name in schemas:
                schema_key = f"{database}.{schema_name}"

                # Skip if already processed from checkpoint
                if (
                    schema_key in processed_schemas
                    and schema_key not in failed_schemas
                ):
                    logger.debug(f"Skipping checkpointed schema: {schema_key}")
                    continue

                # LAST_ALTERED optimization: skip unchanged schemas
                if not force:
                    cached_max_la = cache.get_schema_last_altered(
                        database, schema_name
                    )
                    if cached_max_la and _schema_unchanged(
                        connection, database, schema_name, cached_max_la
                    ):
                        schemas_skipped += 1
                        logger.debug(f"Skipping unchanged schema: {schema_key}")
                        continue

                try:
                    logger.info(f"Scanning {schema_key}")

                    # Query 1: Table metadata (lightweight, no column details)
                    tables_query = f"""
                    SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
                           ROW_COUNT, BYTES, COMMENT, LAST_ALTERED
                    FROM {database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema_name}'
                    ORDER BY TABLE_NAME
                    """
                    tables_result = connection.execute_query(tables_query)

                    if tables_result.data:
                        # Query 2: Column counts per table (fast GROUP BY)
                        counts_query = f"""
                        SELECT TABLE_NAME, COUNT(*) as COLUMN_COUNT
                        FROM {database}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema_name}'
                        GROUP BY TABLE_NAME
                        """
                        counts_result = connection.execute_query(counts_query)

                        column_counts: dict[str, int] = {}
                        if counts_result.data:
                            column_counts = {
                                row["TABLE_NAME"]: int(row["COLUMN_COUNT"])
                                for row in counts_result.data
                            }

                        max_la = _compute_max_last_altered(tables_result.data)
                        table_count = cache.merge_schema_results(
                            database,
                            schema_name,
                            tables_result.data,
                            column_counts=column_counts,
                            max_last_altered=max_la,
                        )
                        cache.save_checkpoint(
                            database, schema_name, tables_result.data
                        )

                        processed_schemas.add(schema_key)
                        schemas_scanned += 1
                        total_tables += table_count

                        logger.info(
                            f"Scanned {schema_key}: {table_count} tables"
                        )
                    else:
                        # Schema exists but has no tables
                        cache.remove_schema(database, schema_name)
                        processed_schemas.add(schema_key)
                        schemas_scanned += 1

                    # Remove from failed if it was there
                    failed_schemas.pop(schema_key, None)

                except Exception as e:
                    error_msg = f"Failed to scan {schema_key}: {str(e)}"
                    logger.warning(error_msg)
                    errors[schema_key] = error_msg
                    continue

        # Save error log if there were any errors
        if errors:
            cache.save_error_log(errors)

        # Persist the incrementally-updated cache
        cache.last_refresh = datetime.now()
        cache.save()

        # Clear checkpoints on successful completion
        cache.clear_checkpoints()
        if not errors:
            cache.clear_error_log()

        stats = cache.get_statistics()

        return {
            "status": "success" if not errors else "partial_success",
            "message": f"Catalog refreshed {'successfully' if not errors else 'with some errors'}",
            "tables_found": total_tables,
            "schemas_scanned": schemas_scanned,
            "schemas_skipped": schemas_skipped,
            "databases_failed": len(
                {k.split(".")[0] for k in errors}
            ),
            "errors": list(errors.values()) if errors else None,
            "failed_schemas": list(errors.keys()) if errors else None,
            "statistics": stats,
        }

    except Exception as e:
        logger.error(f"Catalog refresh failed: {str(e)}")
        cache.refresh_in_progress = False
        return {"status": "error", "message": f"Catalog refresh failed: {str(e)}"}
    finally:
        cache.refresh_in_progress = False


def _compute_max_last_altered(rows: list[dict]) -> str | None:
    """Compute the max LAST_ALTERED from a list of result rows."""
    values = [
        str(r.get("LAST_ALTERED", r.get("last_altered", "")))
        for r in rows
        if r.get("LAST_ALTERED") or r.get("last_altered")
    ]
    return max(values) if values else None


def _schema_unchanged(
    connection: SnowflakeConnection,
    database: str,
    schema: str,
    cached_max_la: str,
) -> bool:
    """Check if a schema's max LAST_ALTERED matches the cached value.

    Returns True if unchanged (safe to skip), False otherwise.
    """
    try:
        result = connection.execute_query(
            f"""
            SELECT MAX(LAST_ALTERED) as MAX_LA
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            """
        )
        if result.data and result.data[0].get("MAX_LA"):
            return str(result.data[0]["MAX_LA"]) == cached_max_la
    except Exception:
        pass  # If the check fails, rescan to be safe
    return False
