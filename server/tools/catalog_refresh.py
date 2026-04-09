"""Catalog refresh tool for Snowflake schema discovery."""

import contextlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

import snowflake.connector
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import DictCursor

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)

# Max concurrent async INFORMATION_SCHEMA queries on a single connection.
# These queries are served by Snowflake's cloud services layer (not the
# warehouse), so high concurrency is safe and free.
MAX_ASYNC_QUERIES = 50

# Polling interval when waiting for async query results
POLL_INTERVAL_SECONDS = 0.5


@dataclass
class _SchemaJob:
    """Tracks the async queries for a single schema scan."""

    database: str
    schema: str
    # Phase 1: TABLES query
    tables_qid: str | None = None
    tables_cursor: DictCursor | None = None
    tables_data: list[dict] | None = None
    # Phase 2: COLUMNS count query (submitted after TABLES completes)
    counts_qid: str | None = None
    counts_cursor: DictCursor | None = None
    column_counts: dict[str, int] = field(default_factory=dict)
    # Result
    done: bool = False
    error: str | None = None

    @property
    def schema_key(self) -> str:
        return f"{self.database}.{self.schema}"


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

    Scans up to MAX_ASYNC_QUERIES schemas concurrently using Snowflake's
    async query API on a single connection.

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

        # Get list of all databases and collect schemas to scan
        databases = connection.get_databases()
        logger.info(f"Found {len(databases)} accessible databases")

        errors: dict[str, str] = {}
        schemas_skipped = 0

        work_items: list[tuple[str, str]] = []

        for database in databases:
            if database.upper() in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                continue

            try:
                schemas = connection.get_schemas(database)
            except Exception as e:
                error_msg = f"Failed to list schemas in {database}: {str(e)}"
                logger.warning(error_msg)
                errors[database] = error_msg
                continue

            schemas = [s for s in schemas if s.upper() not in ("INFORMATION_SCHEMA",)]

            for schema_name in schemas:
                schema_key = f"{database}.{schema_name}"

                if schema_key in processed_schemas and schema_key not in failed_schemas:
                    continue

                if not force:
                    cached_max_la = cache.get_schema_last_altered(database, schema_name)
                    if cached_max_la and _schema_unchanged(
                        connection, database, schema_name, cached_max_la
                    ):
                        schemas_skipped += 1
                        logger.debug(f"Skipping unchanged schema: {schema_key}")
                        continue

                work_items.append((database, schema_name))

        logger.info(
            f"Scanning {len(work_items)} schemas "
            f"({MAX_ASYNC_QUERIES} concurrent async queries)"
        )

        # Run the async scan
        schemas_scanned, total_tables, scan_errors = _scan_schemas_async(
            connection, cache, work_items
        )
        errors.update(scan_errors)

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
            "message": (
                "Catalog refreshed "
                f"{'successfully' if not errors else 'with some errors'}"
            ),
            "tables_found": total_tables,
            "schemas_scanned": schemas_scanned,
            "schemas_skipped": schemas_skipped,
            "databases_failed": len({k.split(".")[0] for k in errors}),
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


def _scan_schemas_async(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    work_items: list[tuple[str, str]],
) -> tuple[int, int, dict[str, str]]:
    """Scan schemas using async queries on a single connection.

    Submits TABLES queries in batches of MAX_ASYNC_QUERIES, polls for
    completion, then submits COLUMNS count queries for each completed
    TABLES result. Merges into cache as results arrive.

    Returns:
        (schemas_scanned, total_tables, errors)
    """
    if not work_items:
        return 0, 0, {}

    conn = connection.connection
    if conn is None:
        return 0, 0, {"_connection": "Not connected to Snowflake"}

    errors: dict[str, str] = {}
    schemas_scanned = 0
    total_tables = 0

    # Queue of work items not yet submitted
    queue = list(work_items)
    # Active jobs: schema_key -> _SchemaJob
    active: dict[str, _SchemaJob] = {}

    while queue or active:
        # Fill up to MAX_ASYNC_QUERIES active jobs
        while queue and len(active) < MAX_ASYNC_QUERIES:
            database, schema = queue.pop(0)
            job = _SchemaJob(database=database, schema=schema)
            try:
                _submit_tables_query(conn, job)
                active[job.schema_key] = job
            except Exception as e:
                errors[job.schema_key] = (
                    f"Failed to submit query for {job.schema_key}: {e}"
                )
                logger.warning(errors[job.schema_key])

        if not active:
            break

        # Poll active jobs
        completed_keys: list[str] = []
        for key, job in active.items():
            try:
                if job.error:
                    completed_keys.append(key)
                    continue

                if job.tables_data is None:
                    # Phase 1: waiting for TABLES query
                    status = conn.get_query_status_throw_if_error(job.tables_qid)
                    if status == QueryStatus.SUCCESS:
                        job.tables_cursor.get_results_from_sfqid(job.tables_qid)
                        job.tables_data = job.tables_cursor.fetchall()
                        job.tables_cursor.close()
                        job.tables_cursor = None

                        if job.tables_data:
                            # Submit phase 2: COLUMNS count query
                            _submit_counts_query(conn, job)
                        else:
                            # Empty schema
                            job.done = True

                elif not job.done:
                    # Phase 2: waiting for COLUMNS count query
                    status = conn.get_query_status_throw_if_error(job.counts_qid)
                    if status == QueryStatus.SUCCESS:
                        job.counts_cursor.get_results_from_sfqid(job.counts_qid)
                        rows = job.counts_cursor.fetchall()
                        job.counts_cursor.close()
                        job.counts_cursor = None
                        if rows:
                            job.column_counts = {
                                row["TABLE_NAME"]: int(row["COLUMN_COUNT"])
                                for row in rows
                            }
                        job.done = True

                if job.done:
                    completed_keys.append(key)

            except Exception as e:
                job.error = f"Failed to scan {key}: {e}"
                logger.warning(job.error)
                # Clean up cursors
                for cursor in (job.tables_cursor, job.counts_cursor):
                    if cursor:
                        with contextlib.suppress(Exception):
                            cursor.close()
                completed_keys.append(key)

        # Process completed jobs
        for key in completed_keys:
            job = active.pop(key)

            if job.error:
                errors[key] = job.error
                continue

            if job.tables_data:
                max_la = _compute_max_last_altered(job.tables_data)
                table_count = cache.merge_schema_results(
                    job.database,
                    job.schema,
                    job.tables_data,
                    column_counts=job.column_counts,
                    max_last_altered=max_la,
                )
                cache.save_checkpoint(job.database, job.schema, job.tables_data)
                total_tables += table_count
                logger.info(f"Scanned {key}: {table_count} tables")
            else:
                cache.remove_schema(job.database, job.schema)

            schemas_scanned += 1

        # Don't spin — wait before polling again
        if active:
            time.sleep(POLL_INTERVAL_SECONDS)

    return schemas_scanned, total_tables, errors


def _submit_tables_query(
    conn: snowflake.connector.SnowflakeConnection, job: _SchemaJob
) -> None:
    """Submit an async TABLES metadata query for a schema."""
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute_async(
        f"""
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
               ROW_COUNT, BYTES, COMMENT, LAST_ALTERED
        FROM {job.database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{job.schema}'
        ORDER BY TABLE_NAME
        """
    )
    job.tables_qid = cursor.sfqid
    job.tables_cursor = cursor


def _submit_counts_query(
    conn: snowflake.connector.SnowflakeConnection, job: _SchemaJob
) -> None:
    """Submit an async COLUMNS count query for a schema."""
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute_async(
        f"""
        SELECT TABLE_NAME, COUNT(*) as COLUMN_COUNT
        FROM {job.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{job.schema}'
        GROUP BY TABLE_NAME
        """
    )
    job.counts_qid = cursor.sfqid
    job.counts_cursor = cursor


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
    """Check if a schema's max LAST_ALTERED matches the cached value."""
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
        pass
    return False
