"""Schema inspection tool for exploring Snowflake database structures."""

import logging
from collections import Counter
from typing import Any

from server.constants import (
    FIND_TABLES_INLINE_CHAR_BUDGET,
    FIND_TABLES_TOP_GROUPS,
)
from server.schema_cache import SchemaCache, TableInfo
from server.serialization import build_tsv
from server.snowflake_connection import SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog
from server.tools.query_executor import spill_rows_to_disk

logger = logging.getLogger(__name__)

# TSV column order for spilled find_tables results. ``full_name`` folds
# database/schema/table into one greppable field the agent greps directly.
#
# Deliberately NO ``comment`` column. find_tables still MATCHES on comment text
# (see SchemaCache.search_tables) -- a cryptically-named table whose comment
# mentions the term is still found -- but the comment is not echoed in the
# output. Comment is the one unbounded, heavy-tailed field here (almost always
# null, occasionally a multi-KB doc-block), so echoing it reintroduces exactly
# the token blowup the spill exists to prevent. Every column kept is bounded
# (identifier-length name, enum type, integer count), so the projection can never
# blow up regardless of input. To read a specific table's comment, the agent uses
# describe_table -- the route whose job is explaining one table, where that cost
# is justified.
_FIND_TABLES_TSV_COLUMNS = ["full_name", "type", "columns"]


def _find_tables_row(table: TableInfo) -> dict[str, Any]:
    """Project a matched table into a flat row for both inline and file output.

    Keyed by :data:`_FIND_TABLES_TSV_COLUMNS` so the same dict drives the inline
    result rows and the spilled file (TSV with header).
    """
    return {
        "full_name": table.full_name,
        "type": table.table_type,
        "columns": table.column_count,
    }


def _top_group_breakdown(tables: list[TableInfo]) -> str:
    """Summarize where find_tables hits cluster, bounded to a fixed line length.

    Returns a single physical line of the top
    :data:`FIND_TABLES_TOP_GROUPS` ``database.schema`` groups by hit count, with
    a ``(+X more groups, Y hits)`` tail marker for everything outside the top-N::

        GDC.INTEGRATION=3801 GDC.PUBLIC=120 ... (+812 more groups, 1100 hits)

    The breakdown is the agent's primary narrowing signal: it maps onto
    show_tables's database_pattern + schema_pattern filters, and the tail marker
    distinguishes a concentrated result (scope to the top group) from a diffuse
    one (narrow the keyword). It is bounded by N regardless of how many groups
    matched, so it can never reblow the token budget the spill was meant to cap.
    """
    counts = Counter(f"{t.database}.{t.schema}" for t in tables)
    top = counts.most_common(FIND_TABLES_TOP_GROUPS)
    parts = [f"{group}={hits}" for group, hits in top]

    remaining_groups = len(counts) - len(top)
    if remaining_groups > 0:
        remaining_hits = len(tables) - sum(hits for _group, hits in top)
        group_word = "group" if remaining_groups == 1 else "groups"
        parts.append(f"(+{remaining_groups} more {group_word}, {remaining_hits} hits)")

    return " ".join(parts)


async def show_tables(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    database_pattern: str | None = None,
    schema_pattern: str | None = None,
    table_pattern: str | None = None,
) -> dict[str, Any]:
    """
    Browse databases, schemas, and tables using pattern-based filtering.

    This tool provides a hierarchical view of the database structure,
    with optional filtering by patterns. Similar to SQL's SHOW TABLES.

    NOTE: When database_pattern is used, column information is omitted
    to reduce response size and avoid token limits.

    Args:
        connection: Active Snowflake connection (for auto-refresh only)
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
            if database_pattern and database_pattern.upper() not in database.upper():
                continue

            db_schemas: dict[str, Any] = {}
            schemas = cache.get_schemas(database)

            for schema in schemas:
                # Apply schema filter
                if schema_pattern and schema_pattern.upper() not in schema.upper():
                    continue

                tables = cache.get_tables_in_schema(database, schema)
                schema_tables: list[Any] = []

                for table in tables:
                    # Apply table filter
                    if (
                        table_pattern
                        and table_pattern.upper() not in table.table_name.upper()
                    ):
                        continue

                    # When filtering by database, just use table names (most compact)
                    if not include_columns:
                        schema_tables.append(table.table_name)
                    else:
                        # Include full details when not filtering by database
                        table_info = {
                            "name": table.table_name,
                            "columns": table.column_count,
                        }
                        total_columns += table.column_count

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


async def find_tables(
    connection: SnowflakeConnection, cache: SchemaCache, search_term: str
) -> dict[str, Any]:
    """
    Search for tables by keyword across all databases.

    Searches both table names and comments for the specified keyword.

    Args:
        connection: Active Snowflake connection (for auto-refresh only)
        cache: Schema cache instance
        search_term: Keyword to search for in table names and comments

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

        # Flat rows shared by the inline payload and the spill file (same keys,
        # same order) so the two can never drift in shape.
        rows = [_find_tables_row(table) for table in matching_tables]

        # Gate on the EXACT TSV bytes we would emit inline. A generic term can
        # match thousands of tables (the worst output-token offender), but a
        # flood of matches means "term too broad" and the agent's next move is to
        # narrow -- it rarely needs to read every hit. So over budget we spill the
        # COMPLETE result to a file and return a compact, narrowing-focused
        # summary instead of a wall of matches.
        inline_tsv = build_tsv(rows, _FIND_TABLES_TSV_COLUMNS)
        if len(inline_tsv) > FIND_TABLES_INLINE_CHAR_BUDGET:
            return _spilled_find_tables_response(search_term, matching_tables, rows)

        # Under budget: return the full match set inline. ``comment`` is omitted
        # for the same reason it is absent from the spill projection: it is the
        # one unbounded, heavy-tailed field, and a single multi-KB doc-comment can
        # bloat an otherwise small result. find_tables still MATCHES on comment
        # text; to read a table's comment, use describe_table.
        results = [
            {
                "database": table.database,
                "schema": table.schema,
                "table": table.table_name,
                "type": table.table_type,
                "full_name": table.full_name,
                "columns": table.column_count,
            }
            for table in matching_tables
        ]
        return {
            "status": "success",
            "search_term": search_term,
            "results": results,
            "count": len(results),
        }

    except Exception as e:
        logger.error(f"Table search failed: {str(e)}")
        return {"status": "error", "message": f"Search failed: {str(e)}"}


def _spilled_find_tables_response(
    search_term: str,
    matching_tables: list[TableInfo],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Spill a too-broad find_tables result and return a narrowing-focused summary.

    Writes the COMPLETE match set to a temp ``.tsv`` file (shared spill namespace,
    swept like every other spill) and returns ONLY bounded fields: the true total
    hit count, a top-N ``database.schema`` breakdown with a tail marker, the file
    path, and a hint pointing at show_tables for scoped narrowing. Every field is
    bounded by a constant or a count, so the summary can never reblow the token
    budget that triggered the spill -- no matter how many tables matched.

    Deliberately NO example rows. When a search is too broad the agent's job is to
    NARROW, not to read individual hits, so sampling rows back inline both
    undercuts that message and (via the unbounded ``comment``) reintroduces the
    very blowup the spill prevents. The counts and breakdown drive the narrowing
    decision; the agent reads the file only if it genuinely wants the full set.
    """
    spill_path, written = spill_rows_to_disk(
        rows, _FIND_TABLES_TSV_COLUMNS, infix="find"
    )
    logger.warning(
        f"find_tables('{search_term}') spilled to disk: {written:,} matches "
        f"across {len({(t.database, t.schema) for t in matching_tables})} "
        f"database.schema groups -> {spill_path}"
    )
    return {
        "status": "success",
        "search_term": search_term,
        "total_hits": written,
        "results_file": spill_path,
        # Bounded top-N (db.schema) breakdown with a tail marker -- the agent's
        # primary signal for whether/how to narrow (maps to show_tables filters).
        "top_groups": _top_group_breakdown(matching_tables),
        # Too many hits to read inline. The actionable next step is to narrow via
        # show_tables, scoped using top_groups (database_pattern + schema_pattern)
        # and/or a more specific table_pattern -- not to blindly re-search.
        "spilled": (
            "too many matches to return inline; full results in results_file. "
            "To narrow, call show_tables with database_pattern/schema_pattern "
            "from top_groups and/or a more specific table_pattern."
        ),
    }
