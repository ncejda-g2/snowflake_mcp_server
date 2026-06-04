"""Schema inspection tool for exploring Snowflake database structures."""

import json
import logging
from collections import Counter
from typing import Any

from server.constants import (
    FIND_TABLES_INLINE_CHAR_BUDGET,
    FIND_TABLES_TOP_GROUPS,
    SHOW_TABLES_INLINE_CHAR_BUDGET,
    SHOW_TABLES_TOP_GROUPS,
)
from server.schema_cache import SchemaCache, TableInfo
from server.serialization import build_tsv
from server.snowflake_connection import SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog
from server.tools.query_executor import spill_json_to_disk, spill_rows_to_disk

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
# (identifier-length name, enum type), so the projection can never blow up
# regardless of input. To read a specific table's comment, the agent uses
# describe_table -- the route whose job is explaining one table, where that cost
# is justified.
#
# Deliberately NO ``columns`` count either. find_tables is a locator: its job is
# to point at the right ``full_name`` to then describe_table. A column *count*
# does not discriminate between candidate tables (you read describe_table for the
# real column list regardless), so it adds a token per row with no decision
# attached. It is also not loaded at refresh time, so dropping it lets the
# catalog scan skip its per-schema COLUMNS query entirely.
_FIND_TABLES_TSV_COLUMNS = ["full_name", "type"]


def _find_tables_row(table: TableInfo) -> dict[str, Any]:
    """Project a matched table into a flat row for both inline and file output.

    Keyed by :data:`_FIND_TABLES_TSV_COLUMNS` so the same dict drives the inline
    result rows and the spilled file (TSV with header).
    """
    return {
        "full_name": table.full_name,
        "type": table.table_type,
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

    The response is BYTE-BOUNDED: if the matching tree would exceed the inline
    budget (e.g. an unfiltered call, or a broad database_pattern matching tens of
    thousands of tables), the COMPLETE tree spills to a temp .json file and a
    compact, narrowing-focused summary is returned instead (totals, results_file,
    an adaptive top_databases/top_schemas breakdown, and a spilled hint).

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
        # The compact map -- {database: {schema: [table_name, ...]}} -- is the
        # canonical shape for BOTH the byte-budget gate and the spill file. It is
        # the most token-dense representation of the result (bare names, no
        # per-table object wrapping), so measuring it gates on the true floor of
        # what we would emit, and spilling it gives the agent the lightest,
        # most jq-navigable on-disk tree. Built once here, used for everything
        # below; the richer inline shapes are derived from it only when we are
        # already known to be under budget.
        compact: dict[str, dict[str, list[str]]] = {}
        total_tables = 0

        for database in cache.get_databases():
            # Apply database filter
            if database_pattern and database_pattern.upper() not in database.upper():
                continue

            db_schemas: dict[str, list[str]] = {}

            for schema in cache.get_schemas(database):
                # Apply schema filter
                if schema_pattern and schema_pattern.upper() not in schema.upper():
                    continue

                schema_tables: list[str] = []
                for table in cache.get_tables_in_schema(database, schema):
                    # Apply table filter
                    if (
                        table_pattern
                        and table_pattern.upper() not in table.table_name.upper()
                    ):
                        continue
                    schema_tables.append(table.table_name)
                    total_tables += 1

                if schema_tables:
                    db_schemas[schema] = schema_tables

            if db_schemas:
                compact[database] = db_schemas

        # No matches
        if not compact:
            return {
                "status": "no_results",
                "message": "No database objects found matching the specified filters",
                "filters": {
                    "database_pattern": database_pattern,
                    "schema_pattern": schema_pattern,
                    "table_pattern": table_pattern,
                },
            }

        # Gate on the EXACT compact-JSON bytes we would emit inline. show_tables
        # advertises a hard output ceiling: an unfiltered call -- or even a broad
        # database_pattern like "GDC" (30k+ tables) -- would otherwise serialize
        # to megabytes and blow the context budget. Over budget we spill the
        # COMPLETE tree to a .json file and return a bounded, narrowing-focused
        # summary instead of the full tree.
        if len(json.dumps(compact)) > SHOW_TABLES_INLINE_CHAR_BUDGET:
            return _spilled_show_tables_response(
                compact,
                total_tables,
                database_pattern,
                schema_pattern,
                table_pattern,
            )

        # Under budget: return the result inline. When filtering by database we
        # keep the ultra-compact map as-is; otherwise we enrich it with per-table
        # comments and per-schema/db counts (the fuller "browse everything"
        # shape). Both are safe here because we are already known to be under the
        # ceiling.
        if database_pattern is not None:
            return {
                "status": "success",
                "data": compact,
                "total_tables": total_tables,
            }

        return _detailed_inline_response(
            compact,
            cache,
            total_tables,
            database_pattern,
            schema_pattern,
            table_pattern,
        )

    except Exception as e:
        logger.error(f"Schema inspection failed: {str(e)}")
        return {"status": "error", "message": f"Failed to inspect schemas: {str(e)}"}


def _detailed_inline_response(
    compact: dict[str, dict[str, list[str]]],
    cache: SchemaCache,
    total_tables: int,
    database_pattern: str | None,
    schema_pattern: str | None,
    table_pattern: str | None,
) -> dict[str, Any]:
    """Enrich the compact map into the fuller browse shape for the inline case.

    Only reached when the result is already under the byte ceiling, so the extra
    per-table object wrapping and (rare) comments cannot reblow the budget. Adds
    per-table comments where present and per-schema/per-database counts, matching
    the historical no-filter response shape.
    """
    hierarchy: dict[str, Any] = {}
    for database, db_schemas in compact.items():
        enriched_schemas: dict[str, Any] = {}
        for schema, table_names in db_schemas.items():
            tables_out: list[dict[str, Any]] = []
            for name in table_names:
                table_info: dict[str, Any] = {"name": name}
                table = cache.get_table(database, schema, name)
                if table is not None and table.comment:
                    table_info["comment"] = table.comment
                tables_out.append(table_info)
            enriched_schemas[schema] = {
                "tables": tables_out,
                "table_count": len(tables_out),
            }
        hierarchy[database] = {
            "schemas": enriched_schemas,
            "schema_count": len(enriched_schemas),
            "total_tables": sum(len(v) for v in db_schemas.values()),
        }

    return {
        "status": "success",
        "hierarchy": hierarchy,
        "summary": {
            "databases": len(hierarchy),
            "total_schemas": sum(len(s) for s in compact.values()),
            "total_tables": total_tables,
        },
        "filters_applied": {
            "database_pattern": database_pattern,
            "schema_pattern": schema_pattern,
            "table_pattern": table_pattern,
        },
    }


def _show_tables_breakdown(
    compact: dict[str, dict[str, list[str]]], total_tables: int
) -> tuple[str, str]:
    """Build the bounded narrowing breakdown for a spilled show_tables result.

    Returns ``(key, line)`` where ``key`` is the summary field name and ``line``
    is a single bounded physical line. The axis is ADAPTIVE to what is left to
    narrow:

      * ONE database in the result -> the only remaining axis is the schema, so
        report the top-N ``database.schema`` groups by table count.
      * SEVERAL databases -> report the top-N databases by table count (showing
        even just two, e.g. ``GDC`` + ``GDC_TESTING``, is honest and actionable:
        the agent picks one and re-calls with a tighter database_pattern).

    Either way the breakdown is capped at :data:`SHOW_TABLES_TOP_GROUPS` with a
    ``(+X more ..., Y tables)`` tail marker, so it can never reblow the budget the
    spill exists to cap, no matter how many databases/schemas the account has.
    """
    if len(compact) == 1:
        ((_db, db_schemas),) = compact.items()
        counts = Counter(
            {f"{_db}.{schema}": len(names) for schema, names in db_schemas.items()}
        )
        return "top_schemas", _bounded_top_line(counts, total_tables, "schema")

    counts = Counter(
        {db: sum(len(n) for n in schemas.values()) for db, schemas in compact.items()}
    )
    return "top_databases", _bounded_top_line(counts, total_tables, "database")


def _bounded_top_line(counts: Counter, total_tables: int, noun: str) -> str:
    """Render top-N ``name=count`` pairs with a ``(+X more <noun>s, Y tables)`` tail.

    Bounded by :data:`SHOW_TABLES_TOP_GROUPS` regardless of how many groups
    exist, so the line length is capped no matter how sprawling the account is.
    """
    top = counts.most_common(SHOW_TABLES_TOP_GROUPS)
    parts = [f"{name}={count}" for name, count in top]

    remaining = len(counts) - len(top)
    if remaining > 0:
        remaining_tables = total_tables - sum(count for _name, count in top)
        noun_word = noun if remaining == 1 else f"{noun}s"
        parts.append(f"(+{remaining} more {noun_word}, {remaining_tables} tables)")

    return " ".join(parts)


def _spilled_show_tables_response(
    compact: dict[str, dict[str, list[str]]],
    total_tables: int,
    database_pattern: str | None,
    schema_pattern: str | None,
    table_pattern: str | None,
) -> dict[str, Any]:
    """Spill an over-budget show_tables tree and return a bounded summary.

    Writes the COMPLETE compact tree to a temp ``.json`` file (shared spill
    namespace, swept like every other spill) and returns ONLY bounded fields.
    Two tiers, gated by the SAME byte ceiling applied one level down:

      * tier B (menu fits): include the adaptive top-N breakdown
        (``top_schemas`` for a single-database result, else ``top_databases``)
        -- the agent's primary narrowing signal, every entry a valid next-call
        filter.
      * tier C (even the breakdown busts the budget): drop the list and return
        only totals + the file path. The breakdown is itself capped at
        SHOW_TABLES_TOP_GROUPS, so tier C is effectively unreachable in practice;
        it exists as a hard guarantee that the response is ALWAYS bounded.

    The static spill contract -- the file's ``{database: {schema: [table, ...]}}``
    shape and the jq / python recipes for reading it without loading every table
    name into context -- lives in the tool description (read once), not here
    (paid every spill). The summary carries only instance data plus a terse
    pointer back to those docs.
    """
    total_schemas = sum(len(schemas) for schemas in compact.values())
    spill_path = spill_json_to_disk(compact, infix="show")
    logger.warning(
        f"show_tables spilled to disk: {total_tables:,} tables across "
        f"{len(compact)} databases / {total_schemas} schemas -> {spill_path}"
    )

    response: dict[str, Any] = {
        "status": "success",
        "total_tables": total_tables,
        "total_schemas": total_schemas,
        "results_file": spill_path,
        "filters_applied": {
            "database_pattern": database_pattern,
            "schema_pattern": schema_pattern,
            "table_pattern": table_pattern,
        },
    }

    key, line = _show_tables_breakdown(compact, total_tables)
    # B->C fallback, gated by the SAME ceiling one level down: if even the
    # bounded breakdown line would bust the budget, drop it (tier C) and tell the
    # agent to read the file / re-call with a tighter filter.
    if len(line) <= SHOW_TABLES_INLINE_CHAR_BUDGET:
        response[key] = line
        response["spilled"] = (
            "too many tables to return inline; full tree in results_file. To "
            "narrow, re-call show_tables with a tighter database_pattern/"
            f"schema_pattern from {key} (see show_tables docs for jq/python "
            "recipes to read results_file)."
        )
    else:
        response["spilled"] = (
            "too many tables to return inline; full tree in results_file. To "
            "narrow, re-call show_tables with a tighter database_pattern/"
            "schema_pattern (see show_tables docs for jq/python recipes to read "
            "results_file)."
        )

    return response


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
