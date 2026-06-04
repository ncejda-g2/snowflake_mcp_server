"""Tests for server/tools/schema_inspector.py -- find_tables auto-spill.

find_tables searches table names + comments across the WHOLE cache, so a generic
term can match thousands of tables: historically the single worst output-token
offender. These tests pin the budget-gated behavior:

  * UNDER budget  -> full match set inline, unchanged dict shape.
  * OVER budget   -> spill the COMPLETE result to a shared-namespace .tsv file and
                     return a compact, narrowing-focused summary (total hits,
                     bounded top-N db.schema breakdown with tail marker, examples,
                     file path, show_tables hint).
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from server.schema_cache import SchemaCache, TableInfo
from server.snowflake_connection import SnowflakeConnection
from server.tools import query_executor, schema_inspector
from server.tools.schema_inspector import find_tables


@pytest.fixture
def temp_cache_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def cache(temp_cache_dir):
    """A real (non-expired, non-empty) cache so find_tables skips refresh."""
    c = SchemaCache(ttl_days=5, cache_dir=temp_cache_dir)
    # Seed one table so is_empty() is False; individual tests add their own.
    c.add_table(
        TableInfo(
            database="SEED_DB",
            schema="SEED_SCHEMA",
            table_name="SEED_TABLE",
            table_type="BASE TABLE",
            columns=[],
            column_count=1,
            comment=None,
        )
    )
    from datetime import datetime

    c.last_refresh = datetime.now()
    return c


@pytest.fixture
def connection():
    """find_tables only uses the connection for the (here-skipped) refresh path."""
    return Mock(spec=SnowflakeConnection)


def _add_tables(cache: SchemaCache, specs: list[tuple[str, str, str]]) -> None:
    """Add (database, schema, table) tuples as cached tables."""
    for database, schema, table in specs:
        cache.add_table(
            TableInfo(
                database=database,
                schema=schema,
                table_name=table,
                table_type="BASE TABLE",
                columns=[],
                column_count=3,
                comment=None,
            )
        )


class TestFindTablesInline:
    """Under-budget results return the full match set inline, unchanged."""

    @pytest.mark.asyncio
    async def test_small_result_returned_inline(self, cache, connection):
        _add_tables(cache, [("DB1", "S1", "PRODUCT_A"), ("DB1", "S1", "PRODUCT_B")])

        result = await find_tables(connection, cache, "PRODUCT")

        assert result["status"] == "success"
        assert result["count"] == 2
        assert "results" in result
        assert "results_file" not in result
        full_names = {r["full_name"] for r in result["results"]}
        assert full_names == {"DB1.S1.PRODUCT_A", "DB1.S1.PRODUCT_B"}
        # comment is never echoed -- it is the one unbounded field.
        assert all("comment" not in r for r in result["results"])
        # a column count does not help locate; it is not echoed either.
        assert all("columns" not in r for r in result["results"])

    @pytest.mark.asyncio
    async def test_no_results(self, cache, connection):
        result = await find_tables(connection, cache, "ZZZ_NO_MATCH")
        assert result["status"] == "no_results"
        assert result["search_term"] == "ZZZ_NO_MATCH"

    @pytest.mark.asyncio
    async def test_matches_on_comment_but_omits_it(self, cache, connection):
        """A cryptically-named table is still FOUND via its comment, yet the
        (potentially huge) comment is not returned in the result."""
        cache.add_table(
            TableInfo(
                database="DB1",
                schema="S1",
                table_name="PRD_CCT_ATK_ARS",  # opaque name, no "customer" in it
                table_type="BASE TABLE",
                columns=[],
                column_count=3,
                comment="Contains product customer counts " + ("x" * 5000),
            )
        )

        result = await find_tables(connection, cache, "CUSTOMER")

        # Found purely via the comment...
        assert result["count"] == 1
        assert result["results"][0]["full_name"] == "DB1.S1.PRD_CCT_ATK_ARS"
        # ...but the 5KB comment is NOT echoed back.
        assert "comment" not in result["results"][0]


class TestFindTablesSpill:
    """Over-budget results spill the full set and return a compact summary."""

    @pytest.mark.asyncio
    async def test_large_result_spills_and_summarizes(
        self, cache, connection, tmp_path
    ):
        # 200 matches across 3 db.schema groups, concentrated in GDC.INTEGRATION.
        specs = [("GDC", "INTEGRATION", f"PRODUCT_{i}") for i in range(150)]
        specs += [("GDC", "PUBLIC", f"PRODUCT_{i}") for i in range(40)]
        specs += [("SALES", "WEB", f"PRODUCT_{i}") for i in range(10)]
        _add_tables(cache, specs)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(schema_inspector, "FIND_TABLES_INLINE_CHAR_BUDGET", 100),
        ):
            result = await find_tables(connection, cache, "PRODUCT")

        # Compact summary, not a wall of matches.
        assert result["status"] == "success"
        assert result["total_hits"] == 200
        assert "results" not in result  # full set is NOT inline
        assert "spilled" in result and "show_tables" in result["spilled"]

        # The spill file holds the COMPLETE result and lives in the shared
        # namespace (so the single sweep cleans it up).
        spill_path = result["results_file"]
        assert os.path.exists(spill_path)
        assert os.path.basename(spill_path).startswith(query_executor._SPILL_PREFIX)
        with open(spill_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        assert lines[0] == "full_name\ttype"  # TSV header, no comment, no count
        assert len(lines) == 1 + 200  # header + every match

    @pytest.mark.asyncio
    async def test_spill_summary_is_bounded_fields_only(
        self, cache, connection, tmp_path
    ):
        """The spill return carries only bounded fields -- no example rows (which
        would reintroduce unbounded comment text and undercut 'go narrow')."""
        specs = [("GDC", "INTEGRATION", f"P_{i}") for i in range(150)]
        specs += [("GDC", "PUBLIC", f"P_{i}") for i in range(40)]
        specs += [("SALES", "WEB", f"P_{i}") for i in range(10)]
        _add_tables(cache, specs)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(schema_inspector, "FIND_TABLES_INLINE_CHAR_BUDGET", 100),
            patch.object(schema_inspector, "FIND_TABLES_TOP_GROUPS", 2),
        ):
            result = await find_tables(connection, cache, "P_")

        top = result["top_groups"]
        # Top-2 groups by hit count, dominant one first.
        assert "GDC.INTEGRATION=150" in top
        assert "GDC.PUBLIC=40" in top
        # The 3rd group is collapsed into the tail marker, not listed.
        assert "SALES.WEB=10" not in top
        assert "(+1 more group, 10 hits)" in top

        # No example rows in the summary -- counts/breakdown drive narrowing.
        assert "examples" not in result
        assert "example_rows" not in result
        # The returned keys are exactly the bounded summary set.
        assert set(result) == {
            "status",
            "search_term",
            "total_hits",
            "results_file",
            "top_groups",
            "spilled",
        }

    @pytest.mark.asyncio
    async def test_no_tail_marker_when_all_groups_fit(
        self, cache, connection, tmp_path
    ):
        # 2 groups, top-N cap of 5 -> every group shown, no tail marker.
        specs = [("DB1", "S1", f"WIDGET_{i}") for i in range(60)]
        specs += [("DB2", "S2", f"WIDGET_{i}") for i in range(60)]
        _add_tables(cache, specs)

        with (
            patch.object(query_executor, "SPILL_DIR", str(tmp_path)),
            patch.object(schema_inspector, "FIND_TABLES_INLINE_CHAR_BUDGET", 100),
            patch.object(schema_inspector, "FIND_TABLES_TOP_GROUPS", 5),
        ):
            result = await find_tables(connection, cache, "WIDGET")

        assert "more group" not in result["top_groups"]
        assert "DB1.S1=60" in result["top_groups"]
        assert "DB2.S2=60" in result["top_groups"]
