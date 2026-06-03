#!/usr/bin/env python3
"""Reproducible token-cost measurement for the execute_query path.

Measures, for a synthetic 50-row x 92-column query, the token cost an MCP
client pays in three forms:

  * BEFORE  -- the legacy format this project shipped before the TSV work:
               an array-of-dicts JSON body, double-serialized by FastMCP into
               both ``content`` (text) and ``structuredContent`` (the same data
               again). This reconstructs that exact shape; it is the baseline
               PR #50 reported against.
  * AFTER   -- what the CURRENT code actually returns for the same query, by
               driving the real ``execute_query`` code path with a mocked
               connection. For 50x92 this auto-spills (92 cols > the inline
               column gate), so the payload is a tiny header + 1-row preview +
               column_index map + file path.

Two scopes are reported, matching PR #50's table:
  * "Payload alone" -- one serialization of the tool result body.
  * "Full envelope" -- what actually crosses the wire. For the BEFORE format
    that is the body serialized TWICE (FastMCP mirrored every dict into both
    ``content`` text and ``structuredContent``). The AFTER path emits a single
    ``TextContent`` and no ``structuredContent``, so its envelope == payload.

Run: ``python scripts/measure_envelope.py``  (needs tiktoken, already a dev dep)
"""

import asyncio
import json
from unittest.mock import Mock

import tiktoken

from server.schema_cache import SchemaCache
from server.snowflake_connection import QueryType, QueryValidator, SnowflakeConnection
from server.tools import query_executor

ENC = tiktoken.get_encoding("cl100k_base")

N_ROWS = 50
N_COLS = 92


def tok(text: str) -> int:
    return len(ENC.encode(text))


def make_columns() -> list[dict]:
    return [
        {"name": f"COLUMN_{i:02d}", "type": "TEXT", "nullable": True}
        for i in range(N_COLS)
    ]


def make_rows() -> list[dict]:
    cols = [f"COLUMN_{i:02d}" for i in range(N_COLS)]
    rows = []
    for r in range(N_ROWS):
        rows.append({c: f"val_{r}_{i}" for i, c in enumerate(cols)})
    return rows


def before_body(columns: list[dict], rows: list[dict]) -> str:
    """One serialization of the legacy array-of-dicts result body.

    Array-of-dicts repeats every column name on every row; the verbose metadata
    block (message/query_metadata) is the old shape PR #50 measured against.
    """
    body = {
        "status": "success",
        "data": rows,  # array of dicts: column name repeated on every row
        "columns": columns,
        "row_count": len(rows),
        "execution_time": 0.123,
        "message": f"Query executed successfully, returned {len(rows)} rows",
        "query_metadata": {
            "sql": "SELECT * FROM wide_table LIMIT 50",
            "database_context": None,
            "schema_context": None,
            "query_id": "01abc-0000-0000",
        },
    }
    return json.dumps(body, default=str)


async def after_payload() -> str:
    """Drive the real current execute_query path for the 50x92 query."""
    columns = make_columns()
    rows = make_rows()

    conn = Mock(spec=SnowflakeConnection)
    result = Mock()
    result.data = rows
    result.columns = columns
    result.execution_time = 0.123
    result.query_id = "01abc-0000-0000"
    conn.execute_query.return_value = result

    cache = Mock(spec=SchemaCache)
    cache.is_empty.return_value = False
    cache.is_expired.return_value = False

    # validate() -> read-only SELECT
    orig = QueryValidator.validate

    def _ok(self, sql):  # noqa: ANN001, ARG001 -- stub matches the method signature
        return (True, None, QueryType.SELECT)

    QueryValidator.validate = _ok
    try:
        return await query_executor.execute_query(
            conn, cache, sql="SELECT * FROM wide_table LIMIT 50"
        )
    finally:
        QueryValidator.validate = orig


def main() -> None:
    columns = make_columns()
    rows = make_rows()

    before = before_body(columns, rows)
    after = asyncio.run(after_payload())

    # Payload alone = one serialization of the result body.
    before_payload_tokens = tok(before)
    after_payload_tokens = tok(after)

    # Full envelope = what crosses the wire. BEFORE: body serialized twice
    # (content + structuredContent). AFTER: single TextContent, no structured
    # copy, so the envelope equals the payload.
    before_full = tok(before + before)
    after_full = after_payload_tokens

    def pct(b: int, a: int) -> str:
        return f"{(a - b) / b * 100:+.1f}%"

    print(f"Synthetic query: {N_ROWS} rows x {N_COLS} cols\n")
    print(f"{'Metric':16s} {'Before':>9s} {'After':>9s} {'Change':>9s}")
    print("-" * 46)
    print(
        f"{'Full envelope':16s} {before_full:9,d} {after_full:9,d} "
        f"{pct(before_full, after_full):>9s}"
    )
    print(
        f"{'Payload alone':16s} {before_payload_tokens:9,d} "
        f"{after_payload_tokens:9,d} "
        f"{pct(before_payload_tokens, after_payload_tokens):>9s}"
    )
    print()
    print("Note: 92 cols exceeds the inline column gate, so the current payload")
    print("auto-spills (header + 1-row preview + column_index map + file path).")
    print("The win combines TSV compression, structuredContent de-dup, and")
    print("auto-spill (the full result lives in a .tsv file the agent greps).")


if __name__ == "__main__":
    main()
