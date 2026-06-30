"""Query execution tool for running read-only SQL queries."""

import glob
import json
import logging
import os
import time
import uuid
from typing import Any

from server.constants import (
    INLINE_RESULT_CHAR_BUDGET,
    MAX_INLINE_COLUMNS,
    MAX_INLINE_ROWS,
    SPILL_DIR,
    SPILL_FILE_TTL_SECONDS,
    SPILL_MAX_FILES,
    SPILL_MAX_TOTAL_BYTES,
    SPILL_MIN_AGE_SECONDS,
)
from server.schema_cache import SchemaCache
from server.serialization import (
    TSV_NULL,
    build_labeled_record,
    build_tsv,
    column_index_map,
    write_tsv_file,
)
from server.serialization import (
    column_names as _column_names,
)
from server.serialization import (
    format_value as _format_value,
)
from server.snowflake_connection import QueryValidator, SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog

logger = logging.getLogger(__name__)

# Re-exported for backward compatibility with callers/tests that imported these
# from query_executor before the serialization module existed.
__all__ = [
    "TSV_NULL",
    "_build_tsv",
    "_column_names",
    "_format_value",
    "execute_query",
    "spill_json_to_disk",
    "spill_rows_to_disk",
]

# Backward-compatible aliases (the canonical implementations live in
# server.serialization now).
_build_tsv = build_tsv

# Shared spill-file namespace. EVERY route that auto-spills a result to SPILL_DIR
# uses the ``spill_`` prefix (e.g. ``spill_query_<uuid>.tsv``,
# ``spill_find_<uuid>.tsv``, ``spill_show_<uuid>.json``), so the single cleanup
# sweep below covers them all under one retention policy and one bounded
# directory. The glob matches the PREFIX ONLY (not a fixed extension) so it
# sweeps every spill format -- TSV result dumps and JSON tree dumps alike -- yet
# is still narrow enough that the sweep only ever touches files WE created, never
# anything else a user might place in (or that shares) the temp dir. Adding
# spilling to a new route -- in any format -- needs no change here: it spills
# with its own infix via the shared prefix and inherits cleanup for free.
_SPILL_PREFIX = "spill_"
_SPILL_GLOB = f"{_SPILL_PREFIX}*"


def sweep_spill_dir() -> int:
    """Bound SPILL_DIR by age, count, then total bytes; return files deleted.

    Enforces the retention policy for spilled result files (see constants),
    applying the passes in order so each narrows the survivors the next sees:
      1. AGE   -- delete any spill file older than SPILL_FILE_TTL_SECONDS.
      2. COUNT -- if more than SPILL_MAX_FILES survive, delete the OLDEST first
         (FIFO by mtime) until at most SPILL_MAX_FILES remain.
      3. BYTES -- if the survivors' combined size still exceeds
         SPILL_MAX_TOTAL_BYTES, delete the OLDEST first (FIFO) until at/under the
         budget. The real disk guard; COUNT cannot bound bytes (files vary wildly).

    MIN-AGE GRACE: the two FIFO passes (COUNT, BYTES) never evict a file younger
    than SPILL_MIN_AGE_SECONDS. Because queries run sequentially, several spills
    in one agent turn each sweep before their own write; without this window a
    later spill's sweep could reclaim an earlier spill's file before the agent
    reads it. The AGE pass ignores the grace -- a file past its TTL is stale by
    definition. (A file can thus survive even when over the byte budget if all
    survivors are within the grace window; the budget is best-effort, never at the
    cost of a just-returned result.)

    Best-effort and race-safe: the directory may not exist yet (nothing to do),
    files may be deleted by another process between listing and unlinking, and a
    single unlink failure (e.g. permissions) must not abort the rest. Called on
    server startup and before every new spill, so cleanup needs no scheduler or
    background thread.
    """
    try:
        paths = glob.glob(os.path.join(SPILL_DIR, _SPILL_GLOB))
    except OSError:
        return 0

    # Snapshot (path, mtime, size); skip any file that vanished between glob and
    # stat. One stat() per file via os.stat covers both mtime and size.
    entries: list[tuple[str, float, int]] = []
    for path in paths:
        try:
            st = os.stat(path)
        except OSError:
            continue
        entries.append((path, st.st_mtime, st.st_size))

    now = time.time()
    deleted = 0

    def _unlink(path: str) -> None:
        nonlocal deleted
        try:
            os.remove(path)
            deleted += 1
        except OSError:
            pass  # already gone, or not ours to remove -- ignore

    # 1. AGE pass -- ignores the min-age grace (a file past TTL is stale).
    survivors: list[tuple[str, float, int]] = []
    for entry in entries:
        path, mtime, _size = entry
        if now - mtime > SPILL_FILE_TTL_SECONDS:
            _unlink(path)
        else:
            survivors.append(entry)

    # Oldest-first ordering drives both FIFO passes below.
    survivors.sort(key=lambda e: e[1])

    # A file is evictable by a FIFO pass only once it is older than the grace
    # window -- a just-returned result must outlive the turn that created it.
    def _evictable(entry: tuple[str, float, int]) -> bool:
        return now - entry[1] > SPILL_MIN_AGE_SECONDS

    # 2. COUNT pass (FIFO, oldest first, grace-protected). Evict only enough of
    # the eligible oldest files to bring the count to the cap.
    overflow = len(survivors) - SPILL_MAX_FILES
    if overflow > 0:
        remaining = []
        for entry in survivors:
            if overflow > 0 and _evictable(entry):
                _unlink(entry[0])
                overflow -= 1
            else:
                remaining.append(entry)
        survivors = remaining

    # 3. BYTES pass (FIFO, oldest first, grace-protected). Evict eligible oldest
    # files until the survivors' combined size is within budget (or only
    # grace-protected files remain, in which case we stop and tolerate overage).
    total = sum(size for _p, _m, size in survivors)
    if total > SPILL_MAX_TOTAL_BYTES:
        for entry in survivors:
            if total <= SPILL_MAX_TOTAL_BYTES:
                break
            if _evictable(entry):
                _unlink(entry[0])
                total -= entry[2]

    if deleted:
        logger.info(f"Spill cleanup removed {deleted} stale file(s) from {SPILL_DIR}")
    return deleted


def spill_rows_to_disk(
    rows: list[dict], names: list[str], infix: str = "query"
) -> tuple[str, int]:
    """Write the full result to a temp TSV file and return (path, rows_written).

    Same TSV format/escaping/NULL sentinel as the inline payload, so the agent
    can grep/awk/wc the file exactly as it would the inline block. Sweeps stale
    spill files first so the directory stays bounded under almost-always-spill.

    ``infix`` distinguishes the spilling route in the filename
    (``spill_<infix>_<uuid>.tsv``) for at-a-glance provenance; it does NOT affect
    cleanup, because every spill shares the ``spill_`` prefix the single sweep
    globs. This is the one shared spill primitive every route calls, so cleanup,
    retention, and the bounded directory are guaranteed identical across routes.
    """
    os.makedirs(SPILL_DIR, exist_ok=True)
    # Prune BEFORE writing so the cap is enforced at the moment the directory
    # grows. The new file is never threatened by this sweep (it does not exist
    # yet) nor by the NEXT one (the min-age grace protects it for
    # SPILL_MIN_AGE_SECONDS), so it always survives long enough for the agent to
    # read the path we return.
    sweep_spill_dir()
    file_path = os.path.join(
        SPILL_DIR, f"{_SPILL_PREFIX}{infix}_{uuid.uuid4().hex}.tsv"
    )
    written = write_tsv_file(file_path, rows, names)
    return file_path, written


def spill_json_to_disk(payload: Any, infix: str = "json") -> str:
    """Write a structured payload to a temp ``.json`` file and return the path.

    The JSON sibling of :func:`spill_rows_to_disk`, for routes whose result is a
    nested structure (e.g. show_tables's database->schema->[table] tree) rather
    than flat tabular rows. It shares the EXACT same lifecycle as the TSV spill:
    the same SPILL_DIR, the same ``spill_`` prefix (so the single sweep -- now
    globbing the prefix, not a fixed extension -- cleans ``.json`` and ``.tsv``
    alike), and the same pre-write prune. ``infix`` names the route in the
    filename (``spill_<infix>_<uuid>.json``) for provenance only; it does not
    affect cleanup.

    Written compactly (no indentation) -- the file is a machine artifact the
    agent inspects with jq / a one-line python / a scoped re-call, not a document
    to read top to bottom, so indentation would only inflate it on disk.
    """
    os.makedirs(SPILL_DIR, exist_ok=True)
    # Prune BEFORE writing (see spill_rows_to_disk): the new file is protected
    # from this sweep (does not exist yet) and the next (min-age grace).
    sweep_spill_dir()
    file_path = os.path.join(
        SPILL_DIR, f"{_SPILL_PREFIX}{infix}_{uuid.uuid4().hex}.json"
    )
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    return file_path


def build_text_response(
    status: str,
    fields: dict[str, Any],
    tsv: str | None = None,
) -> str:
    """Assemble the final text payload returned to the agent.

    Format::

        status: success
        rows: 50
        cols: 92
        ...
        ---
        <TSV header line>
        <TSV data lines...>

    The header is a flat, one-per-line ``key: value`` block (trivially greppable
    and human-skimmable). When ``tsv`` is provided it follows a ``---`` separator.
    Multi-line field values (e.g. a warning) are kept on their key's line by
    collapsing internal newlines, preserving the one-record-per-line guarantee.
    """
    header_lines = [f"status: {status}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", " ").strip()
        header_lines.append(f"{key}: {text}")

    if tsv is None:
        return "\n".join(header_lines)
    return "\n".join(header_lines) + "\n---\n" + tsv


async def execute_query(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    database: str | None = None,
    schema: str | None = None,
) -> str:
    """
    Execute a read-only SQL query with safety checks.

    This tool validates queries for read-only operations, executes them,
    and returns all results as a compact, line-oriented text payload.

    The response is a small ``key: value`` metadata header followed by a
    ``---`` separator and a result block. Returning text (rather than a dict)
    both compresses the payload and suppresses FastMCP's duplicate
    ``structuredContent`` serialization.

    The inline result block has two shapes by row count:
      * ONE row -> an aligned ``NAME  value`` record (one column per line),
        regardless of column count. A lone row is read by label, so a wide row
        is never miscounted by tab position. (Exception: a one-row result whose
        single cell busts the char budget -- e.g. a megabyte of JSON -- spills.)
      * 2..MAX_INLINE_ROWS narrow rows -> a positional TSV (header line of
        column names + one bare tab-separated row each), parsed with grep/awk,
        e.g. ``awk -F'\\t' 'NR>1 && $3=="X"'``.
    NULLs render as ``\\N``; tabs/newlines in values are backslash-escaped so
    each field stays on exactly one line.

    Auto-spill: a multi-row result is returned inline ONLY when it is narrow (a
    handful of columns) AND short (a screenful of rows). If it is too WIDE (more
    columns than fit a by-eye positional read), too TALL (more rows than the
    model can reliably aggregate/scan in context), or too LARGE (the TSV busts
    the char budget), the tool does NOT dump a wall of text or silently
    truncate. It writes the *complete* result to a temp ``.tsv`` file (identical
    format, including a header line) and returns ``results_file: /path`` plus a
    ``column_index`` name->position map -- and NO inline preview: the agent must
    read the file regardless, and its first read shows formatting with the
    header attached. The map lets the agent write ``awk $N`` with no tab
    counting. ``rows:`` is always the true total. NULL/empty semantics are
    identical on disk and inline.

    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        sql: SQL query to execute
        database: Optional database context
        schema: Optional schema context

    Returns:
        A text payload: metadata header + ``---`` + TSV result block.
    """
    # First validate the query for safety (before checking cache)
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    if not is_valid:
        return build_text_response(
            status="error",
            fields={"message": error_msg, "query_type": str(query_type)},
        )

    # Check cache and auto-refresh if needed
    if cache.is_expired() or cache.is_empty():
        logger.info("Cache is expired or empty, refreshing catalog...")
        refresh_result = await refresh_catalog(connection, cache, force=True)
        if refresh_result["status"] != "success":
            return build_text_response(
                status="error",
                fields={
                    "message": "Failed to refresh catalog",
                    "error": refresh_result.get("message"),
                },
            )

    try:
        # Execute query
        result = connection.execute_query(sql=sql, database=database, schema=schema)

        if not result.data:
            return build_text_response(
                status="success",
                fields={
                    "rows": 0,
                    "execution_time": round(result.execution_time, 4),
                    "message": "Query executed successfully but returned no results",
                },
                tsv=_build_tsv([], _column_names(result.columns)),
            )

        column_names = _column_names(result.columns, result.data[0])

        fields: dict[str, Any] = {
            "rows": len(result.data),
            "cols": len(column_names),
            "execution_time": round(result.execution_time, 4),
            "query_id": result.query_id,
        }

        # --- Inline-vs-spill decision -------------------------------------
        #
        # Single row -- the byte cap is the ONLY thing that can push it to disk.
        # A one-row result is rendered as an aligned ``NAME  value`` record
        # (one column per line) regardless of column count: a lone row is read
        # by label, never by tab position, so the wide-result miscount hazard
        # does not apply and the column-count gate is irrelevant. The labels are
        # the column names, so no separate column_index map is needed. We build
        # the labeled text first and return it inline only when it fits the
        # budget; a single giant cell (e.g. a megabyte of nested JSON) still
        # busts the cap and falls through to spill below.
        if len(result.data) == 1:
            labeled = build_labeled_record(result.data[0], column_names)
            if len(labeled) <= INLINE_RESULT_CHAR_BUDGET:
                return build_text_response(status="success", fields=fields, tsv=labeled)

        # Multiple rows (or a one-row giant cell): decide positional-TSV-inline
        # vs spill by shape/size. A result spills if ANY gate trips:
        #   * too WIDE  -- more than MAX_INLINE_COLUMNS columns: a multi-row
        #                  positional TSV this wide forces reading values by
        #                  counting tab fields by eye, exactly where models
        #                  miscount. Spilled, the agent reads by column INDEX
        #                  (column_index map -> awk $N) with zero counting.
        #   * too TALL  -- more than MAX_INLINE_ROWS rows: a wall the model would
        #                  have to aggregate/scan by eye, which it does
        #                  unreliably (measured: miscounts on COUNT-style
        #                  questions). Spilled, the agent uses awk/wc -- exact.
        #   * too LARGE -- the TSV exceeds INLINE_RESULT_CHAR_BUDGET: a backstop
        #                  for a narrow+short result carrying a giant single cell.
        inline_body = _build_tsv(result.data, column_names)
        too_wide = len(column_names) > MAX_INLINE_COLUMNS
        too_tall = len(result.data) > MAX_INLINE_ROWS
        too_large = len(inline_body) > INLINE_RESULT_CHAR_BUDGET

        if too_wide or too_tall or too_large:
            # Spill the COMPLETE result to a temp .tsv. We do NOT emit an inline
            # preview: the agent has to read the file regardless, and its first
            # read (e.g. ``head``) shows value formatting WITH the header line
            # attached -- strictly better than a header-less preview row, which
            # would just re-import into context the wide/miscount-prone data we
            # spilled to get OUT of context. We do surface the column_index map:
            # it is names+positions once (cheap) and lets the agent write
            # ``awk $N`` without counting tab fields against the header (the
            # single most error-prone step on a wide result).
            spill_path, written = spill_rows_to_disk(
                result.data, column_names, infix="query"
            )
            fields["results_file"] = spill_path
            # 1-based name->position map: the sole column reference for a spilled
            # result. The on-disk file still carries its own TSV header line.
            fields["column_index"] = column_index_map(column_names)
            reason = "wide" if too_wide else "tall" if too_tall else "large"
            logger.warning(
                f"Query result spilled to disk ({reason}): {written:,} rows, "
                f"{len(column_names)} cols, {len(inline_body):,} chars "
                f"-> {spill_path}"
            )
            return build_text_response(status="success", fields=fields)

        # Fits inline: 2..MAX_INLINE_ROWS rows, <=MAX_INLINE_COLUMNS cols, within
        # the char budget. ``inline_body`` is a compact positional TSV (header
        # line + bare tab-separated rows). A few columns are trivially read by
        # eye, so no per-cell labeling is needed.
        return build_text_response(status="success", fields=fields, tsv=inline_body)

    except ValueError as e:
        # Query validation errors
        return build_text_response(
            status="error",
            fields={"error_type": "validation_error", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        return build_text_response(
            status="error",
            fields={
                "error_type": "execution_error",
                "message": f"Query execution failed: {str(e)}",
            },
        )
