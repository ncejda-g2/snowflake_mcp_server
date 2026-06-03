"""Constants and configuration values for the Snowflake MCP Server."""

import os
import tempfile

# Auto-spill configuration
#
# When an inline execute_query result is too large to return safely, the tool
# writes the FULL result to a temp file as TSV and returns only a small preview
# plus the file path. The agent never sees a silently-truncated payload and
# never has to re-issue the query.
SPILL_DIR = os.path.join(tempfile.gettempdir(), "snowflake_mcp")


def _env_int(name: str, default: int) -> int:
    """Read a positive int from the environment, falling back to ``default``.

    A missing, empty, non-numeric, or non-positive value yields the default, so
    a typo in an env var can never silently disable spill cleanup (which would
    let the temp dir grow unbounded).
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# Spill-file retention. Under the almost-always-spill design every wide/tall
# query drops a .tsv in SPILL_DIR, so the directory MUST be bounded or it leaks
# disk (and leaves query data in tmp indefinitely). Retention is enforced by
# sweep_spill_dir() both on server startup and before every new spill, applying
# the same two limits:
#   * AGE  -- delete files older than SPILL_FILE_TTL_SECONDS. The agent reads a
#             spilled file within the same turn (seconds), so the TTL is purely a
#             safety margin for an abandoned/paused task; 2h is generous.
#   * COUNT -- after the TTL pass, if more than SPILL_MAX_FILES remain, delete
#             the OLDEST first (FIFO) until at/under the cap. Bounds a burst that
#             creates many files well within the TTL window.
# Both are env-overridable for deployments with different disk/retention needs.
SPILL_FILE_TTL_SECONDS = _env_int("SNOWFLAKE_MCP_SPILL_TTL_SECONDS", 7200)  # 2h
SPILL_MAX_FILES = _env_int("SNOWFLAKE_MCP_SPILL_MAX_FILES", 20)

# Number of data rows included in the inline preview when spilling to disk.
#
# This is deliberately *one* row, not a sample. When a result spills, the
# inline preview's only job is proof-of-shape: show the column header and a
# single concrete data row so the agent can see value formatting (dates,
# nulls, numeric vs string) and write a correct grep/awk against the file.
# The preview is never the answer for a spilled result, so a larger preview is
# pure wasted context: if the task needs row N>1, the agent must read the file
# regardless. One row gives the shape at minimal token cost.
SPILL_PREVIEW_ROWS = 1

# Max column count for an INLINE result. Above this, the result always spills to
# a file -- regardless of how few rows or characters it is.
#
# An inline result is consumed by the LLM *directly from context*: there is no
# shell, so the agent reads values straight out of the payload by eye. That is
# safe only for a narrow positional TSV, where a handful of columns are
# trivially associated with their values. A wide result forces the model to
# silently count tab-separated fields to pair a value with its column name --
# exactly where models misread -- and there is no awk/cut to do it reliably.
#
# Rather than paper over that with an inline ``name=value`` format (which glues
# names to values but bloats every row with repeated column names -- pure token
# cost, and the whole point of this path is tight output), we take the simpler,
# stricter line: if a result is wide enough to be miscount-prone, it does not
# belong in context AT ALL. It spills to a real .tsv file where the agent reads
# it by column INDEX via the column_index map (``63=TYPE`` -> awk ``$63``) with
# zero counting. So column count alone decides inline-vs-spill for shape; the
# row gate below independently catches narrow-but-TALL results.
#
# 5 is a judgment call (no eval set): ~5 fields is about the limit of what is
# reliably read positionally at a glance, so we keep <=5 inline and spill >5.
MAX_INLINE_COLUMNS = 5

# Max ROW count for an INLINE result. Above this, the result spills to a file --
# regardless of how few columns or characters it is.
#
# This is the other half of the shape gate, and it exists because of a measured
# failure mode, not a token estimate. In an eval, a model reading a many-row
# result *straight from context* answered point lookups fine but MISCOUNTED on an
# aggregation ("how many rows have X<50?") over a couple hundred rows -- it gave a
# confident wrong total and only caught it on a second pass. A tall wall of rows
# is a reasoning hazard, not just a token cost: the model is doing by eye what a
# tool does exactly. When the result spills instead, the agent runs the count
# with awk/wc against the file and gets an exact answer.
#
# So once a result is more than a screenful, it leaves context and becomes a
# file. We keep the cap GENEROUS (not e.g. 10) on purpose: the common case is a
# small lookup or a tiny GROUP BY, and forcing those through a file round-trip
# would tax the frequent case to protect the rare one. ~25 rows stays trivially
# scannable inline while any genuine wall (50, 100, 200+ rows) spills.
MAX_INLINE_ROWS = 25

# MCP protocol token limits (estimates - actual limits are configurable on client side)
MCP_TOKEN_LIMIT_ESTIMATE = 25000  # Estimated default token limit in many MCP clients
APPROX_CHARS_PER_TOKEN = 4  # Rough estimate: 1 token ≈ 4 characters in JSON
MCP_CHAR_LIMIT_ESTIMATE = (
    MCP_TOKEN_LIMIT_ESTIMATE * APPROX_CHARS_PER_TOKEN
)  # ~100,000 characters -- the transport ceiling, NOT the inline budget below.

# Inline char budget: a BACKSTOP gate, not the primary one.
#
# Shape is governed by the two gates above: a result is inline only if it is
# narrow (<=MAX_INLINE_COLUMNS) AND short (<=MAX_INLINE_ROWS). For any such
# result the positional TSV is already tiny -- 25 rows x 5 columns of ordinary
# values is a few hundred tokens, far under this budget -- so in normal operation
# this gate never fires on its own; the row/column gates catch first.
#
# It exists for ONE pathological case the row/column gates miss: a result that is
# narrow and short but carries a giant single cell (e.g. one row holding a 5 KB
# JSON blob or a long document). That is few columns and few rows, so shape says
# "inline", yet it would still dump a multi-thousand-token wall into context.
# This char ceiling catches exactly that and spills it to a file.
#
# Sizing: a positional TSV tokenizes at ~1.8 chars/token (digit/delimiter-heavy),
# so ~4,000 chars is ~2.2k tokens -- the hard inline ceiling regardless of shape.
# The gate measures the EXACT positional bytes we would emit. It stays FAR below
# the ~100k-char MCP transport ceiling: that ceiling is "what won't error on the
# wire"; this is "what is worth putting in context".
INLINE_RESULT_CHAR_BUDGET = 4_000  # backstop for giant single cells (~2.2k tokens)
