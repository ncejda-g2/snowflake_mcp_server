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
# these passes IN ORDER (each narrows the survivors the next pass sees):
#   * AGE   -- delete files older than SPILL_FILE_TTL_SECONDS. The agent reads a
#              spilled file within the same turn (seconds), so the TTL is purely
#              a safety margin for an abandoned/paused task; 2h is generous.
#   * COUNT -- if more than SPILL_MAX_FILES remain, delete the OLDEST first (FIFO)
#              until at/under the cap. This is the DOMINANT sweeper in normal
#              operation: a session spills steadily and the count cap is what
#              keeps the dir small turn-to-turn.
#   * BYTES -- if the survivors' combined size still exceeds SPILL_MAX_TOTAL_BYTES,
#              delete the OLDEST first (FIFO) until at/under the budget. COUNT is a
#              poor proxy for disk use (20 files could each be gigabytes), so this
#              total-byte ceiling is the real disk-blowout guard. It is a BACKSTOP,
#              not the primary force: set high enough that it almost never fires,
#              so COUNT/AGE dominate and BYTES only catches the pathological case.
# All limits are env-overridable for deployments with different disk/retention
# needs.
#
# MIN-AGE GRACE (the correctness guard): both FIFO passes (COUNT and BYTES) skip
# any file younger than SPILL_MIN_AGE_SECONDS, so a result we just handed back to
# the agent is never reclaimed before the agent can read it. This is necessary
# because queries run SEQUENTIALLY (single blocking connection on one event loop
# -- verified empirically), so within one agent turn several queries can each run
# their pre-write sweep BEFORE the agent gets control back to read any returned
# path. Without the grace window, a later query's sweep could evict an earlier
# query's still-unread file. The grace is a pragmatic time bound, not a proof: it
# protects the common pattern (one big query, then small ones, all in a turn) but
# a deliberately adversarial case -- two multi-minute giant queries back to back
# where the second outlasts the grace -- can still lose the first. We accept that
# corner rather than build turn-boundary/read-state tracking the tool layer has no
# signal for; a high byte cap makes it nearly impossible to hit in practice.
#
# Deliberately NO per-file byte cap: a spill file's contract is that it holds the
# COMPLETE result (the agent greps/awks it for the real answer), so truncating an
# individual file to fit a budget would silently corrupt that answer. A single
# result is always written in full -- even one larger than the whole budget --
# and the byte pass reclaims it on a LATER sweep (never serving a partial result).
SPILL_FILE_TTL_SECONDS = _env_int("SNOWFLAKE_MCP_SPILL_TTL_SECONDS", 7200)  # 2h
SPILL_MAX_FILES = _env_int("SNOWFLAKE_MCP_SPILL_MAX_FILES", 20)
SPILL_MAX_TOTAL_BYTES = _env_int(
    "SNOWFLAKE_MCP_SPILL_MAX_TOTAL_BYTES", 10_000_000_000
)  # 10 GB -- backstop, not the primary sweeper
SPILL_MIN_AGE_SECONDS = _env_int("SNOWFLAKE_MCP_SPILL_MIN_AGE_SECONDS", 60)

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

# find_tables inline budget + breakdown size.
#
# find_tables searches table names AND comments across the WHOLE cache, so a
# generic term ("product", "survey", "data") can match thousands of tables. The
# old behavior dumped every hit as one JSON blob -- the single worst offender for
# output tokens (observed 66 KB+). But unlike execute_query, the agent almost
# never wants to READ all those hits: a flood of matches means "term too broad",
# and the agent's next move is to narrow. So when the serialized result exceeds
# this budget, find_tables spills the COMPLETE result to a .tsv file and returns
# instead a compact summary built to drive that narrowing decision: the total hit
# count, a BOUNDED top-N breakdown of where the hits cluster, and the file path.
#
# The budget is measured against the SAME positional TSV bytes we would spill, so
# it gates on the real on-the-wire size, not an estimate. It is independent from
# INLINE_RESULT_CHAR_BUDGET (execute_query's giant-single-cell backstop) because
# the two gate unrelated things: that one catches one pathologically huge cell in
# an otherwise tiny result; this one catches a flood of many small rows.
FIND_TABLES_INLINE_CHAR_BUDGET = _env_int(
    "SNOWFLAKE_MCP_FIND_TABLES_CHAR_BUDGET", 4_000
)

# Number of (database.schema) groups in the spilled-find_tables breakdown.
#
# When find_tables spills, the inline summary reports the top-N database.schema
# groups by hit count -- the single most actionable narrowing signal, because it
# maps directly onto show_tables's database_pattern + schema_pattern filters. N
# is a HARD CAP, not a function of how many groups matched: a term hitting 5
# groups or 5,000 groups both yield at most N lines, so this summary can NEVER
# reblow the token budget regardless of how sprawling the account is (some orgs
# have thousands of databases). Everything outside the top-N is collapsed into a
# single "(+X more groups, Y hits)" tail marker so the agent can still tell a
# concentrated result (scope to the top group) from a diffuse one (narrow the
# keyword) -- the count alone would hide that distinction.
FIND_TABLES_TOP_GROUPS = _env_int("SNOWFLAKE_MCP_FIND_TABLES_TOP_GROUPS", 5)

# show_tables inline budget + breakdown size.
#
# show_tables browses the catalog tree. An unfiltered call -- or even a single
# broad database_pattern (e.g. "GDC", which substring-matches GDC + GDC_TESTING
# and pulls 30k+ tables) -- can serialize to MEGABYTES, far past any sane context
# budget. So this route advertises a HARD output ceiling: the response is always
# bounded (~1.5k tokens), whether inline or spilled. When the compact tree
# exceeds this budget, show_tables writes the COMPLETE tree to a temp .json file
# and returns a bounded, narrowing-focused summary instead.
#
# Measured against the EXACT compact-JSON bytes we would emit inline (the
# {database: {schema: [table, ...]}} map), not an estimate. ~6,000 chars is
# ~1.5k tokens of JSON -- deliberately under FIND_TABLES_INLINE_CHAR_BUDGET's
# positional TSV (JSON's braces/quotes/keys tokenize a touch denser per useful
# field). Independent from the other budgets: each gates a different route's
# different shape.
SHOW_TABLES_INLINE_CHAR_BUDGET = _env_int(
    "SNOWFLAKE_MCP_SHOW_TABLES_CHAR_BUDGET", 6_000
)

# Number of groups in the spilled-show_tables breakdown.
#
# When show_tables spills, the summary reports a bounded top-N breakdown to drive
# narrowing. The breakdown axis is ADAPTIVE: if the result spans exactly one
# database, the only remaining narrowing axis is the schema, so it lists the top
# database.schema groups by table count; if it spans several databases, it lists
# the top databases. Either way N is a HARD CAP with a "(+X more ..., Y tables)"
# tail marker, so the summary can never reblow the budget that triggered the
# spill -- no matter how many databases/schemas the account has.
SHOW_TABLES_TOP_GROUPS = _env_int("SNOWFLAKE_MCP_SHOW_TABLES_TOP_GROUPS", 10)

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
