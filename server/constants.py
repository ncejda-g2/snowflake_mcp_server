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

# Width threshold for the inline result format.
#
# An inline result is consumed by the LLM *directly from context* -- there is no
# file and no shell, so the agent never actually runs awk/cut on it; it reads
# the values straight out of the payload. With a narrow positional TSV that is
# fine. But a wide result (many columns, few rows -- e.g. SELECT * ... LIMIT 3)
# can fit inline yet force the model to silently count tabs to associate a value
# with its column name, which is exactly where models misread.
#
# So at/above this column count, the inline payload switches from positional TSV
# to labeled rows (``name=value`` per cell), gluing every value to its own name
# so no counting is required. Below it, the compact positional TSV is kept: it
# is cheaper and a few columns are trivially readable. (The spill path is
# unaffected -- it writes a real file and uses the column_index map for shell.)
#
# The exact cutoff is a judgment call (we have no eval set to derive it). It is
# set deliberately LOW because the costs are asymmetric: switching to labeled
# rows one column too early just repeats a few column names -- pure token cost,
# no correctness risk -- while leaving a result positional one column too late
# risks a SILENT miscount and a wrong answer. ~5 fields is about the limit of
# what is reliably counted at a glance, so 6 catches the zone where miscounting
# starts while keeping genuinely trivial (<=5 col) results in the cheap form.
WIDE_RESULT_COL_THRESHOLD = 6

# MCP protocol token limits (estimates - actual limits are configurable on client side)
MCP_TOKEN_LIMIT_ESTIMATE = 25000  # Estimated default token limit in many MCP clients
APPROX_CHARS_PER_TOKEN = 4  # Rough estimate: 1 token ≈ 4 characters in JSON
MCP_CHAR_LIMIT_ESTIMATE = (
    MCP_TOKEN_LIMIT_ESTIMATE * APPROX_CHARS_PER_TOKEN
)  # ~100,000 characters -- the transport ceiling, NOT the inline budget below.

# Inline result budget: the size above which an execute_query result spills to a
# file instead of being returned inline.
#
# This is deliberately FAR below the ~100k-char MCP transport ceiling, because
# the two answer different questions. The transport ceiling is "what won't error
# on the wire". This budget is "what is actually worth returning straight into
# the model's context". A large result -- especially a wide one, where every
# column name repeats on every row -- is a wall of mostly-redundant tokens that
# nobody reads productively from context; it belongs in a file the agent can
# grep/awk. So we keep inline results small and self-contained (~2k tokens) and
# push everything bigger to disk, where size is free.
#
# The gate is measured against the EXACT bytes we would emit (positional TSV for
# narrow results, labeled rows for wide ones), so a wide+tall result is sized by
# its true, inflated labeled form and spills when it should.
INLINE_RESULT_CHAR_BUDGET = 8_000  # ~2k tokens
