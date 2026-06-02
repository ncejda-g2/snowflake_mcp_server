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

# MCP protocol token limits (estimates - actual limits are configurable on client side)
MCP_TOKEN_LIMIT_ESTIMATE = 25000  # Estimated default token limit in many MCP clients
APPROX_CHARS_PER_TOKEN = 4  # Rough estimate: 1 token ≈ 4 characters in JSON
MCP_CHAR_LIMIT_ESTIMATE = (
    MCP_TOKEN_LIMIT_ESTIMATE * APPROX_CHARS_PER_TOKEN
)  # ~100,000 characters
MCP_CHAR_WARNING_THRESHOLD = int(
    MCP_CHAR_LIMIT_ESTIMATE * 0.8
)  # Warn at 80% of estimated limit (~80,000 chars)
