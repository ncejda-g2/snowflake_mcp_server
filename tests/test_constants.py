"""Tests for server/constants.py module."""

from server import constants


def test_spill_configuration():
    """Test auto-spill configuration constants."""
    assert isinstance(constants.SPILL_DIR, str)
    assert constants.SPILL_DIR  # non-empty path
    assert isinstance(constants.SPILL_PREVIEW_ROWS, int)
    assert constants.SPILL_PREVIEW_ROWS > 0


def test_mcp_token_limits():
    """Test MCP protocol token limit constants (the transport ceiling)."""
    assert constants.MCP_TOKEN_LIMIT_ESTIMATE == 25000
    assert constants.APPROX_CHARS_PER_TOKEN == 4

    # Test calculated values
    assert constants.MCP_CHAR_LIMIT_ESTIMATE == 100000

    # Verify types
    assert all(
        isinstance(val, int)
        for val in [
            constants.MCP_TOKEN_LIMIT_ESTIMATE,
            constants.APPROX_CHARS_PER_TOKEN,
            constants.MCP_CHAR_LIMIT_ESTIMATE,
        ]
    )


def test_inline_result_budget():
    """The inline budget is small and far below the transport ceiling.

    The budget answers "what is worth returning straight into context", not
    "what fits on the wire", so it must sit well under the MCP char limit -- big
    results spill to a file instead. ~4k chars is about ~2k tokens of positional
    TSV, the hard inline ceiling for this token-slashing path.
    """
    assert constants.INLINE_RESULT_CHAR_BUDGET == 4_000
    assert isinstance(constants.INLINE_RESULT_CHAR_BUDGET, int)
    assert constants.INLINE_RESULT_CHAR_BUDGET < constants.MCP_CHAR_LIMIT_ESTIMATE


def test_max_inline_columns():
    """Above MAX_INLINE_COLUMNS a result always spills (width shape gate).

    A small, low cardinality cap: only a handful of columns are reliably read
    positionally by eye; wider results spill to a file and are read by index.
    """
    assert constants.MAX_INLINE_COLUMNS == 5
    assert isinstance(constants.MAX_INLINE_COLUMNS, int)
    assert constants.MAX_INLINE_COLUMNS > 0


def test_max_inline_rows():
    """Above MAX_INLINE_ROWS a result always spills (height shape gate).

    A tall wall of rows is a reasoning hazard in context (the model miscounts
    aggregations), so beyond a screenful it spills to a file. Kept generous so
    common small lookups stay inline without a file round-trip.
    """
    assert constants.MAX_INLINE_ROWS == 25
    assert isinstance(constants.MAX_INLINE_ROWS, int)
    assert constants.MAX_INLINE_ROWS > 0


def test_constants_relationships():
    """Test relationships between constants."""
    # Token and character estimates should be positive
    assert constants.MCP_TOKEN_LIMIT_ESTIMATE > 0
    assert constants.APPROX_CHARS_PER_TOKEN > 0
