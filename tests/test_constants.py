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
    results spill to a file instead.
    """
    assert constants.INLINE_RESULT_CHAR_BUDGET == 8_000
    assert isinstance(constants.INLINE_RESULT_CHAR_BUDGET, int)
    assert constants.INLINE_RESULT_CHAR_BUDGET < constants.MCP_CHAR_LIMIT_ESTIMATE


def test_constants_relationships():
    """Test relationships between constants."""
    # Token and character estimates should be positive
    assert constants.MCP_TOKEN_LIMIT_ESTIMATE > 0
    assert constants.APPROX_CHARS_PER_TOKEN > 0
