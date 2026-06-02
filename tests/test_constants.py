"""Tests for server/constants.py module."""

from server import constants


def test_spill_configuration():
    """Test auto-spill configuration constants."""
    assert isinstance(constants.SPILL_DIR, str)
    assert constants.SPILL_DIR  # non-empty path
    assert isinstance(constants.SPILL_PREVIEW_ROWS, int)
    assert constants.SPILL_PREVIEW_ROWS > 0


def test_mcp_token_limits():
    """Test MCP protocol token limit constants."""
    assert constants.MCP_TOKEN_LIMIT_ESTIMATE == 25000
    assert constants.APPROX_CHARS_PER_TOKEN == 4

    # Test calculated values
    assert constants.MCP_CHAR_LIMIT_ESTIMATE == 100000
    assert constants.MCP_CHAR_WARNING_THRESHOLD == 80000

    # Verify warning threshold is 80% of limit
    assert (
        int(constants.MCP_CHAR_LIMIT_ESTIMATE * 0.8)
        == constants.MCP_CHAR_WARNING_THRESHOLD
    )

    # Verify types
    assert all(
        isinstance(val, int)
        for val in [
            constants.MCP_TOKEN_LIMIT_ESTIMATE,
            constants.APPROX_CHARS_PER_TOKEN,
            constants.MCP_CHAR_LIMIT_ESTIMATE,
            constants.MCP_CHAR_WARNING_THRESHOLD,
        ]
    )


def test_constants_relationships():
    """Test relationships between constants."""
    # Warning threshold should be less than the limit
    assert constants.MCP_CHAR_WARNING_THRESHOLD < constants.MCP_CHAR_LIMIT_ESTIMATE

    # Token and character estimates should be positive
    assert constants.MCP_TOKEN_LIMIT_ESTIMATE > 0
    assert constants.APPROX_CHARS_PER_TOKEN > 0
