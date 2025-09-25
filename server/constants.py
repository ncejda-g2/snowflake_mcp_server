"""Constants and configuration values for the Snowflake MCP Server."""

# Cache configuration
MAX_CACHE_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB limit for query result cache

# CSV export configuration
CSV_DELIMITER = ','
CSV_NULL_VALUE = ''  # Empty string for NULL values
CSV_INCLUDE_HEADERS = True

# MCP protocol token limits (estimates - actual limits are configurable on client side)
MCP_TOKEN_LIMIT_ESTIMATE = 25000  # Estimated default token limit in many MCP clients
APPROX_CHARS_PER_TOKEN = 4  # Rough estimate: 1 token ≈ 4 characters in JSON
MCP_CHAR_LIMIT_ESTIMATE = MCP_TOKEN_LIMIT_ESTIMATE * APPROX_CHARS_PER_TOKEN  # ~100,000 characters
MCP_CHAR_WARNING_THRESHOLD = int(MCP_CHAR_LIMIT_ESTIMATE * 0.8)  # Warn at 80% of estimated limit (~80,000 chars)
