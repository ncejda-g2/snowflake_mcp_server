"""Constants and configuration values for the Snowflake MCP Server."""

# Cache configuration
MAX_CACHE_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB limit for query result cache

# CSV export configuration
CSV_DELIMITER = ','
CSV_NULL_VALUE = ''  # Empty string for NULL values
CSV_INCLUDE_HEADERS = True