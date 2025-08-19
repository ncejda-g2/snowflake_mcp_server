# Snowflake MCP Server - Claude Development Guide

This is a Model Context Protocol (MCP) server that provides secure, read-only access to Snowflake databases using Programmatic Access Token (PAT) authentication.

## Project Overview

A POC/demo implementation of an MCP server for Snowflake that:
- Enforces strict read-only access at multiple levels
- Uses PAT authentication for security
- Provides intelligent schema discovery and caching
- Supports paginated query execution
- Validates all SQL queries for safety

## Project Structure

```
snowflake_mcp_server/
├── server/
│   ├── app.py                    # Main MCP application with tool registration
│   ├── config.py                  # Configuration management
│   ├── snowflake_connection.py   # Snowflake connection with read-only enforcement
│   ├── schema_cache.py           # Schema caching system (5-day TTL)
│   ├── log_utils.py              # Logging configuration
│   └── tools/
│       ├── catalog_refresh.py    # Refresh schema catalog
│       ├── schema_inspector.py   # Browse database structure
│       ├── table_inspector.py    # Get table details
│       └── query_executor.py     # Execute read-only queries
├── main.py                        # Entry point
├── pyproject.toml                # Project configuration
└── IMPLEMENTATION_PLAN.md        # Detailed implementation documentation
```

## Available MCP Tools

### 1. `refresh_catalog`
Scans all accessible databases and caches schema information.
- Required before first query execution
- Cache expires after 5 days
- Can force refresh with `force=true`

### 2. `inspect_schemas`
Browse database structure hierarchically.
- Filter by database, schema, or table patterns
- Returns from cache for fast access
- Auto-refreshes if cache expired

### 3. `search_tables`
Search for tables across all databases.
- Searches table names and comments
- Case-insensitive matching

### 4. `get_table_schema`
Get detailed column information for a specific table.
- Shows column names, types, constraints
- Optional sample data with `include_sample=true`

### 5. `execute_query`
Execute read-only SQL queries with pagination.
- Only allows SELECT, SHOW, DESCRIBE, WITH queries
- Blocks all write operations
- Returns 100 rows per page by default
- Use `page` parameter for pagination

### 6. `get_query_history`
View previously executed queries in the session.
- Shows execution time and status
- Can include failed queries

## Security Features

### Multi-Layer Read-Only Protection
1. **SQL Validation**: Parses and rejects write operations before execution
2. **Transaction-Level**: Each query runs in `BEGIN TRANSACTION READ ONLY`
3. **Session Settings**: Query timeouts and monitoring tags
4. **Connection Validation**: Verifies read-only on connect
5. **Comprehensive Blocking**: INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, MERGE, TRUNCATE, GRANT, REVOKE, COPY, PUT, GET, REMOVE, CALL, EXECUTE

### Query Safety
- Detects CTEs with write operations
- Blocks semicolon-separated statements
- Validates entire query for write keywords
- Clear error messages for rejected queries

## Setup Instructions

### 1. Generate Snowflake PAT
1. Log into Snowsight
2. Go to User Menu → My Profile
3. Click "Programmatic Access"
4. Click "Generate Token"
5. Set expiration (30-90 days recommended)
6. Copy token immediately (shown only once)

### 2. Set Environment Variables
```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USERNAME="user@company.com"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_PAT="<your-token>"

# Optional
export MCP_TRANSPORT="stdio"  # or "http"
export DEBUG="false"          # Set to true for debug logging
export CACHE_TTL_DAYS="5"     # Schema cache TTL
export MAX_QUERY_ROWS="100"   # Default page size
```

### 3. Configure Claude Desktop
Add to Claude Desktop config file:
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "python",
      "args": ["-m", "snowflake_mcp_server"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "xy12345.us-east-1",
        "SNOWFLAKE_USERNAME": "user@company.com",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_PAT": "${SNOWFLAKE_PAT}"
      }
    }
  }
}
```

### 4. Run the Server
```bash
# For local testing
python main.py

# Or using module
python -m snowflake_mcp_server
```

## Usage Examples

### First Time Setup
1. Server starts and connects to Snowflake
2. Run `refresh_catalog` to populate schema cache
3. Use `inspect_schemas` to explore available databases
4. Execute queries with `execute_query`

### Common Workflows

#### Find and Query a Table
```python
# Search for customer tables
search_tables("customer")

# Get table details
get_table_schema("SALES_DB", "PUBLIC", "CUSTOMERS", include_sample=true)

# Query the table
execute_query("SELECT * FROM SALES_DB.PUBLIC.CUSTOMERS WHERE revenue > 10000")
```

#### Paginated Results
```python
# First page (automatic)
execute_query("SELECT * FROM large_table")
# Returns: First 100 rows, pagination info

# Get next page
execute_query("SELECT * FROM large_table", page=2)
# Returns: Rows 101-200
```

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run with debug logging
export DEBUG=true
python main.py

# Check for issues
ruff check
ruff format
```

## Troubleshooting

### Connection Issues
- Verify PAT is valid and not expired
- Check account format (include region)
- Ensure warehouse is running
- Verify network access to Snowflake

### Query Errors
- All queries must be read-only
- Run `refresh_catalog` if cache is empty
- Check query syntax with `SHOW` commands
- Use fully qualified table names when needed

### Cache Issues
- Cache stored in `~/.snowflake_mcp/cache/`
- Delete cache file to force full refresh
- Cache auto-expires after 5 days
- Manual refresh with `refresh_catalog(force=true)`

## Important Notes

- **POC Implementation**: This is a demo, not production-ready
- **Read-Only**: Absolutely no write operations allowed
- **PAT Security**: Never commit tokens to git
- **Cache Required**: Must run `refresh_catalog` before queries
- **Pagination**: Large results are automatically paginated
- **Session Scope**: Query history is per-session only