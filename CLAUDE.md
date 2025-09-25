# Snowflake MCP Server - Claude Development Guide

This is a Model Context Protocol (MCP) server that provides secure, read-only access to Snowflake databases using external browser authentication (SSO).

## Project Overview

A POC/demo implementation of an MCP server for Snowflake that:
- Enforces strict read-only access at multiple levels
- Uses external browser authentication (SSO) for security
- Provides intelligent schema discovery and caching
- Supports CSV export of query results
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
Execute read-only SQL queries.
- Only allows SELECT, SHOW, DESCRIBE, WITH queries
- Blocks all write operations
- Returns all query results
- Caches results for CSV export (if under 5GB)

### 6. `save_last_query_to_csv`
Export the last query results to a CSV file.
- Exports complete results from the last executed query
- Includes column headers
- Optionally exports the SQL query to a .sql file (enabled by default)
- SQL file is formatted for readability with proper indentation
- Results must be under 5GB cache limit

### 7. `get_query_history`
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

### 1. Snowflake Authentication
This server uses external browser authentication (SSO). When you first connect, your default browser will open to authenticate with Snowflake. No tokens or passwords need to be stored.

### 2. Set Environment Variables
```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USERNAME="user@company.com"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"

# Optional
export MCP_TRANSPORT="stdio"  # or "http"
export DEBUG="false"          # Set to true for debug logging
export CACHE_TTL_DAYS="5"     # Schema cache TTL
# export CACHE_TTL_DAYS="5"    # Schema cache TTL
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
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH"
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

#### Export Query Results
```python
# Execute a query
execute_query("SELECT * FROM large_table LIMIT 1000")
# Returns: All 1000 rows

# Export to CSV and SQL (default behavior)
save_last_query_to_csv("~/Downloads/results.csv")
# Creates: results.csv with all query results
# Creates: results.sql with formatted SQL query

# Export only CSV without SQL file
save_last_query_to_csv("~/Downloads/results.csv", export_sql=false)
# Creates only: results.csv with all query results
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
- Ensure browser can open for authentication
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
- **Browser Auth**: Uses secure SSO through your default browser
- **Cache Required**: Must run `refresh_catalog` before queries
- **CSV Export**: Query results can be exported to CSV files
- **Session Scope**: Query history is per-session only
