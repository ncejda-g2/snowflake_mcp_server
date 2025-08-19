# Snowflake MCP Server

A secure, read-only Model Context Protocol (MCP) server for Snowflake data access.

## Features

- 🔒 **Strict Read-Only Access**: Multiple layers of protection against write operations
- 🔑 **PAT Authentication**: Uses Snowflake Programmatic Access Tokens
- 💾 **Smart Caching**: 5-day schema cache for fast metadata access
- 📄 **Pagination**: Automatic pagination for large result sets
- 🛡️ **Query Validation**: Comprehensive SQL validation before execution

## Quick Start

### 1. Generate Snowflake PAT

1. Log into Snowsight
2. User Menu → My Profile → Programmatic Access
3. Generate Token (save it immediately!)

### 2. Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USERNAME="user@company.com"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_PAT="<your-token>"
```

### 3. Install Dependencies

```bash
pip install -e .
```

### 4. Run the Server

```bash
python main.py
```

## Claude Desktop Configuration

Add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "python",
      "args": ["/path/to/snowflake_mcp_server/main.py"],
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

## Available Tools

- `refresh_catalog` - Scan and cache all database schemas
- `inspect_schemas` - Browse database structure
- `search_tables` - Find tables across all databases
- `get_table_schema` - Get detailed table information
- `execute_query` - Run read-only SQL queries
- `get_query_history` - View query execution history

## First Time Usage

1. Start the server
2. Run `refresh_catalog` to populate the schema cache
3. Use `inspect_schemas` to explore available databases
4. Execute queries with `execute_query`

## Security

This server enforces read-only access through:
- SQL query validation
- Transaction-level restrictions
- Session-level settings
- Comprehensive operation blocking

Only SELECT, SHOW, DESCRIBE, and WITH queries are allowed.

## License

MIT