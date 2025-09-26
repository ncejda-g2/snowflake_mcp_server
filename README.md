# Snowflake MCP Server

![GitHub release](https://img.shields.io/github/v/release/ncejda-g2/snowflake_mcp_server?color=blue)
[![Changelog](https://img.shields.io/badge/changelog-Latest%20Changes-blue.svg)](./CHANGELOG.md)
[![MCP](https://img.shields.io/badge/MCP-Model%20Context%20Protocol-green.svg)](https://modelcontextprotocol.io)

A secure, read-only Model Context Protocol (MCP) server for Snowflake data access using external browser authentication (SSO).

## Features

- 🔒 **Strict Read-Only Access**: Multiple layers of protection against write operations
- 🔑 **External Browser Authentication**: Uses Snowflake's secure browser-based SSO
- 💾 **Smart Caching**: 5-day schema cache for fast metadata access
- 📄 **CSV Export**: Export query results directly to CSV files
- 🛡️ **Query Validation**: Comprehensive SQL validation before execution

## Prerequisites

- Python 3.12 or higher
- Snowflake account with SSO access
- Git (for cloning the repository)
- One of the following AI platforms that support MCP servers:
  1. Claude Code
  2. Claude Desktop (Pro)
  3. Gemini CLI
  4. Cursor CLI
  5. Codex CLI with ChatGPT Pro
  6. Or your choice of AI platform that supports MCP servers

## Installation Options

Choose one of the following installation methods:

<details>
<summary><b>Option 1: Using Python venv</b> (Recommended for most users)</summary>

<br>

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd snowflake_mcp_server
   ```

2. **Create a virtual environment**
   ```bash
   python3 -m venv snowflake_mcp_env
   ```

3. **Activate the virtual environment**
   - On macOS/Linux:
     ```bash
     source snowflake_mcp_env/bin/activate
     ```
   - On Windows:
     ```bash
     snowflake_mcp_env\Scripts\activate
     ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Verify installation**
   ```bash
   python main.py --help
   ```

</details>

<details>
<summary><b>Option 2: Using uv</b> (Fast Python package manager)</summary>

<br>

1. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   Or on macOS with Homebrew:
   ```bash
   brew install uv
   ```

2. **Clone and setup**
   ```bash
   git clone <repository-url>
   cd snowflake_mcp_server
   uv sync
   ```

   If the dependencies seem outdated, update them:
   ```bash
   uv lock --upgrade
   uv sync
   ```

3. **Verify installation**
   ```bash
   uv run python main.py --help
   ```

</details>

<details>
<summary><b>Option 3: Using Conda/Miniconda</b></summary>

<br>

1. **Create conda environment**
   ```bash
   conda create -n snowflake-mcp python=3.12
   conda activate snowflake-mcp
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

</details>

## Configure Your AI Platform

Choose the configuration instructions for your AI platform:

<details>
<summary><b>Option 1: Claude Code</b></summary>

<br>

Run the following command to add the MCP server to Claude Code:

```bash
claude mcp add snowflake-readonly \
  --env SNOWFLAKE_ACCOUNT=your-account \
  --env SNOWFLAKE_USERNAME=your-email@company.com \
  --env SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE \
  -- /path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python /path/to/snowflake_mcp_server/main.py
```

Replace the placeholders:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository
- `your-account`: Your Snowflake account identifier (see Configuration Values section)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name

</details>

<details>
<summary><b>Option 2: Claude Desktop (Pro)</b></summary>

<br>

1. **Find your Claude Desktop configuration file**
   - macOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
   - Linux: `~/.config/claude/claude_desktop_config.json`

2. **Add the MCP server configuration**

   <details>
   <summary><b>For Python venv installation</b></summary>

   <br>

   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
         "args": ["/path/to/snowflake_mcp_server/main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

   </details>

   <details>
   <summary><b>For uv installation</b></summary>

   <br>

   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "uv",
         "args": ["--directory", "/path/to/snowflake_mcp_server", "run", "python", "main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

   </details>

   <details>
   <summary><b>For Conda installation</b></summary>

   <br>

   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "/path/to/conda/envs/snowflake-mcp/bin/python",
         "args": ["/path/to/snowflake_mcp_server/main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

   </details>

3. **Restart Claude Desktop** to load the new configuration

</details>

<details>
<summary><b>Option 3: Gemini CLI</b></summary>

<br>

1. **Edit your Gemini settings file**
   - Location: `~/.gemini/settings.json`

2. **Add the MCP server configuration**

   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "uv",
         "args": ["--directory", "/path/to/snowflake_mcp_server", "run", "python", "main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

   Or if using Python venv:
   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
         "args": ["/path/to/snowflake_mcp_server/main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

</details>

<details>
<summary><b>Option 4: Cursor CLI</b></summary>

<br>

Add the MCP server to your Cursor configuration following the same JSON format as Claude Desktop above.

</details>

<details>
<summary><b>Option 5: Codex CLI with ChatGPT Pro</b></summary>

<br>

Configure the MCP server in your Codex settings using the same JSON format as Claude Desktop above.

</details>

<details>
<summary><b>Option 6: Other MCP-Compatible Platforms</b></summary>

<br>

Most MCP-compatible platforms use a similar JSON configuration format. Adapt the Claude Desktop configuration to your platform's specific requirements.

</details>

### Configuration Values

For all platforms, update these values:

- **`SNOWFLAKE_ACCOUNT`**: Your Snowflake account identifier. This can be:
  - **Account Identifier format**: `GJA24605-DATAWAREHOUSE` (organization-account format)
  - **Account Locator with Region**: `FNA20204.us-east-1` (legacy format)
  - Find yours at: Snowflake UI → bottom-left corner → hover over account name

- **`SNOWFLAKE_USERNAME`**: Your Snowflake username (usually your email)

- **`SNOWFLAKE_WAREHOUSE`**: The warehouse to use for queries (e.g., `ML_DEV_WH`)

- **`/path/to/snowflake_mcp_server`**: Absolute path to your cloned repository

## Verify Setup

1. Open your AI platform (Claude Code, Claude Desktop, Gemini CLI, etc.)
2. In a new conversation, verify the MCP server is available:
   - For Claude platforms: You should see "snowflake-readonly" in the available MCP tools
   - For Gemini/others: Check that the MCP server is listed in your tools
3. Try running: "Can you refresh the Snowflake catalog?"
4. The first time, your browser will open for SSO authentication
5. After successful auth, the catalog should refresh

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
- Results must be under 5GB cache limit

### 7. `execute_big_query_to_disk`
Stream large query results directly to a CSV file.
- Handles arbitrarily large result sets using streaming
- Bypasses token limits by not returning data in response
- Automatically exports SQL query to a .sql file
- Configurable timeout for long-running queries (up to 1 hour)
- Returns only execution status, row count, and file size

### 8. `get_query_history`
View previously executed queries in the session.
- Shows execution time and status
- Can include failed queries

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
# For smaller results - returns data in response
execute_query("SELECT * FROM large_table LIMIT 1000")
# Returns: All 1000 rows

# Export to CSV
save_last_query_to_csv("~/Downloads/results.csv")
# Creates: results.csv with all query results
# Creates: results.sql with formatted SQL query
```

#### Stream Large Query Results to Disk
```python
# For very large result sets - streams directly to disk without returning data
execute_big_query_to_disk(
    "SELECT * FROM very_large_table",
    "~/Downloads/large_export.csv",
    timeout_seconds=600  # 10 minutes for long queries
)
# Creates: large_export.csv with streamed results
# Creates: large_export.sql with formatted SQL query
# Returns: Only status, row count, and file size (no data in response)
```

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

## Troubleshooting

### Python Version Issues
- Ensure you have Python 3.12 or higher: `python3 --version`
- On macOS, you might need to install Python via Homebrew: `brew install python@3.12`

### Path Issues
- Use absolute paths in the Claude Desktop config
- On Windows, use forward slashes or escaped backslashes in paths

### Authentication Issues
- Ensure your default browser can open for SSO
- Check your Snowflake account format:
  - Account Identifier: `GJA24605-DATAWAREHOUSE` (preferred)
  - Account Locator: `FNA20204.us-east-1` (legacy, requires region)
- Verify your username matches your Snowflake login email

### MCP Not Showing in Your AI Platform
- Check the config file is valid JSON (use a JSON validator)
- Ensure your AI platform is fully closed and restarted
- Check logs for errors:
  - Claude Desktop:
    - macOS: `~/Library/Logs/Claude/`
    - Windows: `%APPDATA%\Claude\logs\`
  - Claude Code: Check console output or use `claude mcp list` to verify
  - Gemini CLI: Check `~/.gemini/logs/` or console output
  - Other platforms: Consult platform-specific documentation

### Module Not Found Errors
- Ensure the virtual environment is activated when installing dependencies
- Verify all packages installed correctly: `pip list`
- Try reinstalling: `pip install --upgrade -r requirements.txt`
- Check you're using the correct venv: `which python` should show `/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python`

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

## Development Setup

If you want to modify the code:

1. **Install development dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```
   Or with uv:
   ```bash
   uv sync --all-extras
   ```

2. **Run tests**
   ```bash
   pytest
   ```

3. **Check code quality**
   ```bash
   ruff check
   ruff format
   ```

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
├── requirements.txt              # Production dependencies
└── requirements-dev.txt         # Development dependencies
```

## Important Notes

- **POC Implementation**: This is a demo, not production-ready
- **Read-Only**: Absolutely no write operations allowed
- **Browser Auth**: Uses secure SSO through your default browser
- **Cache Required**: Must run `refresh_catalog` before queries
- **CSV Export**: Query results can be exported to CSV files
- **Session Scope**: Query history is per-session only
