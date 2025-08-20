# Snowflake MCP Server

A secure, read-only Model Context Protocol (MCP) server for Snowflake data access using external browser authentication (SSO).

## Features

- 🔒 **Strict Read-Only Access**: Multiple layers of protection against write operations
- 🔑 **External Browser Authentication**: Uses Snowflake's secure browser-based SSO
- 💾 **Smart Caching**: 5-day schema cache for fast metadata access
- 📄 **Pagination**: Automatic pagination for large result sets
- 🛡️ **Query Validation**: Comprehensive SQL validation before execution

## Prerequisites

- Python 3.12 or higher
- Claude Desktop installed
- Snowflake account with SSO access
- Git (for cloning the repository)

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

## Configure Claude Desktop

1. **Find your Claude Desktop configuration file**
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
   - Linux: `~/.config/claude/claude_desktop_config.json`

2. **Add the MCP server configuration**

   For venv installation:
   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
         "args": ["/path/to/snowflake_mcp_server/main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account.region",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```

   For uv installation:
   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "uv",
         "args": ["--directory", "/path/to/snowflake_mcp_server", "run", "python", "main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account.region",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```
   
   Note: The `--directory` flag tells uv which project directory to use, ensuring it finds the correct virtual environment.

   For Conda installation:
   ```json
   {
     "mcpServers": {
       "snowflake-readonly": {
         "command": "/path/to/conda/envs/snowflake-mcp/bin/python",
         "args": ["/path/to/snowflake_mcp_server/main.py"],
         "env": {
           "SNOWFLAKE_ACCOUNT": "your-account.region",
           "SNOWFLAKE_USERNAME": "your-email@company.com",
           "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE"
         }
       }
     }
   }
   ```
   
   Note: Find your conda environment path with `conda info --envs` after activating the environment.

3. **Update the configuration values**
   - `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., "xy12345.us-east-1")
   - `SNOWFLAKE_USERNAME`: Your Snowflake username (usually your email)
   - `SNOWFLAKE_WAREHOUSE`: The warehouse to use for queries

4. **Restart Claude Desktop** to load the new configuration

## Verify Setup

1. Open Claude Desktop
2. In a new conversation, you should see "snowflake-readonly" in the available MCP tools
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
Execute read-only SQL queries with pagination.
- Only allows SELECT, SHOW, DESCRIBE, WITH queries
- Blocks all write operations
- Returns 100 rows per page by default
- Use `page` parameter for pagination

### 6. `get_query_history`
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

#### Paginated Results
```python
# First page (automatic)
execute_query("SELECT * FROM large_table")
# Returns: First 100 rows, pagination info

# Get next page
execute_query("SELECT * FROM large_table", page=2)
# Returns: Rows 101-200
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
- Check that your Snowflake account includes the region (e.g., "xy12345.us-east-1")
- Verify your username matches your Snowflake login email

### MCP Not Showing in Claude
- Check the config file is valid JSON (use a JSON validator)
- Ensure Claude Desktop is fully closed and restarted
- Check Claude Desktop logs for errors:
  - macOS: `~/Library/Logs/Claude/`
  - Windows: `%APPDATA%\Claude\logs\`

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
- **Pagination**: Large results are automatically paginated
- **Session Scope**: Query history is per-session only