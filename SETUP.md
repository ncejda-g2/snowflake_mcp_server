# Snowflake MCP Server - Setup Guide

This guide will help you set up the Snowflake MCP Server on your machine to use with Claude Desktop.

## Prerequisites

- Python 3.12 or higher
- Claude Desktop installed
- Snowflake account with SSO access
- Git (for cloning the repository)

## Installation Options

### Option 1: Using Python venv (Recommended for most users)

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

### Option 2: Using uv (Fast Python package manager)

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

### Option 3: Using Conda/Miniconda

1. **Create conda environment**
   ```bash
   conda create -n snowflake-mcp python=3.12
   conda activate snowflake-mcp
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

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

## Optional: Development Setup

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

## Security Notes

- This server uses SSO authentication - no passwords are stored
- All queries are read-only - the server cannot modify your Snowflake data
- Authentication tokens are managed by the Snowflake connector
- The browser-based auth token expires after a period of inactivity

## Support

If you encounter issues:
1. Check this troubleshooting guide
2. Review the error messages in Claude Desktop
3. Check the server logs (set `DEBUG=true` in the env for detailed logs)
4. Contact your team's Snowflake administrator for account-specific issues