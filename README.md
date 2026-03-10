<div align="center">
  <br />
  <img src="assets/logo.png" alt="Snowflake MCP" width="200" />
  <br />
  <br />

  <h1 align="center">Snowflake MCP Server</h1>

  <p align="center">
    <strong>Secure, read-only access to Snowflake with AI-powered query assistance</strong>
  </p>

  <p align="center">
    <a href="https://github.com/ncejda-g2/snowflake_mcp_server/releases">
      <img src="https://img.shields.io/badge/version-v0.1.15-9b59b6" alt="Version" />
    </a>
    <a href="./CHANGELOG.md">
      <img src="https://img.shields.io/badge/changelog-Latest%20Changes-blue" alt="Changelog" />
    </a>
    <a href="https://modelcontextprotocol.io">
      <img src="https://img.shields.io/badge/MCP-Model%20Context%20Protocol-green" alt="MCP" />
    </a>
  </p>

  <p align="center">
    <a href="#features">Features</a> •
    <a href="#-quick-start">Quick Start</a> •
    <a href="#configuration">Configuration</a> •
    <a href="#available-commands">Commands</a> •
    <a href="./docs">Documentation</a>
  </p>
</div>

<br />

---

## What is Snowflake MCP?

Snowflake MCP Server bridges the gap between your Snowflake data warehouse and AI assistants like Claude. It provides a secure, **read-only** interface that lets AI help you explore schemas, write queries, and analyze data—all while maintaining enterprise-grade security through SSO authentication.

<details>
<summary><b>Demo</b></summary>

<div align="center">
  <br />
  <img src="assets/demo.gif" alt="Demo" width="800" />
  <br />
  <br />
</div>

</details>

## Features

- 🔒 **Strict Read-Only Access**: Multiple layers of protection against write operations
- 🔑 **Flexible Authentication**: Browser-based SSO or headless key-pair auth via credential file
- 💾 **Smart Caching**: 5-day schema cache for fast metadata access, reducing generic Snowflake schema queries and credit usage
- 📄 **CSV Export**: Export query results directly to CSV files
- 🛡️ **Query Validation**: Comprehensive SQL validation before execution
- 🎯 **Responsible Token Management**: Lightweight outputs to minimize token usage

## 🚀 Easy Setup For LLM Agents

Already in Claude Code, OpenCode, Cursor, or another AI coding agent? Paste this into your agent:

```
Set up the Snowflake MCP server for me by following this guide:
https://raw.githubusercontent.com/ncejda-g2/snowflake_mcp_server/main/docs/guide/agent-setup.md
```

Your agent will walk you through everything interactively — including installing Node.js and Homebrew if needed. No manual config editing required.

<details>
<summary><b>Manual setup & from-source install</b></summary>

### npx (no install needed)

Just configure your MCP client using the examples in the [Configuration](#configuration) section below.

### From Source

```bash
git clone git@github.com:ncejda-g2/snowflake_mcp_server.git
cd snowflake_mcp_server
python3 -m venv snowflake_mcp_env
source snowflake_mcp_env/bin/activate  # On Windows: snowflake_mcp_env\Scripts\activate
pip install -r requirements.txt
```

</details>

## Configuration

<details>
<summary><b>Claude Code</b></summary>

Edit your `~/.claude.json` file:

**Using npx (Recommended):**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "npx",
      "args": ["-y", "snowflake-readonly-mcp"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

**Using local clone:**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
      "args": ["/path/to/snowflake_mcp_server/main.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

Replace:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository (local clone only)
- `your-account`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name
- `YOUR_ROLE`: Your Snowflake role (e.g., `ANALYST`, `PUBLIC`)
- `SNOWFLAKE_CREDENTIAL_FILE` *(optional)*: Path to a JSON credential file for headless key-pair auth (omit to use browser SSO)

</details>

<details>
<summary><b>Claude Desktop</b></summary>

Edit your configuration file:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- Linux: `~/.config/claude/claude_desktop_config.json`

**Using npx (Recommended):**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "npx",
      "args": ["-y", "snowflake-readonly-mcp"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

**Using local clone:**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
      "args": ["/path/to/snowflake_mcp_server/main.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

Replace:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository (local clone only)
- `your-account`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name
- `YOUR_ROLE`: Your Snowflake role (e.g., `ANALYST`, `PUBLIC`)
- `SNOWFLAKE_CREDENTIAL_FILE` *(optional)*: Path to a JSON credential file for headless key-pair auth (omit to use browser SSO)

</details>

<details>
<summary><b>Cursor</b></summary>

Edit your Cursor settings:

**Using npx (Recommended):**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "npx",
      "args": ["-y", "snowflake-readonly-mcp"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

**Using local clone:**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
      "args": ["/path/to/snowflake_mcp_server/main.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

Replace:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository (local clone only)
- `your-account`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name
- `YOUR_ROLE`: Your Snowflake role (e.g., `ANALYST`, `PUBLIC`)
- `SNOWFLAKE_CREDENTIAL_FILE` *(optional)*: Path to a JSON credential file for headless key-pair auth (omit to use browser SSO)

</details>

<details>
<summary><b>OpenCode</b></summary>

Edit your `~/.config/opencode/opencode.json` file (global) or `opencode.json` in your project root (project-level):

> **Note:** OpenCode uses `"mcp"` (not `"mcpServers"`), `"command"` as a single array (not separate `command`/`args`), and `"environment"` (not `"env"`).

**Using npx (Recommended):**

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "snowflake-readonly": {
      "type": "local",
      "command": ["npx", "-y", "snowflake-readonly-mcp"],
      "environment": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

**Using local clone:**

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "snowflake-readonly": {
      "type": "local",
      "command": ["/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python", "/path/to/snowflake_mcp_server/main.py"],
      "environment": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

Replace:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository (local clone only)
- `your-account`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name
- `YOUR_ROLE`: Your Snowflake role (e.g., `ANALYST`, `PUBLIC`)
- `SNOWFLAKE_CREDENTIAL_FILE` *(optional)*: Path to a JSON credential file for headless key-pair auth (omit to use browser SSO)

</details>

<details>
<summary><b>Gemini CLI</b></summary>

Edit your `~/.gemini/settings.json` file:

**Using npx (Recommended):**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "npx",
      "args": ["-y", "snowflake-readonly-mcp"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

**Using local clone:**

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "/path/to/snowflake_mcp_server/snowflake_mcp_env/bin/python",
      "args": ["/path/to/snowflake_mcp_server/main.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_ROLE": "YOUR_ROLE",
        "SNOWFLAKE_CREDENTIAL_FILE": "/path/to/credentials.json"  // optional — omit to use browser SSO
      }
    }
  }
}
```

Replace:
- `/path/to/snowflake_mcp_server`: Absolute path to your cloned repository (local clone only)
- `your-account`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `your-email@company.com`: Your Snowflake username
- `YOUR_WAREHOUSE`: Your Snowflake warehouse name
- `YOUR_ROLE`: Your Snowflake role (e.g., `ANALYST`, `PUBLIC`)
- `SNOWFLAKE_CREDENTIAL_FILE` *(optional)*: Path to a JSON credential file for headless key-pair auth (omit to use browser SSO)

</details>

## Available Commands

The server provides powerful tools for interacting with Snowflake:

| Tool | Description |
|------|-------------|
| `refresh_catalog` | Scan and cache all database schemas |
| `show_tables` | Browse database hierarchy with pattern filters (like SHOW TABLES) |
| `find_tables` | Search for tables by keyword across all databases |
| `describe_table` | View detailed column information (like DESCRIBE TABLE) |
| `execute_query` | Run read-only SQL queries |
| `execute_big_query_to_disk` | Stream large results to CSV |
| `save_last_query_to_csv` | Export query results |

## 📚 Documentation

- [Changelog](./CHANGELOG.md) - Version history and updates



---

<div align="center">
  <p>
    <strong>Built with ❄️ for the AI + Data community</strong>
  </p>
  <p>
    <a href="https://github.com/ncejda-g2/snowflake_mcp_server/issues">Report Bug</a> •
    <a href="https://github.com/ncejda-g2/snowflake_mcp_server/issues">Request Feature</a>
  </p>
</div>
