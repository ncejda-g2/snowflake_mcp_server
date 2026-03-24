# Snowflake MCP Server — Agent Setup Guide

> You're an LLM agent helping a user set up the Snowflake MCP Server.
> Walk through each step interactively — ask questions, confirm choices, and handle edge cases.
> Do not skip steps. If something fails, troubleshoot before moving on.

---

## Step 0 — Say Hello

Greet the user briefly:

> "Setting up the Snowflake MCP Server — secure, read-only access to your Snowflake warehouse.
> I'll walk you through everything, including installing any missing prerequisites. Let's go."

---

## Step 1 — Install uv

You need `uvx` (ships with `uv`) to run the MCP server. Check if it's already installed.

```bash
which uvx 2>/dev/null || where uvx 2>/dev/null
```

If found, record the path and **skip to Step 2**.

If not found, install uv:

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

No sudo required. Installs to `~/.local/bin/`.

After install, ensure `~/.local/bin` is on PATH:
```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Windows:**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Verify:**
```bash
uvx --version
```

If this fails, stop and troubleshoot before continuing.

> **GUI-based MCP clients** (Claude Desktop, Cursor) launch processes outside
> your shell and do NOT inherit PATH. Record the absolute path to `uvx`
> (e.g. `~/.local/bin/uvx`) — you'll need it in Step 3.

---

## Step 2 — Collect Snowflake Credentials

Ask the user for all of these at once:

Call the tool with these five questions. Include the "how to find it" navigation
tips in the question text so the user sees the help right when they need it.

```json
{
  "questions": [
    {
      "question": "What's your Snowflake account identifier?\n\nTo find it: open Snowflake in your browser (via app.snowflake.com, your company's Okta/SSO portal, or wherever you sign in). In the bottom-left corner, click your account/org name → hover over Account → click View account details. Copy the identifier — it looks like xy12345.us-east-1 or ORGNAME-ACCOUNTNAME (either format works).",
      "header": "Account Identifier",
      "options": []
    },
    {
      "question": "What's your Snowflake username?\n\nTo find it: on the same View Account Details page from the previous step, look for the field labeled \"Login Name\" — that's your username.",
      "header": "Username",
      "options": []
    },
    {
      "question": "Which Snowflake warehouse should the MCP server use?\n\nTo see your options: in Snowflake, click the \"+ New SQL File\" button (or the \"+\" button) to open a SQL Worksheet. There's a warehouse selector dropdown near the top — it shows your active warehouse and all warehouses you have access to. Any of them will work — the MCP server only runs lightweight read-only queries.",
      "header": "Warehouse Name",
      "options": []
    },
    {
      "question": "Which Snowflake role should the MCP server use?\n\nTo see your options: in a Snowflake SQL Worksheet, run: SELECT CURRENT_ROLE(); to see your current role, or click the role selector dropdown near the top of the worksheet to see all roles available to you. Pick one that has read access to the data you want to query.",
      "header": "Role",
      "options": []
    },
    {
      "question": "Which authentication method do you want to use?",
      "header": "Auth Method",
      "options": [
        {
          "label": "Browser SSO (Recommended)",
          "description": "No extra setup — just approve a pop-up in your browser when connecting"
        },
        {
          "label": "Key-pair credential file",
          "description": "For headless/server use — you'll need the path to your credential file"
        }
      ]
    }
  ]
}
```

If the user picks **key-pair credential file**, follow up and ask for the file path.

---

## Step 3 — Add the MCP Server to Your Client

You are already running inside an MCP client — you know which one you are.
Add a new MCP server with these details:

**Server name:** `snowflake-readonly`

**Command:** `uvx` (or the absolute path to `uvx` for GUI-based clients)
**Arguments:** `["snowflake-readonly-mcp"]`

**Environment variables:**
| Variable                    | Value             | Required?                        |
|-----------------------------|-------------------|----------------------------------|
| `SNOWFLAKE_ACCOUNT`         | User's account ID | Yes                              |
| `SNOWFLAKE_USERNAME`        | User's email      | Yes                              |
| `SNOWFLAKE_WAREHOUSE`       | Warehouse name    | Yes                              |
| `SNOWFLAKE_ROLE`            | Snowflake role    | Yes                              |
| `SNOWFLAKE_CREDENTIAL_FILE` | Path to cred file | Only if user chose key-pair auth |

**CLI command (e.g. Claude Code):**
```bash
claude mcp add snowflake-readonly \
  -e SNOWFLAKE_ACCOUNT="<account>" \
  -e SNOWFLAKE_USERNAME="<username>" \
  -e SNOWFLAKE_WAREHOUSE="<warehouse>" \
  -e SNOWFLAKE_ROLE="<role>" \
  -- uvx snowflake-readonly-mcp
```

**Standard JSON config** (Claude Desktop, Cursor, Gemini CLI, etc.):
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "uvx",
      "args": ["snowflake-readonly-mcp"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>",
        "SNOWFLAKE_ROLE": "<role>"
      }
    }
  }
}
```

**OpenCode JSON** (different schema):
```jsonc
{
  "mcp": {
    "snowflake-readonly": {
      "type": "local",
      "command": ["uvx", "snowflake-readonly-mcp"],
      "environment": {
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>",
        "SNOWFLAKE_ROLE": "<role>"
      }
    }
  }
}
```

### How to add it

Use whichever method your client supports — the CLI command or JSON config above.

**JSON config:** Find your client's config file and merge a new server entry.
**Do NOT overwrite** other servers already defined.

> **Global vs project config:** If your client supports both a user-level (global)
> config and a per-project config, **always use the global config**. Snowflake
> access isn't project-specific — the user will want it available everywhere.

**If you're a client not listed here:** look up your own documentation for how
to add a stdio-based MCP server. The payload is always the same — a command,
args, and environment variables.

**Show the user the final config diff before writing**, so they can confirm.

---

## Step 4 — Wrap Up

Everything is configured. Tell the user:

> "You're all set! Here's what to do next:
>
> 1. **Restart your agent / MCP client** so it picks up the new server.
> 2. In your new session, verify it works by asking:
>    `Show me what databases are available in Snowflake`
>
> If the connection fails, the most common issues are:
> - Wrong account identifier (should look like `xy12345.us-east-1`)
> - Typo in the warehouse or role name
> - Role doesn't have access to the data you're querying
> - Browser SSO pop-up was blocked
> - If the log file doesn't exist (`~/.snowflake_mcp/server.log`), the server
>   never started — check that `uvx` is on PATH or use the absolute path
>
> Check `~/.snowflake_mcp/server.log` for detailed error messages.
>
> If you found this helpful, please star the repo! https://github.com/ncejda-g2/snowflake_mcp_server"

---

## Deprecated: npx install method

> **This method is deprecated.** The uvx method above is recommended — it's
> simpler, has fewer dependencies, and doesn't require Node.js. This section
> is preserved for users who already have a working npx-based setup.

To use npx instead of uvx, replace the command and arguments in Step 3:

**Command:** `npx` (or the absolute path for GUI clients / nvm users)
**Arguments:** `["-y", "snowflake-readonly-mcp@latest"]`

Everything else (env vars, credentials, config location) is identical.

> **Additional dependencies:** The npx method requires both `node` and `npx` on
> PATH (or provided as absolute paths). It also requires Python 3.12–3.13 and
> `uv` to be installed, since the npm package spawns a Python process underneath.
>
> **nvm users:** nvm-managed `npx` and `node` are not on PATH by default. You
> must use full paths (e.g. `$HOME/.nvm/versions/node/v22.15.0/bin/npx`) in the
> MCP config, and ensure `node` from the same directory is also on PATH. GUI-based
> clients (Claude Desktop, Cursor) require absolute paths since they don't inherit
> your shell's PATH. Set the `PATH` env var in your MCP config to include the
> nvm bin directory.
