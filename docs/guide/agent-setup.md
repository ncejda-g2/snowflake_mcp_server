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

## Step 1 — Choose Install Method

Ask which install method the user prefers:

```json
{
  "questions": [
    {
      "question": "How would you like to install the Snowflake MCP Server?",
      "header": "Install Method",
      "options": [
        {
          "label": "uvx (Recommended)",
          "description": "Python-based — no Node.js required. One install command, no sudo needed."
        },
        {
          "label": "npx",
          "description": "Node.js-based — uses npx to run the server. Works if you already have Node.js."
        }
      ]
    }
  ]
}
```

Based on the answer, follow **Step 1a** (uvx) or **Step 1b** (npx).

---

### Step 1a — Install uv (uvx path)

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

**Skip to Step 2.**

---

### Step 1b — Install npx (npx path)

You need `npx` (ships with Node.js) to run the MCP server. Check if it's already installed.

#### Check PATH

```bash
which npx 2>/dev/null || where npx 2>/dev/null
```

If found, record the path and **skip to Step 2**.

#### Check common non-PATH locations

If `which npx` returned nothing, probe these paths:

```bash
for p in /usr/local/bin/npx /opt/homebrew/bin/npx "$HOME/homebrew/bin/npx" "$HOME/.volta/bin/npx" "$HOME/.local/bin/npx"; do
  [ -x "$p" ] && echo "FOUND: $p"
done
# Also check nvm:
ls "$HOME/.nvm/versions/node"/*/bin/npx 2>/dev/null
```

If found anywhere, record the **absolute path**. Skip to Step 2.

#### Install Node.js

If npx isn't installed at all, install it.

**macOS:**

1. Check for Homebrew:
   ```bash
   which brew 2>/dev/null || ls /opt/homebrew/bin/brew "$HOME/homebrew/bin/brew" 2>/dev/null
   ```

2. If Homebrew is missing, install it:
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```
   > Without `sudo`, Homebrew self-installs to `$HOME/homebrew`. That's fine.
   > Follow any post-install PATH instructions Homebrew prints.

3. Install Node:
   ```bash
   brew install node
   ```

4. Find npx after install — it may be at a non-standard path like `$HOME/homebrew/bin/npx`.
   Record the absolute path.

> **Alternative (macOS):** If Homebrew gives you trouble, you can use nvm instead —
> the same approach described in the Linux "Without sudo" section below works on macOS too.

**Linux:**

First, check if the user has sudo access:
```bash
sudo -n true 2>/dev/null && echo "HAS_SUDO" || echo "NO_SUDO"
```

**With sudo:**
```bash
sudo apt-get update && sudo apt-get install -y nodejs npm
```
Then locate npx with `which npx`.

**Without sudo — use nvm (Node Version Manager):**

nvm installs entirely to `$HOME/.nvm` — no root required.

```bash
# Install nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash

# Load nvm into the current shell (the installer adds this to your profile,
# but it won't take effect until the next shell session)
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"

# Install the latest LTS version of Node
nvm install --lts
```

After install, npx will be at:
```
$HOME/.nvm/versions/node/v<VERSION>/bin/npx
```

Find the exact path:
```bash
NVM_NPX="$(dirname "$(nvm which current)")/npx" && echo "$NVM_NPX"
```

Record this absolute path — you'll need it in Step 3.

> **Why the absolute path matters for nvm:** nvm works by sourcing a shell
> function — it doesn't place `npx` on a fixed PATH. Even terminal-based MCP
> clients that inherit PATH won't find an nvm-managed `npx` unless the shell
> profile has already been sourced. You must use the full path like
> `$HOME/.nvm/versions/node/v22.15.0/bin/npx` in the MCP config.

**Windows:**
Tell the user to install Node.js from https://nodejs.org and re-run this setup.

#### Verify

```bash
<npx-path> --version
```

If this fails, stop and troubleshoot before continuing.

> **IMPORTANT:** Record whether npx is on PATH or only at an absolute path.
>
> **Terminal-based MCP clients** (Claude Code, OpenCode, Gemini CLI, etc.) inherit
> your shell's PATH, so just `npx` will work if `which npx` found it.
>
> **GUI-based MCP clients** (Claude Desktop, Cursor, etc.) launch processes outside
> your shell and do NOT inherit PATH. If you're configuring one of these, you must
> use the absolute path (e.g. `/opt/homebrew/bin/npx`) in the MCP config.

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

### If using uvx (from Step 1a)

**Command:** `uvx` (or the absolute path to `uvx` for GUI-based clients)
**Arguments:** `["snowflake-readonly-mcp"]`

**Environment variables:**
| Variable                     | Value              | Required?                           |
| ---------------------------- | ------------------ | ----------------------------------- |
| `SNOWFLAKE_ACCOUNT`          | User's account ID  | Yes                                 |
| `SNOWFLAKE_USERNAME`         | User's email       | Yes                                 |
| `SNOWFLAKE_WAREHOUSE`        | Warehouse name     | Yes                                 |
| `SNOWFLAKE_ROLE`             | Snowflake role     | Yes                                 |
| `SNOWFLAKE_CREDENTIAL_FILE`  | Path to cred file  | Only if user chose key-pair auth    |

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

### If using npx (from Step 1b)

**Command:** `<npx-command>` — choose based on your client type:
- **Terminal-based clients** (Claude Code, OpenCode, Gemini CLI): use `npx` if
  `which npx` found it — these clients inherit your shell's PATH.
- **GUI-based clients** (Claude Desktop, Cursor): use the absolute path from
  Step 1b (e.g. `/opt/homebrew/bin/npx`), since GUI apps don't inherit PATH.
- **nvm users (any client)**: always use the absolute path
  (e.g. `$HOME/.nvm/versions/node/v22.15.0/bin/npx`) — see the nvm note in Step 1b.

**Arguments:** `["-y", "snowflake-readonly-mcp@latest"]`

**Environment variables:**
| Variable                     | Value              | Required?                           |
| ---------------------------- | ------------------ | ----------------------------------- |
| `SNOWFLAKE_ACCOUNT`          | User's account ID  | Yes                                 |
| `SNOWFLAKE_USERNAME`         | User's email       | Yes                                 |
| `SNOWFLAKE_WAREHOUSE`        | Warehouse name     | Yes                                 |
| `SNOWFLAKE_ROLE`             | Snowflake role     | Yes                                 |
| `SNOWFLAKE_CREDENTIAL_FILE`  | Path to cred file  | Only if user chose key-pair auth    |

**CLI command (e.g. Claude Code):**
```bash
claude mcp add snowflake-readonly \
  -e SNOWFLAKE_ACCOUNT="<account>" \
  -e SNOWFLAKE_USERNAME="<username>" \
  -e SNOWFLAKE_WAREHOUSE="<warehouse>" \
  -e SNOWFLAKE_ROLE="<role>" \
  -- <npx-command> -y snowflake-readonly-mcp@latest
```

**Standard JSON config** (Claude Desktop, Cursor, Gemini CLI, etc.):
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "<npx-command>",
      "args": ["-y", "snowflake-readonly-mcp@latest"],
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
      "command": ["<npx-command>", "-y", "snowflake-readonly-mcp@latest"],
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
>   never started — check that `uvx` or `npx` is on PATH or use the absolute path
>
> Check `~/.snowflake_mcp/server.log` for detailed error messages.
>
> If you found this helpful, please star the repo! https://github.com/ncejda-g2/snowflake_mcp_server"
