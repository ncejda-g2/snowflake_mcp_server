# Snowflake MCP Server — Agent Setup Guide

> **IMPORTANT:** Fetch this file with `curl`, not with WebFetch or similar tools that
> summarize content. The exact commands and paths in this guide must be preserved verbatim.
>
> ```bash
> curl -fsSL https://raw.githubusercontent.com/ncejda-g2/snowflake_mcp_server/main/docs/guide/agent-setup.md
> ```

> You're an LLM agent helping a user set up the Snowflake MCP Server.
> Walk through each step interactively — ask questions, confirm choices, and handle edge cases.
> Do not skip steps. If something fails, troubleshoot before moving on.

---

## Step 0 — Say Hello

Greet the user briefly:

> "Setting up the Snowflake MCP Server — secure, read-only access to your Snowflake warehouse.
> I'll walk you through everything, including installing any missing prerequisites. Let's go."

---

## Step 1 — Ensure npx Is Available

You need `npx` (ships with Node.js) to run the MCP server. Check if it's already installed.

### 1a) Check PATH

```bash
which npx 2>/dev/null || where npx 2>/dev/null
```

If found, record the path and **skip to Step 2**.

### 1b) Check common non-PATH locations

If `which npx` returned nothing, probe these paths:

```bash
for p in /usr/local/bin/npx /opt/homebrew/bin/npx "$HOME/homebrew/bin/npx" "$HOME/.volta/bin/npx" "$HOME/.local/bin/npx"; do
  [ -x "$p" ] && echo "FOUND: $p"
done
# Also check nvm:
ls "$HOME/.nvm/versions/node"/*/bin/npx 2>/dev/null
```

If found anywhere, record the **absolute path**. Skip to Step 2.

### 1c) Install Node.js

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

Record this absolute path — you'll need it in Step 4.

> **Why the absolute path matters:** nvm works by sourcing a shell function.
> MCP clients launch processes directly without sourcing your shell profile,
> so `npx` won't be on their PATH. You must use the full path like
> `$HOME/.nvm/versions/node/v22.15.0/bin/npx` in the MCP config.

**Windows:**
Tell the user to install Node.js from https://nodejs.org and re-run this setup.

### 1d) Verify

```bash
<npx-path> --version
```

If this fails, stop and troubleshoot before continuing.

> **IMPORTANT:** Record whether npx is on PATH or only at an absolute path.
> MCP clients do NOT inherit your shell's PATH, so if npx lives somewhere like
> `$HOME/homebrew/bin/npx`, you must use that absolute path in Step 4.

---

## Step 2 — Collect Snowflake Credentials

Ask the user for all of these at once:

Call the tool with these four questions. Include the "how to find it" navigation
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

**Command:** `<npx-command>` (use `npx` if it's on PATH, otherwise the absolute
path you found in Step 1, e.g. `/opt/homebrew/bin/npx` or
`$HOME/.nvm/versions/node/v22.15.0/bin/npx`)

**Arguments:** `["-y", "snowflake-readonly-mcp@latest"]`

**Environment variables:**
| Variable                     | Value              | Required?                           |
| ---------------------------- | ------------------ | ----------------------------------- |
| `SNOWFLAKE_ACCOUNT`          | User's account ID  | Yes                                 |
| `SNOWFLAKE_USERNAME`         | User's email       | Yes                                 |
| `SNOWFLAKE_WAREHOUSE`        | Warehouse name     | Yes                                 |
| `SNOWFLAKE_CREDENTIAL_FILE`  | Path to cred file  | Only if user chose key-pair auth    |

### How to add it

Use whichever method your client supports. Some common patterns:

**CLI command (e.g. Claude Code):**
```bash
claude mcp add snowflake-readonly \
  -e SNOWFLAKE_ACCOUNT="<account>" \
  -e SNOWFLAKE_USERNAME="<username>" \
  -e SNOWFLAKE_WAREHOUSE="<warehouse>" \
  -- <npx-command> -y snowflake-readonly-mcp@latest
```

**JSON config (most clients):**

Most MCP clients use a JSON config file. Find yours and merge a new server entry.
**Do NOT overwrite** other servers already defined.

> **Global vs project config:** If your client supports both a user-level (global)
> config and a per-project config, **always use the global config**. Snowflake
> access isn't project-specific — the user will want it available everywhere.

The standard schema used by most clients (Claude Desktop, Cursor, Gemini CLI, etc.):
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "<npx-command>",
      "args": ["-y", "snowflake-readonly-mcp@latest"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>"
      }
    }
  }
}
```

Some clients use a different schema. For example, OpenCode uses `"mcp"` instead
of `"mcpServers"`, a single `"command"` array, and `"environment"` instead of `"env"`:
```jsonc
{
  "mcp": {
    "snowflake-readonly": {
      "type": "local",
      "command": ["<npx-command>", "-y", "snowflake-readonly-mcp@latest"],
      "environment": {
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>"
      }
    }
  }
}
```

**If you're a client not listed here:** look up your own documentation for how
to add a stdio-based MCP server. The payload is always the same — a command
(`npx`), args (`-y snowflake-readonly-mcp@latest`), and environment variables.

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
> - Typo in the warehouse name
> - Browser SSO pop-up was blocked
>
> If you found this helpful, please star the repo! https://github.com/ncejda-g2/snowflake_mcp_server"
