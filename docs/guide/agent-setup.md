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

After install, npx and node will be at:
```
$HOME/.nvm/versions/node/v<VERSION>/bin/npx
$HOME/.nvm/versions/node/v<VERSION>/bin/node
```

Find the exact paths and record **both the npx path and its parent directory**:
```bash
NVM_BIN="$(dirname "$(nvm which current)")"
echo "npx path:  $NVM_BIN/npx"
echo "node path: $NVM_BIN/node"
echo "bin dir:   $NVM_BIN"
```

Record both:
- **npx absolute path** — you'll use this as the command in Step 4
- **bin directory** — you'll need this to set PATH in Step 4 so `node` is
  discoverable

> **⚠️ Why nvm + GUI clients cause silent failures:** nvm works by sourcing a
> shell function — it doesn't place `node` or `npx` on a system-wide PATH.
> GUI-based MCP clients (Claude Desktop, Cursor) never source your shell profile,
> so even with the absolute path to `npx`, the spawned process can't find `node`.
> The server fails silently with no visible error. Step 4 shows how to fix this
> by injecting the nvm bin directory into the `PATH` env var of your MCP config.

**Windows:**
Tell the user to install Node.js from https://nodejs.org and re-run this setup.

### 1d) Verify both npx AND node

Both `npx` and `node` must be reachable. `npx` downloads the package, but the
package's entry script (`#!/usr/bin/env node`) needs `node` on PATH to execute.

```bash
<npx-path> --version
# Also verify node — it MUST be in the same directory:
"$(dirname "<npx-path>")/node" --version
```

If either fails, stop and troubleshoot before continuing.

Record:
1. The **absolute path to npx** (e.g. `/opt/homebrew/bin/npx`)
2. The **directory containing both npx and node** (e.g. `/opt/homebrew/bin`)
   — you'll need this in Step 4.

> **IMPORTANT: PATH visibility varies by client type.**
>
> **Terminal-based MCP clients** (Claude Code, OpenCode, Gemini CLI, etc.) inherit
> your shell's PATH, so just `npx` will work if `which npx` found it.
>
> **GUI-based MCP clients** (Claude Desktop, Cursor, etc.) launch processes outside
> your shell and do NOT inherit PATH. You must use the absolute path to `npx` AND
> ensure `node` is discoverable — see Step 4 for how to set this up.
>
> **nvm users — critical:** nvm places `node` and `npx` in a version-specific
> directory (e.g. `~/.nvm/versions/node/v22.15.0/bin/`) that is only on PATH
> when your shell profile has been sourced. GUI apps never source shell profiles,
> so `node` will be invisible to them. Step 4 shows how to fix this with a `PATH`
> env var in your MCP config.

---

## Step 2 — Ensure uv and Python Are Available

The MCP server is a Python application managed by [uv](https://docs.astral.sh/uv/).
After `npx` downloads the package, the Node.js wrapper calls `uv run` to start the
Python server. Both `uv` and a compatible Python version (≥3.12, <3.14) must be
available. **Neither requires sudo.**

### 2a) Check for uv

```bash
which uv 2>/dev/null || where uv 2>/dev/null
```

If found, record the absolute path and check the version:
```bash
uv --version
```

If `uv` is already installed, **skip to 2c**.

### 2b) Install uv

uv installs to `~/.local/bin` — no sudo required on any platform.

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After install, ensure `~/.local/bin` is on your PATH. The installer usually prints
instructions — follow them, then **restart your shell** (or `source ~/.bashrc` /
`source ~/.zshrc`).

Verify:
```bash
uv --version
```

**Windows:**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2c) Ensure a compatible Python version

The server requires Python ≥3.12 and <3.14. Check what's available:

```bash
uv python list --only-installed 2>/dev/null | head -5
```

If no 3.12.x or 3.13.x version appears, install one with uv (no sudo needed):

```bash
uv python install 3.13
```

uv manages its own Python installations in `~/.local/share/uv/python/` — this
does not conflict with any system Python.

### 2d) Verify the full chain

Run a quick smoke test to confirm uv can launch the server's Python:

```bash
uv run --python 3.13 python -c "import sys; print(f'Python {sys.version}')"
```

If this prints a version line, you're good. If it errors, troubleshoot before
continuing.

> **Why Python <3.14?** The server depends on `pydantic-core`, which uses compiled
> Rust extensions. As of this writing, pre-built wheels are only available for
> Python ≤3.13. Python 3.14 would require building from source, which often fails.

---

## Step 3 — Collect Snowflake Credentials

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

## Step 4 — Add the MCP Server to Your Client

You are already running inside an MCP client — you know which one you are.
Add a new MCP server with these details:

**Server name:** `snowflake-readonly`

**Command:** `<npx-command>` — choose based on your client type:
- **Terminal-based clients** (Claude Code, OpenCode, Gemini CLI): use `npx` if
  `which npx` found it — these clients inherit your shell's PATH.
- **GUI-based clients** (Claude Desktop, Cursor): use the absolute path from
  Step 1 (e.g. `/opt/homebrew/bin/npx`), since GUI apps don't inherit PATH.
- **nvm users (any client)**: always use the absolute path
  (e.g. `$HOME/.nvm/versions/node/v22.15.0/bin/npx`) — see the nvm note in Step 1.

**Arguments:** `["-y", "snowflake-readonly-mcp@latest"]`

**Environment variables:**
| Variable                    | Value                         | Required?                                          |
| --------------------------- | ----------------------------- | -------------------------------------------------- |
| `SNOWFLAKE_ACCOUNT`         | User's account ID             | Yes                                                |
| `SNOWFLAKE_USERNAME`        | User's email                  | Yes                                                |
| `SNOWFLAKE_WAREHOUSE`       | Warehouse name                | Yes                                                |
| `SNOWFLAKE_ROLE`            | Snowflake role                | Yes                                                |
| `SNOWFLAKE_CREDENTIAL_FILE` | Path to cred file             | Only if user chose key-pair auth                   |
| `PATH`                      | See note below                | **Yes** for GUI clients or nvm users               |

> **⚠️ PATH is required for GUI clients and nvm users.**
>
> GUI-based MCP clients do NOT inherit your shell's PATH. The server needs `node`,
> `uv`, and `python` to be discoverable. Build a PATH that includes every bin
> directory recorded in Steps 1 and 2:
>
> ```
> <node-bin-dir>:<uv-bin-dir>:/usr/local/bin:/usr/bin:/bin
> ```
>
> **Example (nvm + uv):**
> ```
> /Users/you/.nvm/versions/node/v22.15.0/bin:/Users/you/.local/bin:/usr/local/bin:/usr/bin:/bin
> ```
>
> **Example (Homebrew):**
> ```
> /opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
> ```
>
> **Terminal-based clients** generally inherit PATH from your shell, so this is
> only needed if `which npx`, `which uv`, or `which node` fail inside the client.

### How to add it

Use whichever method your client supports. Some common patterns:

**CLI command (e.g. Claude Code):**
```bash
claude mcp add snowflake-readonly \
  -e SNOWFLAKE_ACCOUNT="<account>" \
  -e SNOWFLAKE_USERNAME="<username>" \
  -e SNOWFLAKE_WAREHOUSE="<warehouse>" \
  -e SNOWFLAKE_ROLE="<role>" \
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
        "PATH": "<node-bin-dir>:<uv-bin-dir>:/usr/local/bin:/usr/bin:/bin",
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>",
        "SNOWFLAKE_ROLE": "<role>"
      }
    }
  }
}
```

> **Tip:** For terminal-based clients where PATH is already inherited, you can
> omit the `PATH` entry. For GUI clients, always include it.

Some clients use a different schema. For example, OpenCode uses `"mcp"` instead
of `"mcpServers"`, a single `"command"` array, and `"environment"` instead of `"env"`:
```jsonc
{
  "mcp": {
    "snowflake-readonly": {
      "type": "local",
      "command": ["<npx-command>", "-y", "snowflake-readonly-mcp@latest"],
      "environment": {
        "PATH": "<node-bin-dir>:<uv-bin-dir>:/usr/local/bin:/usr/bin:/bin",
        "SNOWFLAKE_ACCOUNT": "<account>",
        "SNOWFLAKE_USERNAME": "<username>",
        "SNOWFLAKE_WAREHOUSE": "<warehouse>",
        "SNOWFLAKE_ROLE": "<role>"
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

## Step 5 — Wrap Up

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
> - `uv` or Python not found (re-run Step 2 to verify)
>
> Check `~/.snowflake_mcp/server.log` for detailed error messages.
> If the log file doesn't exist, the server never started — re-check
> that `npx`, `uv`, and Python 3.12+ are all reachable from your PATH.
>
> If you found this helpful, please star the repo! https://github.com/ncejda-g2/snowflake_mcp_server"
