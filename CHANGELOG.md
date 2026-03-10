# Changelog

All notable changes to the Snowflake MCP Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.16] - 2026-03-10

### Fixed
- **BREAKING**: `SNOWFLAKE_ROLE` is now a required environment variable — removed hardcoded `ML_DEVELOPER` default that caused silent failures for non-G2 users
- Log file permissions: `~/.snowflake-mcp/` restricted to `0700`, `server.log` to `0600` (previously world-readable)
- `credential_file` config field hidden from repr output

### Changed
- Agent setup guide now asks for Snowflake role in Step 2
- All README config examples include `SNOWFLAKE_ROLE`

## [0.1.15] - 2026-03-10

### Added
- Persistent file logging to `~/.snowflake-mcp/server.log` (RotatingFileHandler, 5MB max, 2 backups) — logs are now always available even in MCP clients that don't capture stderr (Claude Code, Cursor)
- Startup milestone logging: account, warehouse, transport mode, and log file path printed on start
- `PYTHONUNBUFFERED=1` in Node.js wrapper to ensure Python stderr flushes before crash

### Fixed
- Removed duplicate `logging.basicConfig()` in `server/app.py` that competed with `log_utils.py`

## [0.1.14] - 2026-03-10

### Improved
- Agent setup guide: Better credential-finding instructions with inline navigation tips (how to find account ID, username, warehouse)
- Agent setup guide: Use `@latest` tag for `snowflake-readonly-mcp` in all npx examples
- Agent setup guide: Add global vs project config guidance
- Agent setup guide: More enthusiastic star-the-repo CTA

### Fixed
- `uv.lock`: Sync version to match `pyproject.toml`

## [0.1.13] - 2026-03-10

### Added
- Agent-driven interactive setup guide (`docs/guide/agent-setup.md`) — any LLM agent can walk users through full installation including prerequisites (Homebrew, Node.js, nvm) and MCP client config
- README: "Easy Setup For LLM Agents" section with paste-able prompt pointing to the hosted guide

## [0.1.12] - 2026-03-10

### Fixed
- CI: Upgrade npm before publish — npm v10 (Node 22) has a bug where the OIDC token expires between provenance signing and the PUT, causing a 404

## [0.1.11] - 2026-03-10

### Fixed
- CI: Merge npm publish into `tag-version.yml` — `GITHUB_TOKEN` events don't trigger other workflows, so the separate `publish-npm.yml` never fired on release

### Removed
- `.github/workflows/publish-npm.yml` — superseded by unified `tag-version.yml`

## [0.1.10] - 2026-03-09

### Fixed
- `package.json`: Add missing `main.py` to `files` list — caused server crash when installed via `npx` from any directory other than the project root
- `package.json`: Fix bin entry key to match package name (`snowflake-readonly-mcp`) so `npx` resolves correctly
- `package.json`: Update `files` glob from `server/` to `server/**/*.py` to exclude `__pycache__` from npm tarball

### Added
- README: Add OpenCode configuration section with correct `mcp`/`command`/`environment` key format
- README: Collapse "Option 2: From Source" into a default-closed details block
- CI: Add `publish-npm.yml` workflow using npm Trusted Publisher (OIDC) — no more `NPM_TOKEN` secret
- CI: Add GitHub Release creation to `tag-version.yml` (triggers npm publish on release)

## [0.1.9] - 2026-03-03

### Fixed
- `package.json`: Fix bin entry key to match package name (`snowflake-readonly-mcp`) so `npx` resolves correctly
- `package.json`: Update `files` glob from `server/` to `server/**/*.py` to exclude `__pycache__` from npm tarball

### Added
- CI/CD workflow to auto-publish npm package on push to main when version changes

## [0.1.8] - 2026-02-27

### Changed
- Rename npm package from `snowflake-mcp-server` to `snowflake-readonly-mcp` (the former was already taken on npm)
- Update all npx references in README to use new package name

## [0.1.7] - 2026-02-27

### Changed
- README: Add npx as recommended install method with config examples for all MCP clients
- README: Document `SNOWFLAKE_CREDENTIAL_FILE` optional env var with inline comments
- README: Fix broken nav anchors

### Fixed
- `bin/install.js`: Remove unused imports; fix misleading success message on manual setup path
- `bin/snowflake-mcp.js`: Remove dead code, add guard against double `startServer()` call, fix Windows venv path

### Removed
- `tests/manual_test_snowflake_access.py` (contained hardcoded credentials)

## [0.1.6] - 2026-02-12

### Added
- Key-pair authentication for headless/containerized deployments
  - New `credential_file` config option (set via `SNOWFLAKE_CREDENTIAL_FILE` env var)
  - Loads base64-encoded PEM private key from a JSON credential file
  - Falls back to external browser SSO when not set
- npm package wrapper (`package.json`, `bin/`) for `npx`-based installation

## [0.1.5] - 2025-10-29

### Changed
- **BREAKING**: Renamed MCP tools for clarity (SQL-inspired naming)
  - `inspect_schemas` → `show_tables` (like SQL's SHOW TABLES)
  - `search_tables` → `find_tables` (clearer intent: keyword search)
  - `get_table_schema` → `describe_table` (like SQL's DESCRIBE TABLE)
  - Workflow is now: show → find → describe → query
  - Improved descriptions to clarify when to use each tool

### Fixed
- `describe_table` (formerly `get_table_schema`) now cache-only, no longer requires Snowflake authentication
  - Fixed bug where function queried Snowflake directly when table not found in cache
  - Now returns "not found in cache" error instead of attempting to authenticate
  - Only tools that should require auth: `execute_query`, `refresh_catalog`, `execute_big_query_to_disk`

### Removed
- Removed `include_sample` parameter from `describe_table`
  - Users should use `execute_query` tool separately to get sample data
  - Ensures the function remains truly cache-only
- Removed unused internal `describe_table` function (111 lines of dead code)

### Added
- Comprehensive unit tests for cache-only behavior of `describe_table`
  - Verifies no Snowflake queries are made when table not in cache
  - 3 new tests with 100% pass rate

## [0.1.4] - 2025-10-29

### Changed
- Removed redundant SQL query text from all query responses to reduce token usage
  - Removed `sql` field from `query_metadata` in successful responses
  - Removed `sql` field from validation error responses
  - Removed `sql` field from execution error responses
  - Agent already has the query in context, no need to echo it back

## [0.1.3] - 2025-10-29

### Fixed
- CTE queries with LATERAL FLATTEN no longer incorrectly rejected
  - Fixed keyword detection logic that was identifying 'AS' as the first keyword instead of 'WITH'
  - Prioritized statement-level keywords (CTE, DML, DDL) over generic structural keywords
  - Added regression test for CTE with LATERAL FLATTEN pattern

## [0.1.2] - 2025-10-29

### Fixed
- Cache expiration now properly enforced with auto-refresh
  - Previously, expired cache only logged a warning and continued executing queries with stale metadata
  - Now `execute_query` and `execute_big_query_to_disk` auto-refresh expired caches before execution
  - Matches the behavior already implemented in `schema_inspector`
  - Updated tests to verify auto-refresh behavior on cache expiration

### Changed
- Updated README git clone example to use SSH URL instead of HTTPS

### Added
- GitHub Actions workflow for PR validation
  - Enforces version bump in pyproject.toml
  - Validates README version badge matches pyproject.toml
  - Requires CHANGELOG.md update for all PRs

## [0.1.1] - 2025-09-26

### Added
- Always return absolute paths in file export responses for clarity
- Convert relative paths to absolute paths based on MCP server's working directory
- Improved error messages to show absolute paths
- CHANGELOG.md for tracking version history
- Dynamic version badge in README that reads from pyproject.toml

### Fixed
- File path resolution issues where relative paths were ambiguous
- Tool descriptions now clarify that relative paths are resolved from MCP server directory

## [0.1.0] - 2025-08-19

### Added
- Initial release of Snowflake MCP Server
- Strict read-only access enforcement at multiple levels
- External browser authentication (SSO) support for secure access
- Intelligent schema discovery and caching with 5-day TTL
- CSV export functionality for query results
- SQL query validation for safety
- Streaming support for large query results with `execute_big_query_to_disk`
- Comprehensive MCP tools:
  - `refresh_catalog` - Scan and cache database schemas
  - `inspect_schemas` - Browse database structure
  - `search_tables` - Search for tables across databases
  - `get_table_schema` - Get detailed table information
  - `execute_query` - Execute read-only SQL queries
  - `validate_query_without_execution` - Validate SQL without execution
  - `get_query_history` - View query history
  - `save_last_query_to_csv` - Export query results to CSV
  - `execute_big_query_to_disk` - Stream large results directly to disk

### Security
- Multi-layer read-only protection:
  - SQL validation to reject write operations before execution
  - Transaction-level protection with `BEGIN TRANSACTION READ ONLY`
  - Session settings for query timeouts and monitoring
  - Connection validation to verify read-only access
  - Comprehensive blocking of write operations (INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
- CTE write operation detection
- Semicolon-separated statement blocking
- Clear error messages for rejected queries

### Infrastructure
- FastMCP framework integration
- Configurable via environment variables
- Debug logging support
- Schema caching system with efficient storage
- Query result caching for CSV export (5GB limit)
- Batch streaming for large result sets (10,000 rows per batch)

### Testing
- Comprehensive test suite with 42% code coverage
- Unit tests for all major components
- Mocked Snowflake connections for testing
- GitHub Actions CI/CD pipeline

### Documentation
- Detailed README with setup instructions
- IMPLEMENTATION_PLAN.md with technical details
- CLAUDE.md with development guidelines
- Inline documentation for all MCP tools
