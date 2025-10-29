# Changelog

All notable changes to the Snowflake MCP Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [0.1.1] - 2025-01-26

### Added
- Always return absolute paths in file export responses for clarity
- Convert relative paths to absolute paths based on MCP server's working directory
- Improved error messages to show absolute paths
- CHANGELOG.md for tracking version history
- Dynamic version badge in README that reads from pyproject.toml

### Fixed
- File path resolution issues where relative paths were ambiguous
- Tool descriptions now clarify that relative paths are resolved from MCP server directory

## [0.1.0] - 2025-01-09

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
