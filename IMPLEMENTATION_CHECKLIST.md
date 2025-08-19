# Snowflake MCP Server - Implementation Checklist

## Phase 1: Project Setup & Dependencies
- [ ] Update `pyproject.toml` with new project name and dependencies
- [ ] Remove unnecessary G2-specific dependencies
- [ ] Add Snowflake and caching dependencies
- [ ] Update project metadata (name, description, author)

## Phase 2: Configuration Management
- [ ] Update `server/config.py` with Snowflake configuration
- [ ] Remove G2-specific configuration fields
- [ ] Add Snowflake connection parameters (account, username, warehouse, token)
- [ ] Add cache and safety settings
- [ ] Implement `from_env()` method for environment variables

## Phase 3: Snowflake Connection
- [ ] Create `server/snowflake_connection.py`
- [ ] Implement PAT authentication
- [ ] Add read-only session enforcement (AUTOCOMMIT=FALSE, BEGIN TRANSACTION READ ONLY)
- [ ] Implement query execution with logging
- [ ] Add connection validation

## Phase 4: SQL Validation
- [ ] Create `server/sql_validator.py`
- [ ] Implement SQL parsing to detect write operations
- [ ] Define allowed read operations (SELECT, WITH, SHOW, DESCRIBE)
- [ ] Define blocked write operations (INSERT, UPDATE, DELETE, etc.)
- [ ] Add helpful error messages for rejected queries

## Phase 5: Schema Cache System
- [ ] Create `server/schema_cache.py`
- [ ] Implement cache with 5-day TTL
- [ ] Add cache persistence to disk
- [ ] Implement cache load/save methods
- [ ] Add expiration checking

## Phase 6: Remove G2 Tools
- [ ] Delete `server/tools/bi_tools.py`
- [ ] Delete `server/tools/category_tools.py`
- [ ] Delete `server/tools/product_tools.py`
- [ ] Delete `server/tools/review_tools.py`
- [ ] Delete `server/tools/vendor_tools.py`
- [ ] Clean up `server/tools/__init__.py`
- [ ] Update `server/tools/auth_utils.py` for Snowflake (or remove if not needed)

## Phase 7: Implement Snowflake Tools
- [ ] Create `server/tools/catalog_refresh.py`
  - [ ] Query INFORMATION_SCHEMA across all databases
  - [ ] Build schema index
  - [ ] Store in cache with timestamp
  - [ ] Return summary of discovered schemas
  
- [ ] Create `server/tools/schema_inspector.py`
  - [ ] Load from cache (auto-refresh if expired)
  - [ ] Implement pattern filtering
  - [ ] Return hierarchical structure
  
- [ ] Create `server/tools/table_inspector.py`
  - [ ] Get column information from cache or direct query
  - [ ] Optional sample data inclusion
  - [ ] Format results nicely
  
- [ ] Create `server/tools/query_executor.py`
  - [ ] Validate cache is populated
  - [ ] Validate SQL is read-only
  - [ ] Execute query with pagination
  - [ ] Cache results for pagination
  - [ ] Format and return results with metadata

## Phase 8: Main Application Integration
- [ ] Update `server/app.py`
  - [ ] Remove G2 OAuth and tools
  - [ ] Initialize FastMCP with "Snowflake Read-Only MCP" name
  - [ ] Add Snowflake connection initialization
  - [ ] Register new Snowflake tools
  - [ ] Add startup hook for connection and cache check
  - [ ] Keep health check endpoints for HTTP transport

## Phase 9: Entry Point & Logging
- [ ] Update `main.py`
  - [ ] Validate required Snowflake configuration
  - [ ] Add helpful error messages for missing config
  - [ ] Support both STDIO and HTTP transports
  
- [ ] Update `server/log_utils.py`
  - [ ] Update logger name from g2_mcp_server to snowflake_mcp_server
  - [ ] Ensure query logging is implemented
  - [ ] Add audit trail for all operations

## Phase 10: Clean Up & Documentation
- [ ] Update `CLAUDE.md` with Snowflake-specific instructions
- [ ] Remove `server/health.py` if not needed (or keep for HTTP transport)
- [ ] Update `__init__.py` files as needed
- [ ] Remove Docker files if not needed for POC
- [ ] Clean up any remaining G2 references

## Phase 11: Testing Preparation
- [ ] Create example environment variable file (.env.example)
- [ ] Write Claude Desktop configuration example
- [ ] Document how to generate Snowflake PAT
- [ ] Create simple test queries for validation

## Validation Checklist (After Implementation)
- [ ] Server starts without errors
- [ ] Can connect to Snowflake with PAT
- [ ] Schema catalog refresh works
- [ ] Cache persists between restarts
- [ ] Read queries execute successfully
- [ ] Write queries are properly rejected
- [ ] Pagination works for large results
- [ ] Error messages are clear and helpful
- [ ] All queries are logged
- [ ] Works with Claude Desktop (STDIO transport)

## Order of Implementation
We'll proceed through these phases sequentially:
1. Start with Phase 1-2 (Setup & Config)
2. Then Phase 3-5 (Core Infrastructure)
3. Then Phase 6-7 (Replace Tools)
4. Then Phase 8-9 (Integration)
5. Finally Phase 10-11 (Cleanup & Testing)

Each phase should be completed and verified before moving to the next.