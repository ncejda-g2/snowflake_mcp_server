# Snowflake MCP Server - Complete Implementation Plan

## Overview
A Model Context Protocol (MCP) server that provides read-only access to Snowflake databases using Programmatic Access Token (PAT) authentication. This is a POC/demo implementation focused on simplicity and safety.

## Architecture

```
Snowflake MCP Server
├── Catalog Refresh Tool (pre-indexes all schemas with 5-day cache)
├── Schema Inspector Tool (reads from cache)
├── Table Schema Tool (gets detailed column info)
├── Query Executor Tool (runs read-only queries with pagination)
└── Automatic Result Formatter (built into query executor)
```

## Core Principles
- **Simple and Clean**: POC/demo quality, not production-grade yet
- **Read-Only**: Absolutely no write operations allowed, enforced at multiple levels
- **PAT Authentication**: Using Snowflake Programmatic Access Tokens (no OAuth for now)
- **Smart Schema Discovery**: Automatically find relevant databases/tables based on queries
- **Caching**: 5-day cache for schema information to reduce INFORMATION_SCHEMA queries
- **Pagination**: Return 100 rows by default, allow fetching more on request

## Implementation Details

### 1. Project Configuration (`pyproject.toml`)

```toml
[project]
name = "snowflake_mcp_server"
version = "0.1.0"
description = "A Snowflake read-only MCP server"
requires-python = ">=3.12"
dependencies = [
    "fastmcp>=2.9.2",
    "snowflake-connector-python>=3.0.0",
    "pydantic>=2.0.0",
    "cachetools>=5.0.0",
    "sqlparse>=0.4.0",
    "httpx>=0.28.1",
]
```

### 2. Configuration Management (`server/config.py`)

```python
from pydantic import BaseModel, Field

class Config(BaseModel):
    # Snowflake connection (required from user)
    account: str = Field(description="Snowflake account identifier")
    username: str = Field(description="Snowflake username")
    warehouse: str = Field(description="Compute warehouse")
    token: str = Field(description="Snowflake PAT", exclude=True)
    
    # MCP server settings
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    transport: str = Field(default="stdio")  # or "http"
    
    # Cache settings
    cache_ttl_days: int = Field(default=5)
    max_query_rows: int = Field(default=100)
    
    # Safety settings - ALWAYS enforce read-only, NEVER make this configurable
    debug: bool = Field(default=False)
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables"""
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            username=os.getenv("SNOWFLAKE_USERNAME"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            token=os.getenv("SNOWFLAKE_PAT"),
            transport=os.getenv("MCP_TRANSPORT", "stdio"),
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=int(os.getenv("MCP_PORT", "8000")),
            debug=os.getenv("DEBUG", "false").lower() == "true"
        )
```

### 3. Snowflake Connection (`server/snowflake_connection.py`)

```python
import snowflake.connector
from typing import Optional
import logging

class SnowflakeConnection:
    def __init__(self, config: Config):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    async def connect(self):
        """Establish read-only connection to Snowflake"""
        self.connection = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.username,
            authenticator='oauth',
            token=self.config.token,
            warehouse=self.config.warehouse
        )
        
        # Enforce read-only at session level
        cursor = self.connection.cursor()
        try:
            # Set session to read-only mode
            cursor.execute("ALTER SESSION SET AUTOCOMMIT = FALSE")
            cursor.execute("BEGIN TRANSACTION READ ONLY")
            
            # Log successful connection
            self.logger.info(f"Connected to Snowflake as {self.config.username}")
            self.logger.info("Read-only mode enforced at session level")
        finally:
            cursor.close()
            
    async def execute_query(self, sql: str, database: Optional[str] = None):
        """Execute a query with read-only validation"""
        # Additional safety check before execution
        if self._contains_write_operation(sql):
            raise ValueError("Write operations are not permitted")
            
        cursor = self.connection.cursor()
        try:
            if database:
                cursor.execute(f"USE DATABASE {database}")
            
            # Log query for audit
            self.logger.info(f"Executing query: {sql[:200]}...")
            
            cursor.execute(sql)
            return cursor.fetchall(), cursor.description
        finally:
            cursor.close()
    
    def _contains_write_operation(self, sql: str) -> bool:
        """Check if SQL contains write operations"""
        write_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
            'ALTER', 'MERGE', 'TRUNCATE', 'GRANT', 'REVOKE'
        ]
        sql_upper = sql.upper()
        return any(keyword in sql_upper for keyword in write_keywords)
```

### 4. Schema Cache (`server/schema_cache.py`)

```python
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

@dataclass
class TableInfo:
    database: str
    schema: str
    table_name: str
    columns: List[Dict[str, str]]  # [{name, type, nullable, etc}]
    row_count: Optional[int] = None
    
@dataclass
class SchemaCache:
    def __init__(self, ttl_days: int = 5):
        self.ttl_days = ttl_days
        self.cache_file = Path.home() / ".snowflake_mcp" / "schema_cache.json"
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache: Dict[str, TableInfo] = {}
        self.last_refresh: Optional[datetime] = None
        
    def is_expired(self) -> bool:
        """Check if cache has expired"""
        if not self.last_refresh:
            return True
        return datetime.now() - self.last_refresh > timedelta(days=self.ttl_days)
        
    def load(self):
        """Load cache from disk"""
        if self.cache_file.exists():
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
                self.last_refresh = datetime.fromisoformat(data.get('last_refresh'))
                self.cache = {
                    k: TableInfo(**v) for k, v in data.get('tables', {}).items()
                }
                
    def save(self):
        """Persist cache to disk"""
        data = {
            'last_refresh': self.last_refresh.isoformat(),
            'tables': {k: asdict(v) for k, v in self.cache.items()}
        }
        with open(self.cache_file, 'w') as f:
            json.dump(data, f, indent=2)
            
    def get_tables_for_database(self, database: str) -> List[TableInfo]:
        """Get all tables for a specific database"""
        return [t for t in self.cache.values() if t.database == database]
```

### 5. SQL Validator (`server/sql_validator.py`)

```python
import sqlparse
from typing import Tuple

class SQLValidator:
    """Validate SQL queries for read-only operations"""
    
    WRITE_OPERATIONS = {
        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
        'ALTER', 'MERGE', 'TRUNCATE', 'GRANT', 'REVOKE',
        'COPY', 'PUT', 'GET', 'REMOVE', 'CALL'
    }
    
    READ_OPERATIONS = {
        'SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN'
    }
    
    @classmethod
    def validate(cls, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL for read-only operations
        Returns: (is_valid, error_message)
        """
        # Parse SQL
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Unable to parse SQL query"
            
        # Check first statement
        first_token = None
        for statement in parsed:
            for token in statement.tokens:
                if not token.is_whitespace:
                    if token.ttype in (sqlparse.tokens.Keyword.DML, 
                                       sqlparse.tokens.Keyword.DDL,
                                       sqlparse.tokens.Keyword):
                        first_token = token.value.upper()
                        break
            if first_token:
                break
                
        # Validate operation type
        if first_token in cls.WRITE_OPERATIONS:
            return False, f"Write operation '{first_token}' is not permitted. Only read operations are allowed."
            
        if first_token not in cls.READ_OPERATIONS:
            return False, f"Operation '{first_token}' is not recognized as a safe read operation."
            
        # Additional check for suspicious patterns
        sql_upper = sql.upper()
        for write_op in cls.WRITE_OPERATIONS:
            if write_op in sql_upper:
                return False, f"Query contains potential write operation '{write_op}'"
                
        return True, ""
```

### 6. MCP Tools (`server/tools/`)

#### `catalog_refresh.py`
```python
from fastmcp import FastMCP
from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection

@mcp.tool
async def refresh_catalog(force: bool = False) -> str:
    """
    Refresh the schema catalog cache by scanning all accessible databases.
    
    Args:
        force: Force refresh even if cache is not expired
        
    Returns:
        Summary of discovered schemas
    """
    cache = SchemaCache()
    
    # Check if refresh needed
    if not force and not cache.is_expired():
        return "Cache is still valid. Use force=true to refresh anyway."
        
    # Query all accessible databases
    query = """
    SELECT 
        table_catalog as database,
        table_schema as schema,
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
    ORDER BY table_catalog, table_schema, table_name, ordinal_position
    """
    
    results, _ = await connection.execute_query(query)
    
    # Build cache
    tables_processed = set()
    for row in results:
        table_key = f"{row[0]}.{row[1]}.{row[2]}"
        if table_key not in cache.cache:
            cache.cache[table_key] = TableInfo(
                database=row[0],
                schema=row[1],
                table_name=row[2],
                columns=[]
            )
        cache.cache[table_key].columns.append({
            'name': row[3],
            'type': row[4],
            'nullable': row[5]
        })
        tables_processed.add(table_key)
    
    # Save cache
    cache.last_refresh = datetime.now()
    cache.save()
    
    return f"Catalog refreshed. Found {len(tables_processed)} tables across all accessible databases."
```

#### `schema_inspector.py`
```python
@mcp.tool
async def inspect_schemas(
    database_pattern: Optional[str] = None,
    schema_pattern: Optional[str] = None,
    table_pattern: Optional[str] = None
) -> str:
    """
    List available databases, schemas, and tables from cache.
    
    Args:
        database_pattern: Filter databases by pattern (supports wildcards)
        schema_pattern: Filter schemas by pattern
        table_pattern: Filter tables by pattern
        
    Returns:
        Hierarchical structure of matching database objects
    """
    cache = SchemaCache()
    cache.load()
    
    # Auto-refresh if expired
    if cache.is_expired():
        await refresh_catalog()
        cache.load()
    
    # Filter and format results
    results = {}
    for table_info in cache.cache.values():
        # Apply filters
        if database_pattern and not _matches_pattern(table_info.database, database_pattern):
            continue
        if schema_pattern and not _matches_pattern(table_info.schema, schema_pattern):
            continue
        if table_pattern and not _matches_pattern(table_info.table_name, table_pattern):
            continue
            
        # Build hierarchical structure
        if table_info.database not in results:
            results[table_info.database] = {}
        if table_info.schema not in results[table_info.database]:
            results[table_info.database][table_info.schema] = []
        results[table_info.database][table_info.schema].append(table_info.table_name)
    
    return format_schema_hierarchy(results)
```

#### `table_inspector.py`
```python
@mcp.tool
async def get_table_schema(
    database: str,
    schema: str,
    table: str,
    include_sample: bool = False
) -> str:
    """
    Get detailed column information for a specific table.
    
    Args:
        database: Database name
        schema: Schema name
        table: Table name
        include_sample: Include 5 sample rows
        
    Returns:
        Detailed table schema and optionally sample data
    """
    # Get from cache first
    cache = SchemaCache()
    cache.load()
    
    table_key = f"{database}.{schema}.{table}"
    if table_key in cache.cache:
        table_info = cache.cache[table_key]
        result = format_table_schema(table_info)
    else:
        # Query directly if not in cache
        query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM {database}.information_schema.columns
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        results, _ = await connection.execute_query(query)
        result = format_query_results(results)
    
    # Add sample data if requested
    if include_sample:
        sample_query = f"SELECT * FROM {database}.{schema}.{table} LIMIT 5"
        sample_results, _ = await connection.execute_query(sample_query)
        result += "\n\nSample Data:\n" + format_query_results(sample_results)
    
    return result
```

#### `query_executor.py`
```python
from typing import Optional, Dict, Any
import hashlib
import json

# In-memory cache for query results (for pagination)
query_cache: Dict[str, Dict[str, Any]] = {}

@mcp.tool
async def execute_query(
    sql: str,
    database: Optional[str] = None,
    page: int = 1,
    page_size: int = 100
) -> Dict[str, Any]:
    """
    Execute read-only SQL query with safety checks and pagination.
    
    Args:
        sql: SQL query to execute
        database: Optional database context
        page: Page number (1-based)
        page_size: Number of rows per page (default 100)
        
    Returns:
        Dictionary with results, pagination info, and metadata
    """
    # Validate cache is populated
    cache = SchemaCache()
    cache.load()
    if cache.is_expired():
        return {
            "error": "Schema cache is expired. Please run refresh_catalog first.",
            "success": False
        }
    
    # Validate SQL for read-only operations
    is_valid, error_msg = SQLValidator.validate(sql)
    if not is_valid:
        return {
            "error": error_msg,
            "success": False
        }
    
    # Generate query ID for caching
    query_id = hashlib.md5(f"{sql}{database}".encode()).hexdigest()
    
    # Check if we have cached results
    if query_id in query_cache and page > 1:
        cached = query_cache[query_id]
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        return {
            "success": True,
            "query_id": query_id,
            "results": cached["all_results"][start_idx:end_idx],
            "columns": cached["columns"],
            "page": page,
            "page_size": page_size,
            "total_rows": len(cached["all_results"]),
            "has_more": end_idx < len(cached["all_results"]),
            "total_pages": (len(cached["all_results"]) + page_size - 1) // page_size
        }
    
    # Execute query
    try:
        results, description = await connection.execute_query(sql, database)
        columns = [desc[0] for desc in description] if description else []
        
        # Cache full results
        query_cache[query_id] = {
            "all_results": results,
            "columns": columns,
            "sql": sql,
            "database": database
        }
        
        # Limit cache size (keep last 10 queries)
        if len(query_cache) > 10:
            oldest = list(query_cache.keys())[0]
            del query_cache[oldest]
        
        # Return paginated results
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_results = results[start_idx:end_idx]
        
        # Format results nicely
        formatted_results = format_query_results(page_results, columns)
        
        return {
            "success": True,
            "query_id": query_id,
            "results": formatted_results,
            "columns": columns,
            "page": page,
            "page_size": page_size,
            "total_rows": len(results),
            "has_more": end_idx < len(results),
            "total_pages": (len(results) + page_size - 1) // page_size,
            "message": f"Showing rows {start_idx+1}-{min(end_idx, len(results))} of {len(results)}"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@mcp.tool
async def fetch_next_page(query_id: str) -> Dict[str, Any]:
    """
    Fetch the next page of results from a cached query.
    
    Args:
        query_id: ID of the previously executed query
        
    Returns:
        Next page of results or error if query not found
    """
    if query_id not in query_cache:
        return {
            "success": False,
            "error": "Query not found in cache. Please re-execute the query."
        }
    
    cached = query_cache[query_id]
    # Determine current page from last access (would need to track this)
    # For simplicity, caller should use execute_query with page parameter
    
    return {
        "success": False,
        "error": "Please use execute_query with page parameter for pagination"
    }
```

### 7. Main Application (`server/app.py`)

```python
from fastmcp import FastMCP
from server.config import Config
from server.snowflake_connection import SnowflakeConnection
from server.tools.catalog_refresh import refresh_catalog
from server.tools.schema_inspector import inspect_schemas
from server.tools.table_inspector import get_table_schema
from server.tools.query_executor import execute_query, fetch_next_page
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = Config.from_env()

# Initialize MCP server
mcp = FastMCP("Snowflake Read-Only MCP")

# Initialize Snowflake connection
connection = SnowflakeConnection(config)

@mcp.on_startup
async def startup():
    """Initialize connection on server startup"""
    await connection.connect()
    logger.info("Snowflake connection established")
    
    # Check if catalog needs refresh
    from server.schema_cache import SchemaCache
    cache = SchemaCache()
    cache.load()
    if cache.is_expired():
        logger.info("Schema cache expired, refreshing catalog...")
        await refresh_catalog(force=True)

# Register tools
mcp.tool(refresh_catalog)
mcp.tool(inspect_schemas)
mcp.tool(get_table_schema)
mcp.tool(execute_query)
mcp.tool(fetch_next_page)

# Add health check endpoints if using HTTP transport
if config.transport == "http":
    @mcp.get("/_health")
    async def health_check():
        return {"status": "healthy", "service": "snowflake-mcp"}
```

### 8. Entry Point (`main.py`)

```python
#!/usr/bin/env python3

from server.app import mcp
from server.config import Config

if __name__ == "__main__":
    config = Config.from_env()
    
    # Validate required configuration
    if not all([config.account, config.username, config.warehouse, config.token]):
        print("Error: Missing required Snowflake configuration")
        print("Please set: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_PAT")
        exit(1)
    
    # Run server with appropriate transport
    if config.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=config.host, port=config.port)
```

## Environment Configuration

### Required Environment Variables
```bash
# Snowflake Configuration
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USERNAME="user@company.com"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_PAT="<your-programmatic-access-token>"

# MCP Configuration (optional)
export MCP_TRANSPORT="stdio"  # or "http"
export MCP_HOST="0.0.0.0"     # for http transport
export MCP_PORT="8000"        # for http transport
export DEBUG="false"          # set to true for debug logging
```

### Claude Desktop Configuration

Add to your Claude Desktop config file:

```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "python",
      "args": ["-m", "snowflake_mcp_server"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "xy12345.us-east-1",
        "SNOWFLAKE_USERNAME": "user@company.com",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_PAT": "${SNOWFLAKE_PAT}"
      }
    }
  }
}
```

## Safety Features

### Multiple Layers of Protection

1. **PAT Authentication**: Uses Snowflake Programmatic Access Tokens for secure authentication
2. **Session-Level Enforcement**: Sets `AUTOCOMMIT=FALSE` and `BEGIN TRANSACTION READ ONLY`
3. **SQL Parsing**: Validates all queries before execution using sqlparse
4. **Keyword Blocking**: Rejects any SQL containing write operation keywords
5. **Query Logging**: All executed queries are logged for audit purposes
6. **No Role Selection**: Prevents users from selecting roles that might have write permissions
7. **Error Messages**: Clear, helpful error messages when write operations are attempted

### Read-Only Operations Allowed
- SELECT
- WITH (CTEs)
- SHOW
- DESCRIBE/DESC
- EXPLAIN

### Write Operations Blocked
- INSERT, UPDATE, DELETE
- CREATE, DROP, ALTER
- MERGE, TRUNCATE
- GRANT, REVOKE
- COPY, PUT, GET, REMOVE
- CALL (stored procedures)

## Usage Examples

### Basic Query Flow

1. **User asks about data**
   ```
   User: "Show me the top 10 customers by revenue"
   ```

2. **Claude uses tools**
   ```python
   # First, inspect available schemas
   await inspect_schemas(table_pattern="customer%")
   
   # Get table details
   await get_table_schema("SALES_DB", "PUBLIC", "CUSTOMERS")
   
   # Execute query
   await execute_query("""
       SELECT customer_name, total_revenue
       FROM SALES_DB.PUBLIC.CUSTOMERS
       ORDER BY total_revenue DESC
       LIMIT 10
   """)
   ```

3. **Pagination for large results**
   ```python
   # First page (automatic)
   result = await execute_query("SELECT * FROM large_table")
   # Returns: First 100 rows, has_more=True
   
   # User: "Show me more"
   result = await execute_query("SELECT * FROM large_table", page=2)
   # Returns: Rows 101-200
   ```

## Implementation Order

1. **Phase 1: Core Setup**
   - Set up project structure and dependencies
   - Create configuration management
   - Implement Snowflake connection with PAT auth

2. **Phase 2: Safety Layer**
   - Implement SQL validator
   - Add session-level read-only enforcement
   - Set up query logging

3. **Phase 3: Schema Management**
   - Build schema cache system
   - Implement catalog refresh tool
   - Add cache persistence

4. **Phase 4: Query Tools**
   - Create schema inspector tool
   - Add table inspector tool
   - Implement query executor with pagination

5. **Phase 5: Integration**
   - Wire up FastMCP server
   - Add startup initialization
   - Configure transport modes

6. **Phase 6: Polish**
   - Add comprehensive logging
   - Improve error messages
   - Create user documentation

## Testing Checklist

- [ ] PAT authentication works
- [ ] Connection is read-only (try INSERT/UPDATE/DELETE)
- [ ] Schema cache refreshes properly
- [ ] Cache persists across restarts
- [ ] Pagination works for large result sets
- [ ] SQL validation catches all write operations
- [ ] Error messages are clear and helpful
- [ ] Query logging captures all operations
- [ ] Both STDIO and HTTP transports work

## Notes

- This is a POC implementation focused on simplicity
- No mock data testing - real Snowflake testing only
- Future enhancements could include OAuth, better schema intelligence, query optimization