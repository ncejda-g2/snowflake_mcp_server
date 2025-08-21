"""Query execution tool for running read-only SQL queries."""

import hashlib
import json
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

from server.schema_cache import SchemaCache
from server.snowflake_connection import SnowflakeConnection, QueryResult
from server.tools.catalog_refresh import refresh_catalog


logger = logging.getLogger(__name__)

# In-memory cache for query results (for pagination)
query_results_cache: Dict[str, Dict[str, Any]] = {}
MAX_CACHE_SIZE = 10  # Maximum number of cached queries


def _format_value(value: Any) -> Any:
    """Format a value for JSON serialization."""
    if value is None:
        return None
    elif isinstance(value, (datetime,)):
        return value.isoformat()
    elif isinstance(value, (bytes,)):
        return value.decode('utf-8', errors='ignore')
    else:
        return value


def _format_results(results: List[Dict], columns: List[Dict], max_column_width: int = 50) -> str:
    """
    Format query results as a readable table.
    
    Args:
        results: Query results
        columns: Column metadata
        max_column_width: Maximum width for column display
        
    Returns:
        Formatted table string
    """
    if not results:
        return "No results returned"
    
    # Get column names
    col_names = [col['name'] for col in columns]
    
    # Calculate column widths
    col_widths = {}
    for col in col_names:
        # Start with column name length
        col_widths[col] = len(col)
        # Check data widths
        for row in results[:100]:  # Sample first 100 rows for width calculation
            value = str(row.get(col, ''))
            col_widths[col] = min(max(col_widths[col], len(value)), max_column_width)
    
    # Build separator line
    separator = '+'
    for col in col_names:
        separator += '-' * (col_widths[col] + 2) + '+'
    
    # Build header
    lines = [separator]
    header = '|'
    for col in col_names:
        header += f" {col[:col_widths[col]].ljust(col_widths[col])} |"
    lines.append(header)
    lines.append(separator)
    
    # Build data rows
    for row in results:
        line = '|'
        for col in col_names:
            value = str(row.get(col, ''))
            if len(value) > col_widths[col]:
                value = value[:col_widths[col]-3] + '...'
            line += f" {value.ljust(col_widths[col])} |"
        lines.append(line)
    
    lines.append(separator)
    
    return '\n'.join(lines)


async def execute_query(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    page: int = 1,
    page_size: int = 100,
    format_results: bool = True
) -> Dict:
    """
    Execute a read-only SQL query with safety checks and pagination.
    
    This tool validates queries for read-only operations, executes them,
    and returns paginated results with metadata.
    
    Args:
        connection: Active Snowflake connection
        cache: Schema cache instance
        sql: SQL query to execute
        database: Optional database context
        schema: Optional schema context
        page: Page number (1-based) for pagination
        page_size: Number of rows per page
        format_results: Whether to format results as a table
        
    Returns:
        Dictionary with query results, pagination info, and metadata
    """
    # First validate the query for safety (before checking cache)
    from server.snowflake_connection import QueryValidator
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    if not is_valid:
        return {
            "status": "error",
            "message": error_msg,
            "query_type": str(query_type)
        }
    
    # Then validate cache is populated (required before queries)
    if cache.is_empty():
        return {
            "status": "error",
            "message": "Schema cache is empty. Please run refresh_catalog first.",
            "action_required": "refresh_catalog"
        }
    
    if cache.is_expired():
        logger.warning("Schema cache is expired, consider refreshing")
    
    try:
        # Generate query ID for caching
        query_id = hashlib.md5(f"{sql}{database}{schema}".encode()).hexdigest()
        
        # Check if we have cached results for pagination
        if query_id in query_results_cache and page > 1:
            cached = query_results_cache[query_id]
            
            # Calculate pagination
            total_rows = len(cached["all_results"])
            total_pages = (total_rows + page_size - 1) // page_size
            
            if page > total_pages:
                return {
                    "status": "error",
                    "message": f"Page {page} exceeds total pages ({total_pages})",
                    "total_pages": total_pages
                }
            
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_rows)
            page_results = cached["all_results"][start_idx:end_idx]
            
            # Format results
            formatted_data = []
            for row in page_results:
                formatted_row = {k: _format_value(v) for k, v in row.items()}
                formatted_data.append(formatted_row)
            
            response = {
                "status": "success",
                "query_id": query_id,
                "data": formatted_data,
                "columns": cached["columns"],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_rows": total_rows,
                    "total_pages": total_pages,
                    "has_more": page < total_pages,
                    "rows_in_page": len(page_results)
                },
                "source": "cache",
                "message": f"Showing rows {start_idx + 1}-{end_idx} of {total_rows}"
            }
            
            if format_results:
                response["formatted_table"] = _format_results(page_results, cached["columns"])
            
            return response
        
        # Execute new query
        offset = (page - 1) * page_size if page > 1 else 0
        result = connection.execute_query(
            sql=sql,
            database=database,
            schema=schema,
            page_size=page_size,
            offset=offset
        )
        
        if not result.data:
            return {
                "status": "success",
                "query_id": query_id,
                "data": [],
                "columns": result.columns,
                "message": "Query executed successfully but returned no results",
                "execution_time": result.execution_time
            }
        
        # For first page, cache the full results for potential pagination
        if page == 1:
            # Try to get more results to check total count
            full_result = connection.execute_query(
                sql=sql,
                database=database,
                schema=schema,
                max_rows=10000  # Reasonable limit for caching
            )
            
            # Cache results
            query_results_cache[query_id] = {
                "all_results": full_result.data,
                "columns": full_result.columns,
                "sql": sql,
                "database": database,
                "schema": schema,
                "cached_at": datetime.now().isoformat()
            }
            
            # Limit cache size
            if len(query_results_cache) > MAX_CACHE_SIZE:
                # Remove oldest cached query
                oldest_key = next(iter(query_results_cache))
                del query_results_cache[oldest_key]
                logger.debug(f"Removed oldest cached query: {oldest_key}")
            
            # Use full results for response
            total_rows = len(full_result.data)
            page_results = full_result.data[:page_size]
        else:
            total_rows = len(result.data)
            page_results = result.data
        
        # Calculate pagination info
        total_pages = (total_rows + page_size - 1) // page_size
        
        # Format results for JSON
        formatted_data = []
        for row in page_results:
            formatted_row = {k: _format_value(v) for k, v in row.items()}
            formatted_data.append(formatted_row)
        
        response = {
            "status": "success",
            "query_id": query_id,
            "data": formatted_data,
            "columns": result.columns,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows,
                "total_pages": total_pages,
                "has_more": page < total_pages or result.has_more_rows,
                "rows_in_page": len(page_results)
            },
            "execution_time": result.execution_time,
            "source": "query",
            "message": f"Showing rows {(page-1)*page_size + 1}-{(page-1)*page_size + len(page_results)} of {total_rows}"
        }
        
        # Add formatted table if requested
        if format_results:
            response["formatted_table"] = _format_results(page_results, result.columns)
        
        # Add query metadata
        response["query_metadata"] = {
            "sql": sql[:500] + ("..." if len(sql) > 500 else ""),
            "database_context": database,
            "schema_context": schema,
            "query_id": result.query_id
        }
        
        return response
        
    except ValueError as e:
        # Query validation errors
        return {
            "status": "error",
            "message": str(e),
            "error_type": "validation_error",
            "sql": sql[:500] + ("..." if len(sql) > 500 else "")
        }
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        return {
            "status": "error",
            "message": f"Query execution failed: {str(e)}",
            "error_type": "execution_error",
            "sql": sql[:500] + ("..." if len(sql) > 500 else "")
        }


async def validate_query_without_execution(
    connection: SnowflakeConnection,
    cache: SchemaCache,
    sql: str,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict:
    """
    Validate and prepare a SQL query without executing it.
    
    This tool validates a query for safety (read-only), syntax, and schema references,
    but does not execute it. Useful for generating queries that users want to review
    or execute elsewhere.
    
    Args:
        connection: Active Snowflake connection (for context, not execution)
        cache: Schema cache instance
        sql: SQL query to validate
        database: Optional database context
        schema: Optional schema context
        
    Returns:
        Dictionary with validation results and the prepared query
    """
    # Validate the query for safety (read-only check)
    from server.snowflake_connection import QueryValidator
    validator = QueryValidator()
    is_valid, error_msg, query_type = validator.validate(sql)
    
    validation_result = {
        "is_read_only": is_valid,
        "query_type": str(query_type),
        "safety_message": error_msg if not is_valid else "Query is read-only and safe"
    }
    
    # Check if cache is populated (recommended but not required for validation)
    cache_status = {
        "is_populated": not cache.is_empty(),
        "is_expired": cache.is_expired() if not cache.is_empty() else None
    }
    
    if cache.is_empty():
        cache_status["warning"] = "Schema cache is empty. Consider running refresh_catalog for better validation."
    
    # Prepare the final query with context if provided
    final_query = sql.strip()
    if final_query.endswith(';'):
        final_query = final_query[:-1]
    
    # Add database/schema context comment if provided
    context_info = []
    if database:
        context_info.append(f"Database: {database}")
    if schema:
        context_info.append(f"Schema: {schema}")
    
    if context_info:
        final_query = f"-- Context: {', '.join(context_info)}\n{final_query}"
    
    # Try to extract table references from the query (basic parsing)
    table_references = []
    try:
        # Simple regex to find potential table names after FROM/JOIN
        from_pattern = r'\b(?:FROM|JOIN)\s+([^\s,()]+)'
        matches = re.findall(from_pattern, sql.upper())
        for match in matches:
            # Clean up and add to references
            table_ref = match.strip().replace('"', '').replace('`', '')
            if table_ref and not table_ref.startswith('('):
                table_references.append(table_ref)
    except Exception:
        pass  # Ignore parsing errors
    
    # Build response
    response = {
        "status": "success",
        "query": final_query,
        "validation": validation_result,
        "cache_status": cache_status,
        "metadata": {
            "database_context": database,
            "schema_context": schema,
            "table_references": list(set(table_references)) if table_references else [],
            "query_length": len(sql),
            "estimated_complexity": "simple" if len(table_references) <= 1 else "moderate" if len(table_references) <= 3 else "complex"
        }
    }
    
    # Add syntax hints if query might need adjustment
    hints = []
    if database and schema and not any(x in sql.upper() for x in [f"{database.upper()}.{schema.upper()}", "USE DATABASE", "USE SCHEMA"]):
        hints.append(f"Consider using fully qualified table names: {database}.{schema}.table_name")
    
    if "LIMIT" not in sql.upper() and query_type == QueryValidator._identify_query_type(sql.upper()) and str(query_type) == "QueryType.SELECT":
        hints.append("Consider adding a LIMIT clause to control result size")
    
    if hints:
        response["hints"] = hints
    
    # Add a note about execution
    response["note"] = "This query has been validated but not executed. Use execute_query to run it."
    
    return response


async def get_query_history(
    connection: SnowflakeConnection,
    limit: int = 10,
    only_successful: bool = True
) -> Dict:
    """
    Get the history of executed queries.
    
    Args:
        connection: Active Snowflake connection
        limit: Maximum number of queries to return
        only_successful: Only return successful queries
        
    Returns:
        Dictionary with query history
    """
    try:
        history = connection.get_query_history(limit=limit, only_successful=only_successful)
        
        if not history:
            return {
                "status": "success",
                "message": "No query history available",
                "history": []
            }
        
        # Format history for response
        formatted_history = []
        for entry in history:
            formatted_entry = {
                "timestamp": datetime.fromtimestamp(entry['timestamp']).isoformat(),
                "sql": entry['sql'],
                "status": entry.get('status', 'unknown'),
                "execution_time": entry.get('execution_time'),
                "row_count": entry.get('row_count'),
                "database": entry.get('database'),
                "schema": entry.get('schema'),
                "error": entry.get('error')
            }
            formatted_history.append(formatted_entry)
        
        return {
            "status": "success",
            "history": formatted_history,
            "count": len(formatted_history),
            "limit": limit,
            "filter": "successful_only" if only_successful else "all"
        }
        
    except Exception as e:
        logger.error(f"Failed to get query history: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to get query history: {str(e)}"
        }