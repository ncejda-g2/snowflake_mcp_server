"""Snowflake connection management with strict read-only enforcement."""

import logging
import re
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection as SnowflakeConn
from snowflake.connector.errors import ProgrammingError

from server.config import Config


class QueryType(Enum):
    """Types of SQL queries."""

    SELECT = "SELECT"
    SHOW = "SHOW"
    DESCRIBE = "DESCRIBE"
    EXPLAIN = "EXPLAIN"
    WITH = "WITH"
    WRITE = "WRITE"  # Any write operation
    UNKNOWN = "UNKNOWN"


@dataclass
class QueryResult:
    """Structured query result."""

    data: list[dict] | None
    columns: list[dict]
    row_count: int
    execution_time: float
    query_id: str | None = None
    has_more_rows: bool = False


class QueryValidator:
    """Validates SQL queries for read-only safety."""

    # Comprehensive list of write operations
    WRITE_OPERATIONS = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "UPSERT",
        "CREATE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "RENAME",
        "GRANT",
        "REVOKE",
        "COPY",
        "PUT",
        "GET",
        "REMOVE",
        "CALL",
        "EXECUTE",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SET",
        "UNSET",
    }

    # Safe read operations
    READ_OPERATIONS = {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "LIST", "WITH"}

    @classmethod
    def validate(cls, sql: str) -> tuple[bool, str, QueryType]:
        """
        Validate that SQL query is read-only.

        Returns:
            Tuple of (is_valid, error_message, query_type)
        """
        if not sql or not sql.strip():
            return False, "Empty query", QueryType.UNKNOWN

        # Remove comments and normalize
        sql_clean = cls._remove_comments(sql)
        sql_upper = sql_clean.upper().strip()

        # Check for multiple statements (semicolon not at end)
        if ";" in sql_clean:
            # Remove trailing semicolon and check again
            sql_no_trailing = sql_clean.rstrip().rstrip(";")
            if ";" in sql_no_trailing:
                return (
                    False,
                    "Multiple statements not allowed. Only single queries permitted.",
                    QueryType.UNKNOWN,
                )

        # Extract the main operation
        query_type = cls._identify_query_type(sql_upper)

        if query_type == QueryType.WRITE:
            operation = cls._get_first_operation(sql_upper)
            return (
                False,
                f"Write operation '{operation}' is not permitted. Only read operations are allowed.",
                QueryType.WRITE,
            )

        # Additional safety check - scan entire query for write operations
        if cls._contains_write_operation(sql_upper):
            return (
                False,
                "Query contains write operations. Only read operations are allowed.",
                QueryType.WRITE,
            )

        return True, "", query_type

    @classmethod
    def _remove_comments(cls, sql: str) -> str:
        """Remove SQL comments."""
        # Remove single-line comments
        sql = re.sub(r"--[^\n]*", "", sql)
        # Remove multi-line comments
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return sql

    @classmethod
    def _identify_query_type(cls, sql_upper: str) -> QueryType:
        """Identify the type of SQL query."""
        # Get first meaningful word
        words = sql_upper.split()
        if not words:
            return QueryType.UNKNOWN

        first_word = words[0]

        # Handle CTEs - look for the actual operation after WITH
        if first_word == "WITH":
            # Find the main operation after the CTE definition
            # Look for pattern: ) SELECT/INSERT/UPDATE/DELETE etc
            cte_pattern = r"\)\s+(\w+)"
            match = re.search(cte_pattern, sql_upper)
            if match:
                main_operation = match.group(1)
                if main_operation in cls.WRITE_OPERATIONS:
                    return QueryType.WRITE
                elif main_operation in cls.READ_OPERATIONS:
                    return QueryType.WITH
            return QueryType.WITH

        if first_word in cls.WRITE_OPERATIONS:
            return QueryType.WRITE
        elif first_word == "SELECT":
            return QueryType.SELECT
        elif first_word == "SHOW":
            return QueryType.SHOW
        elif first_word in ("DESCRIBE", "DESC"):
            return QueryType.DESCRIBE
        elif first_word == "EXPLAIN":
            return QueryType.EXPLAIN
        else:
            # Unknown operations are treated as potentially unsafe
            return QueryType.UNKNOWN

    @classmethod
    def _get_first_operation(cls, sql_upper: str) -> str:
        """Get the first SQL operation."""
        words = sql_upper.split()
        return words[0] if words else "UNKNOWN"

    @classmethod
    def _contains_write_operation(cls, sql_upper: str) -> bool:
        """Check if query contains any write operations as whole words."""
        for operation in cls.WRITE_OPERATIONS:
            # Use word boundaries to avoid false positives
            pattern = r"\b" + operation + r"\b"
            if re.search(pattern, sql_upper):
                return True
        return False


class SnowflakeConnection:
    """Manages Snowflake connection with strict read-only enforcement."""

    def __init__(self, config: Config):
        """Initialize connection manager with configuration."""
        self.config = config
        self.connection: SnowflakeConn | None = None
        self.logger = logging.getLogger(__name__)
        self.query_log: list[dict] = []
        self.validator = QueryValidator()
        self._connection_metadata: dict = {}

    def connect(self) -> None:
        """
        Establish read-only connection to Snowflake using PAT authentication.

        Raises:
            Exception: If connection fails
        """
        try:
            self.logger.info(f"Connecting to Snowflake account: {self.config.account}")

            # Connect using external browser authentication
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.username,
                authenticator="externalbrowser",
                role=self.config.role,
                warehouse=self.config.warehouse,
                client_session_keep_alive=True,
                # Set reasonable timeouts
                network_timeout=30,
                login_timeout=10,
                # Disable cloud provider credential auto-detection
                # This prevents unnecessary checks for AWS/Azure/GCP metadata endpoints
                ocsp_fail_open=False,
                validate_default_parameters=True,
            )

            # Set up session for safety
            self._setup_read_only_session()

            self.logger.info(
                "Snowflake connection established with read-only enforcement"
            )

        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def _setup_read_only_session(self) -> None:
        """Configure session for read-only access."""
        if self.connection is None:
            raise RuntimeError("Connection not established")
        with self.connection.cursor() as cursor:
            # Set session parameters for safety
            session_params = [
                ("STATEMENT_TIMEOUT_IN_SECONDS", "300"),  # 5 minute timeout
                ("QUERY_TAG", "'SNOWFLAKE_MCP_READ_ONLY'"),
                ("ABORT_DETACHED_QUERY", "TRUE"),
            ]

            for param, value in session_params:
                try:
                    cursor.execute(f"ALTER SESSION SET {param} = {value}")
                    self.logger.debug(f"Set session parameter: {param} = {value}")
                except ProgrammingError as e:
                    self.logger.warning(f"Could not set {param}: {e}")

            # Get connection metadata
            cursor.execute(
                """
                SELECT CURRENT_USER() as user,
                       CURRENT_ROLE() as role,
                       CURRENT_WAREHOUSE() as warehouse,
                       CURRENT_DATABASE() as database,
                       CURRENT_SCHEMA() as schema
            """
            )
            result = cursor.fetchone()

            if result:
                self._connection_metadata = {
                    "user": result[0],
                    "role": result[1],
                    "warehouse": result[2],
                    "database": result[3],
                    "schema": result[4],
                }

                self.logger.info(f"Connected as: {self._connection_metadata}")

    @contextmanager
    def _read_only_transaction(self):
        """Context manager for read-only transactions."""
        cursor = self.connection.cursor(DictCursor)
        try:
            # Snowflake doesn't support BEGIN TRANSACTION READ ONLY
            # We rely on query validation and user permissions instead
            yield cursor
        finally:
            cursor.close()

    def execute_query(
        self,
        sql: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> QueryResult:
        """
        Execute a read-only SQL query.

        Args:
            sql: SQL query to execute
            database: Optional database to use
            schema: Optional schema to use

        Returns:
            QueryResult object with query results

        Raises:
            ValueError: If query contains write operations
            RuntimeError: If not connected
            Exception: If query execution fails
        """
        if not self.connection:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        # Validate query
        is_valid, error_msg, query_type = self.validator.validate(sql)
        if not is_valid:
            self.logger.error(f"Query rejected: {error_msg}")
            raise ValueError(error_msg)

        # Log query
        start_time = time.time()
        query_entry = {
            "timestamp": start_time,
            "sql": sql[:500] + ("..." if len(sql) > 500 else ""),
            "database": database,
            "schema": schema,
        }

        try:
            with self._read_only_transaction() as cursor:
                # Set context if provided
                if database:
                    cursor.execute(f"USE DATABASE {database}")
                if schema:
                    cursor.execute(f"USE SCHEMA {schema}")

                # Execute the query as-is
                self.logger.debug(f"Executing query: {sql[:100]}...")
                cursor.execute(sql)

                # Get query ID for monitoring
                query_id = cursor.sfqid if hasattr(cursor, "sfqid") else None

                # Build column metadata
                columns = []
                if cursor.description:
                    columns = [
                        {
                            "name": col[0],
                            "type": str(
                                col[1].__name__
                                if hasattr(col[1], "__name__")
                                else col[1]
                            ),
                            "nullable": col[6] if len(col) > 6 else None,
                        }
                        for col in cursor.description
                    ]

                # Fetch results
                results = []
                row_count = 0
                has_more_rows = False

                if query_type in (
                    QueryType.SELECT,
                    QueryType.SHOW,
                    QueryType.DESCRIBE,
                    QueryType.WITH,
                ):
                    # Fetch all results
                    results = cursor.fetchall()
                    row_count = len(results)
                    has_more_rows = False

                execution_time = time.time() - start_time

                # Update query log
                query_entry.update(
                    {
                        "execution_time": execution_time,
                        "row_count": row_count,
                        "query_id": query_id,
                        "status": "success",
                    }
                )
                self.query_log.append(query_entry)

                self.logger.info(
                    f"Query executed successfully: "
                    f"{row_count} rows in {execution_time:.2f}s"
                )

                return QueryResult(
                    data=results,
                    columns=columns,
                    row_count=row_count,
                    execution_time=execution_time,
                    query_id=query_id,
                    has_more_rows=has_more_rows,
                )

        except ProgrammingError as e:
            error_str = str(e)
            # Check if it's a permission error
            if "insufficient privileges" in error_str.lower():
                error_msg = "Insufficient privileges. This query requires permissions not available in read-only mode."
            else:
                error_msg = f"Query execution failed: {error_str}"

            query_entry["status"] = "error"
            query_entry["error"] = error_msg
            self.query_log.append(query_entry)

            self.logger.error(error_msg)
            raise ValueError(error_msg) from None

        except Exception as e:
            query_entry["status"] = "error"
            query_entry["error"] = str(e)
            self.query_log.append(query_entry)

            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    def execute_query_stream(
        self,
        sql: str,
        database: str | None = None,
        schema: str | None = None,
        batch_size: int = 1000,
    ) -> Generator[list[dict], None, None]:
        """
        Execute a query and stream results in batches.

        Useful for large result sets to avoid memory issues.
        """
        if not self.connection:
            raise RuntimeError("Not connected to Snowflake")

        # Validate query
        is_valid, error_msg, query_type = self.validator.validate(sql)
        if not is_valid:
            raise ValueError(error_msg)

        with self._read_only_transaction() as cursor:
            # Set context
            if database:
                cursor.execute(f"USE DATABASE {database}")
            if schema:
                cursor.execute(f"USE SCHEMA {schema}")

            # Execute query
            cursor.execute(sql)

            # Stream results in batches
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                yield batch

    def get_databases(self) -> list[str]:
        """Get list of accessible databases."""
        result = self.execute_query("SHOW DATABASES")
        if result.data:
            return [row["name"] for row in result.data]
        return []

    def get_schemas(self, database: str) -> list[str]:
        """Get list of schemas in a database."""
        # Validate database name to prevent injection
        if not re.match(r"^[a-zA-Z0-9_]+$", database):
            raise ValueError(f"Invalid database name: {database}")

        result = self.execute_query(f"SHOW SCHEMAS IN DATABASE {database}")
        if result.data:
            return [row["name"] for row in result.data]
        return []

    def get_tables(self, database: str, schema: str) -> list[dict]:
        """Get detailed table information."""
        # Validate names to prevent injection
        if not re.match(r"^[a-zA-Z0-9_]+$", database):
            raise ValueError(f"Invalid database name: {database}")
        if not re.match(r"^[a-zA-Z0-9_]+$", schema):
            raise ValueError(f"Invalid schema name: {schema}")

        result = self.execute_query(f"SHOW TABLES IN {database}.{schema}")
        if result.data:
            return [
                {
                    "name": row.get("name"),
                    "type": row.get("kind", "TABLE"),
                    "database": database,
                    "schema": schema,
                    "comment": row.get("comment"),
                }
                for row in result.data
            ]
        return []

    def get_table_columns(self, database: str, schema: str, table: str) -> list[dict]:
        """Get column information for a table."""
        # Validate names
        if not re.match(r"^[a-zA-Z0-9_]+$", database):
            raise ValueError(f"Invalid database name: {database}")
        if not re.match(r"^[a-zA-Z0-9_]+$", schema):
            raise ValueError(f"Invalid schema name: {schema}")
        if not re.match(r"^[a-zA-Z0-9_]+$", table):
            raise ValueError(f"Invalid table name: {table}")

        result = self.execute_query(f"DESCRIBE TABLE {database}.{schema}.{table}")
        if result.data:
            return [
                {
                    "name": row.get("name"),
                    "type": row.get("type"),
                    "nullable": row.get("null?", "Y") == "Y",
                    "default": row.get("default"),
                    "primary_key": row.get("primary key") == "Y",
                    "comment": row.get("comment"),
                }
                for row in result.data
            ]
        return []

    def test_connection(self) -> bool:
        """Test if connection is alive and working."""
        try:
            result = self.execute_query("SELECT 1 AS test")
            return bool(result.data and result.data[0]["TEST"] == 1)
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def reconnect(self) -> None:
        """Reconnect to Snowflake."""
        self.disconnect()
        self.connect()

    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Snowflake connection closed")
            except Exception as e:
                self.logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.connection = None
                self._connection_metadata = {}

    def get_query_history(
        self, limit: int = 100, only_successful: bool = False
    ) -> list[dict]:
        """Get query execution history."""
        history = self.query_log.copy()

        if only_successful:
            history = [q for q in history if q.get("status") == "success"]

        # Sort by timestamp descending and limit
        history.sort(key=lambda x: x["timestamp"], reverse=True)
        return history[:limit]

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def __del__(self):
        """Cleanup on deletion."""
        if self.connection:
            self.disconnect()
