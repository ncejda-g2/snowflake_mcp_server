"""Snowflake connection management with strict read-only enforcement."""

import base64
import json
import logging
import re
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import snowflake.connector
import sqlparse
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection as SnowflakeConn
from snowflake.connector.errors import ProgrammingError
from sqlparse import tokens as T

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
    """Validates SQL queries for read-only safety using proper SQL parsing."""

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
        Validate that SQL query is read-only using proper SQL parsing.

        Returns:
            Tuple of (is_valid, error_message, query_type)
        """
        if not sql or not sql.strip():
            return False, "Empty query", QueryType.UNKNOWN

        # Parse SQL using sqlparse
        try:
            parsed = sqlparse.parse(sql)
        except Exception as e:
            return False, f"Failed to parse SQL: {str(e)}", QueryType.UNKNOWN

        if not parsed:
            return False, "Empty or invalid query", QueryType.UNKNOWN

        # Check for multiple statements
        if len(parsed) > 1:
            return (
                False,
                "Multiple statements not allowed. Only single queries permitted.",
                QueryType.UNKNOWN,
            )

        statement = parsed[0]

        # Get first meaningful keyword token
        query_type, first_keyword = cls._identify_query_type(statement)

        # Check for any write operations and provide detailed position info
        write_ops_found = cls._find_write_operations(statement, sql)
        if write_ops_found:
            error_details = cls._format_write_operation_errors(write_ops_found)
            return (
                False,
                f"Query contains write operations. Only read operations are allowed.\n\n{error_details}",
                QueryType.WRITE,
            )

        if query_type == QueryType.WRITE:
            # Fallback in case _find_write_operations missed it
            return (
                False,
                f"Write operation '{first_keyword}' is not permitted. Only read operations (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) are allowed.",
                QueryType.WRITE,
            )

        if query_type == QueryType.UNKNOWN:
            return (
                False,
                f"Unknown or disallowed operation '{first_keyword}'. Only read operations (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) are allowed.",
                QueryType.UNKNOWN,
            )

        return True, "", query_type

    @classmethod
    def _identify_query_type(cls, statement) -> tuple[QueryType, str]:
        """
        Identify the type of SQL query by examining keyword tokens.

        Returns:
            Tuple of (QueryType, first_keyword_text)
        """
        # Get first meaningful keyword token
        # Prioritize statement-level keywords (CTE, DML, DDL) over generic keywords
        first_keyword = None
        for token in statement.flatten():
            # Check for statement-level keywords first
            if token.ttype in (T.Keyword.CTE, T.Keyword.DML, T.Keyword.DDL):
                first_keyword = token.value.upper().strip()
                break
            # Fall back to generic keywords if no statement-level keyword found yet
            if not first_keyword and token.ttype in (T.Keyword, T.Keyword.Order):
                candidate = token.value.upper().strip()
                # Only accept statement-starting keywords, not structural keywords like AS, ON, etc
                if candidate in cls.READ_OPERATIONS | cls.WRITE_OPERATIONS:
                    first_keyword = candidate
                    break

        if not first_keyword:
            return QueryType.UNKNOWN, "UNKNOWN"

        # Handle CTEs - look for the actual operation after WITH
        if first_keyword == "WITH":
            # For WITH queries, look for the main operation keyword after the CTE
            for token in statement.flatten():
                if token.ttype == T.Keyword.DML:
                    keyword_upper = token.value.upper().strip()
                    # Found the main operation after WITH
                    if keyword_upper in cls.WRITE_OPERATIONS:
                        return QueryType.WRITE, keyword_upper
                    elif keyword_upper == "SELECT":
                        return QueryType.WITH, first_keyword
            return QueryType.WITH, first_keyword

        if first_keyword in cls.WRITE_OPERATIONS:
            return QueryType.WRITE, first_keyword
        elif first_keyword == "SELECT":
            return QueryType.SELECT, first_keyword
        elif first_keyword == "SHOW":
            return QueryType.SHOW, first_keyword
        elif first_keyword in ("DESCRIBE", "DESC"):
            return QueryType.DESCRIBE, first_keyword
        elif first_keyword == "EXPLAIN":
            return QueryType.EXPLAIN, first_keyword
        else:
            # Unknown operations are treated as potentially unsafe
            return QueryType.UNKNOWN, first_keyword

    @classmethod
    def _find_write_operations(cls, statement, original_sql: str) -> list[dict]:
        """
        Find write operation keywords in the parsed statement.

        Only examines actual SQL keywords, not string literals or identifiers.

        Returns:
            List of dicts with: {keyword, position, line, column}
        """
        write_ops_found = []

        for token in statement.flatten():
            # Only check keyword tokens (not strings, identifiers, etc)
            if token.ttype in (T.Keyword.DML, T.Keyword.DDL, T.Keyword, T.Keyword.CTE):
                keyword_upper = token.value.upper().strip()

                # Skip the allowed first keywords for CTEs and read operations
                if keyword_upper in cls.READ_OPERATIONS:
                    continue

                if keyword_upper in cls.WRITE_OPERATIONS:
                    # Find position in original SQL
                    position = cls._find_token_position(original_sql, token.value)
                    write_ops_found.append(
                        {
                            "keyword": keyword_upper,
                            "position": position["char_pos"],
                            "line": position["line"],
                            "column": position["column"],
                            "context": position["context"],
                        }
                    )

        return write_ops_found

    @classmethod
    def _find_token_position(cls, sql: str, token_value: str) -> dict:
        """
        Find the position of a token in the original SQL.

        Returns:
            Dict with char_pos, line, column, and context
        """
        lines = sql.split("\n")
        char_pos = 0

        for line_num, line in enumerate(lines, 1):
            # Case-insensitive search for the token
            col = line.upper().find(token_value.upper())
            if col != -1:
                return {
                    "char_pos": char_pos + col,
                    "line": line_num,
                    "column": col + 1,
                    "context": line.strip(),
                }
            char_pos += len(line) + 1  # +1 for newline

        # Fallback if not found
        return {"char_pos": 0, "line": 1, "column": 1, "context": sql[:100]}

    @classmethod
    def _format_write_operation_errors(cls, write_ops: list[dict]) -> str:
        """
        Format detailed error message showing which write operations were found.

        Args:
            write_ops: List of dicts with keyword, line, column, context

        Returns:
            Formatted error string
        """
        if not write_ops:
            return ""

        error_lines = ["Detected write operations:"]
        for op in write_ops:
            error_lines.append(
                f"  - '{op['keyword']}' at line {op['line']}, column {op['column']}"
            )

        return "\n".join(error_lines)


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

    @staticmethod
    def _load_private_key(private_key_b64: str, passphrase: str) -> bytes:
        """Decode a base64 PEM private key to DER bytes for Snowflake."""
        pem_data = base64.b64decode(private_key_b64)
        private_key = serialization.load_pem_private_key(
            pem_data, password=passphrase.encode()
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def _load_credential_file(self) -> dict:
        """Load and parse the JSON credential file."""
        cred_path = Path(self.config.credential_file)
        if not cred_path.is_absolute():
            cred_path = Path.cwd() / cred_path

        with open(cred_path) as f:
            return json.load(f)

    def connect(self) -> None:
        """
        Establish read-only connection to Snowflake.

        When ``credential_file`` is set in config, uses key-pair auth
        (headless).  Otherwise falls back to external browser SSO.

        Raises:
            Exception: If connection fails
        """
        try:
            self.logger.info(f"Connecting to Snowflake account: {self.config.account}")

            if self.config.credential_file:
                # Key-pair authentication (headless)
                creds = self._load_credential_file()
                private_key_der = self._load_private_key(
                    creds["private_key_b64"],
                    creds["private_key_passphrase"],
                )
                self.logger.info("Using key-pair authentication (headless)")
                self.connection = snowflake.connector.connect(
                    account=creds.get("account", self.config.account),
                    user=creds.get("user", self.config.username),
                    private_key=private_key_der,
                    role=creds.get("role", self.config.role),
                    warehouse=creds.get("warehouse", self.config.warehouse),
                    client_session_keep_alive=True,
                    network_timeout=30,
                    login_timeout=10,
                    ocsp_fail_open=False,
                    validate_default_parameters=True,
                )
            else:
                # External browser SSO (interactive)
                self.logger.info("Using external browser authentication (SSO)")
                self.connection = snowflake.connector.connect(
                    account=self.config.account,
                    user=self.config.username,
                    authenticator="externalbrowser",
                    role=self.config.role,
                    warehouse=self.config.warehouse,
                    client_session_keep_alive=True,
                    network_timeout=30,
                    login_timeout=10,
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
