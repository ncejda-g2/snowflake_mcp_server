"""Schema cache management for Snowflake MCP Server."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    comment: str | None = None
    default_value: str | None = None
    is_primary_key: bool = False


@dataclass
class TableInfo:
    """Information about a database table."""

    database: str
    schema: str
    table_name: str
    table_type: str  # TABLE, VIEW, etc.
    columns: list[ColumnInfo]
    column_count: int = 0
    row_count: int | None = None
    bytes: int | None = None
    comment: str | None = None
    last_altered: str | None = None

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.table_name}"

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "database": self.database,
            "schema": self.schema,
            "table_name": self.table_name,
            "table_type": self.table_type,
            "columns": [asdict(col) for col in self.columns],
            "column_count": self.column_count,
            "row_count": self.row_count,
            "bytes": self.bytes,
            "comment": self.comment,
            "last_altered": self.last_altered,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TableInfo":
        """Create from dictionary."""
        columns = [ColumnInfo(**col) for col in data.get("columns", [])]
        column_count = data.get("column_count", len(columns))
        return cls(
            database=data["database"],
            schema=data["schema"],
            table_name=data["table_name"],
            table_type=data.get("table_type", "TABLE"),
            columns=columns,
            column_count=column_count,
            row_count=data.get("row_count"),
            bytes=data.get("bytes"),
            comment=data.get("comment"),
            last_altered=data.get("last_altered"),
        )


class SchemaCache:
    """Manages cached schema information with TTL and persistence."""

    def __init__(self, ttl_days: int = 5, cache_dir: Path | None = None):
        """
        Initialize schema cache.

        Args:
            ttl_days: Time-to-live for cache in days
            cache_dir: Directory for cache persistence (defaults to ~/.snowflake_mcp/cache)
        """
        self.ttl_days = ttl_days
        self.cache_dir = cache_dir or (Path.home() / ".snowflake_mcp" / "cache")
        self.cache_file = self.cache_dir / "schema_cache.json"
        self.checkpoint_dir = self.cache_dir / "checkpoints"
        self.error_log_file = self.cache_dir / "refresh_errors.json"

        # Create cache directory and checkpoint directory if they don't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Cache storage
        self.tables: dict[str, TableInfo] = {}
        self.databases: set[str] = set()
        self.last_refresh: datetime | None = None
        self.refresh_in_progress: bool = False
        self.processed_databases: set[str] = (
            set()
        )  # Track processed databases for resume
        self.schema_last_altered: dict[
            str, str
        ] = {}  # "DB.SCHEMA" -> max LAST_ALTERED ISO string

        # Thread safety
        self._lock = Lock()

        # Logging
        self.logger = logging.getLogger(__name__)

        # Load existing cache
        self.load()

    def is_expired(self) -> bool:
        """Check if cache has expired."""
        if not self.last_refresh:
            return True

        expiry_time = self.last_refresh + timedelta(days=self.ttl_days)
        return datetime.now() > expiry_time

    def is_empty(self) -> bool:
        """Check if cache is empty."""
        return len(self.tables) == 0

    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            self.tables.clear()
            self.databases.clear()
            self.last_refresh = None
            self.logger.info("Schema cache cleared")

    def add_table(self, table_info: TableInfo) -> None:
        """Add or update table information in cache."""
        with self._lock:
            key = table_info.full_name.upper()
            self.tables[key] = table_info
            self.databases.add(table_info.database.upper())

    def get_table(self, database: str, schema: str, table: str) -> TableInfo | None:
        """
        Get table information from cache.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            TableInfo if found, None otherwise
        """
        key = f"{database}.{schema}.{table}".upper()
        return self.tables.get(key)

    def get_tables_in_database(self, database: str) -> list[TableInfo]:
        """Get all tables in a specific database."""
        database_upper = database.upper()
        return [
            table
            for table in self.tables.values()
            if table.database.upper() == database_upper
        ]

    def get_tables_in_schema(self, database: str, schema: str) -> list[TableInfo]:
        """Get all tables in a specific schema."""
        database_upper = database.upper()
        schema_upper = schema.upper()
        return [
            table
            for table in self.tables.values()
            if table.database.upper() == database_upper
            and table.schema.upper() == schema_upper
        ]

    def search_tables(self, pattern: str) -> list[TableInfo]:
        """
        Search for tables matching a pattern.

        Args:
            pattern: Search pattern (case-insensitive)

        Returns:
            List of matching tables
        """
        pattern_upper = pattern.upper()
        results = []

        for table in self.tables.values():
            if (
                pattern_upper in table.table_name.upper()
                or pattern_upper in table.full_name.upper()
                or (table.comment and pattern_upper in table.comment.upper())
            ):
                results.append(table)

        return results

    def get_schema_last_altered(self, database: str, schema: str) -> str | None:
        """Get the stored max LAST_ALTERED for a schema, or None if unknown."""
        key = f"{database}.{schema}".upper()
        return self.schema_last_altered.get(key)

    def merge_schema_results(
        self,
        database: str,
        schema: str,
        table_results: list[dict],
        column_counts: dict[str, int] | None = None,
        max_last_altered: str | None = None,
    ) -> int:
        """Merge table-level results for a single schema into the cache.

        Removes all existing entries for this database.schema,
        then adds the new ones. Tables in other schemas are untouched.
        Columns are NOT loaded here — they are fetched on-demand by describe_table.

        Args:
            database: Database name
            schema: Schema name
            table_results: INFORMATION_SCHEMA.TABLES query results (one row per table)
            column_counts: Optional dict mapping TABLE_NAME -> column count
            max_last_altered: The max LAST_ALTERED value for this schema

        Returns:
            Number of tables in this schema after merge
        """
        if column_counts is None:
            column_counts = {}

        with self._lock:
            prefix = f"{database}.{schema}.".upper()

            # Remove existing entries for this schema (preserve columns if table still exists)
            old_columns: dict[str, list[ColumnInfo]] = {}
            keys_to_remove = [k for k in self.tables if k.startswith(prefix)]
            for key in keys_to_remove:
                old_table = self.tables[key]
                if old_table.columns:
                    old_columns[key] = old_table.columns
                del self.tables[key]

            table_count = 0
            for row in table_results:
                db = row.get("TABLE_CATALOG", row.get("table_catalog", ""))
                sch = row.get("TABLE_SCHEMA", row.get("table_schema", ""))
                table_name = row.get("TABLE_NAME", row.get("table_name", ""))
                table_type = row.get("TABLE_TYPE", row.get("table_type", "TABLE"))

                if sch.upper() in ("INFORMATION_SCHEMA", "SNOWFLAKE"):
                    continue

                table_key = f"{db}.{sch}.{table_name}".upper()

                # Preserve previously-loaded columns if available
                columns = old_columns.get(table_key, [])
                col_count = column_counts.get(table_name, len(columns))

                table_info = TableInfo(
                    database=db,
                    schema=sch,
                    table_name=table_name,
                    table_type=table_type,
                    columns=columns,
                    column_count=col_count,
                    row_count=row.get("ROW_COUNT", row.get("row_count")),
                    bytes=row.get("BYTES", row.get("bytes")),
                    comment=row.get("COMMENT", row.get("comment")),
                    last_altered=str(row.get("LAST_ALTERED", ""))
                    if row.get("LAST_ALTERED")
                    else None,
                )
                self.tables[table_key] = table_info
                self.databases.add(db.upper())
                table_count += 1

            # Update schema_last_altered
            schema_key = f"{database}.{schema}".upper()
            if max_last_altered:
                self.schema_last_altered[schema_key] = max_last_altered

            return table_count

    def remove_schema(self, database: str, schema: str) -> int:
        """Remove all cached entries for a database.schema.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            prefix = f"{database}.{schema}.".upper()
            keys_to_remove = [k for k in self.tables if k.startswith(prefix)]
            for key in keys_to_remove:
                del self.tables[key]

            schema_key = f"{database}.{schema}".upper()
            self.schema_last_altered.pop(schema_key, None)

            return len(keys_to_remove)

    def get_databases(self) -> list[str]:
        """Get list of all cached databases."""
        return sorted(self.databases)

    def get_schemas(self, database: str) -> list[str]:
        """Get list of all schemas in a database."""
        database_upper = database.upper()
        schemas = set()

        for table in self.tables.values():
            if table.database.upper() == database_upper:
                schemas.add(table.schema)

        return sorted(schemas)

    def save_checkpoint(self, database: str, schema: str, results: list[dict]) -> None:
        """
        Save a checkpoint file for a single schema.

        Args:
            database: Database name
            schema: Schema name
            results: Query results for this schema
        """
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{database}__{schema}.json"

        try:
            checkpoint_data = {
                "database": database,
                "schema": schema,
                "timestamp": datetime.now().isoformat(),
                "results": results,
            }

            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint_data, f, default=str)

            self.logger.debug(f"Checkpoint saved for {database}.{schema}")

        except Exception as e:
            self.logger.error(f"Failed to save checkpoint for {database}.{schema}: {e}")

    def load_checkpoints(self) -> tuple[list[dict], set[str]]:
        """
        Load all checkpoint files and return combined results.

        Returns:
            Tuple of (combined results, set of processed schema keys "DB.SCHEMA")
        """
        all_results: list[dict] = []
        processed_schemas: set[str] = set()

        if not self.checkpoint_dir.exists():
            return all_results, processed_schemas

        checkpoint_files = list(self.checkpoint_dir.glob("checkpoint_*__*.json"))

        for checkpoint_file in checkpoint_files:
            try:
                with open(checkpoint_file) as f:
                    checkpoint_data = json.load(f)

                database = checkpoint_data["database"]
                schema = checkpoint_data["schema"]
                results = checkpoint_data["results"]

                all_results.extend(results)
                processed_schemas.add(f"{database}.{schema}")

                self.logger.debug(f"Loaded checkpoint for {database}.{schema}")

            except Exception as e:
                self.logger.error(f"Failed to load checkpoint {checkpoint_file}: {e}")

        self.logger.info(
            f"Loaded {len(checkpoint_files)} checkpoints with {len(all_results)} total results"
        )
        return all_results, processed_schemas

    def clear_checkpoints(self) -> None:
        """
        Remove all checkpoint files after successful completion.
        Handles both old (per-database) and new (per-schema) checkpoint formats.
        """
        if not self.checkpoint_dir.exists():
            return

        checkpoint_files = list(self.checkpoint_dir.glob("checkpoint_*.json"))

        for checkpoint_file in checkpoint_files:
            try:
                checkpoint_file.unlink()
                self.logger.debug(f"Removed checkpoint {checkpoint_file}")
            except Exception as e:
                self.logger.error(f"Failed to remove checkpoint {checkpoint_file}: {e}")

        self.logger.info(f"Cleared {len(checkpoint_files)} checkpoint files")

    def save_error_log(self, errors: dict[str, str]) -> None:
        """
        Save error log for failed databases.

        Args:
            errors: Dictionary mapping database names to error messages
        """
        try:
            error_data = {"timestamp": datetime.now().isoformat(), "errors": errors}

            with open(self.error_log_file, "w") as f:
                json.dump(error_data, f, indent=2)

            self.logger.info(f"Error log saved with {len(errors)} errors")

        except Exception as e:
            self.logger.error(f"Failed to save error log: {e}")

    def load_error_log(self) -> dict[str, str]:
        """
        Load error log to identify failed databases.

        Returns:
            Dictionary mapping database names to error messages
        """
        if not self.error_log_file.exists():
            return {}

        try:
            with open(self.error_log_file) as f:
                error_data = json.load(f)

            return error_data.get("errors", {})

        except Exception as e:
            self.logger.error(f"Failed to load error log: {e}")
            return {}

    def clear_error_log(self) -> None:
        """
        Remove error log file after successful completion.
        """
        if self.error_log_file.exists():
            try:
                self.error_log_file.unlink()
                self.logger.debug("Cleared error log")
            except Exception as e:
                self.logger.error(f"Failed to clear error log: {e}")

    def update_from_information_schema(self, results: list[dict]) -> int:
        """
        Update cache from INFORMATION_SCHEMA query results.

        Args:
            results: Query results from INFORMATION_SCHEMA.COLUMNS

        Returns:
            Number of tables processed
        """
        with self._lock:
            # Clear existing cache
            self.tables.clear()
            self.databases.clear()

            # Group columns by table
            tables_data: dict[str, dict] = {}

            for row in results:
                # Extract table information
                database = row.get("TABLE_CATALOG", row.get("table_catalog", ""))
                schema = row.get("TABLE_SCHEMA", row.get("table_schema", ""))
                table_name = row.get("TABLE_NAME", row.get("table_name", ""))
                table_type = row.get("TABLE_TYPE", row.get("table_type", "TABLE"))

                # Skip system schemas
                if schema.upper() in ("INFORMATION_SCHEMA", "SNOWFLAKE"):
                    continue

                table_key = f"{database}.{schema}.{table_name}".upper()

                # Initialize table entry if needed
                if table_key not in tables_data:
                    tables_data[table_key] = {
                        "database": database,
                        "schema": schema,
                        "table_name": table_name,
                        "table_type": table_type,
                        "columns": [],
                        "row_count": row.get("ROW_COUNT", row.get("row_count")),
                        "bytes": row.get("BYTES", row.get("bytes")),
                        "comment": row.get("TABLE_COMMENT", row.get("table_comment")),
                    }

                # Add column information
                column = ColumnInfo(
                    name=row.get("COLUMN_NAME", row.get("column_name", "")),
                    data_type=row.get("DATA_TYPE", row.get("data_type", "")),
                    is_nullable=row.get("IS_NULLABLE", row.get("is_nullable", "YES"))
                    == "YES",
                    ordinal_position=int(
                        row.get("ORDINAL_POSITION", row.get("ordinal_position", 0))
                    ),
                    comment=row.get("COLUMN_COMMENT", row.get("column_comment")),
                    default_value=row.get("COLUMN_DEFAULT", row.get("column_default")),
                )

                tables_data[table_key]["columns"].append(column)

            # Create TableInfo objects
            for table_data in tables_data.values():
                # Sort columns by ordinal position
                table_data["columns"].sort(key=lambda x: x.ordinal_position)

                table_info = TableInfo(
                    database=table_data["database"],
                    schema=table_data["schema"],
                    table_name=table_data["table_name"],
                    table_type=table_data["table_type"],
                    columns=table_data["columns"],
                    row_count=table_data.get("row_count"),
                    bytes=table_data.get("bytes"),
                    comment=table_data.get("comment"),
                )

                # Directly add to tables without calling add_table (we already hold the lock)
                key = table_info.full_name.upper()
                self.tables[key] = table_info
                self.databases.add(table_info.database.upper())

            # Update refresh timestamp
            self.last_refresh = datetime.now()
            self.refresh_in_progress = False

            self.logger.info(
                f"Cache updated with {len(self.tables)} tables from {len(self.databases)} databases"
            )

            # Save to disk
            self.save()

            return len(self.tables)

    def save(self) -> None:
        """Save cache to disk."""
        try:
            cache_data = {
                "version": "1.2",
                "last_refresh": self.last_refresh.isoformat()
                if self.last_refresh
                else None,
                "ttl_days": self.ttl_days,
                "tables": {key: table.to_dict() for key, table in self.tables.items()},
                "databases": list(self.databases),
                "schema_last_altered": self.schema_last_altered,
            }

            # Write to temporary file first (atomic write)
            temp_file = self.cache_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(cache_data, f, indent=2)

            # Move temporary file to actual cache file
            temp_file.replace(self.cache_file)

            self.logger.debug(f"Cache saved to {self.cache_file}")

        except Exception as e:
            self.logger.error(f"Failed to save cache: {e}")

    def load(self) -> bool:
        """
        Load cache from disk.

        Returns:
            True if cache was loaded successfully, False otherwise
        """
        if not self.cache_file.exists():
            self.logger.debug("No cache file found")
            return False

        try:
            with open(self.cache_file) as f:
                cache_data = json.load(f)

            # Check version compatibility
            version = cache_data.get("version", "0.0")
            if version not in ("1.0", "1.1", "1.2"):
                self.logger.warning(f"Cache version mismatch: {version}")
                return False

            # Load data
            with self._lock:
                self.last_refresh = (
                    datetime.fromisoformat(cache_data["last_refresh"])
                    if cache_data.get("last_refresh")
                    else None
                )
                self.ttl_days = cache_data.get("ttl_days", self.ttl_days)
                self.databases = set(cache_data.get("databases", []))
                self.schema_last_altered = cache_data.get("schema_last_altered", {})

                # Load tables
                self.tables.clear()
                for key, table_data in cache_data.get("tables", {}).items():
                    try:
                        table_info = TableInfo.from_dict(table_data)
                        self.tables[key] = table_info
                    except Exception as e:
                        self.logger.warning(f"Failed to load table {key}: {e}")

            self.logger.info(
                f"Cache loaded: {len(self.tables)} tables, "
                f"last refresh: {self.last_refresh}"
            )

            return True

        except Exception as e:
            self.logger.error(f"Failed to load cache: {e}")
            return False

    def get_statistics(self) -> dict[str, Any]:
        """Get cache statistics."""
        stats: dict[str, Any] = {
            "total_tables": len(self.tables),
            "total_databases": len(self.databases),
            "last_refresh": self.last_refresh.isoformat()
            if self.last_refresh
            else None,
            "is_expired": self.is_expired(),
            "ttl_days": self.ttl_days,
            "cache_file": str(self.cache_file),
            "cache_size_bytes": self.cache_file.stat().st_size
            if self.cache_file.exists()
            else 0,
        }

        # Add per-database statistics
        db_stats = {}
        for db in self.databases:
            tables = self.get_tables_in_database(db)
            db_stats[db] = {
                "table_count": len(tables),
                "total_columns": sum(t.column_count for t in tables),
            }
        stats["databases"] = db_stats

        return stats

    def __str__(self) -> str:
        """String representation of cache."""
        return (
            f"SchemaCache(tables={len(self.tables)}, "
            f"databases={len(self.databases)}, "
            f"expired={self.is_expired()})"
        )
