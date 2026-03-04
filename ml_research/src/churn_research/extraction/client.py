from __future__ import annotations

import logging
from collections.abc import Generator
from typing import TYPE_CHECKING

import snowflake.connector

if TYPE_CHECKING:
    import pyarrow as pa

    from churn_research.config.settings import SnowflakeSettings

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """Context manager for Snowflake connections with Arrow batch support."""

    def __init__(self, settings: SnowflakeSettings) -> None:
        self._settings = settings
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    def __enter__(self) -> SnowflakeClient:
        logger.info("Connecting to Snowflake account=%s", self._settings.account)
        self._conn = snowflake.connector.connect(**self._settings.connection_params())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed")

    @property
    def connection(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None:
            msg = "Not connected. Use 'with SnowflakeClient(...) as client:'"
            raise RuntimeError(msg)
        return self._conn

    def execute_arrow_batches(self, sql: str) -> Generator[pa.RecordBatch]:
        """Execute a query and yield Arrow RecordBatches for streaming."""
        logger.info("Executing query: %.120s...", sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            batches = cursor.fetch_arrow_batches()
            for batch in batches:
                yield batch
        finally:
            cursor.close()

    def execute_count(self, sql: str) -> int:
        """Execute a COUNT query and return the result."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            cursor.close()
