from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import snowflake.connector

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

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

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed")

    @property
    def connection(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None:
            msg = "Not connected. Use 'with SnowflakeClient(...) as client:'"
            raise RuntimeError(msg)
        return self._conn

    def execute_arrow_batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield Arrow batches for streaming."""
        logger.info("Executing query: %.120s...", sql)
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            yield from cursor.fetch_arrow_batches()

    def execute_count(self, sql: str) -> int:
        """Execute a COUNT query and return the result."""
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
