from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from churn_research.extraction.client import SnowflakeClient
from churn_research.extraction.parquet_writer import ParquetExporter
from churn_research.extraction.query_builder import (
    QuerySpec,
    build_partitioned_queries,
    build_select,
)

if TYPE_CHECKING:
    from churn_research.config.settings import SnowflakeSettings

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when one or more partitions fail to download."""

    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__(f"{len(failures)} partition(s) failed: {', '.join(failures)}")


class SnowflakeDownloader:
    """Orchestrates Snowflake data extraction to Parquet files."""

    def __init__(self, settings: SnowflakeSettings) -> None:
        self._settings = settings
        self._exporter = ParquetExporter()

    def download(
        self,
        spec: QuerySpec,
        output_dir: str | Path = "data/raw",
    ) -> list[Path]:
        """Download data from Snowflake to Parquet files.

        If spec has date_column/start_date/end_date, creates partitioned files.
        Otherwise, downloads everything into a single file.

        Returns list of created Parquet file paths.

        Raises:
            DownloadError: If any partition fails during partitioned download.
        """
        output_dir = Path(output_dir)

        with SnowflakeClient(self._settings) as client:
            if spec.date_column and spec.start_date and spec.end_date:
                created_files = self._download_partitioned(client, spec, output_dir)
            else:
                created_files = self._download_single(client, spec, output_dir)

        logger.info("Download complete: %d file(s) created in %s", len(created_files), output_dir)
        return created_files

    def _download_partitioned(
        self,
        client: SnowflakeClient,
        spec: QuerySpec,
        output_dir: Path,
    ) -> list[Path]:
        queries = build_partitioned_queries(spec)
        created: list[Path] = []
        failures: list[str] = []

        logger.info("Starting partitioned download: %d partitions", len(queries))

        for i, (sql, label) in enumerate(queries, 1):
            output_path = output_dir / f"{label}.parquet"

            if output_path.exists():
                logger.info("[%d/%d] Skipping %s (already exists)", i, len(queries), label)
                created.append(output_path)
                continue

            logger.info("[%d/%d] Downloading partition %s", i, len(queries), label)

            try:
                batches = client.execute_arrow_batches(sql)
                rows = self._exporter.write_batches(batches, output_path, column_renames=spec.column_aliases or None)
                if rows > 0:
                    created.append(output_path)
                else:
                    logger.info("[%d/%d] Partition %s is empty, skipping", i, len(queries), label)
            except Exception:
                logger.exception("[%d/%d] Failed to download partition %s", i, len(queries), label)
                failures.append(label)

        if failures:
            raise DownloadError(failures)

        return created

    def _download_single(
        self,
        client: SnowflakeClient,
        spec: QuerySpec,
        output_dir: Path,
    ) -> list[Path]:
        output_path = output_dir / f"{spec.table}.parquet"

        logger.info("Downloading full table to %s", output_path)

        sql = build_select(spec)
        batches = client.execute_arrow_batches(sql)
        rows = self._exporter.write_batches(batches, output_path, column_renames=spec.column_aliases or None)

        if rows > 0:
            return [output_path]
        logger.info("No rows returned, no file created")
        return []
