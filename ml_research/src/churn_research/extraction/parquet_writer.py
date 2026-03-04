from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetExporter:
    """Streams Arrow batches to a Parquet file with zstd compression."""

    def __init__(self, compression: str = "zstd") -> None:
        self._compression = compression

    def write_batches(
        self,
        batches: Iterable[pa.RecordBatch],
        output_path: Path,
        column_renames: dict[str, str] | None = None,
    ) -> int:
        """Write Arrow batches to a Parquet file. Returns total rows written."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        writer: pq.ParquetWriter | None = None
        total_rows = 0

        try:
            for chunk in batches:
                # fetch_arrow_batches() can return Table or RecordBatch
                table = chunk if isinstance(chunk, pa.Table) else pa.Table.from_batches([chunk])

                if column_renames:
                    new_names = [column_renames.get(name, name) for name in table.column_names]
                    table = table.rename_columns(new_names)

                if writer is None:
                    writer = pq.ParquetWriter(
                        str(output_path),
                        schema=table.schema,
                        compression=self._compression,
                    )

                writer.write_table(table)
                total_rows += table.num_rows

                if total_rows % 500_000 < table.num_rows:
                    logger.info("  ... %s rows written", f"{total_rows:,}")

        finally:
            if writer is not None:
                writer.close()

        logger.info("Wrote %s rows to %s", f"{total_rows:,}", output_path)
        return total_rows
