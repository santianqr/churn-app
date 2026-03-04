from __future__ import annotations

import logging
import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


class ParquetExporter:
    """Streams Arrow batches to a Parquet file with zstd compression."""

    def __init__(self, compression: Literal["zstd", "snappy", "gzip", "lz4", "none"] = "zstd") -> None:
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
        tmp_fd = None
        tmp_path: Path | None = None

        try:
            tmp_fd, tmp_path_str = tempfile.mkstemp(
                suffix=".parquet.tmp",
                dir=str(output_path.parent),
            )
            # Close fd immediately — ParquetWriter opens the file by name
            os.close(tmp_fd)
            tmp_fd = None
            tmp_path = Path(tmp_path_str)

            for chunk in batches:
                table = chunk if isinstance(chunk, pa.Table) else pa.Table.from_batches([chunk])

                if column_renames:
                    new_names = [column_renames.get(name, name) for name in table.column_names]
                    table = table.rename_columns(new_names)

                if writer is None:
                    writer = pq.ParquetWriter(str(tmp_path), schema=table.schema, compression=self._compression)

                writer.write_table(table)
                total_rows += table.num_rows

                prev_rows = total_rows - table.num_rows
                if total_rows // 500_000 != prev_rows // 500_000:
                    logger.info("  ... %s rows written", f"{total_rows:,}")

            if writer is not None:
                writer.close()
                writer = None

            # Atomic rename on success
            tmp_path.replace(output_path)
            tmp_path = None

            logger.info("Wrote %s rows to %s", f"{total_rows:,}", output_path)

        finally:
            if writer is not None:
                writer.close()
            if tmp_fd is not None:
                os.close(tmp_fd)
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        return total_rows
