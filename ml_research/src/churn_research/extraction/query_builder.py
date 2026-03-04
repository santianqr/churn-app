from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal


@dataclass(frozen=True)
class QuerySpec:
    """Specification for a Snowflake data extraction query."""

    table: str
    database: str | None = None
    schema: str | None = None
    columns: list[str] = field(default_factory=list)
    column_aliases: dict[str, str] = field(default_factory=dict)
    filters: dict[str, str] = field(default_factory=dict)
    date_column: str | None = None
    start_date: date | None = None
    end_date: date | None = None

    @property
    def full_table_name(self) -> str:
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)


def build_select(spec: QuerySpec, extra_where: str | None = None) -> str:
    """Build a SELECT query from a QuerySpec."""
    # Columns
    if spec.columns:
        col_parts = []
        for col in spec.columns:
            alias = spec.column_aliases.get(col)
            col_parts.append(f"{col} AS {alias}" if alias else col)
        columns_sql = ", ".join(col_parts)
    else:
        columns_sql = "*"

    sql = f"SELECT {columns_sql} FROM {spec.full_table_name}"

    # WHERE clauses
    conditions = []
    for col, value in spec.filters.items():
        conditions.append(f"{col} = '{value}'")

    if extra_where:
        conditions.append(extra_where)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    return sql


def build_count(spec: QuerySpec, extra_where: str | None = None) -> str:
    """Build a COUNT query from a QuerySpec."""
    sql = f"SELECT COUNT(*) FROM {spec.full_table_name}"

    conditions = []
    for col, value in spec.filters.items():
        conditions.append(f"{col} = '{value}'")

    if extra_where:
        conditions.append(extra_where)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    return sql


def _month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    """Generate monthly date ranges from start to end (inclusive)."""
    ranges = []
    current = start.replace(day=1)
    while current <= end:
        next_month = current.month % 12 + 1
        next_year = current.year + (1 if current.month == 12 else 0)
        month_end = date(next_year, next_month, 1)
        ranges.append((current, month_end))
        current = month_end
    return ranges


def build_partitioned_queries(
    spec: QuerySpec,
    freq: Literal["month"] = "month",
) -> list[tuple[str, str]]:
    """Build date-partitioned queries, returns list of (sql, partition_label)."""
    if not spec.date_column or not spec.start_date or not spec.end_date:
        msg = "date_column, start_date, and end_date are required for partitioned queries"
        raise ValueError(msg)

    queries = []
    for range_start, range_end in _month_ranges(spec.start_date, spec.end_date):
        date_filter = (
            f"{spec.date_column} >= '{range_start.isoformat()}' "
            f"AND {spec.date_column} < '{range_end.isoformat()}'"
        )
        sql = build_select(spec, extra_where=date_filter)
        label = f"{range_start.year}/{range_start.month:02d}"
        queries.append((sql, label))

    return queries
