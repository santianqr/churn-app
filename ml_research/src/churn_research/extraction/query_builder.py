from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(name):
        msg = f"Invalid SQL identifier: {name!r}"
        raise ValueError(msg)
    return name


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
        """Fully qualified table name: database.schema.table."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)


def _build_where(spec: QuerySpec, extra_where: str | None = None) -> str:
    """Build WHERE clause from spec filters and optional extra condition."""
    conditions = []
    for col, value in spec.filters.items():
        safe_col = _validate_identifier(col)
        escaped_value = value.replace("'", "''")
        conditions.append(f"{safe_col} = '{escaped_value}'")

    if extra_where:
        conditions.append(extra_where)

    if conditions:
        return " WHERE " + " AND ".join(conditions)
    return ""


def _build_columns(spec: QuerySpec) -> str:
    """Build column list with optional aliases."""
    if not spec.columns:
        return "*"

    col_parts = []
    for col in spec.columns:
        safe_col = _validate_identifier(col)
        alias = spec.column_aliases.get(col)
        if alias:
            safe_alias = _validate_identifier(alias)
            col_parts.append(f"{safe_col} AS {safe_alias}")
        else:
            col_parts.append(safe_col)
    return ", ".join(col_parts)


def build_select(spec: QuerySpec, extra_where: str | None = None) -> str:
    """Build a SELECT query from a QuerySpec."""
    columns_sql = _build_columns(spec)
    table = _validate_identifier(spec.full_table_name)
    where = _build_where(spec, extra_where)
    return f"SELECT {columns_sql} FROM {table}{where}"


def build_count(spec: QuerySpec, extra_where: str | None = None) -> str:
    """Build a COUNT query from a QuerySpec."""
    table = _validate_identifier(spec.full_table_name)
    where = _build_where(spec, extra_where)
    return f"SELECT COUNT(*) FROM {table}{where}"


def _month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    """Generate monthly [start, end) date ranges from start to end."""
    ranges = []
    current = start.replace(day=1)
    while current <= end:
        next_month = current.month % 12 + 1
        next_year = current.year + (1 if current.month == 12 else 0)
        month_end = date(next_year, next_month, 1)
        ranges.append((current, month_end))
        current = month_end
    return ranges


def build_partitioned_queries(spec: QuerySpec) -> list[tuple[str, str]]:
    """Build monthly date-partitioned queries. Returns list of (sql, partition_label)."""
    if not spec.date_column or not spec.start_date or not spec.end_date:
        msg = "date_column, start_date, and end_date are required for partitioned queries"
        raise ValueError(msg)

    safe_date_col = _validate_identifier(spec.date_column)
    queries = []
    for range_start, range_end in _month_ranges(spec.start_date, spec.end_date):
        date_filter = f"{safe_date_col} >= '{range_start.isoformat()}' AND {safe_date_col} < '{range_end.isoformat()}'"
        sql = build_select(spec, extra_where=date_filter)
        label = f"{range_start.year}/{range_start.month:02d}"
        queries.append((sql, label))

    return queries
