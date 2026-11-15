"""Translate DuckDB's pushed-down predicates into CKAN request parameters.

Two targets:

* **Solr ``fq``** for ``package_search`` (``datasets``, ``resources``, facets):
  equality / ``IN`` / ranges on mapped columns become Solr clauses such as
  ``organization:"health"`` or ``metadata_modified:[2024-01-01T00:00:00Z TO *]``.
* **DataStore ``filters``** for ``datastore_search``: equality and ``IN`` only.

Every scan sets ``auto_apply_filters``: the SDK re-applies all predicates to each
batch, so a translation only has to *narrow* what the portal returns (return a
superset of the matching rows), never be exact. Anything not translated is
simply evaluated locally.
"""

from __future__ import annotations

import datetime as dt
import decimal
from collections.abc import Callable
from typing import Any

import pyarrow as pa
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    Filter,
    InFilter,
    PushdownFilters,
)


def solr_quote(value: Any) -> str:
    """Quote a value as a Solr phrase: wrapped in double quotes, backslashes and quotes escaped."""
    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def solr_value(value: Any) -> str:
    """Render a constant for a Solr range or term: dates as UTC ``...Z``, numbers bare."""
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return aware.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, dt.date):
        return f"{value.isoformat()}T00:00:00Z"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | decimal.Decimal):
        return str(value)
    return solr_quote(value)


def _floor_ms(value: Any) -> Any:
    """Round a datetime down to Solr's millisecond precision (keeps a lower bound a superset)."""
    if isinstance(value, dt.datetime):
        return value.replace(microsecond=value.microsecond // 1000 * 1000)
    return value


def _ceil_ms(value: Any) -> Any:
    """Round a datetime up to millisecond precision (keeps an upper bound a superset)."""
    if isinstance(value, dt.datetime) and value.microsecond % 1000:
        return value.replace(microsecond=value.microsecond // 1000 * 1000) + dt.timedelta(milliseconds=1)
    return value


class SolrField:
    """How one SQL column maps onto a Solr field."""

    def __init__(self, name: str, *, ranged: bool = False, convert: Callable[[Any], Any] | None = None) -> None:
        """Store the mapping.

        Args:
            name: The Solr field name.
            ranged: The field supports ``[a TO b]`` range queries (dates, ints).
            convert: Maps the SQL constant to the indexed value (e.g. ``private`` ->
                ``capacity``).

        """
        self.name = name
        self.ranged = ranged
        self.convert = convert or (lambda v: v)


def _scalar(value: pa.Scalar[Any]) -> Any:
    return value.as_py() if isinstance(value, pa.Scalar) else value


def solr_clause(f: Filter, fields: dict[str, SolrField]) -> str | None:
    """One pushed-down filter as a Solr ``fq`` clause, or None when it can't be expressed."""
    if isinstance(f, AndFilter):
        parts = [solr_clause(c, fields) for c in f.children]
        kept = [p for p in parts if p]
        return " AND ".join(f"({p})" for p in kept) if kept else None
    field = fields.get(f.column_name)
    if field is None:
        return None
    if isinstance(f, InFilter):
        values = [field.convert(v) for v in f.values.to_pylist() if v is not None]
        if not values:
            return None
        return f"{field.name}:(" + " OR ".join(solr_value(v) for v in values) + ")"
    if isinstance(f, ConstantFilter):
        value = _scalar(f.value)
        if value is None:
            return None
        value = field.convert(value)
        if f.op == ComparisonOp.EQ:
            if isinstance(value, dt.datetime):
                # Solr keeps milliseconds; match the whole millisecond.
                return f"{field.name}:[{solr_value(_floor_ms(value))} TO {solr_value(_ceil_ms(value))}]"
            return f"{field.name}:{solr_value(value)}"
        if not field.ranged:
            return None
        # Strict bounds widen to inclusive ones at millisecond precision; the
        # local re-check trims the edge.
        if f.op in (ComparisonOp.GT, ComparisonOp.GE):
            return f"{field.name}:[{solr_value(_floor_ms(value))} TO *]"
        if f.op in (ComparisonOp.LT, ComparisonOp.LE):
            return f"{field.name}:[* TO {solr_value(_ceil_ms(value))}]"
    return None


def solr_fq(filters: PushdownFilters | None, fields: dict[str, SolrField]) -> list[str]:
    """All translatable top-level filters as Solr ``fq`` clauses (ANDed by the caller)."""
    if not filters:
        return []
    out: list[str] = []
    for f in filters.filters:
        clause = solr_clause(f, fields)
        if clause and clause not in out:
            out.append(clause)
    return out


def datastore_filters(filters: PushdownFilters | None, columns: set[str]) -> dict[str, Any]:
    """Equality / ``IN`` predicates as a DataStore ``filters`` object.

    DataStore matches ``{"col": value}`` exactly and ``{"col": [a, b]}`` as
    ``IN``. Values are sent as text for text columns and as JSON numbers
    otherwise, which PostgreSQL compares exactly.

    Args:
        filters: The pushed-down filters.
        columns: Columns that exist in the DataStore table.

    Returns:
        The ``filters`` parameter (empty when nothing translates).

    """
    out: dict[str, Any] = {}
    if not filters:
        return out
    for f in filters.filters:
        if f.column_name not in columns or f.column_name in out:
            continue
        if isinstance(f, ConstantFilter) and f.op == ComparisonOp.EQ:
            value = _jsonable(_scalar(f.value))
            if value is not None:
                out[f.column_name] = value
        elif isinstance(f, InFilter):
            values = [_jsonable(v) for v in f.values.to_pylist() if v is not None]
            if values:
                out[f.column_name] = values
    return out


def _jsonable(value: Any) -> Any:
    """Return a filter constant in a form ``json.dumps`` and DataStore accept."""
    if isinstance(value, dt.datetime | dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return None
    return value


def filter_columns(filters: PushdownFilters | None) -> set[str]:
    """Columns referenced by any pushed-down filter."""
    return set(filters.filtered_columns) if filters else set()


def passing_rows(filters: PushdownFilters | None, batch: pa.RecordBatch) -> int | None:
    """How many rows of ``batch`` satisfy every pushed-down filter (None: can't tell)."""
    if not filters or batch.num_rows == 0:
        return batch.num_rows
    if not filter_columns(filters) <= set(batch.schema.names):
        return None
    mask = filters.evaluate(batch)
    return int(mask.fill_null(False).to_pylist().count(True))
