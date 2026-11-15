"""DataStore: typed rows of tabular resources (``datastore_search`` / ``datastore_search_sql``).

The output schema comes from the DataStore's own field types (one bind-time
request with ``limit=0``): ``int*`` -> BIGINT, ``numeric``/``float*`` -> DOUBLE,
``timestamp`` -> TIMESTAMP, ``timestamptz`` -> TIMESTAMPTZ, ``date`` -> DATE,
``bool`` -> BOOLEAN, ``json``/``jsonb`` -> JSON, ``_type`` arrays -> LIST, and
text (and anything else) -> VARCHAR.

Pushdown into ``datastore_search``: SQL ``=`` / ``IN`` predicates become the
``filters`` object, the projected columns become ``fields``, and DuckDB's
top-N hint on a numeric / temporal column becomes ``sort`` with an early stop.
Paging is ``limit``/``offset`` over a stable ``_id`` sort with the offset kept
in the externalized cursor.

``resource_data`` reads a resource through DataStore when it is loaded there;
otherwise it returns the resource's download URL and a ready-made
``read_csv`` / ``read_parquet`` / ``read_json`` call, because file parsing is
DuckDB's job, not this worker's.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg, Param, Returns
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.table_function import (
    BindParams,
    OrderByDirection,
    OrderByNullOrder,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import convert as cv
from .client import ActionNotAvailableError, CkanClient, CkanError
from .common import PORTAL_DOC, bind_token, client_for, emit_rows, portal_of, request_secret
from .datasets import JSON
from .meta import IRELAND, PORTAL_NOTE, QUEENSLAND, Q, column, function_tags
from .pushdown import datastore_filters, filter_columns, passing_rows

COMMENT = "DataStore column '{name}' (DataStore type {type})."
ORDERABLE = {"int", "numeric", "float", "timestamp", "timestamptz", "date"}


def type_family(ds_type: str) -> str:
    """Collapse a DataStore / PostgreSQL type name into one of our families."""
    t = (ds_type or "text").strip().lower()
    if t.startswith("_") or t.endswith("[]"):
        return "array"
    if t in ("int", "int2", "int4", "int8", "integer", "bigint", "smallint", "serial", "bigserial", "oid"):
        return "int"
    if t in ("numeric", "decimal", "money"):
        return "numeric"
    if t in ("float", "float4", "float8", "real", "double precision", "double"):
        return "float"
    if t in ("bool", "boolean"):
        return "bool"
    if t in ("timestamp", "timestamp without time zone"):
        return "timestamp"
    if t in ("timestamptz", "timestamp with time zone"):
        return "timestamptz"
    if t == "date":
        return "date"
    if t in ("json", "jsonb"):
        return "json"
    return "text"


def element_type(ds_type: str) -> str:
    """Element type of an array type (``_int4`` / ``int4[]`` -> ``int4``)."""
    t = ds_type.strip().lower()
    return t[1:] if t.startswith("_") else t.removesuffix("[]")


def arrow_type(ds_type: str) -> pa.DataType:
    """Return the Arrow type for a DataStore field type."""
    fam = type_family(ds_type)
    if fam == "array":
        return pa.list_(arrow_type(element_type(ds_type)))
    types: dict[str, pa.DataType] = {
        "int": pa.int64(),
        "numeric": pa.float64(),
        "float": pa.float64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "json": JSON,
    }
    return types.get(fam, pa.string())


def convert_value(ds_type: str, value: Any) -> Any:
    """One DataStore JSON value as a Python value of :func:`arrow_type`."""
    if value is None:
        return None
    fam = type_family(ds_type)
    if fam == "array":
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                return None
        return [convert_value(element_type(ds_type), v) for v in value] if isinstance(value, list) else None
    if fam == "int":
        return cv.integer(value)
    if fam in ("numeric", "float"):
        return cv.number(value)
    if fam == "bool":
        return cv.boolean(value)
    if fam == "timestamp":
        parsed = cv.ts(value)
        return parsed.astimezone(dt.UTC).replace(tzinfo=None) if parsed else None
    if fam == "timestamptz":
        return cv.ts(value)
    if fam == "date":
        try:
            return dt.date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    if fam == "json":
        return value if isinstance(value, str) else cv.json_text(value)
    return cv.text(value)


def schema_from_fields(fields: list[dict[str, Any]], *, include_id: bool = True) -> pa.Schema:
    """Return the output schema for DataStore ``fields`` (``_full_text`` dropped)."""
    out = []
    for f in fields:
        name = str(f.get("id"))
        if name == "_full_text" or (name == "_id" and not include_id):
            continue
        ds_type = str(f.get("type") or "text")
        raw_info = f.get("info")
        info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
        label = cv.blank_to_null(info.get("label")) or cv.blank_to_null(info.get("notes"))
        comment = COMMENT.format(name=name, type=ds_type) + (f" {label}" if label and label != name else "")
        if name == "_id":
            comment = "DataStore row id (1-based, stable): use it to page or join."
        out.append(column(name, arrow_type(ds_type), comment, nullable=name != "_id"))
    return pa.schema(out)


def field_types(schema: pa.Schema) -> dict[str, str]:
    """Recover each column's DataStore type from the schema comments."""
    out: dict[str, str] = {}
    for f in schema:
        comment = (f.metadata or {}).get(b"comment", b"").decode()
        marker = "(DataStore type "
        if marker in comment:
            out[f.name] = comment.split(marker, 1)[1].split(")", 1)[0]
        else:
            out[f.name] = "int" if f.name == "_id" else "text"
    return out


_DESCRIBE_CACHE: dict[tuple[str, str, bool], tuple[float, pa.Schema]] = {}
DESCRIBE_TTL_SECONDS = 60.0


def describe(client: CkanClient, resource_id: str) -> pa.Schema:
    """Bind-time: the DataStore table's typed schema (``datastore_search`` with ``limit=0``).

    DuckDB binds a call more than once (the secret round trip, then planning), so
    the answer is kept for a minute per (portal, resource, has-token).
    """
    key = (client.portal, resource_id, bool(client.token))
    hit = _DESCRIBE_CACHE.get(key)
    if hit is not None and time.monotonic() - hit[0] < DESCRIBE_TTL_SECONDS:
        return hit[1]
    schema = _describe(client, resource_id)
    _DESCRIBE_CACHE[key] = (time.monotonic(), schema)
    return schema


def _describe(client: CkanClient, resource_id: str) -> pa.Schema:
    try:
        result = client.call("datastore_search", {"resource_id": resource_id, "limit": 0})
    except ActionNotAvailableError as err:
        raise CkanError(f"{client.portal} does not have the DataStore plugin enabled") from err
    except CkanError as err:
        if "Authorization Error" in str(err):
            raise
        # CKAN 2.9/2.10 answer 404, 2.11 answers 500 for a resource without a DataStore table.
        try:
            in_datastore = cv.boolean(_resource(client, resource_id).get("datastore_active"))
        except CkanError:
            in_datastore = None
        if in_datastore:
            raise
        raise CkanError(
            f"resource {resource_id} is not in {client.portal}'s DataStore (check resources().datastore_active, "
            f"or read its url with read_csv / read_parquet, see resource_data()): {err}"
        ) from err
    return schema_from_fields(list(result.get("fields") or []))


def _resource(client: CkanClient, resource_id: str) -> dict[str, Any]:
    res = client.call("resource_show", {"id": resource_id})
    if not isinstance(res, dict):
        raise CkanError(f"resource_show returned {type(res).__name__}")
    return res


@dataclass(slots=True, frozen=True, kw_only=True)
class DatastoreArgs:
    """Arguments for ``datastore(portal, resource_id, ...)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    resource_id: Annotated[str, Arg(1, doc="Id of a resource whose rows are loaded into DataStore.", pattern=r"\S")]
    filters: Annotated[
        str | None,
        Arg(
            "filters",
            default=None,
            doc="Remote equality filters: a JSON object mapping a column to a value, or to a list of values.",
        ),
    ]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search across the table's text columns.")]
    page_size: Annotated[
        int,
        Arg(
            "page_size", default=1000, doc="Rows per request, 1-32000 (the DataStore default maximum).", ge=1, le=32000
        ),
    ]


@dataclass(kw_only=True)
class DatastoreCursor(ArrowSerializableDataclass):
    """Resumable position in a DataStore table."""

    offset: int = 0
    matched: int = 0
    done: bool = False
    # The remote filters of the first page, reused so the offset stays valid
    # when DuckDB tightens dynamic filters mid-scan.
    filters: str = ""
    frozen: bool = False


def _user_filters(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError as err:
        raise CkanError(f'filters must be a JSON object such as \'{{"column": "value"}}\': {err}') from err
    if not isinstance(parsed, dict):
        raise CkanError('filters must be a JSON object such as \'{"column": "value"}\'')
    return parsed


def _sort_hint(params: ProcessParams[Any], types: dict[str, str]) -> tuple[str, int] | None:
    """``(sort, limit)`` for DuckDB's top-N hint when PostgreSQL orders the same way."""
    init = params.init_call
    if init is None or not init.order_by_column_name or not init.order_by_limit:
        return None
    col = init.order_by_column_name
    if col not in types or (col != "_id" and type_family(types[col]) not in ORDERABLE):
        return None
    desc = init.order_by_direction == OrderByDirection.DESC
    nulls = "first" if init.order_by_null_order == OrderByNullOrder.NULLS_FIRST else "last"
    ident = '"' + col.replace('"', '""') + '"'
    return f"{ident} {'desc' if desc else 'asc'} nulls {nulls}, _id asc", int(init.order_by_limit)


def scan_datastore(
    params: ProcessParams[Any],
    state: DatastoreCursor,
    out: OutputCollector,
    *,
    resource_id: str,
    user_filters: dict[str, Any],
    q: str | None,
    page_size: int,
) -> None:
    """One ``datastore_search`` page into the bound (projected) schema."""
    if state.done:
        out.finish()
        return
    client = client_for(params)
    bound = params.init_call.output_schema if params.init_call is not None else params.output_schema
    types = field_types(bound)
    pushed = params.current_pushdown_filters
    filters = dict(user_filters)
    for k, v in datastore_filters(pushed, set(types)).items():
        filters.setdefault(k, v)
    if not state.frozen:
        state.filters, state.frozen = json.dumps(filters, sort_keys=True) if filters else "", True
    filters = json.loads(state.filters) if state.filters else {}
    wanted = list(params.output_schema.names)
    needed = list(dict.fromkeys([*wanted, *sorted(filter_columns(pushed) & set(types))]))
    hint = _sort_hint(params, types)
    limit = page_size if hint is None else min(page_size, max(hint[1] - state.matched, 1))
    query: dict[str, Any] = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": state.offset,
        "sort": hint[0] if hint else "_id asc",
        "filters": filters or None,
        "q": q or None,
        "fields": ",".join(needed) if needed else "_id",
    }
    result = client.call("datastore_search", query)
    records = list(result.get("records") or [])
    state.offset += len(records)
    total = result.get("total")
    if len(records) < limit or (isinstance(total, int) and state.offset >= total):
        state.done = True
    if not records:
        out.finish()
        return
    schema = params.output_schema
    rows = [{name: convert_value(types.get(name, "text"), rec.get(name)) for name in schema.names} for rec in records]
    batch = emit_rows(params, out, rows)
    if hint is not None and not state.done:
        passed = passing_rows(pushed, batch)
        if passed is not None:
            state.matched += passed
            state.done = state.matched >= hint[1]


@init_single_worker
class Datastore(TableFunctionGenerator[DatastoreArgs, DatastoreCursor]):
    """Read a DataStore table with typed columns and pushdown."""

    class Meta:
        """Function metadata."""

        name = "datastore"
        description = "Read the rows of a CKAN DataStore resource with typed columns, filters pushed to the portal"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = function_tags(
            title="DataStore Rows",
            category="datastore",
            keywords="ckan, datastore, datastore_search, table data, rows, csv data, records",
            doc_llm=(
                "datastore(portal, resource_id, filters := NULL, q := NULL, page_size := 1000) reads every row of a "
                "resource loaded into the portal's DataStore (resources().datastore_active is true). "
                + PORTAL_NOTE
                + " Columns are the table's own, typed from DataStore field types (int -> `BIGINT`, numeric -> "
                "`DOUBLE`, timestamp -> `TIMESTAMP`, date -> `DATE`, bool -> `BOOLEAN`, json -> `JSON`, arrays -> "
                "`LIST`, text -> `VARCHAR`), plus _id (row id). Many portals load CSVs as all-text columns: "
                "cast them in SQL. "
                "WHERE col = value / col IN (...) and the selected columns are sent to the portal; ranges and other "
                'predicates run locally. filters := \'{"col": "value"}\' adds remote equality filters; q := '
                "full-text search."
            ),
            doc_md=(
                "## datastore(portal, resource_id, filters :=, q :=, page_size := 1000)\n\n"
                "Rows of a DataStore table with real column types. Equality / IN filters and projections are sent "
                "to `datastore_search`."
            ),
            dynamic_md=(
                "Always present:\n\n"
                "| Name | Type | Description |\n|---|---|---|\n"
                "| _id | BIGINT | DataStore row id (1-based). |\n\n"
                "Then one column per field of the resource's DataStore table, named as on the portal and typed from "
                "its DataStore type: `BIGINT`, `DOUBLE`, `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `BOOLEAN`, `JSON`, a "
                "`LIST`, or `VARCHAR`."
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    'SELECT "Supplier Name", sum("Amount") AS total '
                    f"FROM {Q}.datastore('{IRELAND}', 'dfb0930a-98a5-45a0-a6b7-e4beb66340e5') "
                    "WHERE paid_yes_no = 'Y' GROUP BY ALL ORDER BY total DESC LIMIT 5"
                ),
                description="Largest paid suppliers in a data.gov.ie purchase-order table (filter sent to the portal)",
            ),
            FunctionExample(
                sql=(
                    'SELECT "Site", avg("Hs"::DOUBLE) AS mean_wave_height_m '
                    f"FROM {Q}.datastore('{QUEENSLAND}', '2bbef99e-9974-49b9-a316-57402b00609c', "
                    'filters := \'{"Site": ["Caloundra", "Mooloolaba"]}\') GROUP BY ALL ORDER BY 1'
                ),
                description="Mean significant wave height at two Queensland buoys",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DatastoreArgs]) -> BindResponse:
        """Describe the DataStore table (after DuckDB resolves the scoped secret)."""
        if not request_secret(params):
            return BindResponse(output_schema=pa.schema([]))
        _user_filters(params.args.filters)
        client = CkanClient(portal_of(params), bind_token(params))
        return BindResponse(output_schema=describe(client, params.args.resource_id))

    @classmethod
    def initial_state(cls, params: ProcessParams[DatastoreArgs]) -> DatastoreCursor:
        """Start at the first row."""
        return DatastoreCursor()

    @classmethod
    def process(cls, params: ProcessParams[DatastoreArgs], state: DatastoreCursor, out: OutputCollector) -> None:
        """Read one page."""
        a = params.args
        scan_datastore(
            params,
            state,
            out,
            resource_id=a.resource_id,
            user_filters=_user_filters(a.filters),
            q=a.q,
            page_size=a.page_size,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class SqlArgs:
    """Arguments for ``datastore_sql(portal, sql)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    sql: Annotated[
        str,
        Arg(
            1,
            doc="One read-only PostgreSQL query; each table is a resource id written as a double-quoted identifier.",
            pattern=r"(?i)^\s*(select|with)\b",
        ),
    ]


def _clean_sql(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


@init_single_worker
class DatastoreSql(TableFunctionGenerator[SqlArgs, DatastoreCursor]):
    """Run read-only SQL on the portal's DataStore (datastore_search_sql)."""

    class Meta:
        """Function metadata."""

        name = "datastore_sql"
        description = "Run a read-only PostgreSQL query on a CKAN portal's DataStore, where the portal enables it"
        projection_pushdown = True
        tags = function_tags(
            title="DataStore SQL",
            category="datastore",
            keywords="ckan, datastore_search_sql, sql, postgresql, aggregate, server-side query",
            doc_llm=(
                "datastore_sql(portal, sql) runs one read-only PostgreSQL statement on the portal's DataStore via "
                "datastore_search_sql and returns its rows with typed columns. In the statement each table is a "
                "resource id written as a double-quoted identifier. "
                + PORTAL_NOTE
                + " Only some portals enable it (ckan.datastore.sqlsearch.enabled; e.g. data.qld.gov.au); others "
                "raise 'not available'. The portal caps result size (ckan.datastore.search.rows_max, 32000 by "
                "default): when a result is truncated this raises, so aggregate server-side or page with an "
                "ORDER BY plus LIMIT and OFFSET in the statement. Use it to aggregate a big table on the portal "
                "instead of "
                "downloading it."
            ),
            doc_md=(
                "## datastore_sql(portal, sql)\n\n"
                "Read-only SQL on the portal's PostgreSQL DataStore, where enabled. Results larger than the portal's "
                "row cap raise instead of being silently truncated."
            ),
            dynamic_md=(
                "One column per item of the statement's select list, named as in the statement and typed from the "
                "PostgreSQL result type: `BIGINT`, `DOUBLE`, `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `BOOLEAN`, `JSON`, "
                "a `LIST`, or `VARCHAR`.\n\n"
                "Readings per buoy (the first example):\n\n"
                "| Name | Type | Description |\n|---|---|---|\n"
                "| Site | VARCHAR | Buoy name (a text column). |\n"
                "| readings | BIGINT | count(*) per site (int8). |\n\n"
                "Latest reading (the second example, a timestamp cast):\n\n"
                "| Name | Type | Description |\n|---|---|---|\n"
                "| ts | TIMESTAMP | The text DateTime column cast to timestamp. |"
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT \"Site\", readings FROM {Q}.datastore_sql('{QUEENSLAND}', "
                    '\'SELECT "Site", count(*) AS readings FROM "2bbef99e-9974-49b9-a316-57402b00609c" '
                    "GROUP BY 1') ORDER BY readings DESC LIMIT 5"
                ),
                description="Readings per wave buoy, counted by the Queensland portal's PostgreSQL",
            ),
            FunctionExample(
                sql=(
                    f"SELECT max(ts) AS latest FROM {Q}.datastore_sql('{QUEENSLAND}', "
                    '\'SELECT "DateTime"::timestamp AS ts FROM "2bbef99e-9974-49b9-a316-57402b00609c" '
                    "WHERE \"Site\" = ''Caloundra'' ')"
                ),
                description="Latest reading time at the Caloundra buoy, as a typed TIMESTAMP",
            ),
        ]

    @classmethod
    def call_sql(cls, client: CkanClient, sql: str) -> Any:
        """Call datastore_search_sql with a helpful error when the portal does not offer it."""
        try:
            return client.call("datastore_search_sql", {"sql": sql})
        except ActionNotAvailableError as err:
            raise CkanError(
                f"{client.portal} does not enable datastore_search_sql (ckan.datastore.sqlsearch.enabled); "
                "use datastore() instead"
            ) from err

    @classmethod
    def on_bind(cls, params: BindParams[SqlArgs]) -> BindResponse:
        """Learn the result types by running the query with ``LIMIT 0``."""
        if not request_secret(params):
            return BindResponse(output_schema=pa.schema([]))
        client = CkanClient(portal_of(params), bind_token(params))
        sql = _clean_sql(params.args.sql)
        result = cls.call_sql(client, f"SELECT * FROM ({sql}) AS _vgi_ckan LIMIT 0")
        schema = schema_from_fields(list(result.get("fields") or []))
        if not len(schema):
            raise CkanError("datastore_sql: the query returns no columns")
        return BindResponse(output_schema=schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[SqlArgs]) -> DatastoreCursor:
        """Not run yet."""
        return DatastoreCursor()

    @classmethod
    def process(cls, params: ProcessParams[SqlArgs], state: DatastoreCursor, out: OutputCollector) -> None:
        """Run the query once and emit its rows."""
        if state.done:
            out.finish()
            return
        state.done = True
        client = client_for(params)
        result = cls.call_sql(client, _clean_sql(params.args.sql))
        if result.get("records_truncated"):
            raise CkanError(
                "datastore_sql: the result was truncated by the portal's row limit; aggregate on the portal or "
                "page with ORDER BY ... LIMIT n OFFSET m in the SQL"
            )
        bound = params.init_call.output_schema if params.init_call is not None else params.output_schema
        types = field_types(bound)
        records = list(result.get("records") or [])
        if not records:
            out.finish()
            return
        names = params.output_schema.names
        emit_rows(params, out, [{n: convert_value(types.get(n, "text"), r.get(n)) for n in names} for r in records])


LINK_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("resource_id", pa.string(), "Resource id.", nullable=False),
        column("dataset_id", pa.string(), "Id of the dataset the resource belongs to."),
        column("title", pa.string(), "Resource name."),
        column("format", pa.string(), "Declared format, e.g. 'CSV'."),
        column("url", pa.string(), "Download URL: read it with read_csv / read_parquet / read_json."),
        column(
            "read_sql", pa.string(), "A DuckDB query that reads the file, chosen from the format (NULL if unknown)."
        ),
        column("datastore_active", pa.bool_(), "False: the resource is not in DataStore, read it by URL."),
    ]
)

READERS = {
    "csv": "read_csv",
    "tsv": "read_csv",
    "txt": "read_csv",
    "parquet": "read_parquet",
    "json": "read_json_auto",
    "geojson": "read_json_auto",
    "ndjson": "read_json_auto",
    "jsonl": "read_json_auto",
    "xlsx": "read_xlsx",
}


def read_sql(fmt: str | None, url: str | None) -> str | None:
    """Return the DuckDB call that reads a downloadable file of this format, or None."""
    if not url:
        return None
    key = (fmt or "").strip().lower().lstrip(".")
    if not key:
        key = url.rsplit("?", 1)[0].rsplit(".", 1)[-1].lower()
    reader = READERS.get(key)
    if reader is None:
        return None
    return f"SELECT * FROM {reader}('{url.replace(chr(39), chr(39) * 2)}')"


@dataclass(slots=True, frozen=True, kw_only=True)
class ResourceArgs:
    """Arguments for ``resource_data(portal, resource_id)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    resource_id: Annotated[str, Arg(1, doc="Id of the resource to read.", pattern=r"\S")]
    page_size: Annotated[
        int, Arg("page_size", default=1000, doc="DataStore rows per request, 1-32000.", ge=1, le=32000)
    ]


@init_single_worker
class ResourceData(TableFunctionGenerator[ResourceArgs, DatastoreCursor]):
    """DataStore rows when available, else the resource's download link."""

    class Meta:
        """Function metadata."""

        name = "resource_data"
        description = "Read a CKAN resource: DataStore rows when loaded, otherwise its download URL for read_csv"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = function_tags(
            title="Resource Data",
            category="datastore",
            keywords="ckan, resource, download, read_csv, read_parquet, datastore, file url",
            doc_llm=(
                "resource_data(portal, resource_id, page_size := 1000): if the resource is loaded in DataStore "
                "it returns the same typed rows as datastore(portal, resource_id); otherwise it returns ONE row "
                "(portal, resource_id, dataset_id, title, format, url, read_sql, datastore_active=false) where url "
                "is the download link and read_sql a ready DuckDB query calling read_csv, read_parquet or "
                "read_json_auto on it. The worker does not parse files itself: run DuckDB's reader on the url. "
                + PORTAL_NOTE
            ),
            doc_md=(
                "## resource_data(portal, resource_id)\n\n"
                "DataStore rows, or for plain files one row with the `url` and a `read_sql` query that loads it "
                "with DuckDB's `read_csv` / `read_parquet` / `read_json_auto`."
            ),
            dynamic_md=(
                "Two shapes, chosen when the query is planned.\n\n"
                "Resource in DataStore (same as datastore()):\n\n"
                "| Name | Type | Description |\n|---|---|---|\n"
                "| _id | BIGINT | DataStore row id. |\n\n"
                "followed by one typed column per DataStore field.\n\n"
                "Plain file:\n\n"
                "| Name | Type | Description |\n|---|---|---|\n"
                "| portal | VARCHAR | Base URL of the portal. |\n"
                "| resource_id | VARCHAR | Resource id. |\n"
                "| dataset_id | VARCHAR | Dataset the resource belongs to. |\n"
                "| title | VARCHAR | Resource name. |\n"
                "| format | VARCHAR | Declared format. |\n"
                "| url | VARCHAR | Download URL. |\n"
                "| read_sql | VARCHAR | DuckDB query that loads the URL (NULL when no reader fits). |\n"
                "| datastore_active | BOOLEAN | Always false in this shape. |"
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    'SELECT count(*) AS orders, sum("Amount") AS total '
                    f"FROM {Q}.resource_data('{IRELAND}', 'dfb0930a-98a5-45a0-a6b7-e4beb66340e5')"
                ),
                description="A DataStore-backed resource reads as typed rows",
            ),
            FunctionExample(
                sql=(
                    f"SELECT format, url, read_sql FROM {Q}.resource_data('https://demo.ckan.org', "
                    "'73127990-4645-4de7-a5f0-f016d028d984')"
                ),
                description="A plain file returns its URL and the read_csv call to load it",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ResourceArgs]) -> BindResponse:
        """DataStore schema when loaded there, else the link schema."""
        if not request_secret(params):
            return BindResponse(output_schema=pa.schema([]))
        client = CkanClient(portal_of(params), bind_token(params))
        res = _resource(client, params.args.resource_id)
        if cv.boolean(res.get("datastore_active")):
            return BindResponse(output_schema=describe(client, params.args.resource_id))
        return BindResponse(output_schema=LINK_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ResourceArgs]) -> DatastoreCursor:
        """Start at the first row."""
        return DatastoreCursor()

    @classmethod
    def process(cls, params: ProcessParams[ResourceArgs], state: DatastoreCursor, out: OutputCollector) -> None:
        """Page DataStore rows, or emit the single link row."""
        bound = params.init_call.output_schema if params.init_call is not None else params.output_schema
        if "read_sql" not in bound.names or "_id" in bound.names:
            scan_datastore(
                params,
                state,
                out,
                resource_id=params.args.resource_id,
                user_filters={},
                q=None,
                page_size=params.args.page_size,
            )
            return
        if state.done:
            out.finish()
            return
        state.done = True
        client = client_for(params)
        res = _resource(client, params.args.resource_id)
        url = cv.blank_to_null(res.get("url"))
        fmt = cv.blank_to_null(res.get("format"))
        emit_rows(
            params,
            out,
            [
                {
                    "portal": client.portal,
                    "resource_id": res.get("id") or params.args.resource_id,
                    "dataset_id": res.get("package_id"),
                    "title": cv.translated(res, "name"),
                    "format": fmt,
                    "url": url,
                    "read_sql": read_sql(fmt, url),
                    "datastore_active": False,
                }
            ],
        )


_FORMAT_DOC = "The resource's declared format, e.g. 'CSV' or 'Parquet'; blank falls back to the URL's extension."
_URL_DOC = "The resource's download URL (resources().url)."


class ReadSql(ScalarFunction):
    """``read_sql(resource_format, url)``: the DuckDB query that reads a downloadable resource."""

    class Meta:
        """Function metadata."""

        name = "read_sql"
        description = "Build the DuckDB query (read_csv / read_parquet / read_json_auto) that loads a resource URL"
        tags = {
            **function_tags(
                title="Reader Query For A Resource",
                category="datastore",
                keywords="read_csv, read_parquet, read_json, download, file reader, resource url, load file",
                doc_llm=(
                    "read_sql(resource_format, url) returns the DuckDB query text that loads a downloadable CKAN "
                    "resource: "
                    "read_csv for CSV/TSV/TXT, read_parquet for Parquet, read_json_auto for JSON/GeoJSON/NDJSON, "
                    "read_xlsx for XLSX; NULL for formats DuckDB cannot read (PDF, HTML, ZIP, ...) or a NULL url. "
                    "The format is matched case-insensitively; a blank format falls back to the URL's file "
                    "extension. Use it over resources() rows to pick what to load, then run the query."
                ),
                doc_md=(
                    "## read_sql(resource_format, url)\n\n"
                    "The `read_csv` / `read_parquet` / `read_json_auto` query for a resource, or NULL when DuckDB "
                    "has no reader for the format."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The reader query for a CSV resource",
                        "sql": "SELECT ckan.main.read_sql('CSV', 'https://example.org/data.csv') AS q",
                        "expected_result": [["SELECT * FROM read_csv('https://example.org/data.csv')"]],
                    },
                    {
                        "description": "Formats DuckDB can and cannot read",
                        "sql": (
                            "SELECT f, ckan.main.read_sql(f, 'https://example.org/file') IS NOT NULL AS readable "
                            "FROM (VALUES ('Parquet'), ('GeoJSON'), ('PDF')) AS t(f) ORDER BY f"
                        ),
                        "expected_result": [["GeoJSON", "true"], ["PDF", "false"], ["Parquet", "true"]],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT title, {Q}.read_sql(format, url) AS query FROM {Q}.resources('https://demo.ckan.org') "
                    f"WHERE {Q}.read_sql(format, url) IS NOT NULL ORDER BY title LIMIT 5"
                ),
                description="Reader queries for the loadable files on the CKAN demo portal",
            ),
            FunctionExample(
                sql="SELECT ckan.main.read_sql('', 'https://example.org/export.parquet?download=1') AS q",
                description="A blank format falls back to the URL's extension",
            ),
        ]

    @classmethod
    def compute(
        cls,
        resource_format: Annotated[pa.StringArray, Param(doc=_FORMAT_DOC)],
        url: Annotated[pa.StringArray, Param(doc=_URL_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return the reader query for every row (NULL when there is none)."""
        return pa.array(
            [read_sql(f, u) for f, u in zip(resource_format.to_pylist(), url.to_pylist(), strict=True)],
            type=pa.string(),
        )
