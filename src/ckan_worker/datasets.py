"""Catalog functions over ``package_search`` / ``package_show``: datasets, dataset, resources, tags, facets.

Pushdown into ``package_search``:

* SQL predicates on ``organization``, ``license_id``, ``type``, ``private``,
  ``dataset_id``, ``name`` (resources: ``format``, ``dataset_id``,
  ``dataset_name``) become Solr ``fq`` clauses; ``created`` / ``modified``
  ranges become ``metadata_created`` / ``metadata_modified`` range queries.
* ``q :=`` is free text, ``fq :=`` a raw Solr filter ANDed with the above,
  ``tag :=`` / ``group_name :=`` / ``res_format :=`` shortcuts for list fields.
* Projection: when every column a query needs is stored in the search index,
  the request asks only for those fields (``fl``), which is many times smaller
  than full dataset dictionaries.
* ``ORDER BY modified|created|name|dataset_id ... LIMIT n``: DuckDB's top-N hint
  becomes the Solr ``sort``, and the scan stops once n matching rows are out.

Paging uses ``rows``/``start`` with a stable ``id asc`` sort; the cursor is
externalized (``start``) so scans resume on any worker over HTTP. ``rows`` is
clamped by the portal (``ckan.search.rows_max``, 1000 by default), so the
cursor advances by the rows actually returned.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, TypeVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.protocol import OrderByDirection
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import convert as cv
from .client import CkanClient, NotFoundError
from .common import PORTAL_DOC, client_for, emit_rows, request_secret
from .meta import CANADA, DEMO, IRELAND, PORTAL_NOTE, Q, column, function_tags
from .pushdown import SolrField, filter_columns, passing_rows, solr_fq

JSON = pa.json_(pa.string())
TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())

DATASET_FIELDS: list[pa.Field[Any]] = [
    column("portal", pa.string(), "Base URL of the CKAN portal the row came from.", nullable=False),
    column("dataset_id", pa.string(), "Dataset (package) id.", nullable=False),
    column("name", pa.string(), "Dataset URL slug (unique per portal).", nullable=False),
    column("title", pa.string(), "Dataset title (the portal locale's translation on multilingual portals)."),
    column("description", pa.string(), "Dataset description (CKAN 'notes'), usually Markdown."),
    column("organization", pa.string(), "Publishing organization's slug (name)."),
    column("organization_title", pa.string(), "Publishing organization's display title."),
    column("groups", STR_LIST, "Slugs of the groups (themes) the dataset belongs to."),
    column("tags", STR_LIST, "Tag names (keywords)."),
    column("formats", STR_LIST, "Distinct formats of the dataset's resources, sorted, e.g. ['CSV', 'JSON']."),
    column("license_id", pa.string(), "License id, e.g. 'cc-by', 'CC-BY-4.0', 'uk-ogl' (see licenses())."),
    column("license_title", pa.string(), "License display title."),
    column("num_resources", pa.int32(), "Number of resources (files / APIs) in the dataset."),
    column("private", pa.bool_(), "True for private datasets (visible only with an API token)."),
    column("state", pa.string(), "Lifecycle state: 'active' for published datasets."),
    column("type", pa.string(), "Dataset type: 'dataset' unless the portal defines custom types."),
    column("author", pa.string(), "Author named in the metadata."),
    column("maintainer", pa.string(), "Maintainer named in the metadata."),
    column("version", pa.string(), "Version string from the metadata."),
    column("source_url", pa.string(), "The dataset's own 'url' field: a link to the source or homepage."),
    column("url", pa.string(), "The dataset's page on the portal: <portal>/dataset/<name>."),
    column("created", TSTZ, "When the dataset record was created on the portal (metadata_created, UTC)."),
    column("modified", TSTZ, "When the dataset record last changed (metadata_modified, UTC)."),
    column("extras", pa.map_(pa.string(), pa.string()), "Custom key/value metadata ('extras')."),
    column("raw", JSON, "The full dataset dictionary as returned by the portal (portal-specific fields)."),
]
DATASET_SCHEMA = pa.schema(DATASET_FIELDS)

RESOURCE_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal the row came from.", nullable=False),
        column("resource_id", pa.string(), "Resource id (use it with datastore()).", nullable=False),
        column("dataset_id", pa.string(), "Id of the dataset the resource belongs to.", nullable=False),
        column("dataset_name", pa.string(), "Slug of the dataset the resource belongs to."),
        column("dataset_title", pa.string(), "Title of the dataset the resource belongs to."),
        column("organization", pa.string(), "Publishing organization's slug."),
        column("title", pa.string(), "Resource name (its title on the portal)."),
        column("description", pa.string(), "Resource description."),
        column(
            "format", pa.string(), "Declared format, e.g. 'CSV', 'JSON', 'XLSX', 'API' (as typed by the publisher)."
        ),
        column("mimetype", pa.string(), "MIME type, when the portal records one."),
        column("url", pa.string(), "Download or access URL: pass it to read_csv / read_parquet / read_json."),
        column("size", pa.int64(), "Size in bytes when the portal records a number."),
        column("datastore_active", pa.bool_(), "True when the rows are loaded into DataStore (read with datastore())."),
        column("position", pa.int32(), "Order of the resource within its dataset (0-based)."),
        column("resource_type", pa.string(), "Resource type, e.g. 'file', 'api', 'documentation'."),
        column("url_type", pa.string(), "'upload' for files stored on the portal, NULL/empty for links."),
        column("state", pa.string(), "Lifecycle state: 'active'."),
        column("created", TSTZ, "When the resource was created (UTC)."),
        column("modified", TSTZ, "When the resource's data last changed (last_modified, else metadata_modified, UTC)."),
        column("dataset_url", pa.string(), "The dataset's page on the portal."),
        column("raw", JSON, "The full resource dictionary as returned by the portal."),
    ]
)

FACET_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("field", pa.string(), "The faceted search field, e.g. 'res_format'.", nullable=False),
        column("value", pa.string(), "One value of the field."),
        column("display_name", pa.string(), "The value's display name (e.g. an organization's title)."),
        column("dataset_count", pa.int64(), "Number of matching datasets with this value.", nullable=False),
    ]
)
TAG_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("tag", pa.string(), "Tag name.", nullable=False),
        column("dataset_count", pa.int64(), "Number of matching datasets carrying the tag.", nullable=False),
    ]
)

# SQL column -> Solr field, for fq pushdown.
DATASET_SOLR: dict[str, SolrField] = {
    "dataset_id": SolrField("id"),
    "name": SolrField("name"),
    "organization": SolrField("organization"),
    "license_id": SolrField("license_id"),
    "state": SolrField("state"),
    "type": SolrField("dataset_type"),
    "private": SolrField("capacity", convert=lambda v: "private" if v else "public"),
    "num_resources": SolrField("num_resources", ranged=True),
    "created": SolrField("metadata_created", ranged=True),
    "modified": SolrField("metadata_modified", ranged=True),
}
RESOURCE_SOLR: dict[str, SolrField] = {
    "dataset_id": SolrField("id"),
    "dataset_name": SolrField("name"),
    "organization": SolrField("organization"),
    "format": SolrField("res_format"),
}
# Columns that can be served from the search index alone (``fl``), and the fields they need.
FL_FIELDS: dict[str, tuple[str, ...]] = {
    "portal": (),
    "dataset_id": ("id",),
    "name": ("name",),
    "url": ("name",),
    "title": ("title",),
    "organization": ("organization",),
    "groups": ("groups",),
    "tags": ("tags",),
    "formats": ("res_format",),
    "license_id": ("license_id",),
    "state": ("state",),
    "type": ("dataset_type",),
    "private": ("capacity",),
    "created": ("metadata_created",),
    "modified": ("metadata_modified",),
}
SORTABLE = {"modified": "metadata_modified", "created": "metadata_created", "name": "name", "dataset_id": "id"}


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """Arguments for ``datasets`` / ``resources``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    q: Annotated[
        str | None, Arg("q", default=None, doc="Free-text search (Solr syntax), e.g. 'air quality' or 'title:budget'.")
    ]
    fq: Annotated[
        str | None,
        Arg(
            "fq",
            default=None,
            doc="Raw Solr filter query ANDed with the pushed-down filters, e.g. 'num_tags:[5 TO *]'.",
        ),
    ]
    tag: Annotated[str | None, Arg("tag", default=None, doc="Only datasets with this tag.")]
    group_name: Annotated[str | None, Arg("group_name", default=None, doc="Only datasets in this group (slug).")]
    res_format: Annotated[
        str | None, Arg("res_format", default=None, doc="Only datasets with a resource of this format, e.g. 'CSV'.")
    ]
    page_size: Annotated[
        int,
        Arg(
            "page_size",
            default=100,
            doc="Datasets per request, 1-1000 (portals cap it, 1000 by default).",
            ge=1,
            le=1000,
        ),
    ]


@dataclass(kw_only=True)
class SearchCursor(ArrowSerializableDataclass):
    """Resumable position in a package_search listing."""

    start: int = 0
    matched: int = 0
    done: bool = False
    # The fq sent with the first page. DuckDB tightens dynamic filters (top-N,
    # join keys) mid-scan; changing fq would shift the result set under
    # ``start``, so later pages reuse it and newer filters apply locally.
    fq: str = ""
    frozen: bool = False


def dataset_row(portal: str, p: dict[str, Any], *, full: bool = True) -> dict[str, Any]:
    """One package dict (full, or an ``fl`` subset) as a ``datasets`` row."""
    org = p.get("organization")
    org_name = org.get("name") if isinstance(org, dict) else org
    resources = p.get("resources") if isinstance(p.get("resources"), list) else None
    if resources is not None:
        formats = cv.distinct_sorted([str(r.get("format") or "") for r in resources])
    else:
        formats = cv.distinct_sorted([str(f) for f in p.get("res_format") or []])
    private = p.get("private")
    if private is None and p.get("capacity") is not None:
        private = p.get("capacity") == "private"
    name = str(p.get("name") or p.get("id") or "")
    return {
        "portal": portal,
        "dataset_id": p.get("id"),
        "name": name,
        "title": cv.translated(p, "title"),
        "description": cv.translated(p, "notes"),
        "organization": cv.blank_to_null(org_name),
        "organization_title": cv.blank_to_null(org.get("title")) if isinstance(org, dict) else None,
        "groups": cv.names(p.get("groups")),
        "tags": cv.names(p.get("tags")),
        "formats": formats,
        "license_id": cv.blank_to_null(p.get("license_id")),
        "license_title": cv.blank_to_null(p.get("license_title")),
        "num_resources": len(resources) if resources is not None else cv.integer(p.get("num_resources")),
        "private": cv.boolean(private),
        "state": p.get("state"),
        "type": p.get("type") or p.get("dataset_type"),
        "author": cv.blank_to_null(p.get("author")),
        "maintainer": cv.blank_to_null(p.get("maintainer")),
        "version": cv.blank_to_null(p.get("version")),
        "source_url": cv.blank_to_null(p.get("url")),
        "url": f"{portal}/dataset/{name}",
        "created": cv.ts_ms(p.get("metadata_created")),
        "modified": cv.ts_ms(p.get("metadata_modified")),
        "extras": cv.extras_map(p.get("extras")),
        "raw": cv.json_text(p) if full else None,
    }


def resource_rows(portal: str, p: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the resources of one full package dict as ``resources`` rows."""
    org = p.get("organization")
    name = str(p.get("name") or p.get("id") or "")
    out = []
    for i, r in enumerate(p.get("resources") or []):
        out.append(
            {
                "portal": portal,
                "resource_id": r.get("id"),
                "dataset_id": p.get("id"),
                "dataset_name": name,
                "dataset_title": cv.translated(p, "title"),
                "organization": cv.blank_to_null(org.get("name") if isinstance(org, dict) else org),
                "title": cv.translated(r, "name"),
                "description": cv.translated(r, "description"),
                "format": cv.blank_to_null(r.get("format")),
                "mimetype": cv.blank_to_null(r.get("mimetype")),
                "url": cv.blank_to_null(r.get("url")),
                "size": cv.integer(r.get("size")),
                "datastore_active": bool(cv.boolean(r.get("datastore_active"))),
                "position": cv.integer(r.get("position")) if r.get("position") is not None else i,
                "resource_type": cv.blank_to_null(r.get("resource_type")),
                "url_type": cv.blank_to_null(r.get("url_type")),
                "state": r.get("state"),
                "created": cv.ts(r.get("created")),
                "modified": cv.ts(r.get("last_modified")) or cv.ts(r.get("metadata_modified")),
                "dataset_url": f"{portal}/dataset/{name}",
                "raw": cv.json_text(r),
            }
        )
    return out


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def search_params(
    args: Any, filters: Any, solr: dict[str, SolrField], *, token: bool, fl: list[str] | None
) -> dict[str, Any]:
    """Return the constant part of a package_search request: q, fq, fl, include_private."""
    clauses = solr_fq(filters, solr)
    if getattr(args, "fq", None):
        clauses.insert(0, f"({args.fq})")
    if getattr(args, "tag", None):
        clauses.append(f"tags:{_quote(args.tag)}")
    if getattr(args, "group_name", None):
        clauses.append(f"groups:{_quote(args.group_name)}")
    if getattr(args, "res_format", None):
        clauses.append(f"res_format:{_quote(args.res_format)}")
    params: dict[str, Any] = {
        "q": args.q or None,
        "fq": " AND ".join(clauses) if clauses else None,
        "fl": ",".join(fl) if fl else None,
    }
    if token:
        params["include_private"] = True
    return params


def fl_for(needed: set[str]) -> list[str] | None:
    """Return the ``fl`` field list when every needed column is in the index, else None (full dicts)."""
    if not needed <= set(FL_FIELDS):
        return None
    fields = {"id", "name"}
    for col in needed:
        fields.update(FL_FIELDS[col])
    return sorted(fields)


def order_hint(params: ProcessParams[Any], sortable: dict[str, str]) -> tuple[str, int] | None:
    """``(solr sort, limit)`` from DuckDB's top-N hint, when it maps onto an indexed field."""
    init = params.init_call
    if init is None or not init.order_by_column_name or not init.order_by_limit:
        return None
    field = sortable.get(init.order_by_column_name)
    if field is None:
        return None
    direction = "desc" if init.order_by_direction == OrderByDirection.DESC else "asc"
    return f"{field} {direction}, id asc", int(init.order_by_limit)


class _SearchBase(TableFunctionGenerator[SearchArgs, SearchCursor]):
    """Shared paging over package_search."""

    SOLR: ClassVar[dict[str, SolrField]] = DATASET_SOLR
    SORTABLE: ClassVar[dict[str, str]] = SORTABLE
    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASET_SCHEMA

    @classmethod
    def on_bind(cls, params: BindParams[SearchArgs]) -> BindResponse:
        """Validate the portal and ask DuckDB for the portal-scoped ``ckan`` secret."""
        request_secret(params)
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchArgs]) -> SearchCursor:
        """Start at the first row."""
        return SearchCursor()

    @classmethod
    def fields(cls, params: ProcessParams[SearchArgs]) -> list[str] | None:
        """Projection: the ``fl`` list, or None for full dictionaries."""
        return None

    @classmethod
    def rows(cls, portal: str, results: list[dict[str, Any]], full: bool) -> list[dict[str, Any]]:
        """Map one page of package dicts to output rows."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[SearchArgs], state: SearchCursor, out: OutputCollector) -> None:
        """Fetch one page, emit it and advance the cursor."""
        if state.done:
            out.finish()
            return
        client = client_for(params)
        filters = params.current_pushdown_filters
        fl = cls.fields(params)
        query = search_params(params.args, filters, cls.SOLR, token=bool(client.token), fl=fl)
        if not state.frozen:
            state.fq, state.frozen = query["fq"] or "", True
        query["fq"] = state.fq or None
        hint = order_hint(params, cls.SORTABLE)
        rows_wanted = params.args.page_size
        query["sort"] = "id asc"
        if hint is not None:
            query["sort"] = hint[0]
            rows_wanted = min(rows_wanted, max(hint[1] - state.matched, 1))
        query.update({"rows": rows_wanted, "start": state.start})
        result = client.call("package_search", query)
        results = list(result.get("results") or [])
        count = int(result.get("count") or 0)
        state.start += len(results)
        if not results or state.start >= count:
            state.done = True
        if results:
            batch = emit_rows(params, out, cls.rows(client.portal, results, fl is None))
            if hint is not None and not state.done:
                passed = passing_rows(filters, batch)
                if passed is not None:
                    state.matched += passed
                    if state.matched >= hint[1]:
                        state.done = True
            return
        out.finish()


@init_single_worker
class Datasets(_SearchBase):
    """Search a portal's datasets (package_search) with filter, projection and top-N pushdown."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASET_SCHEMA

    class Meta:
        """Function metadata."""

        name = "datasets"
        description = "Search and list a CKAN portal's datasets, with filters pushed into the portal's search index"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = function_tags(
            title="Datasets",
            category="catalog",
            keywords="ckan, datasets, packages, package_search, open data, catalog search, metadata",
            doc_llm=(
                "datasets(portal, q := NULL, fq := NULL, tag := NULL, group_name := NULL, res_format := NULL, "
                "page_size := 100) lists every dataset (CKAN package) of the portal matching the search, one row "
                "each. " + PORTAL_NOTE + " q is free text in Solr syntax; fq a raw Solr filter. WHERE filters on "
                "organization, license_id, type, private, name, dataset_id, num_resources and created/modified "
                "ranges are pushed into the portal's search index, so filter there rather than scanning a large "
                "portal. tags/groups/formats are lists: use tag :=, group_name := or res_format := to filter on them "
                "remotely (list_contains() works too but is evaluated locally). Columns: portal, dataset_id, name, "
                "title, description, organization, organization_title, groups, tags, formats, license_id, "
                "license_title, num_resources, private, state, type, author, maintainer, version, source_url, url "
                "(dataset page), created, modified (`TIMESTAMPTZ`), extras (`MAP`), raw (`JSON` of the portal's full "
                "record, including portal-specific fields such as title_translated)."
            ),
            doc_md=(
                "## datasets(portal, q :=, fq :=, tag :=, group_name :=, res_format :=, page_size := 100)\n\n"
                "Every dataset matching the search. Filters on `organization`, `license_id`, `type`, `private`, "
                "`created` and `modified` are sent to the portal's Solr index; selecting only indexed columns "
                "(`name`, `title`, `organization`, `tags`, `formats`, `modified`, ...) requests just those fields."
            ),
            schema=DATASET_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT organization, count(*) AS datasets FROM {Q}.datasets('{DEMO}') "
                    "GROUP BY organization ORDER BY datasets DESC, organization"
                ),
                description="Datasets per publishing organization on the CKAN demo portal",
            ),
            FunctionExample(
                sql=(
                    f"SELECT name, title, modified FROM {Q}.datasets('{IRELAND}', q := 'air quality') "
                    "WHERE organization = 'environmental-protection-agency' "
                    "AND modified >= TIMESTAMPTZ '2025-01-01' ORDER BY modified DESC LIMIT 10"
                ),
                description="Recently updated EPA air-quality datasets on data.gov.ie (filters run in the portal)",
            ),
        ]

    @classmethod
    def fields(cls, params: ProcessParams[SearchArgs]) -> list[str] | None:
        """Use ``fl`` when the query needs only indexed columns."""
        needed = set(params.output_schema.names) | filter_columns(params.current_pushdown_filters)
        return fl_for(needed)

    @classmethod
    def rows(cls, portal: str, results: list[dict[str, Any]], full: bool) -> list[dict[str, Any]]:
        """Map package dicts to dataset rows."""
        return [dataset_row(portal, p, full=full) for p in results]


@init_single_worker
class Resources(_SearchBase):
    """Every resource of every matching dataset, one row per resource."""

    SOLR: ClassVar[dict[str, SolrField]] = RESOURCE_SOLR
    # Resource rows come out grouped by dataset; only dataset keys order them.
    SORTABLE: ClassVar[dict[str, str]] = {"dataset_id": "id", "dataset_name": "name"}
    FIXED_SCHEMA: ClassVar[pa.Schema] = RESOURCE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "resources"
        description = "List the resources (files, APIs) of a CKAN portal's datasets, one row per resource"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = function_tags(
            title="Resources",
            category="catalog",
            keywords="ckan, resources, files, downloads, csv, distribution, formats, datastore",
            doc_llm=(
                "resources(portal, q := NULL, fq := NULL, tag := NULL, group_name := NULL, res_format := NULL, "
                "page_size := 100) flattens the resources (files, links, APIs) of every dataset matching the search "
                "into one row each. " + PORTAL_NOTE + " WHERE filters on format, organization, dataset_id and "
                "dataset_name narrow the dataset search remotely. Columns: portal, resource_id, dataset_id, "
                "dataset_name, dataset_title, organization, title, description, format, mimetype, url (download "
                "link: read it with read_csv/read_parquet/read_json), size, datastore_active (true -> read rows "
                "with datastore(portal, resource_id)), position, resource_type, url_type, state, created, modified, "
                "dataset_url, raw (JSON)."
            ),
            doc_md=(
                "## resources(portal, q :=, fq :=, tag :=, group_name :=, res_format :=, page_size := 100)\n\n"
                "One row per resource. `format` filters are pushed to the search index. Resources with "
                "`datastore_active` can be read with `datastore()`; others by passing `url` to `read_csv`, "
                "`read_parquet` or `read_json` (the `read_sql` scalar builds that call)."
            ),
            schema=RESOURCE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT format, count(*) AS resources FROM {Q}.resources('{DEMO}') "
                    "GROUP BY format ORDER BY resources DESC, format"
                ),
                description="Resource formats on the CKAN demo portal",
            ),
            FunctionExample(
                sql=(
                    f"SELECT dataset_name, title, url FROM {Q}.resources('{IRELAND}', q := 'purchase orders') "
                    "WHERE format = 'CSV' AND datastore_active ORDER BY dataset_name, position LIMIT 20"
                ),
                description="CSV purchase-order files on data.gov.ie that are also queryable in DataStore",
            ),
        ]

    @classmethod
    def rows(cls, portal: str, results: list[dict[str, Any]], full: bool) -> list[dict[str, Any]]:
        """Flatten each package's resources."""
        return [row for p in results for row in resource_rows(portal, p)]


@dataclass(slots=True, frozen=True, kw_only=True)
class DatasetArgs:
    """Arguments for ``dataset(portal, id)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    id: Annotated[str, Arg(1, doc="Dataset id or name (its URL slug).", pattern=r"\S")]


@dataclass(kw_only=True)
class OnceState(ArrowSerializableDataclass):
    """Single-request functions: done after the first batch."""

    done: bool = False


@init_single_worker
class Dataset(TableFunctionGenerator[DatasetArgs, OnceState]):
    """One dataset by id or name (package_show); zero rows when it does not exist."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASET_SCHEMA

    class Meta:
        """Function metadata."""

        name = "dataset"
        description = "One CKAN dataset by id or name, with the same columns as datasets()"
        projection_pushdown = True
        tags = function_tags(
            title="Dataset Details",
            category="catalog",
            keywords="ckan, dataset, package_show, dataset details, metadata, lookup",
            doc_llm=(
                "dataset(portal, id) returns the one dataset whose id or name (slug) is given, via "
                "package_show, with the same columns as datasets(); no rows when it does not exist or is not "
                "visible. " + PORTAL_NOTE + " Use raw (JSON) for portal-specific fields."
            ),
            doc_md="## dataset(portal, id)\n\nOne dataset by id or slug (`package_show`). Same columns as `datasets`.",
            schema=DATASET_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT title, organization, license_id, num_resources, modified "
                    f"FROM {Q}.dataset('{IRELAND}', 'courts-service-purchase-orders-greater-than-20-000-2026')"
                ),
                description="Metadata of one data.gov.ie dataset",
            ),
            FunctionExample(
                sql=(
                    f"SELECT title, organization, unnest(formats) AS format FROM {Q}.dataset('{CANADA}', "
                    "'ee9bd7e8-90a5-45db-9287-85c8cf3589b6') ORDER BY format"
                ),
                description="Resource formats of one open.canada.ca dataset, looked up by id",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DatasetArgs]) -> BindResponse:
        """Ask DuckDB for the portal-scoped secret."""
        request_secret(params)
        return BindResponse(output_schema=DATASET_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[DatasetArgs]) -> OnceState:
        """Not fetched yet."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[DatasetArgs], state: OnceState, out: OutputCollector) -> None:
        """Fetch and emit the dataset."""
        if state.done:
            out.finish()
            return
        state.done = True
        client = client_for(params)
        try:
            package = client.call("package_show", {"id": params.args.id})
        except NotFoundError:
            out.finish()
            return
        emit_rows(params, out, [dataset_row(client.portal, package)])


@dataclass(slots=True, frozen=True, kw_only=True)
class FacetArgs:
    """Arguments for ``facets(portal, field, ...)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    field: Annotated[
        str,
        Arg(
            1,
            doc="Search field to count: organization, groups, tags, res_format, license_id, or any indexed field.",
            pattern=r"^[A-Za-z0-9_]+$",
        ),
    ]
    q: Annotated[str | None, Arg("q", default=None, doc="Free-text search restricting the datasets counted.")]
    fq: Annotated[str | None, Arg("fq", default=None, doc="Raw Solr filter restricting the datasets counted.")]
    max_values: Annotated[
        int,
        Arg("max_values", default=1000, doc="Most values to return (largest counts first), 1-100000.", ge=1, le=100000),
    ]


@dataclass(slots=True, frozen=True, kw_only=True)
class TagArgs:
    """Arguments for ``tags(portal, ...)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    q: Annotated[str | None, Arg("q", default=None, doc="Free-text search restricting the datasets counted.")]
    fq: Annotated[str | None, Arg("fq", default=None, doc="Raw Solr filter restricting the datasets counted.")]
    max_values: Annotated[
        int, Arg("max_values", default=1000, doc="Most tags to return (most used first), 1-100000.", ge=1, le=100000)
    ]


def facet_items(client: CkanClient, field: str, q: str | None, fq: str | None, limit: int) -> list[dict[str, Any]]:
    """``{name, display_name, count}`` items of one facet, largest count first."""
    result = client.call(
        "package_search",
        {
            "q": q or None,
            "fq": fq or None,
            "rows": 0,
            "facet.field": [field],
            "facet.limit": limit,
            "facet.mincount": 1,
            "include_private": True if client.token else None,
        },
    )
    items = ((result.get("search_facets") or {}).get(field) or {}).get("items")
    if items is None:
        # Older portals only fill ``facets`` ({value: count}).
        counts = (result.get("facets") or {}).get(field) or {}
        items = [{"name": k, "display_name": k, "count": v} for k, v in counts.items()]
    items = sorted(items, key=lambda i: (-int(i.get("count") or 0), str(i.get("name"))))
    return items[:limit]


A = TypeVar("A")


class _OnceBase(TableFunctionGenerator[A, OnceState]):
    """A function that answers with one request."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    @classmethod
    def on_bind(cls, params: BindParams[A]) -> BindResponse:
        """Ask DuckDB for the portal-scoped secret."""
        request_secret(params)
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[A]) -> OnceState:
        """Not fetched yet."""
        return OnceState()

    @classmethod
    def fetch(cls, client: CkanClient, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """All rows."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[Any], state: OnceState, out: OutputCollector) -> None:
        """Fetch and emit."""
        if state.done:
            out.finish()
            return
        state.done = True
        rows = cls.fetch(client_for(params), params)
        if not rows:
            out.finish()
            return
        emit_rows(params, out, rows)


@init_single_worker
class Facets(_OnceBase[Any]):
    """Dataset counts per value of a search field."""

    FunctionArguments = FacetArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = FACET_SCHEMA

    class Meta:
        """Function metadata."""

        name = "facets"
        description = "Count a CKAN portal's datasets per value of a search field (formats, licenses, organizations)"
        tags = function_tags(
            title="Search Facets",
            category="directory",
            keywords="ckan, facets, counts, formats, licenses, organizations, groups, statistics",
            doc_llm=(
                "facets(portal, field, q := NULL, fq := NULL, max_values := 1000) counts the portal's datasets per "
                "value of a search field in one request (Solr facet): 'organization', 'groups', 'tags', "
                "'res_format', 'license_id' or any indexed field. " + PORTAL_NOTE + " q / fq restrict the "
                "datasets counted. Columns: portal, field, value, display_name, dataset_count. Much cheaper than "
                "aggregating datasets() on a large portal."
            ),
            doc_md=(
                "## facets(portal, field, q :=, fq :=, max_values := 1000)\n\n"
                "Dataset counts per value of `field`, computed by the portal's search index."
            ),
            schema=FACET_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT value AS format, dataset_count FROM {Q}.facets('{IRELAND}', 'res_format') "
                    "ORDER BY dataset_count DESC LIMIT 10"
                ),
                description="The ten most common resource formats on data.gov.ie",
            ),
            FunctionExample(
                sql=(
                    f"SELECT display_name, dataset_count FROM {Q}.facets('{CANADA}', 'organization', "
                    "fq := 'res_format:CSV') ORDER BY dataset_count DESC LIMIT 5"
                ),
                description="Organizations publishing the most CSV datasets on open.canada.ca",
            ),
        ]

    @classmethod
    def fetch(cls, client: CkanClient, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """One faceted search."""
        a = params.args
        return [
            {
                "portal": client.portal,
                "field": a.field,
                "value": cv.text(i.get("name")),
                "display_name": cv.text(i.get("display_name")),
                "dataset_count": int(i.get("count") or 0),
            }
            for i in facet_items(client, a.field, a.q, a.fq, a.max_values)
        ]


@init_single_worker
class Tags(_OnceBase[Any]):
    """Tags with the number of datasets carrying each (a tags facet)."""

    FunctionArguments = TagArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = TAG_SCHEMA

    class Meta:
        """Function metadata."""

        name = "tags"
        description = "List a CKAN portal's tags with how many datasets carry each"
        tags = function_tags(
            title="Tags",
            category="directory",
            keywords="ckan, tags, keywords, vocabulary, topics, dataset counts",
            doc_llm=(
                "tags(portal, q := NULL, fq := NULL, max_values := 1000) lists the portal's free tags with "
                "dataset_count, most used first (a 'tags' facet of package_search). " + PORTAL_NOTE + " q / fq "
                "restrict the datasets counted, e.g. fq := 'organization:health'. Columns: portal, tag, "
                "dataset_count. Some portals (open.canada.ca) use a multilingual 'keywords' field instead of "
                "tags; facets(portal, 'keywords_en') may help there."
            ),
            doc_md="## tags(portal, q :=, fq :=, max_values := 1000)\n\nTags and their dataset counts.",
            schema=TAG_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=f"SELECT tag, dataset_count FROM {Q}.tags('{IRELAND}') ORDER BY dataset_count DESC, tag LIMIT 20",
                description="The twenty most used tags on data.gov.ie",
            ),
            FunctionExample(
                sql=f"SELECT count(*) AS tags, sum(dataset_count) AS taggings FROM {Q}.tags('{DEMO}')",
                description="How many distinct tags the CKAN demo portal uses",
            ),
        ]

    @classmethod
    def fetch(cls, client: CkanClient, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """One tags facet."""
        a = params.args
        return [
            {"portal": client.portal, "tag": cv.text(i.get("name")), "dataset_count": int(i.get("count") or 0)}
            for i in facet_items(client, "tags", a.q, a.fq, a.max_values)
            if i.get("name") is not None
        ]
