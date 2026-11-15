"""Discovery metadata: catalog, schema and per-function tags.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every function carries ``function_tags(...)``: a title, a category from
``SCHEMA_CATEGORIES``, keywords, LLM- and human-facing docs, and its result
columns.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "ckan"
Q = f"{CATALOG_NAME}.main"

SOURCE_URL = "https://github.com/Query-farm/vgi-ckan"
CKAN_API_DOCS = "https://docs.ckan.org/en/latest/api/index.html"
DATASTORE_API_DOCS = "https://docs.ckan.org/en/latest/maintaining/datastore.html#the-datastore-api"

# Public portals used in the examples (all keyless CKAN sites).
DEMO = "https://demo.ckan.org"
IRELAND = "https://data.gov.ie"
CANADA = "https://open.canada.ca/data"
QUEENSLAND = "https://www.data.qld.gov.au"

SCHEMA_CATEGORIES = [
    {"name": "catalog", "title": "Dataset Catalog", "description": "Datasets and their resources (files, APIs)."},
    {
        "name": "directory",
        "title": "Portal Directory",
        "description": "Organizations, groups, tags, facets, licenses, portal status and recent changes.",
    },
    {"name": "datastore", "title": "DataStore", "description": "Rows of tabular resources loaded into DataStore."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB names it (for ``vgi.result_columns_schema``)."""
    if getattr(t, "extension_name", None) == "arrow.json":
        return "JSON"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_map(t):
        return f"MAP({duckdb_type(t.key_type)}, {duckdb_type(t.item_type)})"
    simple = {
        pa.string(): "VARCHAR",
        pa.int64(): "BIGINT",
        pa.int32(): "INTEGER",
        pa.bool_(): "BOOLEAN",
        pa.float64(): "DOUBLE",
        pa.date32(): "DATE",
    }
    return simple.get(t, str(t).upper())


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` for a fixed output schema."""
    return json.dumps(
        [
            {"name": f.name, "type": duckdb_type(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def function_tags(
    *,
    title: str,
    category: str,
    keywords: str,
    doc_llm: str,
    doc_md: str,
    schema: pa.Schema | None = None,
    dynamic_md: str | None = None,
) -> dict[str, str]:
    """Build the standard per-function discovery tags.

    Args:
        title: Human title.
        category: One of ``SCHEMA_CATEGORIES``.
        keywords: Comma-separated search keywords.
        doc_llm: Agent-facing description.
        doc_md: Human-facing Markdown.
        schema: The fixed result schema, documented as result columns.
        dynamic_md: For schemas that vary by argument: Markdown Name|Type|Description tables.

    Returns:
        The tag dict.

    Raises:
        ValueError: Unknown category.

    """
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    tags = {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
        "domain": "open data",
    }
    if schema is not None:
        tags["vgi.result_columns_schema"] = result_columns(schema)
    elif dynamic_md is not None:
        tags["vgi.result_dynamic_columns_md"] = dynamic_md
    return tags


PORTAL_NOTE = (
    "The first argument is the portal's base URL (such as https://data.gov.ie or https://open.canada.ca/data); "
    "an overload without it uses the PORTAL option given at ATTACH."
)

CATALOG_DESCRIPTION = (
    "Any CKAN open-data portal as SQL: search datasets and resources, browse organizations, groups, tags and "
    "licenses, and read DataStore tables, from data.gov.ie, open.canada.ca and thousands of other CKAN sites."
)

CATALOG_DOC_LLM = (
    "Read-only access to any CKAN open-data portal through the CKAN Action API v3 (CKAN 2.9, 2.10 and 2.11). "
    "One worker covers every CKAN site: every function takes the portal's base URL as its first argument "
    "(for example the Irish national portal https://data.gov.ie, or https://open.canada.ca/data where the API "
    "lives under a path), or ATTACH with PORTAL '<url>' and call the functions without it. Most portals need no "
    "credentials; private datasets need an API token in a `ckan` secret scoped to the portal URL. Key concepts: "
    "a dataset (CKAN 'package') is a metadata record owned by an organization, tagged and grouped into themes, "
    "holding resources; a resource is one file, link or API; resources whose rows were loaded into the portal's "
    "DataStore (datastore_active) can be read as typed rows, other files are read by URL with DuckDB's own "
    "readers. Filters on dataset columns are translated into the portal's Solr search, so filter in WHERE "
    "rather than scanning a whole national portal; facet counts are computed by the portal. Columns named "
    "portal, dataset_id, resource_id, title, format, url, created and modified are shared so results from "
    "several portals can be combined with UNION ALL."
)

CATALOG_DOC_MD = (
    "# CKAN open-data portals in DuckDB\n\n"
    "Query **any [CKAN](https://ckan.org) portal** (data.gov.ie, open.canada.ca, data.qld.gov.au, "
    "demo.ckan.org and thousands of municipal and national sites) with SQL: search the catalog, list "
    "resources and read DataStore tables with typed columns.\n\n"
    "- Pass the portal URL as the first argument, or ATTACH with `PORTAL '<url>'`.\n"
    "- `WHERE` filters on organization, license, type and dates run in the portal's search index.\n"
    "- DataStore equality filters and column selections run on the portal.\n"
    "- Files that are not in DataStore come back as URLs for `read_csv` / `read_parquet`.\n"
    "- Private datasets need an API token in a `ckan` secret scoped to the portal:\n\n"
    "```sql\nCREATE SECRET (TYPE ckan, API_TOKEN '...', SCOPE 'https://data.example.org');\n```"
)

SCHEMA_DOC_LLM = (
    "Table functions over the CKAN Action API, all taking the portal URL first (or nothing after ATTACH ... "
    "PORTAL). Catalog functions search package_search / package_show and push WHERE filters into Solr; "
    "directory functions describe the portal (publishers, themes, vocabularies, licenses, plugins, activity); "
    "DataStore functions return typed rows. Timestamps are `TIMESTAMPTZ` in UTC. Lists such as tags and "
    "formats are `VARCHAR[]`."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "CKAN Action API functions in three groups: the dataset **catalog**, the portal **directory**, and "
    "**DataStore** tables.\n\n"
    "Pass the portal URL first, or ATTACH with `PORTAL` and leave it out. Filters in `WHERE` are sent to the "
    "portal wherever its API can apply them."
)

AGENT_TEST_TASKS = [
    {
        "name": "demo_formats",
        "prompt": (
            f"On the CKAN portal {DEMO}, how many resources are there of each format? "
            "Return format and count, most common first."
        ),
    },
    {
        "name": "ireland_recent_courts_datasets",
        "prompt": (
            f"Which datasets published by the organization 'the-courts-service' on {IRELAND} were modified "
            "since 1 January 2026? Give their names."
        ),
    },
    {"name": "demo_ckan_version", "prompt": f"Which CKAN version does the portal at {DEMO} run?"},
    {
        "name": "ireland_paid_orders",
        "prompt": (
            f"The DataStore resource dfb0930a-98a5-45a0-a6b7-e4beb66340e5 on {IRELAND} lists Courts Service "
            "purchase orders. What is the total Amount of the orders marked as paid (paid_yes_no = 'Y')?"
        ),
    },
    {
        "name": "ireland_dataset_license",
        "prompt": (
            f"On {IRELAND}, which license and organization does the dataset "
            "'courts-service-purchase-orders-greater-than-20-000-2026' have, and how many resources?"
        ),
    },
    {
        "name": "demo_organizations",
        "prompt": f"List the organizations on {DEMO} with how many datasets each has published, largest first.",
    },
    {
        "name": "ireland_top_formats",
        "prompt": f"What are the five most common resource formats on {IRELAND}, by number of datasets?",
    },
    {
        "name": "demo_top_tags",
        "prompt": f"Which tags are used most on {DEMO}? Show the top five with their dataset counts.",
    },
    {
        "name": "ireland_groups_and_licenses",
        "prompt": (
            f"How many themes (groups) does {IRELAND} have, and how many licenses does {DEMO} offer that are "
            "approved by the Open Definition?"
        ),
    },
    {
        "name": "ireland_latest_changes",
        "prompt": f"Which five datasets changed most recently on {IRELAND}, and when?",
    },
    {
        "name": "qld_buoy_readings",
        "prompt": (
            f"Using server-side SQL on {QUEENSLAND}, count the readings per site in DataStore resource "
            "2bbef99e-9974-49b9-a316-57402b00609c and give the three sites with the most readings."
        ),
    },
    {
        "name": "demo_file_reader",
        "prompt": (
            f"Resource 73127990-4645-4de7-a5f0-f016d028d984 on {DEMO} is a plain file, not in DataStore. "
            "What URL holds its data and what DuckDB query would load it?"
        ),
    },
]

CATALOG_TAGS = {
    "vgi.title": "CKAN Open Data Portals",
    "vgi.subtitle": "Search datasets and read DataStore tables from any CKAN open-data portal.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "ckan, open data, government data, data portal, datasets, datastore, data.gov, catalog, opendata"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
