"""Assemble the ``ckan`` catalog and run the worker.

Run it:

    vgi-ckan-worker                      # stdio: what DuckDB launches by default
    vgi-ckan-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'ckan' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-ckan-worker');
    SELECT title FROM ckan.main.datasets('https://demo.ckan.org');

    -- or bind the catalog to one portal and leave the argument out:
    ATTACH 'ckan' (TYPE vgi, LOCATION '...', PORTAL 'https://data.gov.ie');
    SELECT title FROM ckan.main.datasets();
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .client import CKAN_SECRET
from .common import without_portal
from .datasets import Dataset, Datasets, Facets, Resources, Tags
from .datastore import Datastore, DatastoreSql, ReadSql, ResourceData
from .directory import Groups, Licenses, Organizations, PortalInfo, RecentChanges
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)

BASE_FUNCTIONS: list[type[Any]] = [
    Datasets,
    Dataset,
    Resources,
    Organizations,
    Groups,
    Tags,
    Facets,
    Licenses,
    PortalInfo,
    RecentChanges,
    Datastore,
    DatastoreSql,
    ResourceData,
]


def attach_overload(fn: type[Any]) -> type[Any]:
    """Return the same function without the ``portal`` argument, reading the ``portal`` ATTACH option."""
    args = without_portal(fn.FunctionArguments, f"{fn.FunctionArguments.__name__}NoPortal")
    meta = type(
        "Meta",
        (fn.Meta,),
        {
            "__doc__": "Overload metadata.",
            "name": fn.Meta.name,
            "description": fn.Meta.description + " (portal from the ATTACH PORTAL option)",
        },
    )
    return type(
        f"{fn.__name__}AttachedPortal",
        (fn,),
        {
            "FunctionArguments": args,
            "Meta": meta,
            "__doc__": f"{fn.__doc__} Uses the ATTACH portal.",
            "__module__": fn.__module__,
        },
    )


FUNCTIONS: list[type[Any]] = [*(f for fn in BASE_FUNCTIONS for f in (fn, attach_overload(fn))), ReadSql]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; overloads share one SQL name."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entries[0] for entries in by_name.values() if entries]


_SCHEMA_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class AttachOptions:
    """ATTACH options."""

    portal: Annotated[
        str | None,
        AttachOption(desc="Default CKAN portal base URL for functions called without one, e.g. 'https://data.gov.ie'."),
    ] = None


class CkanWorker(Worker):
    """The worker process: the ``ckan`` catalog with one ``main`` schema."""

    AttachOptions = AttachOptions

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="CKAN Action API functions: dataset search, portal directory and DataStore tables.",
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "CKAN Portal Functions",
                    "domain": "open data",
                    "vgi.keywords": keywords_array(
                        "datasets, resources, organizations, groups, tags, licenses, datastore, package_search"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_SCHEMA_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [CKAN_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    CkanWorker.main()


if __name__ == "__main__":
    main()
