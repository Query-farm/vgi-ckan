"""Portal directory: organizations, groups, licenses, status and the recent-changes activity stream."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import convert as cv
from .client import ActionNotAvailableError, CkanClient, CkanError
from .common import PORTAL_DOC, client_for, emit_rows, request_secret
from .datasets import TSTZ, OnceState, _OnceBase
from .meta import CANADA, DEMO, IRELAND, PORTAL_NOTE, QUEENSLAND, Q, column, function_tags

# CKAN caps all_fields listings (ckan.group_and_organization_list_all_fields_max, 25 by default).
GROUP_PAGE = 25


@dataclass(slots=True, frozen=True, kw_only=True)
class PortalArgs:
    """Functions whose only argument is the portal."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]


@dataclass(kw_only=True)
class OffsetCursor(ArrowSerializableDataclass):
    """Resumable offset into a paged listing."""

    offset: int = 0
    done: bool = False


def _group_schema(kind: str) -> pa.Schema:
    return pa.schema(
        [
            column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
            column(f"{kind}_id", pa.string(), f"The {kind}'s id, a UUID.", nullable=False),
            column("name", pa.string(), f"The {kind}'s URL slug, as used in datasets().{kind}.", nullable=False),
            column("title", pa.string(), "Display title."),
            column("description", pa.string(), "Description, usually Markdown."),
            column("dataset_count", pa.int64(), "Number of public datasets."),
            column("state", pa.string(), "Lifecycle state: 'active'."),
            column("image_url", pa.string(), "Logo / image URL."),
            column("created", TSTZ, "When it was created on the portal (UTC)."),
            column("url", pa.string(), f"Its page on the portal: <portal>/{kind}/<name>."),
        ]
    )


ORG_SCHEMA = _group_schema("organization")
GROUP_SCHEMA = _group_schema("group")


class _GroupListBase(TableFunctionGenerator[PortalArgs, OffsetCursor]):
    """Pages through organization_list / group_list with all_fields."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ORG_SCHEMA
    ACTION: ClassVar[str] = "organization_list"
    KIND: ClassVar[str] = "organization"

    @classmethod
    def on_bind(cls, params: BindParams[PortalArgs]) -> BindResponse:
        """Ask DuckDB for the portal-scoped secret."""
        request_secret(params)
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PortalArgs]) -> OffsetCursor:
        """Start at offset 0."""
        return OffsetCursor()

    @classmethod
    def process(cls, params: ProcessParams[PortalArgs], state: OffsetCursor, out: OutputCollector) -> None:
        """Fetch one page and emit it."""
        if state.done:
            out.finish()
            return
        client = client_for(params)
        items = client.call(
            cls.ACTION,
            {"all_fields": True, "include_dataset_count": True, "limit": GROUP_PAGE, "offset": state.offset},
        )
        items = [i for i in items or [] if isinstance(i, dict)]
        state.offset += len(items)
        if len(items) < GROUP_PAGE:
            state.done = True
        if not items:
            out.finish()
            return
        rows = [
            {
                "portal": client.portal,
                f"{cls.KIND}_id": i.get("id"),
                "name": i.get("name"),
                "title": cv.blank_to_null(i.get("title")) or cv.blank_to_null(i.get("display_name")),
                "description": cv.blank_to_null(i.get("description")),
                "dataset_count": cv.integer(i.get("package_count")),
                "state": i.get("state"),
                "image_url": cv.blank_to_null(i.get("image_display_url")) or cv.blank_to_null(i.get("image_url")),
                "created": cv.ts(i.get("created")),
                "url": f"{client.portal}/{cls.KIND}/{i.get('name')}",
            }
            for i in items
        ]
        emit_rows(params, out, rows)


@init_single_worker
class Organizations(_GroupListBase):
    """The portal's publishing organizations."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ORG_SCHEMA

    class Meta:
        """Function metadata."""

        name = "organizations"
        description = "List a CKAN portal's publishing organizations with their dataset counts"
        projection_pushdown = True
        tags = function_tags(
            title="Organizations",
            category="directory",
            keywords="ckan, organizations, publishers, agencies, departments, owners",
            doc_llm=(
                "organizations(portal) lists every publishing organization (organization_list with all_fields): "
                "portal, organization_id, name (slug, matches datasets().organization), title, description, "
                "dataset_count, state, image_url, created, url. " + PORTAL_NOTE
            ),
            doc_md="## organizations(portal)\n\nPublishing organizations and their dataset counts.",
            schema=ORG_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT name, title, dataset_count FROM {Q}.organizations('{DEMO}') "
                    "ORDER BY dataset_count DESC, name"
                ),
                description="Organizations on the CKAN demo portal by number of datasets",
            ),
            FunctionExample(
                sql=(
                    f"SELECT count(*) AS organizations, count(*) FILTER (WHERE dataset_count = 0) AS empty "
                    f"FROM {Q}.organizations('{IRELAND}')"
                ),
                description="How many data.gov.ie organizations have published nothing yet",
            ),
        ]


@init_single_worker
class Groups(_GroupListBase):
    """The portal's groups (themes / categories)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GROUP_SCHEMA
    ACTION: ClassVar[str] = "group_list"
    KIND: ClassVar[str] = "group"

    class Meta:
        """Function metadata."""

        name = "groups"
        description = "List a CKAN portal's groups (themes) with their dataset counts"
        projection_pushdown = True
        tags = function_tags(
            title="Groups",
            category="directory",
            keywords="ckan, groups, themes, categories, topics, collections",
            doc_llm=(
                "groups(portal) lists the portal's groups, its thematic categories (group_list with all_fields): "
                "portal, group_id, name (slug, matches datasets().groups and group_name :=), title, description, "
                "dataset_count, state, image_url, created, url. " + PORTAL_NOTE
            ),
            doc_md="## groups(portal)\n\nThemes / categories and their dataset counts.",
            schema=GROUP_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=f"SELECT name, title, dataset_count FROM {Q}.groups('{IRELAND}') ORDER BY dataset_count DESC",
                description="data.gov.ie themes by number of datasets",
            ),
            FunctionExample(
                sql=f"SELECT count(*) AS groups FROM {Q}.groups('{DEMO}')",
                description="How many groups the CKAN demo portal has",
            ),
        ]


LICENSE_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("license_id", pa.string(), "License id, as in datasets().license_id.", nullable=False),
        column("title", pa.string(), "License title."),
        column("url", pa.string(), "License text URL."),
        column("family", pa.string(), "License family, when given."),
        column("status", pa.string(), "Status, e.g. 'active'."),
        column("od_conformance", pa.string(), "Open Definition conformance: 'approved', 'not reviewed', ..."),
        column("osd_conformance", pa.string(), "Open Source Definition conformance."),
        column("is_generic", pa.bool_(), "True for generic placeholders such as 'other-open'."),
        column("is_okd_compliant", pa.bool_(), "Open Knowledge Definition compliant."),
        column("is_osi_compliant", pa.bool_(), "OSI-approved (software licenses)."),
        column("domain_content", pa.bool_(), "Applies to content."),
        column("domain_data", pa.bool_(), "Applies to data."),
        column("domain_software", pa.bool_(), "Applies to software."),
    ]
)


@init_single_worker
class Licenses(_OnceBase[Any]):
    """The licenses a portal offers."""

    FunctionArguments = PortalArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = LICENSE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "licenses"
        description = "List the licenses a CKAN portal offers for datasets"
        tags = function_tags(
            title="Licenses",
            category="directory",
            keywords="ckan, licenses, open license, cc-by, open definition, terms of use",
            doc_llm=(
                "licenses(portal) lists the licenses the portal offers (license_list): portal, license_id "
                "(join to datasets().license_id), title, url, family, status, od_conformance, osd_conformance, "
                "is_generic, is_okd_compliant, is_osi_compliant, domain_content, domain_data, domain_software. "
                + PORTAL_NOTE
                + " Booleans are normalised (CKAN 2.11 sends them as 'True'/'False' strings)."
            ),
            doc_md="## licenses(portal)\n\nThe portal's license register.",
            schema=LICENSE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT license_id, title FROM {Q}.licenses('{DEMO}') WHERE od_conformance = 'approved' ORDER BY 1"
                ),
                description="Open Definition-approved licenses on the CKAN demo portal",
            ),
            FunctionExample(
                sql=(
                    f"SELECT l.title, count(*) AS datasets FROM {Q}.datasets('{DEMO}') d "
                    f"JOIN {Q}.licenses('{DEMO}') l USING (license_id) GROUP BY ALL ORDER BY datasets DESC"
                ),
                description="Datasets per license on the CKAN demo portal",
            ),
        ]

    @classmethod
    def fetch(cls, client: CkanClient, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """One license_list call."""
        out = []
        for i in client.call("license_list") or []:
            out.append(
                {
                    "portal": client.portal,
                    "license_id": i.get("id"),
                    "title": cv.blank_to_null(i.get("title")),
                    "url": cv.blank_to_null(i.get("url")),
                    "family": cv.blank_to_null(i.get("family")),
                    "status": cv.blank_to_null(i.get("status")),
                    "od_conformance": _conformance(i.get("od_conformance")),
                    "osd_conformance": _conformance(i.get("osd_conformance")),
                    "is_generic": cv.boolean(i.get("is_generic")),
                    "is_okd_compliant": cv.boolean(i.get("is_okd_compliant")),
                    "is_osi_compliant": cv.boolean(i.get("is_osi_compliant")),
                    "domain_content": cv.boolean(i.get("domain_content")),
                    "domain_data": cv.boolean(i.get("domain_data")),
                    "domain_software": cv.boolean(i.get("domain_software")),
                }
            )
        return out


def _conformance(value: Any) -> str | None:
    """Conformance is a string ('approved', 'not reviewed'); some portals send false/''."""
    if value is None or value is False or value == "":
        return None
    return cv.text(value)


STATUS_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("site_title", pa.string(), "The portal's title."),
        column("site_url", pa.string(), "The portal's configured public URL."),
        column("site_description", pa.string(), "The portal's description."),
        column("ckan_version", pa.string(), "CKAN version, e.g. '2.10.8' (NULL when the portal hides it)."),
        column("locale_default", pa.string(), "Default locale, e.g. 'en' or 'en_GB'."),
        column("extensions", pa.list_(pa.string()), "Enabled CKAN plugins, e.g. ['datastore', 'dcat', ...]."),
        column("datastore_enabled", pa.bool_(), "True when the datastore plugin is enabled (datastore() works)."),
    ]
)


@init_single_worker
class PortalInfo(_OnceBase[Any]):
    """Portal metadata: title, CKAN version, plugins (status_show)."""

    FunctionArguments = PortalArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = STATUS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "portal_info"
        description = "Show a CKAN portal's title, CKAN version and enabled plugins"
        tags = function_tags(
            title="Portal Status",
            category="directory",
            keywords="ckan, status, version, ckan version, plugins, extensions, site info",
            doc_llm=(
                "portal_info(portal) returns one row from status_show: portal, site_title, site_url, site_description, "
                "ckan_version (NULL when hidden), locale_default, extensions (enabled plugins) and "
                "datastore_enabled. " + PORTAL_NOTE + " Use it to check that a URL is a CKAN portal and whether "
                "DataStore or activity streams are available."
            ),
            doc_md="## portal_info(portal)\n\nSite title, CKAN version and plugins (`status_show`).",
            schema=STATUS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=f"SELECT site_title, ckan_version, datastore_enabled FROM {Q}.portal_info('{DEMO}')",
                description="Which CKAN version the demo portal runs",
            ),
            FunctionExample(
                sql=(
                    "SELECT portal, ckan_version, list_contains(extensions, 'dcat') AS dcat FROM ("
                    f"SELECT * FROM {Q}.portal_info('{IRELAND}') UNION ALL "
                    f"SELECT * FROM {Q}.portal_info('{CANADA}') UNION ALL "
                    f"SELECT * FROM {Q}.portal_info('{QUEENSLAND}')) ORDER BY portal"
                ),
                description="Compare CKAN versions and DCAT support across three national portals",
            ),
        ]

    @classmethod
    def fetch(cls, client: CkanClient, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """One status_show call."""
        s = client.call("status_show")
        if not isinstance(s, dict):
            raise CkanError(f"status_show on {client.portal} returned {type(s).__name__}, not an object")
        ext = [str(e) for e in s.get("extensions") or []]
        return [
            {
                "portal": client.portal,
                "site_title": cv.blank_to_null(s.get("site_title")),
                "site_url": cv.blank_to_null(s.get("site_url")),
                "site_description": cv.blank_to_null(s.get("site_description")),
                "ckan_version": cv.blank_to_null(s.get("ckan_version")),
                "locale_default": cv.blank_to_null(s.get("locale_default")),
                "extensions": ext,
                "datastore_enabled": "datastore" in ext,
            }
        ]


ACTIVITY_SCHEMA = pa.schema(
    [
        column("portal", pa.string(), "Base URL of the CKAN portal.", nullable=False),
        column("activity_id", pa.string(), "Activity id.", nullable=False),
        column("changed_at", TSTZ, "When the change happened (UTC).", nullable=False),
        column("activity_type", pa.string(), "'new package', 'changed package' or 'deleted package'."),
        column("dataset_id", pa.string(), "Id of the dataset that changed."),
        column("dataset_name", pa.string(), "Slug of the dataset (NULL when the portal omits the snapshot)."),
        column("title", pa.string(), "Title of the dataset at the time of the change."),
        column("user_id", pa.string(), "Id of the user who made the change."),
        column("url", pa.string(), "The dataset's page on the portal."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ActivityArgs:
    """Arguments for ``recent_changes``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=r"\S")]
    page_size: Annotated[
        int, Arg("page_size", default=100, doc="Activities per request, 1-100 (portals cap it).", ge=1, le=100)
    ]


@init_single_worker
class RecentChanges(TableFunctionGenerator[ActivityArgs, OffsetCursor]):
    """The portal-wide dataset activity stream, newest first."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ACTIVITY_SCHEMA

    class Meta:
        """Function metadata."""

        name = "recent_changes"
        description = "List recently created, changed and deleted datasets on a CKAN portal, newest first"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = function_tags(
            title="Recent Changes",
            category="directory",
            keywords="ckan, activity stream, recent changes, updates, changelog, new datasets, audit",
            doc_llm=(
                "recent_changes(portal, page_size := 100) reads the portal's dataset activity stream "
                "(recently_changed_packages_activity_list), newest first: portal, activity_id, changed_at, "
                "activity_type ('new package' / 'changed package' / 'deleted package'), dataset_id, dataset_name, "
                "title, user_id, url. " + PORTAL_NOTE + " A lower bound on changed_at (WHERE changed_at >= ...) "
                "stops paging once older activity is reached; always bound it or use LIMIT, the stream goes back "
                "to the portal's start. CKAN 2.10+ portals need the 'activity' plugin; without it this raises."
            ),
            doc_md=(
                "## recent_changes(portal, page_size := 100)\n\n"
                "The activity stream, newest first. `changed_at >= ...` stops paging early."
            ),
            schema=ACTIVITY_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT changed_at, activity_type, title FROM {Q}.recent_changes('{IRELAND}') "
                    "ORDER BY changed_at DESC LIMIT 10"
                ),
                description="The ten latest dataset changes on data.gov.ie",
            ),
            FunctionExample(
                sql=(
                    "SELECT activity_type, count(*) AS changes FROM ("
                    f"SELECT activity_type FROM {Q}.recent_changes('{CANADA}', page_size := 25) LIMIT 50"
                    ") GROUP BY ALL ORDER BY changes DESC"
                ),
                description="Kinds of change among the 50 latest on open.canada.ca",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActivityArgs]) -> BindResponse:
        """Ask DuckDB for the portal-scoped secret."""
        request_secret(params)
        return BindResponse(output_schema=ACTIVITY_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ActivityArgs]) -> OffsetCursor:
        """Start at the newest activity."""
        return OffsetCursor()

    @classmethod
    def lower_bound(cls, params: ProcessParams[ActivityArgs]) -> Any:
        """Return the earliest ``changed_at`` the query can match, if it says so."""
        filters = params.current_pushdown_filters
        best = None
        for f in filters.filters if filters else ():
            if (
                isinstance(f, ConstantFilter)
                and f.column_name == "changed_at"
                and f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ)
            ):
                v = f.value.as_py()
                best = v if best is None or v > best else best
        if best is not None and best.tzinfo is None:
            best = best.replace(tzinfo=dt.UTC)
        return best

    @classmethod
    def process(cls, params: ProcessParams[ActivityArgs], state: OffsetCursor, out: OutputCollector) -> None:
        """Fetch one page of activity and emit it."""
        if state.done:
            out.finish()
            return
        client = client_for(params)
        try:
            items = client.call(
                "recently_changed_packages_activity_list", {"limit": params.args.page_size, "offset": state.offset}
            )
        except ActionNotAvailableError as err:
            raise CkanError(
                f"{client.portal} has no activity stream (CKAN 2.10+ needs the 'activity' plugin): {err}"
            ) from err
        items = [i for i in items or [] if isinstance(i, dict)]
        state.offset += len(items)
        if not items:
            out.finish()
            return
        rows = []
        for a in items:
            pkg = (a.get("data") or {}).get("package") or {}
            name = cv.blank_to_null(pkg.get("name"))
            rows.append(
                {
                    "portal": client.portal,
                    "activity_id": a.get("id"),
                    "changed_at": cv.ts(a.get("timestamp")),
                    "activity_type": a.get("activity_type"),
                    "dataset_id": a.get("object_id"),
                    "dataset_name": name,
                    "title": cv.translated(pkg, "title"),
                    "user_id": a.get("user_id"),
                    "url": f"{client.portal}/dataset/{name or a.get('object_id')}",
                }
            )
        bound = cls.lower_bound(params)
        oldest = min((r["changed_at"] for r in rows if r["changed_at"] is not None), default=None)
        if bound is not None and oldest is not None and oldest < bound:
            state.done = True
        emit_rows(params, out, rows)


__all__ = ["Groups", "Licenses", "OnceState", "Organizations", "PortalInfo", "RecentChanges"]
