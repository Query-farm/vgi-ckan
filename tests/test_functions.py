"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arg
from vgi.table_filter_pushdown import AndFilter, ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from ckan_worker import convert as cv
from ckan_worker.client import ActionNotAvailableError, CkanClient, CkanError, NotFoundError, _unwrap, normalize_portal
from ckan_worker.common import token_from, without_portal
from ckan_worker.datasets import DATASET_SOLR, SearchArgs, dataset_row, fl_for, resource_rows
from ckan_worker.datastore import arrow_type, convert_value, field_types, read_sql, schema_from_fields
from ckan_worker.pushdown import datastore_filters, solr_clause, solr_fq

# ------------------------------------------------------------------ conversions


def test_timestamps_naive_are_utc_and_ms_truncation() -> None:
    assert cv.ts("2025-11-03T12:39:47.163835") == dt.datetime(2025, 11, 3, 12, 39, 47, 163835, tzinfo=dt.UTC)
    assert cv.ts("2025-11-03T12:39:47.163Z") == dt.datetime(2025, 11, 3, 12, 39, 47, 163000, tzinfo=dt.UTC)
    assert cv.ts_ms("2025-11-03T12:39:47.163835") == cv.ts("2025-11-03T12:39:47.163Z")
    assert cv.ts("") is None and cv.ts("not a date") is None


def test_string_booleans_and_sizes() -> None:
    assert cv.boolean("True") is True and cv.boolean("False") is False and cv.boolean(None) is None
    assert cv.integer(523) == 523 and cv.integer("523") == 523 and cv.integer("523 KiB") is None
    assert cv.integer(True) is None


def test_translated_fallbacks() -> None:
    assert cv.translated({"title": "Plain"}, "title") == "Plain"
    assert cv.translated({"title": "", "title_translated": {"fr": "Bonjour", "en": "Hello"}}, "title") == "Hello"
    assert cv.translated({"notes_translated": {"ga": "Dia duit"}}, "notes") == "Dia duit"
    assert cv.translated({}, "title") is None


def test_extras_map() -> None:
    assert cv.extras_map([{"key": "b", "value": 2}, {"key": "a", "value": "x"}]) == [("a", "x"), ("b", "2")]


# -------------------------------------------------------------------- portals


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("https://demo.ckan.org/", "https://demo.ckan.org"),
        ("demo.ckan.org", "https://demo.ckan.org"),
        ("https://open.canada.ca/data/en/dataset/abc", "https://open.canada.ca/data"),
        ("https://data.gov.ie/api/3/action/package_search", "https://data.gov.ie"),
        ("http://127.0.0.1:8000/ireland/api/3", "http://127.0.0.1:8000/ireland"),
        ("https://data.gov.ie/dataset/x", "https://data.gov.ie"),
    ],
)
def test_normalize_portal(given: str, expected: str) -> None:
    assert normalize_portal(given) == expected


def test_normalize_portal_rejects_non_http() -> None:
    with pytest.raises(CkanError):
        normalize_portal("ftp://example.org")
    with pytest.raises(CkanError):
        normalize_portal("  ")


def test_action_url_encodes_lists_as_json() -> None:
    url = CkanClient("https://x.org").action_url("package_search", {"facet.field": ["tags"], "q": None, "b": True})
    assert url == "https://x.org/api/3/action/package_search?b=true&facet.field=%5B%22tags%22%5D"


def test_unwrap_error_shapes() -> None:
    assert _unwrap("a", "u", 200, b'{"success": true, "result": [1]}') == [1]
    with pytest.raises(NotFoundError):
        _unwrap("a", "u", 404, b'{"success": false, "error": {"__type": "Not Found Error", "message": "Not found"}}')
    with pytest.raises(ActionNotAvailableError):
        _unwrap("a", "u", 400, b'"Bad request - Action name not known: a"')
    with pytest.raises(CkanError, match="API token"):
        _unwrap("a", "u", 403, b'{"success": false, "error": {"__type": "Authorization Error", "message": "no"}}')
    with pytest.raises(CkanError, match=r"Validation Error.*rows"):
        _unwrap("a", "u", 409, b'{"success": false, "error": {"__type": "Validation Error", "rows": ["bad"]}}')
    with pytest.raises(CkanError, match="not a CKAN Action API endpoint"):
        _unwrap("a", "u", 404, b"<html>nope</html>")


# --------------------------------------------------------------------- secrets


def _secret(token: str, scope: str) -> dict[str, Any]:
    return {"type": pa.scalar("ckan"), "api_token": pa.scalar(token), "scope": pa.scalar(scope)}


def test_token_needs_a_matching_scope_at_a_path_boundary() -> None:
    secrets = [_secret("a", "https://data.gov.ie"), _secret("b", "https://data.gov.ie/private"), _secret("c", "")]
    assert token_from(secrets, "https://data.gov.ie") == "a"
    assert token_from(secrets, "https://data.gov.ie/private") == "b"  # longest scope wins
    assert token_from(secrets, "https://data.gov.ie.evil.com") is None  # string prefix only: no
    assert token_from(secrets, "https://demo.ckan.org") is None  # the unscoped secret is never used
    assert token_from([_secret("d", "data.gov.ie")], "https://data.gov.ie") == "d"


# -------------------------------------------------------------------- pushdown


def _eq(col: str, value: Any, op: ComparisonOp = ComparisonOp.EQ) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value))


def test_solr_clauses() -> None:
    assert solr_clause(_eq("organization", 'a"b'), DATASET_SOLR) == 'organization:"a\\"b"'
    assert solr_clause(_eq("private", True), DATASET_SOLR) == 'capacity:"private"'
    when = dt.datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=dt.UTC)
    assert solr_clause(_eq("modified", when, ComparisonOp.GT), DATASET_SOLR) == (
        "metadata_modified:[2026-01-02T03:04:05.678Z TO *]"
    )
    assert solr_clause(_eq("modified", when, ComparisonOp.LT), DATASET_SOLR) == (
        "metadata_modified:[* TO 2026-01-02T03:04:05.679Z]"
    )
    assert solr_clause(_eq("license_id", "x", ComparisonOp.GT), DATASET_SOLR) is None  # not a range field
    assert solr_clause(_eq("title", "x"), DATASET_SOLR) is None  # free text: not exact
    in_f = InFilter(column_name="organization", column_index=0, values=pa.array(["a", None, "b"]))
    assert solr_clause(in_f, DATASET_SOLR) == 'organization:("a" OR "b")'
    both = AndFilter(column_name="num_resources", column_index=0, children=(_eq("num_resources", 2, ComparisonOp.GE),))
    assert solr_clause(both, DATASET_SOLR) == "(num_resources:[2 TO *])"


def test_solr_fq_dedupes_and_skips_untranslatable() -> None:
    filters = PushdownFilters(filters=(_eq("organization", "a"), _eq("organization", "a"), _eq("title", "t")))
    assert solr_fq(filters, DATASET_SOLR) == ['organization:"a"']
    assert solr_fq(None, DATASET_SOLR) == []


def test_datastore_filters() -> None:
    filters = PushdownFilters(
        filters=(
            _eq("site", "x"),
            _eq("depth", 3, ComparisonOp.GT),
            InFilter(column_name="kind", column_index=0, values=pa.array(["a", "b"])),
            _eq("missing", 1),
        )
    )
    assert datastore_filters(filters, {"site", "depth", "kind"}) == {"site": "x", "kind": ["a", "b"]}


def test_fl_only_when_every_column_is_indexed() -> None:
    assert fl_for({"name", "modified", "formats"}) == ["id", "metadata_modified", "name", "res_format"]
    assert fl_for({"name", "num_resources"}) is None


# ---------------------------------------------------------------------- shapes


def test_dataset_row_full_and_fl_agree() -> None:
    full = {
        "id": "i",
        "name": "n",
        "title": "T",
        "organization": {"name": "org", "title": "Org"},
        "resources": [{"format": "CSV"}, {"format": "csv "}, {"format": "JSON"}, {"format": ""}],
        "tags": [{"name": "t1"}],
        "groups": [],
        "private": False,
        "metadata_modified": "2025-01-01T00:00:00.123456",
        "extras": [{"key": "k", "value": "v"}],
    }
    fl = {
        "id": "i",
        "name": "n",
        "title": "T",
        "organization": "org",
        "res_format": ["CSV", "JSON", "csv"],
        "tags": ["t1"],
        "capacity": "public",
        "metadata_modified": "2025-01-01T00:00:00.123Z",
    }
    a, b = dataset_row("https://p", full), dataset_row("https://p", fl, full=False)
    for col in ("dataset_id", "name", "organization", "tags", "formats", "private", "modified", "url"):
        assert a[col] == b[col], col
    assert a["num_resources"] == 4 and a["organization_title"] == "Org" and a["extras"] == [("k", "v")]
    assert json.loads(a["raw"])["id"] == "i" and b["raw"] is None


def test_resource_rows() -> None:
    pkg = {"id": "d", "name": "n", "resources": [{"id": "r", "size": "523 KiB", "datastore_active": "True"}]}
    (row,) = resource_rows("https://p", pkg)
    assert row["size"] is None and row["datastore_active"] is True and row["position"] == 0
    assert row["dataset_url"] == "https://p/dataset/n"


def test_datastore_types() -> None:
    assert arrow_type("int4") == pa.int64() and arrow_type("numeric") == pa.float64()
    assert arrow_type("timestamp") == pa.timestamp("us") and arrow_type("_text") == pa.list_(pa.string())
    assert arrow_type("tsvector") == pa.string()
    assert convert_value("numeric", "980.00") == 980.0
    assert convert_value("timestamp", "2026-11-08T00:30:00") == dt.datetime(2026, 11, 8, 0, 30)
    assert convert_value("date", "2026-11-08") == dt.date(2026, 11, 8)
    assert convert_value("_int4", "[1, 2]") == [1, 2]
    assert convert_value("json", {"a": 1}) == '{"a": 1}'
    schema = schema_from_fields(
        [{"id": "_id", "type": "int"}, {"id": "_full_text", "type": "tsvector"}, {"id": "a b", "type": "float8"}]
    )
    assert schema.names == ["_id", "a b"] and not schema.field("_id").nullable
    assert field_types(schema) == {"_id": "int", "a b": "float8"}


def test_read_sql_by_format() -> None:
    assert read_sql("CSV", "https://x/a.csv") == "SELECT * FROM read_csv('https://x/a.csv')"
    assert read_sql("", "https://x/a.parquet?dl=1") == "SELECT * FROM read_parquet('https://x/a.parquet?dl=1')"
    assert read_sql("PDF", "https://x/a.pdf") is None
    assert read_sql("csv", "https://x/it's.csv") == "SELECT * FROM read_csv('https://x/it''s.csv')"


def test_attach_overload_drops_portal_and_shifts_positions() -> None:
    import typing

    made = without_portal(SearchArgs, "NoPortal")
    hints = typing.get_type_hints(made, include_extras=True)
    assert "portal" not in hints and "q" in hints

    from ckan_worker.datastore import DatastoreArgs

    hints = typing.get_type_hints(without_portal(DatastoreArgs, "X"), include_extras=True)
    (arg,) = [a for a in typing.get_args(hints["resource_id"])[1:] if isinstance(a, Arg)]
    assert arg.position == 0


# --------------------------------------------- the HTTP client vs. the replay
# (The in-process vgi Client does not answer bind-time secret-scope requests, so
# whole functions are exercised through DuckDB in test/sql instead.)


def test_client_against_replay(mock_api: str) -> None:
    status = CkanClient(f"{mock_api}/ireland").call("status_show")
    assert status["ckan_version"] == "2.9.16"
    with pytest.raises(ActionNotAvailableError):
        CkanClient(f"{mock_api}/demo").call("recently_changed_packages_activity_list", {"limit": 100, "offset": 0})


def test_replay_is_strict(mock_api: str) -> None:
    with pytest.raises(CkanError, match="Unrecorded Request"):
        CkanClient(f"{mock_api}/demo").call("package_show", {"id": "never-recorded-id"})
    # A token sent to a public portal is rejected, so a leak cannot pass silently.
    with pytest.raises(CkanError, match="Unexpected Credential"):
        CkanClient(f"{mock_api}/demo", token="t").call("status_show")
    with pytest.raises(CkanError, match="API token"):
        CkanClient(f"{mock_api}/private").call("status_show")
    assert CkanClient(f"{mock_api}/private", token="test-token-1234").call("status_show")["ckan_version"] == "2.11.5"
