<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Any CKAN Open-Data Portal in DuckDB

> **vgi-ckan** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-ckan/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-ckan/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[CKAN](https://ckan.org) runs thousands of open-data portals: national ones such as
data.gov.ie, open.canada.ca and data.qld.gov.au, and many city and agency sites. This worker
reads any of them through the CKAN Action API v3 (CKAN 2.9, 2.10 and 2.11). You can search
datasets and resources, list organizations, groups, tags, facets and licenses, follow recent
changes, and read DataStore tables with typed columns. Most portals need no credentials.

```sql
-- Which data.gov.ie publishers updated purchase-order datasets this year?
SELECT organization, count(*) AS datasets, max(modified) AS last_change
FROM ckan.main.datasets('https://data.gov.ie', q := 'purchase orders')
WHERE modified >= TIMESTAMPTZ '2026-01-01'
GROUP BY organization ORDER BY datasets DESC;
```

The free-text search and the `modified` range are sent to the portal's Solr index. The query
selects only indexed columns, so the worker requests just those fields (`fl`) instead of full
dataset records.

vgi-ckan 0.2.0 speaks VGI protocol 3.4, so it needs Haybarn 2.0.1 or newer (Haybarn 2.0.0 loads a 3.3 `vgi` extension that cannot attach it).

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'ckan' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-ckan@v0.2.0 vgi-ckan-worker');

SELECT site_title, ckan_version FROM ckan.main.portal_info('https://demo.ckan.org');
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same
`ATTACH`.

**From a checkout:** run `make build`, then use
`LOCATION '/abs/path/to/vgi-ckan/.venv/bin/vgi-ckan-worker'`.

### One portal per catalog

Every function takes the portal URL as its first argument. Each one also has an overload
without that argument, which uses the `PORTAL` ATTACH option:

```sql
ATTACH 'ckan' AS ie (TYPE vgi, LOCATION '...', PORTAL 'https://data.gov.ie');
SELECT name, title FROM ie.main.datasets() WHERE organization = 'the-courts-service';
```

A pasted dataset page or `/api/3/...` URL is trimmed to the portal root. For example,
`https://open.canada.ca/data/en/dataset/<id>` becomes `https://open.canada.ca/data`.

## Functions

All functions are in schema `ckan.main`. Timestamps are `TIMESTAMPTZ` in UTC.

| Function | Returns | CKAN action |
|---|---|---|
| `datasets(portal, q :=, fq :=, tag :=, group_name :=, res_format :=, page_size := 100)` | One row per dataset | `package_search` |
| `dataset(portal, id)` | One dataset by id or name (no rows if it does not exist) | `package_show` |
| `resources(portal, q :=, fq :=, tag :=, group_name :=, res_format :=, page_size := 100)` | One row per resource of the matching datasets | `package_search` |
| `organizations(portal)` / `groups(portal)` | Publishers / themes with dataset counts | `organization_list` / `group_list` |
| `tags(portal, q :=, fq :=, max_values := 1000)` | Tags with dataset counts | `package_search` facet |
| `facets(portal, field, q :=, fq :=, max_values := 1000)` | Dataset counts per value of any indexed field | `package_search` facet |
| `licenses(portal)` | The portal's license register | `license_list` |
| `portal_info(portal)` | Site title, CKAN version, plugins, DataStore availability | `status_show` |
| `recent_changes(portal, page_size := 100)` | The dataset activity stream, newest first | `recently_changed_packages_activity_list` |
| `datastore(portal, resource_id, filters :=, q :=, page_size := 1000)` | Rows of a DataStore table, typed | `datastore_search` |
| `datastore_sql(portal, sql)` | Rows of a read-only PostgreSQL query, typed | `datastore_search_sql` |
| `resource_data(portal, resource_id, page_size := 1000)` | DataStore rows, or the download URL and a reader query | `resource_show` + `datastore_search` |
| `read_sql(resource_format, url)` (scalar) | The `read_csv` / `read_parquet` / `read_json_auto` query for a file | none |

### Columns you can UNION across portals

`datasets` and `resources` share their column names with the sibling open-data workers
(Socrata, OpenDataSoft): `portal`, `dataset_id`, `resource_id`, `title`, `format`, `url`,
`created` and `modified`. Results from several portals combine with `UNION ALL`:

```sql
SELECT portal, name, title, modified FROM ckan.main.datasets('https://data.gov.ie', q := 'air quality')
UNION ALL
SELECT portal, name, title, modified FROM ckan.main.datasets('https://open.canada.ca/data', q := 'air quality');
```

`datasets` also has `organization`, `organization_title`, `groups`, `tags`, `formats`
(`VARCHAR[]`), `license_id`, `license_title`, `num_resources`, `private`, `state`, `type`,
`author`, `maintainer`, `version`, `source_url`, `extras` (`MAP`) and `raw`. `raw` is a `JSON`
column with the portal's full record, including portal-specific fields such as
`title_translated`.

### Reading the data

A resource whose rows were loaded into the portal's **DataStore** (`datastore_active`) is
read with `datastore()`. Its column types come from the DataStore field types: `int*` becomes
`BIGINT`, `numeric`/`float*` `DOUBLE`, `timestamp` `TIMESTAMP`, `timestamptz` `TIMESTAMPTZ`,
`date` `DATE`, `bool` `BOOLEAN`, `json` `JSON`, `_type` arrays `LIST`, and everything else
`VARCHAR`. Many portals load CSVs as all-text columns, so cast those in SQL.

```sql
SELECT "Supplier Name", sum("Amount") AS total
FROM ckan.main.datastore('https://data.gov.ie', 'dfb0930a-98a5-45a0-a6b7-e4beb66340e5')
WHERE paid_yes_no = 'Y' GROUP BY ALL ORDER BY total DESC LIMIT 5;
```

Other resources are plain files. The worker does not parse files: DuckDB's readers do that.
`resource_data()` returns the URL together with a ready query:

```sql
SELECT url, read_sql FROM ckan.main.resource_data('https://demo.ckan.org', '73127990-4645-4de7-a5f0-f016d028d984');
-- read_sql: SELECT * FROM read_csv('https://example.com/data/temperature.csv')
SELECT * FROM read_csv('https://example.com/data/temperature.csv');
```

`datastore_sql()` runs read-only SQL on the portal's PostgreSQL. This works only where the
portal enables `ckan.datastore.sqlsearch.enabled` (data.qld.gov.au does; data.gov.ie,
open.canada.ca and demo.ckan.org do not, and the function says so). Tables are resource ids
in double quotes. If a result exceeds the portal's row cap (`ckan.datastore.search.rows_max`),
the function raises an error instead of returning truncated rows.

## What pushes down

| Function | SQL | Sent to the portal as |
|---|---|---|
| `datasets` | `=`, `IN` on `organization`, `license_id`, `type`, `state`, `private`, `name`, `dataset_id` | Solr `fq` terms (`private` → `capacity`) |
| `datasets` | ranges on `created`, `modified`, `num_resources` | Solr range queries on `metadata_created`, `metadata_modified`, `num_resources` (strict bounds widen to inclusive at millisecond precision) |
| `datasets` | selecting only indexed columns (`name`, `title`, `organization`, `groups`, `tags`, `formats`, `license_id`, `state`, `type`, `private`, `created`, `modified`, `url`) | `fl`, which returns index fields instead of full records |
| `datasets` | `ORDER BY modified / created / name / dataset_id … LIMIT n` | Solr `sort`, and paging stops once n matching rows are out |
| `resources` | `=`, `IN` on `format`, `organization`, `dataset_id`, `dataset_name`; `ORDER BY dataset_id / dataset_name … LIMIT n` | `res_format` / `organization` / `id` / `name` in `fq`; `sort` |
| `datastore` / `resource_data` | `=`, `IN` on any column; the selected columns | `filters` (a list value for `IN`); `fields` |
| `datastore` | `ORDER BY <numeric / temporal column> … LIMIT n` | `sort` with an explicit `NULLS FIRST/LAST` that matches DuckDB, plus an early stop |
| `recent_changes` | `changed_at >= …` | Paging stops at the first page older than the bound |

Every predicate is also re-checked locally (`auto_apply_filters`), so a translation only
narrows what the portal returns. Predicates the API cannot express (`LIKE`, `<>`, list
predicates) are evaluated in DuckDB. For tags, groups and resource formats, use the
`tag :=`, `group_name :=` and `res_format :=` arguments to filter on the portal.

Paging is `rows`/`start` (search) or `limit`/`offset` (DataStore) over a stable sort
(`id asc` / `_id asc`). The cursor is externalized, so a scan can resume on any worker over
HTTP. DuckDB can tighten dynamic filters during a scan, for example a top-N threshold or join
keys. The filters sent with the first page are therefore kept for the rest of the scan, so
later pages stay aligned with `start`/`offset`. Tighter filters apply locally. Requests that
get 429, 502, 503 or 504 are retried with exponential backoff that honours `Retry-After`.

## Private datasets (API tokens)

Public portals need nothing. For private datasets, create a CKAN API token (user settings →
API tokens) and store it in a `ckan` secret **scoped to the portal URL**. Create the secret
after `ATTACH`, which registers the secret type:

```sql
CREATE SECRET my_portal (TYPE ckan, API_TOKEN '...', SCOPE 'https://data.example.org');
```

The token is sent in the `Authorization` header only to URLs under that scope, and only at a
path boundary: a scope of `https://data.example.org` does not match
`https://data.example.org.evil.com`. A `ckan` secret without `SCOPE` is ignored, so a token can
never reach another portal. With a token, `datasets()` and `resources()` add
`include_private=true`. The token is redacted in `duckdb_secrets()` and is never stored in
scan state.

## CKAN version differences handled

Observed while recording the fixtures:

- CKAN 2.11 returns `license_list` booleans as the strings `"True"`/`"False"`; 2.9 and 2.10
  return JSON booleans. Both are normalised.
- Unknown actions return HTTP 400 with a bare JSON string (`"Bad request - Action name not
  known: …"`). This happens for `recently_changed_packages_activity_list` without the activity
  plugin (CKAN 2.10+) and for `datastore_search_sql` where it is disabled. The worker raises a
  clear error in both cases.
- `datastore_search` on a resource without a DataStore table returns 404 on 2.9/2.10 and 500 on
  2.11. Both become an error that points at `resource_data()` / `read_csv`.
- Multilingual (scheming / fluent) portals put text in `title_translated` /
  `notes_translated`. `title` and `description` fall back to the portal locale, then English.
- The 2.9 activity stream has only the title in its snapshot, while 2.10 has the whole
  dataset. `dataset_name` is NULL when the snapshot does not include it.
- `size` can be a number or a human string such as `"523 KiB"` (data.qld.gov.au). Only
  numbers are kept.
- Search-index timestamps have millisecond precision and full records have microseconds.
  `created`/`modified` are truncated to milliseconds, so both kinds of request return equal
  values.
- Some portals (data.qld.gov.au) do not report `ckan_version`, so it is NULL.

## Testing

```bash
make check      # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
```

Running CKAN locally needs Solr, PostgreSQL, Redis and CKAN, which is several GB. The SQL suite
(`test/sql/*.test`, through the real `vgi` extension in Haybarn 2.0) instead runs against
`test/mock_api/server.py`, a **strict replay** of responses recorded once from public portals:

| Prefix | Recorded from | CKAN |
|---|---|---|
| `/demo` | https://demo.ckan.org | 2.11.5 |
| `/canada` | https://open.canada.ca/data | 2.10.8 |
| `/ireland` | https://data.gov.ie | 2.9.16 (with DataStore) |
| `/qld` | https://www.data.qld.gov.au | not reported (`datastore_search_sql` enabled) |
| `/private` | hand-written | a token-protected portal |

The replay answers only the exact recorded request (method, path, sorted query string). Any
other request gets HTTP 501, so a changed request fails the test instead of passing silently.
The `/private` portal requires `Authorization: test-token-1234`. Every other prefix rejects a
request that *carries* a token, so a token sent to the wrong portal fails the tests. The
replay also answers the first request for every URL with 429 + `Retry-After: 0`, which
exercises the retry path. `test/sql/live.test` is an optional smoke test against
demo.ckan.org. It runs when `CKAN_LIVE=1` is set and is skipped otherwise.

To re-record (network; a few hundred KB):

```bash
python3 test/mock_api/server.py --record     # prints PORT:<n>; forwards unrecorded requests and saves them
# then run the SQL tests with CKAN_MOCK=http://127.0.0.1:<n>
```

The metadata check (`make lint-explain`) scores 100/100 with zero findings at
`fail_on = "info"`. The examples read public, keyless portals, so CI runs them live
(`execute: true`).

## Deploying

The same entry point serves stdio (DuckDB starts it) or HTTP
(`vgi-ckan-worker --http --port 8000`, or the Dockerfile). With HTTP, tokens still come from
the caller's DuckDB secret.

## Support

Open an issue on [Query-farm/vgi-ckan](https://github.com/Query-farm/vgi-ckan/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the [BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

CKAN is a project of the Open Knowledge Foundation. This project is not affiliated with or
endorsed by it or by any portal operator. Data read through this worker remains subject to each
portal's own terms and licenses (see `licenses()` and each dataset's `license_id`).
