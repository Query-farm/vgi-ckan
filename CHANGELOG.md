# Changelog

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.

## 0.1.1

- Fix: every query whose filter is pushed down to the worker (a `WHERE` clause, or
  `ORDER BY … LIMIT`) failed with "No DuckDB-compatible engine is installed". The SDK binds
  and evaluates pushed predicates with a local engine, which was only in the dev
  dependency group; the runtime dependency is now `vgi-python[http,haybarn]`.

## 0.1.0

Initial release: any CKAN open-data portal (CKAN 2.9 / 2.10 / 2.11) as SQL.

- Catalog: `datasets`, `dataset`, `resources` with Solr `fq` / `fl` / sort pushdown.
- Directory: `organizations`, `groups`, `tags`, `facets`, `licenses`, `portal_info`, `recent_changes`.
- DataStore: `datastore` (typed columns, filters / fields / sort pushdown), `datastore_sql`,
  `resource_data`, and the `read_sql` scalar for plain files.
- `PORTAL` ATTACH option with portal-less overloads; `ckan` secrets scoped per portal URL.
- Tests replay responses recorded from demo.ckan.org, open.canada.ca, data.gov.ie and
  data.qld.gov.au through a strict replay server.

Built from vgi-starter-python v1.3.0.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
