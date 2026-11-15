"""Shared plumbing: which portal a call targets, its API token, and batch emission.

Every function takes the portal as its first positional argument
(``ckan.main.datasets('https://data.gov.ie')``). Each also has an overload
without it that uses the ``portal`` ATTACH option, so a catalog attached to one
portal reads ``ckan.main.datasets()``.

API tokens come only from ``ckan`` secrets whose ``SCOPE`` is the portal URL (or
a prefix of it ending at a path boundary). An unscoped ``ckan`` secret is
ignored, so a token for one portal is never sent to another.
"""

from __future__ import annotations

import copy
import dataclasses
import logging
import typing
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.table_function import OutputCollector, ProcessParams

from .client import SECRET_TYPE, CkanClient, CkanError, normalize_portal

log = logging.getLogger(__name__)

PORTAL_DOC = (
    "Base URL of the CKAN site, e.g. 'https://demo.ckan.org', 'https://data.gov.ie' or "
    "'https://open.canada.ca/data'. A pasted dataset or /api/3 URL is trimmed to the site root."
)
PORTAL_PATTERN = r"^(https?://)?[A-Za-z0-9.-]+"


def attach_options_of(params: Any) -> dict[str, Any]:
    """Return the ATTACH options of this call (empty when attached without any)."""
    try:
        return dict(params.attach_options or {})
    except Exception:
        return {}


def portal_of(params: Any) -> str:
    """Return the normalized portal URL: the ``portal`` argument, else the ATTACH option.

    Args:
        params: Bind or process parameters.

    Returns:
        The canonical portal base URL.

    Raises:
        CkanError: Neither the argument nor the ATTACH option names a portal.

    """
    value = getattr(params.args, "portal", None) or attach_options_of(params).get("portal")
    if not value:
        raise CkanError(
            "no CKAN portal given: pass it as the first argument, e.g. ckan.main.datasets('https://demo.ckan.org'), "
            "or ATTACH with the portal option: ATTACH 'ckan' (TYPE vgi, LOCATION '...', PORTAL 'https://demo.ckan.org')"
        )
    return normalize_portal(str(value))


def _str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "as_py"):
        value = value.as_py()
    return "" if value is None else str(value)


def _scope_matches(scope: str, portal: str) -> int:
    """Length of the longest scope prefix covering ``portal`` at a path boundary, else -1."""
    best = -1
    for prefix in scope.split("\n"):
        p = prefix.strip().rstrip("/")
        if not p:
            continue
        if "://" not in p:
            p = "https://" + p
        if (portal == p or portal.startswith(p + "/")) and len(p) > best:
            best = len(p)
    return best


def token_from(secrets: Any, portal: str) -> str | None:
    """Return the API token of the best-matching *scoped* ``ckan`` secret, or None.

    Args:
        secrets: The resolved secrets (``ProcessParams.secrets``) or a list of secret dicts.
        portal: The normalized portal URL.

    Returns:
        The token, or None for anonymous access.

    """
    best: tuple[int, str] | None = None
    unscoped = False
    candidates = secrets if isinstance(secrets, list) else secrets.of_type(SECRET_TYPE) if secrets else []
    for fields in candidates:
        token = _str(fields.get("api_token"))
        scope = _str(fields.get("scope"))
        if not scope:
            unscoped = unscoped or bool(token)
            continue
        n = _scope_matches(scope, portal)
        if n >= 0 and token and (best is None or n > best[0]):
            best = (n, token)
    if best is None and unscoped:
        log.warning("ignoring a ckan secret without SCOPE; scope it to the portal URL to use it")
    return best[1] if best else None


def request_secret(params: Any) -> bool:
    """Ask DuckDB for the ``ckan`` secret scoped to this call's portal (bind time).

    Args:
        params: The bind parameters.

    Returns:
        True once secrets are resolved (the bind may proceed), False while the
        framework still has to round-trip a secret-scope request to DuckDB.

    """
    portal = portal_of(params)
    params.secrets.get(SECRET_TYPE, scope=portal)
    return bool(params.secrets.all_resolved)


def bind_token(params: Any) -> str | None:
    """Return the token for bind-time API calls (after :func:`request_secret` returned True)."""
    portal = portal_of(params)
    secret = params.secrets.get(SECRET_TYPE, scope=portal)
    return token_from([secret] if secret else [], portal)


def client_for(params: ProcessParams[Any]) -> CkanClient:
    """Return a client for this call's portal, with its scoped token if one exists."""
    portal = portal_of(params)
    return CkanClient(portal, token_from(params.secrets, portal))


def emit_rows(params: ProcessParams[Any], out: OutputCollector, rows: list[dict[str, Any]]) -> pa.RecordBatch:
    """Emit rows (dicts keyed by column) as one batch in the bound, projected schema."""
    schema = params.output_schema
    batch = pa.RecordBatch.from_pydict({f.name: [r.get(f.name) for r in rows] for f in schema}, schema=schema)
    out.emit(batch)
    return batch


def without_portal(args_cls: type, name: str) -> type:
    """Return a copy of a frozen args dataclass without its positional ``portal`` (for the ATTACH overload).

    The remaining positional arguments shift down by one.
    """
    hints = typing.get_type_hints(args_cls, include_extras=True)
    fields: list[tuple[str, Any]] = []
    for f in dataclasses.fields(args_cls):
        if f.name == "portal":
            continue
        hint = hints[f.name]
        base, *extras = typing.get_args(hint)
        shifted = []
        for e in extras:
            if isinstance(e, Arg) and isinstance(e.position, int):
                position: int = e.position
                e = copy.copy(e)
                e.position = position - 1
            shifted.append(e)
        fields.append((f.name, Annotated[(base, *shifted)]))
    made = dataclasses.make_dataclass(name, fields, frozen=True, kw_only=True, slots=True)
    made.__doc__ = f"Arguments of the ATTACH-portal overload of {args_cls.__name__}."
    return made
