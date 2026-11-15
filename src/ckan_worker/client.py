"""A small, dependency-free client for the CKAN Action API v3 (read-only).

Every CKAN site answers ``GET <portal>/api/3/action/<action>?<params>`` with
``{"success": true, "result": ...}`` or ``{"success": false, "error": {...}}``.
Older and newer releases differ in details (HTTP status codes, plain-string
``400`` bodies for unknown actions, string booleans in ``license_list``); this
module turns all of them into a result or a :class:`CkanError` with a message a
SQL user can act on.

Credentials: an optional API token from a ``ckan`` DuckDB secret whose ``SCOPE``
is a prefix of the portal URL. CKAN expects the raw token in the
``Authorization`` header (no ``Bearer``). The token is passed in per call and
never stored in table-function state.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "ckan"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
# Rate limits and gateway hiccups. A 500 from CKAN is an application error
# (e.g. CKAN 2.11 answers datastore_search on a non-DataStore resource with 500)
# and is reported at once.
RETRY_STATUSES = {429, 502, 503, 504}
USER_AGENT = "vgi-ckan-worker/0.1 (+https://github.com/Query-farm/vgi-ckan)"
SECRETS_DOC_URL = "https://github.com/Query-farm/vgi-ckan#private-datasets-api-tokens"

# CREATE SECRET (TYPE ckan, API_TOKEN '...', SCOPE 'https://data.example.org');
CKAN_SECRET = SecretTypeSpec(
    name=SECRET_TYPE,
    description=(
        "CKAN API token for one portal (scope the secret to the portal URL). Only needed for private "
        "datasets and private DataStore tables; public portals need no secret."
    ),
    schema=pa.schema(
        [
            pa.field(
                "api_token",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": "CKAN API token (user settings -> API tokens), sent as the Authorization header.",
                },
            )
        ],
        metadata={"vgi_doc_url": SECRETS_DOC_URL},
    ),
)


class CkanError(RuntimeError):
    """The portal returned an error (or something that is not the CKAN API)."""


class NotFoundError(CkanError):
    """The dataset, resource or object does not exist (or is not visible)."""


class ActionNotAvailableError(CkanError):
    """The portal does not offer this action (plugin not enabled, or older CKAN)."""


def normalize_portal(portal: str) -> str:
    """Return the portal's base URL: scheme + host + path, no trailing slash or ``/api/3/...``.

    Users paste all sorts of URLs (``https://data.gov.ie/``,
    ``https://open.canada.ca/data/en/dataset``, ``.../api/3/action``); the
    action API always lives at ``<base>/api/3/action``.

    Args:
        portal: The URL as written in SQL.

    Returns:
        The canonical base URL.

    Raises:
        CkanError: The value is not an http(s) URL.

    """
    raw = (portal or "").strip()
    if not raw:
        raise CkanError("portal is required: pass the CKAN site URL, e.g. 'https://demo.ckan.org'")
    if "://" not in raw:
        raw = "https://" + raw
    parts = urllib.parse.urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise CkanError(f"portal must be an http(s) URL, got {portal!r}")
    path = parts.path.rstrip("/")
    for marker in ("/api/3", "/api/action", "/api"):
        idx = path.find(marker)
        if idx >= 0 and (len(path) == idx + len(marker) or path[idx + len(marker)] == "/"):
            path = path[:idx]
            break
    # Strip a localised UI suffix like /en/dataset/... (CKAN UI path, not the API root).
    for ui in ("/dataset", "/organization", "/group"):
        idx = path.find(ui)
        if idx >= 0:
            path = path[:idx]
            break
    head, _, last = path.rpartition("/")
    if last.lower() in _LOCALES:
        # a trailing locale such as /en or /pt_BR left over from a UI link
        path = head
    return f"{parts.scheme}://{parts.netloc}{path}"


_LOCALES = {"en", "fr", "de", "es", "it", "nl", "pt", "ga", "cy", "pt_br", "en_gb", "en_au", "zh_cn"}


@dataclass(frozen=True)
class CkanClient:
    """Calls actions on one portal, optionally with an API token."""

    portal: str
    token: str | None = None

    def action_url(self, action: str, params: dict[str, Any] | None = None) -> str:
        """Build the GET URL for an action, encoding list/dict values as JSON like CKAN expects."""
        query: list[tuple[str, str]] = []
        for key, value in sorted((params or {}).items()):
            if value is None:
                continue
            if isinstance(value, bool):
                query.append((key, "true" if value else "false"))
            elif isinstance(value, list | dict):
                query.append((key, json.dumps(value, separators=(",", ":"), sort_keys=True)))
            else:
                query.append((key, str(value)))
        qs = urllib.parse.urlencode(query, quote_via=urllib.parse.quote)
        return f"{self.portal}/api/3/action/{action}" + (f"?{qs}" if qs else "")

    def call(self, action: str, params: dict[str, Any] | None = None) -> Any:
        """Call a read-only action and return its ``result``.

        Args:
            action: Action name, e.g. ``package_search``.
            params: Query parameters (lists/dicts are JSON-encoded).

        Returns:
            The decoded ``result`` value.

        Raises:
            CkanError: The portal reported an error or did not answer like CKAN.
            AssertionError: Never: the retry loop always returns or raises.

        """
        url = self.action_url(action, params)
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = self.token
        log.debug("GET %s", url)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                    return _unwrap(action, url, response.status, response.read())
            except urllib.error.HTTPError as err:
                body = err.read()
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    time.sleep(_backoff(err.headers.get("Retry-After"), attempt))
                    continue
                return _unwrap(action, url, err.code, body)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise CkanError(f"{action}: cannot reach {self.portal} ({reason})") from err
                time.sleep(_backoff(None, attempt))
        raise AssertionError("unreachable")


def _backoff(retry_after: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt + 1``: ``Retry-After`` if given, else exponential."""
    if retry_after:
        try:
            return float(min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS))
        except ValueError:
            pass
    return float(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))


def _unwrap(action: str, url: str, status: int, raw: bytes) -> Any:
    """Decode one action response into its result, or raise a typed error."""
    text = raw.decode("utf-8", errors="replace")
    try:
        body = json.loads(text)
    except ValueError:
        body = None
    if isinstance(body, dict) and body.get("success") is True and "result" in body:
        return body["result"]
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err = body["error"]
        etype = str(err.get("__type") or "Error")
        detail = {k: v for k, v in err.items() if k != "__type"}
        message = detail.pop("message", None) or "; ".join(f"{k}: {v}" for k, v in sorted(detail.items()))
        text_msg = f"{action} failed (HTTP {status}, {etype}): {message}"
        if etype == "Not Found Error" or status == 404:
            raise NotFoundError(text_msg)
        if etype == "Authorization Error" or status == 403:
            raise CkanError(
                text_msg + ". This needs an API token: CREATE SECRET (TYPE ckan, API_TOKEN '...', SCOPE '<portal>')"
            )
        raise CkanError(text_msg)
    if isinstance(body, str) and "Action name not known" in body:
        # CKAN answers an unknown action with a JSON string and HTTP 400.
        raise ActionNotAvailableError(f"{action} is not available on this portal ({body.strip()})")
    snippet = " ".join(text.split())[:200]
    if status == 404:
        raise CkanError(f"{action}: {url} is not a CKAN Action API endpoint (HTTP 404). Check the portal URL.")
    raise CkanError(f"{action} failed (HTTP {status}): {snippet or 'empty response'}")
