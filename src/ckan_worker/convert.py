"""Turn CKAN's loosely typed JSON into typed column values.

CKAN 2.9, 2.10 and 2.11 (and their many plugins) disagree on small things:
timestamps come with or without ``Z``, booleans can be the strings ``"True"`` /
``"False"`` (``license_list`` on 2.11), ``size`` can be ``523`` or ``"523 KiB"``,
and ckanext-scheming / fluent portals put titles and descriptions in
``title_translated`` / ``notes_translated`` dicts instead of ``title`` / ``notes``.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any


def ts(value: Any) -> dt.datetime | None:
    """Parse a CKAN ISO-8601 timestamp; naive values are UTC (CKAN stores UTC)."""
    if value is None or value == "":
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def ts_ms(value: Any) -> dt.datetime | None:
    """Parse like :func:`ts`, truncated to milliseconds.

    The search index keeps only milliseconds, so full records and index-only
    (``fl``) records agree.
    """
    parsed = ts(value)
    return parsed.replace(microsecond=parsed.microsecond // 1000 * 1000) if parsed else None


def boolean(value: Any) -> bool | None:
    """Coerce CKAN booleans, including ``"True"``/``"false"`` strings, keeping NULL."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("true", "t", "yes", "y", "1"):
        return True
    if text in ("false", "f", "no", "n", "0"):
        return False
    return None


def integer(value: Any) -> int | None:
    """Coerce to int when the value is a whole number (``"523 KiB"`` is NULL)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip()
    return int(text) if text.lstrip("-").isdigit() else None


def number(value: Any) -> float | None:
    """Coerce to float, keeping NULL for blanks and non-numbers."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def text(value: Any) -> str | None:
    """Return a string column value: scalars as text, JSON for structures, '' kept."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def blank_to_null(value: Any) -> str | None:
    """Like :func:`text`, but an empty or whitespace-only string is NULL."""
    out = text(value)
    return out if out is not None and out.strip() else None


def translated(item: dict[str, Any], key: str, locale: str | None = None) -> str | None:
    """``item[key]`` or, when blank, the best entry of ``item[key + '_translated']``.

    The best entry is the portal locale's, then English, then the first
    non-empty one (scheming/fluent portals such as open.canada.ca, data.gov.ie).
    """
    plain = blank_to_null(item.get(key))
    if plain is not None:
        return plain
    tr = item.get(f"{key}_translated")
    if isinstance(tr, dict):
        prefs = [p for p in (locale, (locale or "")[:2], "en") if p]
        for p in prefs:
            v = blank_to_null(tr.get(p))
            if v is not None:
                return v
        for v in tr.values():
            if blank_to_null(v) is not None:
                return str(v)
    return None


def names(values: Any, key: str = "name") -> list[str]:
    """Names out of a list of dicts or strings (tags, groups), in order."""
    out: list[str] = []
    for v in values or []:
        name = v.get(key) if isinstance(v, dict) else v
        if name is not None and str(name) != "":
            out.append(str(name))
    return out


def distinct_sorted(values: list[str]) -> list[str]:
    """Distinct non-empty strings, sorted (stable formats lists across API modes)."""
    return sorted({v.strip() for v in values if v and v.strip()})


def json_text(value: Any) -> str | None:
    """Serialize a JSON-able value for a JSON column (``None`` stays NULL)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def extras_map(extras: Any) -> list[tuple[str, str | None]]:
    """CKAN ``extras`` (a list of ``{key, value}``) as MAP entries, sorted by key."""
    out: dict[str, str | None] = {}
    if isinstance(extras, list):
        for e in extras:
            if isinstance(e, dict) and e.get("key") is not None:
                out[str(e["key"])] = text(e.get("value"))
    elif isinstance(extras, dict):
        out = {str(k): text(v) for k, v in extras.items()}
    return sorted(out.items())
