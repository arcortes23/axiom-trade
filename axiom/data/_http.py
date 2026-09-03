"""Small standard-library HTTP and timestamp helpers for public adapters."""
from __future__ import annotations

import json
import math
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from ..domain import ensure_utc

JsonOpener = Callable[..., Any]


def parse_timestamp(value: Any) -> datetime | None:
    """Parse exchange milliseconds/seconds or ISO-8601 into UTC."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            number = float(value)
            # Unix timestamps above this threshold are conventionally millis.
            if abs(number) > 100_000_000_000:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=timezone.utc)
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return parse_timestamp(int(text))
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return ensure_utc(parsed)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def iso_or_none(value: datetime | None) -> str | None:
    return ensure_utc(value).isoformat() if value is not None else None


def query_url(base: str, path: str, params: Mapping[str, Any] | None = None) -> str:
    url = base.rstrip("/") + "/" + path.lstrip("/")
    if params:
        clean = {key: value for key, value in params.items() if value is not None}
        if clean:
            return url + "?" + urllib.parse.urlencode(clean, doseq=True)
    return url


def fetch_json(
    url: str,
    timeout: float,
    opener: JsonOpener | None = None,
) -> Any | None:
    """Fetch and decode JSON, returning ``None`` for unavailable endpoints.

    A timeout is always supplied, including when a test or application injects
    an opener.  No response data is manufactured on network or JSON errors.
    """
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "axiom-platform/0.1"},
    )
    open_fn = opener or urllib.request.urlopen
    try:
        response = open_fn(request, timeout=timeout)
        # urllib responses are context managers; simple test doubles often are
        # not, so close explicitly only when available.
        try:
            raw = response.read()
        finally:
            close = getattr(response, "close", None)
            if close is not None:
                close()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except (OSError, urllib.error.URLError, TimeoutError, ValueError, TypeError, json.JSONDecodeError):
        return None

def as_float(value: Any) -> float | None:
    try:
        number = float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None
    return number if number is not None and math.isfinite(number) else None


def as_int(value: Any) -> int | None:
    try:
        number = int(value) if value is not None and value != "" else None
    except (TypeError, ValueError, OverflowError):
        return None
    return number


def decode_jsonish(value: Any) -> Any:
    """Decode Gamma fields that are either native values or JSON strings."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return value
