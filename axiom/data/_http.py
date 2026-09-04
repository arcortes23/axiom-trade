"""Small standard-library HTTP and timestamp helpers for public adapters."""
from __future__ import annotations

import email.utils
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from ..domain import ensure_utc

JsonOpener = Callable[..., Any]


class HTTPFetchError(RuntimeError):
    """Typed public-endpoint failure used by collector retry policy."""

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status: int | None = None,
        retry_after: float | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.url = str(url)
        self.status = int(status) if status is not None else None
        self.retry_after = float(retry_after) if retry_after is not None else None
        self.retryable = bool(retryable)



def parse_timestamp(value: Any) -> datetime | None:
    """Parse exchange milliseconds/seconds or ISO-8601 into UTC."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value).strip()
            if not text:
                return None
            try:
                number = float(text)
            except ValueError:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                return ensure_utc(datetime.fromisoformat(text))
        if not math.isfinite(number):
            return None
        if abs(number) > 100_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
def _retry_after(headers: Any) -> float | None:
    value = None
    if headers is not None:
        getter = getattr(headers, "get", None)
        if callable(getter):
            value = getter("Retry-After") or getter("retry-after")
    if value is None:
        return None
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            parsed = email.utils.parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed is None:
            return None
        delay = (ensure_utc(parsed) - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delay) if math.isfinite(delay) else None
def query_url(base_url: str, path: str, params: Mapping[str, Any] | None = None) -> str:
    """Join a public endpoint path and encode non-null query parameters."""
    url = str(base_url).rstrip("/") + "/" + str(path).lstrip("/")
    values = {str(key): value for key, value in (params or {}).items() if value is not None}
    if values:
        return url + "?" + urllib.parse.urlencode(values, doseq=True)
    return url



def fetch_json_strict(
    url: str,
    timeout: float,
    opener: JsonOpener | None = None,
) -> Any:
    """Fetch JSON or raise a typed error retaining status and Retry-After."""
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "axiom-platform/0.1"},
    )
    open_fn = opener or urllib.request.urlopen
    try:
        response = open_fn(request, timeout=timeout)
        try:
            status = getattr(response, "status", None)
            if status is None:
                getcode = getattr(response, "getcode", None)
                status = getcode() if callable(getcode) else None
            headers = getattr(response, "headers", None)
            if status is not None and int(status) >= 400:
                raise HTTPFetchError(
                    f"HTTP {int(status)} from {url}",
                    url=url,
                    status=int(status),
                    retry_after=_retry_after(headers),
                    retryable=int(status) == 429 or int(status) >= 500,
                )
            raw = response.read()
        finally:
            close = getattr(response, "close", None)
            if close is not None:
                close()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except HTTPFetchError:
        raise
    except urllib.error.HTTPError as exc:
        status = int(exc.code) if exc.code is not None else None
        try:
            raise HTTPFetchError(
                f"HTTP {status or 'error'} from {url}",
                url=url,
                status=status,
                retry_after=_retry_after(getattr(exc, "headers", None)),
                retryable=status == 429 or (status is not None and status >= 500),
            ) from exc
        finally:
            close = getattr(exc, "close", None)
            if close is not None:
                close()
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        raise HTTPFetchError(
            f"network failure from {url}: {exc}",
            url=url,
            retryable=True,
        ) from exc
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise HTTPFetchError(
            f"invalid JSON from {url}: {exc}",
            url=url,
            retryable=False,
        ) from exc


def fetch_json(
    url: str,
    timeout: float,
    opener: JsonOpener | None = None,
) -> Any | None:
    """Fetch and decode JSON, returning ``None`` for unavailable endpoints."""
    try:
        return fetch_json_strict(url, timeout, opener)
    except HTTPFetchError:
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
