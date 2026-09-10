"""Read-only Polymarket Gamma and CLOB adapter.

Gamma supplies market identity, question, rules, tags and lifecycle fields;
the CLOB supplies token prices, price history and displayed depth.  The
adapter never submits orders.  Public endpoints may be unavailable from an
offline environment, in which case empty/``None`` values are returned without
fabricating probabilities or settlement outcomes.
"""
from __future__ import annotations

import hashlib
import json
import math
import urllib.parse

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Callable, Mapping, Sequence

from ..domain import (
    InstrumentMetadata,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    Side,
    TradePrint,
    ensure_utc,
)
from ._http import HTTPFetchError, as_float, decode_jsonish, fetch_json_strict, parse_timestamp, query_url
from .interfaces import PredictionMarketDataProvider


@dataclass(frozen=True, slots=True)
class MarketDiscoveryPage:
    """One read-only Gamma keyset page and its request accounting."""

    snapshots: tuple[PredictionMarketSnapshot, ...]
    next_cursor: str | None
    request_path: str
    query: Mapping[str, Any]
    query_fingerprint: str
    raw_count: int
    unique_count: int
    duplicate_count: int
    malformed_count: int
    coverage_status: str
    error_reason: str | None = None


_EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)


class PolymarketPayloadError(ValueError):
    """Base class for malformed official Gamma/CLOB payloads."""


class PolymarketIdentityError(PolymarketPayloadError):
    """A Gamma market identity is missing or internally inconsistent."""


class PolymarketTokenMappingError(PolymarketPayloadError):
    """Gamma outcomes and CLOB tokens are not an aligned YES/NO pair."""


class PolymarketBookIdentityError(PolymarketPayloadError):
    """A CLOB book identifies a different asset or condition."""


class PolymarketAdapter(PredictionMarketDataProvider):
    """Polymarket public Gamma/CLOB adapter.

    ``base_url`` and ``clob_url`` can point at fixture servers.  Both services
    use the same explicit timeout and injected ``opener`` as BinanceAdapter,
    which makes offline failure behavior deterministic in research jobs.
    """

    provider_name = "polymarket"

    def __init__(
        self,
        *,
        gamma_url: str = "https://gamma-api.polymarket.com",
        data_api_url: str = "https://data-api.polymarket.com",
        clob_url: str = "https://clob.polymarket.com",
        base_url: str | None = None,
        timeout: float = 10.0,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        timeout_value = float(timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        self.gamma_url = str(base_url or gamma_url).rstrip("/")
        self.data_api_url = str(data_api_url).rstrip("/")
        self.clob_url = str(clob_url).rstrip("/")
        self.timeout = timeout_value
        self._opener = opener
        self._raw_cache: dict[str, Mapping[str, Any]] = {}
        self._token_context: dict[str, tuple[str, str]] = {}
        self._validation_errors: list[PolymarketPayloadError] = []
        self._transport_errors: list[HTTPFetchError] = []
        self._provider_timestamps: dict[tuple[str, str], datetime | None] = {}
        self._book_provider_timestamps: dict[str, datetime | None] = {}
        self._trade_provenance: dict[str, Mapping[str, Any]] = {}
        self._last_trades_complete = True
        self._last_trade_cursor: str | None = None
        self._last_trade_query: Mapping[str, Any] = {}

    def isolated_worker_factory(self) -> Callable[[], "PolymarketAdapter"]:
        """Return a factory for workers with independent mutable adapter state."""
        gamma_url, data_api_url, clob_url, timeout, opener = (
            self.gamma_url,
            self.data_api_url,
            self.clob_url,
            self.timeout,
            self._opener,
        )

        def create() -> "PolymarketAdapter":
            return PolymarketAdapter(
                gamma_url=gamma_url,
                data_api_url=data_api_url,
                clob_url=clob_url,
                timeout=timeout,
                opener=opener,
            )

        return create

    def provider_timestamp_for(self, market_id: str, kind: str = "market") -> datetime | None:
        if kind in {"yes_order_book", "no_order_book", "order_book"}:
            tokens = self.token_ids(str(market_id))
            token = tokens.get("yes" if kind == "yes_order_book" else "no" if kind == "no_order_book" else "yes")
            return self._book_provider_timestamps.get(str(token)) if token else None
        return self._provider_timestamps.get((str(kind), str(market_id)))

    def consume_validation_errors(self) -> tuple[PolymarketPayloadError, ...]:
        errors = tuple(self._validation_errors)
        self._validation_errors.clear()
        return errors

    def consume_transport_errors(self) -> tuple[HTTPFetchError, ...]:
        errors = tuple(self._transport_errors)
        self._transport_errors.clear()
        return errors

    def _gamma_get(self, path: str, **params: Any) -> Any | None:
        try:
            return fetch_json_strict(query_url(self.gamma_url, path, params), self.timeout, self._opener)
        except HTTPFetchError as exc:
            self._transport_errors.append(exc)
            return None

    def _clob_get(self, path: str, **params: Any) -> Any | None:
        try:
            return fetch_json_strict(query_url(self.clob_url, path, params), self.timeout, self._opener)
        except HTTPFetchError as exc:
            self._transport_errors.append(exc)
            return None
    def _data_get(self, path: str, **params: Any) -> Any | None:
        try:
            return fetch_json_strict(query_url(self.data_api_url, path, params), self.timeout, self._opener)
        except HTTPFetchError as exc:
            self._transport_errors.append(exc)
            return None

    @staticmethod
    def _keyset_market_limit(limit: int | None) -> int:
        return min(100, limit) if limit is not None else 100

    def markets(self, active: bool = True, *, limit: int | None = None) -> Sequence[PredictionMarketSnapshot]:
        """Return a bounded keyset-paginated Gamma market inventory."""
        if not isinstance(active, bool):
            raise ValueError("active must be a boolean")
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer")
        if limit == 0:
            return []
        page_size = self._keyset_market_limit(limit)
        result: list[PredictionMarketSnapshot] = []
        seen_ids: set[str] = set()
        cursor: str | None = None
        for _ in range(100):
            page = self.market_page(page_size, after_cursor=cursor, closed=not active)
            if page.coverage_status == "ERROR":
                break
            for snapshot in page.snapshots:
                if snapshot.market_id in seen_ids:
                    continue
                seen_ids.add(snapshot.market_id)
                result.append(snapshot)
                if limit is not None and len(result) >= limit:
                    return result
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return result

    def resolve_tag_slug(self, slug: str) -> int | None:
        """Resolve an exact Gamma tag slug to its documented numeric id."""
        normalized = str(slug).strip()
        if not normalized:
            return None
        payload = self._gamma_get("/tags/slug/" + urllib.parse.quote(normalized, safe=""))
        if not isinstance(payload, Mapping) or payload.get("slug") != normalized:
            return None
        raw_id = payload.get("id")
        if isinstance(raw_id, bool):
            return None
        if isinstance(raw_id, int):
            return raw_id if raw_id > 0 else None
        if not isinstance(raw_id, str) or not raw_id.isdecimal():
            return None
        identifier = int(raw_id)
        return identifier if identifier > 0 else None

    def market_page(
        self,
        limit: int,
        after_cursor: str | None = None,
        closed: bool = False,
        tag_ids: Sequence[int] = (),
        include_tag: bool = True,
        liquidity_num_min: int | float | None = None,
        end_date_min: Any | None = None,
        end_date_max: Any | None = None,
    ) -> MarketDiscoveryPage:
        """Fetch one Gamma keyset page without broadening the requested scope."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        if not isinstance(closed, bool):
            raise ValueError("closed must be a boolean")
        if not isinstance(include_tag, bool):
            raise ValueError("include_tag must be a boolean")
        if after_cursor is not None and (
            not isinstance(after_cursor, str) or not after_cursor
        ):
            raise ValueError("after_cursor must be an opaque non-empty string")
        try:
            raw_tag_ids = tuple(tag_ids)
        except TypeError as exc:
            raise ValueError("tag_ids must be a sequence of positive integers") from exc
        normalized_tags: list[str] = []
        for tag_id in raw_tag_ids:
            if isinstance(tag_id, bool) or not isinstance(tag_id, int) or tag_id <= 0:
                raise ValueError("tag_ids must be a sequence of positive integers")
            normalized_tags.append(str(tag_id))

        request_path = "/markets/keyset"
        query: dict[str, Any] = {
            "limit": str(limit),
            "closed": str(closed).lower(),
            "include_tag": str(include_tag).lower(),
        }
        if after_cursor is not None:
            query["after_cursor"] = after_cursor
        if normalized_tags:
            query["tag_id[]"] = tuple(normalized_tags)
        if liquidity_num_min is not None:
            query["liquidity_num_min"] = _normalized_number(
                liquidity_num_min, "liquidity_num_min", minimum=0.0
            )
        if end_date_min is not None:
            query["end_date_min"] = _normalized_date_filter(end_date_min, "end_date_min")
        if end_date_max is not None:
            query["end_date_max"] = _normalized_date_filter(end_date_max, "end_date_max")
        fingerprint = _market_query_fingerprint(request_path, query)
        payload = self._gamma_get(request_path, **query)
        if not isinstance(payload, Mapping):
            return MarketDiscoveryPage(
                snapshots=(),
                next_cursor=None,
                request_path=request_path,
                query=query,
                query_fingerprint=fingerprint,
                raw_count=0,
                unique_count=0,
                duplicate_count=0,
                malformed_count=1,
                coverage_status="ERROR",
            )
        records = payload.get("markets")
        if not isinstance(records, list):
            return MarketDiscoveryPage(
                snapshots=(),
                next_cursor=None,
                request_path=request_path,
                query=query,
                query_fingerprint=fingerprint,
                raw_count=0,
                unique_count=0,
                duplicate_count=0,
                malformed_count=1,
                coverage_status="ERROR",
            )
        returned_cursor = payload.get("next_cursor")
        if returned_cursor is not None and not isinstance(returned_cursor, str):
            return MarketDiscoveryPage(
                snapshots=(),
                next_cursor=None,
                request_path=request_path,
                query=query,
                query_fingerprint=fingerprint,
                raw_count=len(records),
                unique_count=0,
                duplicate_count=0,
                malformed_count=1,
                coverage_status="ERROR",
            )
        next_cursor = returned_cursor or None
        snapshots: list[PredictionMarketSnapshot] = []
        seen_ids: set[str] = set()
        duplicate_count = 0
        malformed_count = 0
        for record in records:
            if not isinstance(record, Mapping):
                malformed_count += 1
                continue
            raw_identifier = record.get(
                "id", record.get("market_id", record.get("conditionId"))
            )
            if raw_identifier is not None and str(raw_identifier) in seen_ids:
                duplicate_count += 1
                continue
            try:
                snapshot = self._snapshot(record)
            except (TypeError, ValueError, KeyError):
                snapshot = None
            if snapshot is None:
                malformed_count += 1
                continue
            if snapshot.market_id in seen_ids:
                duplicate_count += 1
                continue
            seen_ids.add(snapshot.market_id)
            snapshots.append(snapshot)
        coverage_status = "PARTIAL" if next_cursor is not None else "COMPLETE"
        error_reason: str | None = None
        if after_cursor is not None and next_cursor == after_cursor:
            # A repeated opaque cursor cannot make progress.  Stop without
            # handing the caller a continuation that would repeat forever.
            next_cursor = None
            coverage_status = "ERROR"
            error_reason = "REPEATED_CURSOR"
        return MarketDiscoveryPage(
            snapshots=tuple(snapshots),
            next_cursor=next_cursor,
            request_path=request_path,
            query=query,
            query_fingerprint=fingerprint,
            raw_count=len(records),
            unique_count=len(snapshots),
            duplicate_count=duplicate_count,
            malformed_count=malformed_count,
            coverage_status=coverage_status,
            error_reason=error_reason,
        )


    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        identifier = _text(market_id)
        if identifier is None:
            return None
        self._provider_timestamps.pop(("market", identifier), None)
        payload = self._gamma_get("/markets/" + urllib.parse.quote(identifier, safe=""))
        if not isinstance(payload, Mapping):
            return None
        self._remember(payload)
        return self._snapshot(payload, expected_market_id=identifier)

    def price_history(
        self,
        market_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        token_id = self._yes_token(market_id)
        if token_id is None:
            return []
        payload = self._clob_get(
            "/prices-history",
            market=token_id,
            interval="max",
            startTs=int(ensure_utc(start).timestamp()) if start is not None else None,
            endTs=int(ensure_utc(end).timestamp()) if end is not None else None,
        )
        if isinstance(payload, Mapping):
            expected_condition, _ = self._token_context.get(token_id, (None, None))
            try:
                asset = _present_text(payload, "asset_id", "assetId", "token_id")
                returned_condition = _present_text(payload, "market", "condition_id", "conditionId")
            except PolymarketBookIdentityError as exc:
                self._validation_errors.append(exc)
                return []
            if asset is not None and asset != token_id:
                self._validation_errors.append(PolymarketBookIdentityError("CLOB history asset does not match token"))
                return []
            if returned_condition is not None and expected_condition is not None and returned_condition != expected_condition:
                self._validation_errors.append(PolymarketBookIdentityError("CLOB history market does not match conditionId"))
                return []
            payload = payload.get("history", payload.get("data", []))
        if not isinstance(payload, list):
            return []
        result: list[dict[str, Any]] = []
        for point in payload:
            if isinstance(point, Mapping):
                stamp = parse_timestamp(point.get("t", point.get("timestamp", point.get("time"))))
                raw_price = point.get("p", point.get("price", point.get("value")))
                price = None if isinstance(raw_price, bool) else as_float(raw_price)
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                price = None if isinstance(point[1], bool) else as_float(point[1])
                stamp = parse_timestamp(point[0])
            else:
                continue
            if stamp is not None and price is not None and 0.0 <= price <= 1.0:
                result.append({"timestamp": stamp, "price": price, "token_id": token_id})
        result.sort(key=lambda item: item["timestamp"])
        return result

    def token_ids(self, market_id: str) -> Mapping[str, str]:
        """Return the official aligned Gamma outcome/token mapping.

        Gamma publishes ``outcomes`` and ``clobTokenIds`` as parallel arrays.
        The adapter never assigns token position by assumption: both arrays
        must contain exactly one YES and one NO entry.
        """
        identifier = str(market_id).strip()
        raw = self._raw_cache.get(identifier)
        if raw is None and identifier:
            self.market(identifier)
            raw = self._raw_cache.get(identifier)
        if not isinstance(raw, Mapping):
            return {}
        try:
            market_key, condition_id = _market_identity(raw)
            mapping = _token_mapping(raw)
            if market_key != identifier:
                raise PolymarketIdentityError("Gamma market id does not match requested market")
        except PolymarketPayloadError as exc:
            self._validation_errors.append(exc)
            return {}
        for outcome, token in mapping.items():
            self._token_context[token] = (condition_id, market_key)
        return mapping

    def order_book_for_token(self, token_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        token = _text(token_id)
        if token is None:
            return None
        self._book_provider_timestamps.pop(token, None)
        payload = self._clob_get("/book", token_id=token)
        if not isinstance(payload, Mapping):
            return None
        try:
            expected_condition, expected_market = self._token_context.get(token, (None, None))
            asset = _present_text(payload, "asset_id", "assetId", "token_id")
            if asset is not None and asset != token:
                raise PolymarketBookIdentityError("CLOB asset_id does not match requested token")
            returned_condition = _present_text(payload, "market", "condition_id", "conditionId")
            if returned_condition is not None and expected_condition is not None and returned_condition != expected_condition:
                raise PolymarketBookIdentityError("CLOB market does not match Gamma conditionId")
            if "bids" not in payload or "asks" not in payload:
                return None
            bids = self._levels(payload.get("bids"), reverse=True, depth=depth)
            asks = self._levels(payload.get("asks"), reverse=False, depth=depth)
            timestamp_raw = payload.get("timestamp", payload.get("ts", payload.get("time")))
            timestamp = parse_timestamp(timestamp_raw)
            if timestamp_raw not in (None, "") and timestamp is None:
                raise ValueError("CLOB timestamp is malformed")
            min_order_size = _optional_number(payload, "min_order_size", "minOrderSize", positive=True)
            tick_size = _optional_number(payload, "tick_size", "tickSize", positive=True)
            neg_risk = _optional_bool(payload, "neg_risk", "negRisk")
            book_hash = _present_text(payload, "hash", "book_hash")
            snapshot = OrderBookSnapshot(
                timestamp=timestamp or _EPOCH,
                provider_timestamp=timestamp,
                bids=tuple(bids),
                asks=tuple(asks),
                token_id=token,
                condition_id=returned_condition or expected_condition,
                book_hash=book_hash,
                min_order_size=min_order_size,
                tick_size=tick_size,
                neg_risk=neg_risk,
                available=True,
                source=self.provider_name,
            )
        except (PolymarketPayloadError, TypeError, ValueError) as exc:
            self._validation_errors.append(
                exc if isinstance(exc, PolymarketPayloadError) else PolymarketPayloadError(str(exc))
            )
            return None
        self._book_provider_timestamps[token] = timestamp
        return snapshot

    def order_book(self, market_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        token_id = self._yes_token(market_id)
        return self.order_book_for_token(token_id, depth=depth) if token_id else None

    def order_books(self, market_id: str, depth: int = 20) -> Mapping[str, OrderBookSnapshot]:
        tokens = self.token_ids(market_id)
        result: dict[str, OrderBookSnapshot] = {}
        for outcome in ("yes", "no"):
            token = tokens.get(outcome)
            if token:
                book = self.order_book_for_token(token, depth=depth)
                if book is not None:
                    result[outcome] = book
        return result

    @property
    def public_trade_history_available(self) -> bool:
        """Public Data API market prints are available without credentials."""
        return True

    @property
    def last_trade_cursor(self) -> str | None:
        return self._last_trade_cursor

    @property
    def last_trades_complete(self) -> bool:
        return self._last_trades_complete

    def trade_provenance(self, trade: TradePrint) -> Mapping[str, Any]:
        return dict(self._trade_provenance.get(str(trade.trade_id), {}))

    def trades(
        self,
        market_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        *,
        max_pages: int = 100,
        cursor: str | None = None,
    ) -> Sequence[TradePrint]:
        """Read public market trades from the credential-free Data API."""
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
        identifier = _text(market_id)
        if identifier is None:
            raise ValueError("market_id is required")
        start_utc = ensure_utc(start) if start is not None else None
        end_utc = ensure_utc(end) if end is not None else None
        if start_utc is not None and end_utc is not None and start_utc > end_utc:
            raise ValueError("trade start must not be after end")
        cursor_text = str(cursor).strip() if cursor is not None else ""
        window_end = end_utc
        offset_text = cursor_text
        coverage_gap = False
        resume_overlap = False
        overlap_fingerprint: str | None = None
        gap_boundary: datetime | None = None
        if cursor_text.startswith("gap:"):
            try:
                gap_boundary = datetime.fromtimestamp(
                    float(cursor_text.split(":", 1)[1]), tz=timezone.utc
                )
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise ValueError("trade cursor gap is malformed") from exc
            self._last_trades_complete = False
            self._last_trade_cursor = cursor_text
            return ()
        if cursor_text.startswith("window:"):
            parts = cursor_text.split(":")
            if len(parts) not in (3, 6) or (len(parts) == 6 and parts[3] != "partial"):
                raise ValueError("trade cursor window is malformed")
            try:
                window_end = datetime.fromtimestamp(float(parts[1]), tz=timezone.utc)
                offset_text = parts[2]
                if len(parts) == 6:
                    coverage_gap = True
                    resume_overlap = parts[4] != "-"
                    overlap_fingerprint = parts[4] if resume_overlap else None
                    gap_boundary = datetime.fromtimestamp(float(parts[5]), tz=timezone.utc)
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise ValueError("trade cursor window is malformed") from exc
        try:
            offset = int(offset_text) if offset_text else 0
        except (TypeError, ValueError) as exc:
            raise ValueError("trade cursor must be a non-negative offset") from exc
        if offset < 0 or offset > 10_000:
            raise ValueError("trade cursor offset must be in [0,10000]")
        if (
            start_utc is not None
            and window_end is not None
            and start_utc > window_end
            and not coverage_gap
        ):
            self._last_trades_complete = True
            self._last_trade_cursor = None
            return ()
        request_start = start_utc
        if coverage_gap and request_start is not None and window_end is not None and request_start > window_end:
            # A continuation window intentionally backfills below the latest
            # stored print; the cursor is the authoritative lower boundary.
            request_start = None
        snapshot = self._raw_cache.get(identifier)
        condition_id: str | None = None
        if isinstance(snapshot, Mapping):
            condition_id = _text(snapshot.get("conditionId", snapshot.get("condition_id")))
        if condition_id is None:
            loaded = self.market(identifier)
            condition_id = loaded.condition_id if loaded is not None else None
        # Data API validates market as a 0x-prefixed 64-hex condition id.
        if condition_id is None or re.fullmatch(r"0x[0-9a-fA-F]{64}", condition_id) is None:
            self._last_trades_complete = True
            self._last_trade_cursor = None
            return ()
        limit = 1000
        rows: list[TradePrint] = []
        seen: set[str] = set()
        complete = not coverage_gap
        next_offset = offset
        continuation_cursor: str | None = None
        window_first_fingerprint: str | None = None
        if coverage_gap and gap_boundary is None:
            raise ValueError("trade cursor partial window is missing its gap boundary")
        for _ in range(max_pages):
            # Track valid timestamps for this page only.  The offset cap uses
            # the oldest valid print as its inclusive continuation boundary;
            # carrying timestamps across pages could skip a same-second fill.
            page_timestamps: list[datetime] = []
            page_identities: list[str] = []
            query: dict[str, Any] = {
                "limit": limit,
                "offset": next_offset,
                "takerOnly": "true",
                "market": condition_id,
            }
            if request_start is not None:
                query["start"] = max(0, int(request_start.timestamp()))
            if window_end is not None:
                query["end"] = max(0, int(window_end.timestamp()))
            self._last_trade_query = dict(query)
            payload = self._data_get("/trades", **query)
            if payload is None:
                complete = False
                break
            if isinstance(payload, Mapping):
                payload = payload.get("trades", payload.get("data", []))
            if not isinstance(payload, list):
                self._validation_errors.append(PolymarketPayloadError("Data API trades payload is not an array"))
                complete = False
                break
            for raw in payload:
                if not isinstance(raw, Mapping):
                    self._validation_errors.append(PolymarketPayloadError("Data API trade row is malformed"))
                    continue
                returned_condition = _text(raw.get("conditionId", raw.get("condition_id")))
                if returned_condition is not None and returned_condition != condition_id:
                    self._validation_errors.append(
                        PolymarketBookIdentityError("Data API trade conditionId does not match requested market")
                    )
                    continue
                timestamp = parse_timestamp(raw.get("timestamp", raw.get("time")))
                if timestamp is None:
                    try:
                        timestamp = datetime.fromtimestamp(float(raw.get("timestamp")), tz=timezone.utc)
                    except (TypeError, ValueError, OverflowError):
                        timestamp = None
                price = as_float(raw.get("price"))
                size = as_float(raw.get("size", raw.get("quantity")))
                side_text = _text(raw.get("side"))
                side = side_text.lower() if side_text and side_text.upper() in {"BUY", "SELL"} else None
                if timestamp is None or price is None or size is None or price <= 0 or size <= 0 or side is None:
                    self._validation_errors.append(PolymarketPayloadError("Data API trade row is invalid"))
                    continue
                page_timestamps.append(timestamp)
                token_id = _text(raw.get("asset")) or _text(raw.get("token_id"))
                source_identity = _trade_source_identity(raw)
                identity = _trade_identity(
                    condition_id=condition_id,
                    raw=raw,
                    timestamp=timestamp,
                    token_id=token_id,
                    side=side,
                    price=price,
                    size=size,
                )
                page_identities.append(identity)
                if identity in seen:
                    continue
                seen.add(identity)
                trade = TradePrint(
                    timestamp=timestamp,
                    price=price,
                    size=size,
                    side=side,
                    trade_id=identity,
                    market_id=identifier,
                    token_id=token_id,
                )
                rows.append(trade)
                self._trade_provenance.setdefault(
                    identity,
                    {
                        "source_type": "FORWARD_COLLECTED",
                        "provider": self.provider_name,
                        "endpoint": "/trades",
                        "query": dict(query),
                        "condition_id": condition_id,
                        "source_identity": source_identity,
                        "response_timestamp": timestamp.isoformat(),
                    },
                )
            page_fingerprint = _trade_page_fingerprint(page_identities)
            if window_first_fingerprint is None:
                window_first_fingerprint = page_fingerprint
            if resume_overlap:
                resume_overlap = False
                if overlap_fingerprint is not None and page_fingerprint == overlap_fingerprint:
                    # Replaying the inclusive boundary is intentional.  Once
                    # the overlap is confirmed, move below that second so a
                    # bounded poll does not restart page one forever.
                    boundary = gap_boundary
                    if boundary is None:
                        raise ValueError("trade cursor partial window is missing its gap boundary")
                    window_end = boundary - timedelta(seconds=1)
                    request_start = None
                    next_offset = 0
                    overlap_fingerprint = None
                    window_first_fingerprint = None
                    if len(payload) < limit:
                        continuation_cursor = f"gap:{boundary.timestamp():.6f}"
                        break
                    continue
            next_offset += len(payload)
            if len(payload) < limit:
                if coverage_gap and gap_boundary is not None:
                    continuation_cursor = f"gap:{gap_boundary.timestamp():.6f}"
                break
            complete = False
            if next_offset > 10_000:
                if not page_timestamps:
                    self._validation_errors.append(
                        PolymarketPayloadError("Data API trade page has no valid timestamps for continuation")
                    )
                    break
                boundary = min(page_timestamps)
                coverage_gap = True
                if gap_boundary is None:
                    gap_boundary = boundary
                overlap_fingerprint = window_first_fingerprint or page_fingerprint
                continuation_cursor = (
                    f"window:{boundary.timestamp():.6f}:0:partial:"
                    f"{overlap_fingerprint}:{gap_boundary.timestamp():.6f}"
                )
                self._validation_errors.append(
                    PolymarketPayloadError(
                        "Data API trade pagination reached the offset cap; "
                        "the boundary timestamp may contain an unrecoverable partial gap"
                    )
                )
                break
        if continuation_cursor is None and not complete:
            if coverage_gap and gap_boundary is not None:
                current_end = window_end.timestamp() if window_end is not None else 0.0
                continuation_cursor = (
                    f"window:{current_end:.6f}:{next_offset}:partial:-:"
                    f"{gap_boundary.timestamp():.6f}"
                )
            else:
                continuation_cursor = str(next_offset)
        self._last_trades_complete = bool(complete and not coverage_gap)
        self._last_trade_cursor = continuation_cursor if not self._last_trades_complete else None
        rows.sort(key=lambda item: (item.timestamp, item.trade_id or ""))
        return tuple(rows)

    @staticmethod
    def _levels(value: Any, *, reverse: bool, depth: int) -> list[OrderBookLevel]:
        if not isinstance(value, list):
            raise ValueError("CLOB order-book side must be an array")
        levels: list[OrderBookLevel] = []
        for point in value:
            if isinstance(point, Mapping):
                price_raw = point.get("price", point.get("p"))
                size_raw = point.get("size", point.get("quantity", point.get("q")))
            elif isinstance(point, (list, tuple)) and len(point) == 2:
                price_raw, size_raw = point
            else:
                raise ValueError("CLOB order-book level is malformed")
            if isinstance(price_raw, bool) or isinstance(size_raw, bool):
                raise ValueError("CLOB order-book level numeric fields are invalid")
            price, size = as_float(price_raw), as_float(size_raw)
            if price is None or size is None or not 0.0 <= price <= 1.0 or size <= 0:
                raise ValueError("CLOB order-book level numeric fields are invalid")
            levels.append(OrderBookLevel(price=price, size=size))
        levels.sort(key=lambda level: level.price, reverse=reverse)
        return levels[:depth]
    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        identifier = str(market_id).strip()
        raw = self._raw_cache.get(identifier)
        # ``market`` already fetched and cached this exact Gamma payload in a
        # collector pass.  Reusing it avoids a second metadata request.
        snapshot = self._snapshot(raw) if raw is not None else self.market(identifier)
        if snapshot is None:
            return None
        raw = self._raw_cache.get(snapshot.market_id, raw if isinstance(raw, Mapping) else {})
        tokens = self.token_ids(snapshot.market_id)
        lifecycle_keys = {
            "active": ("active",),
            "closed": ("closed",),
            "archived": ("archived",),
            "acceptingOrders": ("acceptingOrders", "accepting_orders"),
            "enableOrderBook": ("enableOrderBook", "enable_order_book"),
        }
        lifecycle_raw = {
            name: _first_present(raw, *keys) for name, keys in lifecycle_keys.items()
        }
        lifecycle_normalized = {
            name: _optional_bool_value(value) for name, value in lifecycle_raw.items()
        }
        timestamp_keys = {
            "start": ("startDate", "start_date"),
            "end": ("endDate", "end_date", "endDateIso", "expirationDate"),
            "created_at": ("createdAt", "created_at"),
            "updated_at": ("updatedAt", "updated_at"),
            "closed_at": ("closedTime", "closed_time"),
        }
        timestamps = {
            name: _first_present(raw, *keys) for name, keys in timestamp_keys.items()
        }
        parsed_timestamps = {
            name: parse_timestamp(value) for name, value in timestamps.items()
        }
        raw_timestamps = {
            key: raw[key]
            for key in (
                "startDate", "start_date", "endDate", "end_date", "endDateIso",
                "expirationDate", "createdAt", "created_at", "updatedAt",
                "updated_at", "closedTime", "closed_time",
            )
            if key in raw
        }
        timestamps.update(raw_timestamps)
        rules_field = _first_present_key(raw, "resolutionCriteria", "resolution_criteria", "rules", "description")
        try:
            min_order_size = _optional_number(raw, "orderMinSize", "order_min_size", positive=True)
            tick_size = _optional_number(
                raw, "orderPriceMinTickSize", "order_price_min_tick_size", "tickSize", "tick_size", positive=True
            )
            neg_risk = _optional_bool(raw, "negRisk", "neg_risk")
        except ValueError:
            return None
        return InstrumentMetadata(
            symbol=snapshot.slug or snapshot.market_id,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            currency="USD",
            market_id=snapshot.market_id,
            condition_id=snapshot.condition_id,
            slug=snapshot.slug,
            tick_size=tick_size,
            question=snapshot.question,
            resolution_criteria=snapshot.resolution_criteria,
            category=snapshot.category,
            tags=snapshot.tags,
            expiry=snapshot.expiry,
            provider_timestamp=snapshot.provider_timestamp,
            active=lifecycle_normalized["active"],
            closed=lifecycle_normalized["closed"],
            archived=lifecycle_normalized["archived"],
            accepting_orders=lifecycle_normalized["acceptingOrders"],
            enable_order_book=lifecycle_normalized["enableOrderBook"],
            min_order_size=min_order_size,
            order_book_available=lifecycle_normalized["enableOrderBook"],
            neg_risk=neg_risk,
            extra={
                "market_id": snapshot.market_id,
                "gamma_market_id": snapshot.market_id,
                "condition_id": snapshot.condition_id,
                "slug": snapshot.slug,
                "question": snapshot.question,
                "question_source": "gamma",
                "question_provenance": {
                    "source": "gamma",
                    "field": "question",
                    "value": raw.get("question"),
                },
                "rules": snapshot.resolution_criteria,
                "rules_source": "gamma",
                "rules_provenance": {
                    "source": "gamma",
                    "field": rules_field,
                    "value": raw.get(rules_field) if rules_field else None,
                },
                "settlement": snapshot.settlement.value,
                "volume": snapshot.volume,
                "liquidity": snapshot.liquidity,
                "active": lifecycle_raw["active"],
                "closed": lifecycle_raw["closed"],
                "archived": lifecycle_raw["archived"],
                "acceptingOrders": lifecycle_raw["acceptingOrders"],
                "enableOrderBook": lifecycle_raw["enableOrderBook"],
                "lifecycle": lifecycle_raw,
                "lifecycle_normalized": lifecycle_normalized,
                "outcomes": decode_jsonish(raw.get("outcomes", [])),
                "timestamps": timestamps,
                "raw_timestamps": raw_timestamps,
                "timestamp_provenance": {"source": "gamma", "raw": dict(raw_timestamps)},
                "start": timestamps["start"],
                "end": timestamps["end"],
                "created_at": timestamps["created_at"],
                "updated_at": timestamps["updated_at"],
                "closed_at": timestamps["closed_at"],
                "parsed_timestamps": parsed_timestamps,
                "source_timestamp": snapshot.provider_timestamp,
                "clob_token_ids": decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids", []))),
                "token_ids": dict(tokens),
                "yes_token_id": tokens.get("yes"),
                "no_token_id": tokens.get("no"),
                "min_order_size": min_order_size,
                "order_min_size": raw.get("orderMinSize", raw.get("order_min_size")),
                "tick_size": tick_size,
                "order_price_min_tick_size": raw.get(
                    "orderPriceMinTickSize", raw.get("order_price_min_tick_size")
                ),
                "neg_risk": raw.get("negRisk", raw.get("neg_risk")),
                "negRisk": raw.get("negRisk"),
                "neg_risk_normalized": neg_risk,
                "neg_risk_market_id": raw.get("negRiskMarketID", raw.get("neg_risk_market_id")),
                "availability": {
                    "accepting_orders": lifecycle_normalized["acceptingOrders"],
                    "enable_order_book": lifecycle_normalized["enableOrderBook"],
                },
                "provenance": {
                    "provider": self.provider_name,
                    "gamma": {
                        "market_id": snapshot.market_id,
                        "condition_id": snapshot.condition_id,
                        "slug": snapshot.slug,
                        "question": raw.get("question"),
                        "rules": raw.get(rules_field) if rules_field else None,
                        "lifecycle": dict(lifecycle_raw),
                        "timestamps": dict(timestamps),
                    },
                    "clob": {
                        "token_ids": dict(tokens),
                        "min_order_size": min_order_size,
                        "tick_size": tick_size,
                        "neg_risk": raw.get("negRisk", raw.get("neg_risk")),
                    },
                },
            },
        )

    def _remember(self, payload: Mapping[str, Any]) -> None:
        identifiers = (payload.get("id"), payload.get("market_id"))
        for identifier in identifiers:
            if identifier is not None and str(identifier):
                self._raw_cache[str(identifier)] = payload

    def _yes_token(self, market_id: str) -> str | None:
        tokens = self.token_ids(market_id)
        return tokens.get("yes")

    def _snapshot(
        self,
        raw: Mapping[str, Any],
        *,
        expected_market_id: str | None = None,
    ) -> PredictionMarketSnapshot | None:
        try:
            identifier, condition_id = _market_identity(raw)
            if expected_market_id is not None and identifier != expected_market_id:
                raise PolymarketIdentityError("Gamma market id does not match requested market")
            token_mapping = _token_mapping(raw)
        except PolymarketPayloadError as exc:
            self._validation_errors.append(exc)
            return None
        self._provider_timestamps.pop(("market", identifier), None)
        self._remember(raw)
        outcomes = decode_jsonish(raw.get("outcomes"))
        prices = decode_jsonish(raw.get("outcomePrices", raw.get("outcome_prices")))
        outcome_indexes = _outcome_indexes(outcomes)
        yes_index, no_index = outcome_indexes["yes"], outcome_indexes["no"]
        yes_mid = _probability(_indexed_float(prices, yes_index))
        no_mid = _probability(_indexed_float(prices, no_index))
        yes_bid = _probability(_first_float(raw, "yesBid", "yes_bid", "bestBid", "best_bid"))
        yes_ask = _probability(_first_float(raw, "yesAsk", "yes_ask", "bestAsk", "best_ask"))
        no_bid = _probability(_first_float(raw, "noBid", "no_bid"))
        no_ask = _probability(_first_float(raw, "noAsk", "no_ask"))
        if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
            yes_bid = yes_ask = None
        if no_bid is not None and no_ask is not None and no_bid > no_ask:
            no_bid = no_ask = None
        timestamp_raw = _first_present(raw, "updatedAt", "updated_at")
        timestamp = parse_timestamp(timestamp_raw)
        if timestamp_raw not in (None, "") and timestamp is None:
            self._validation_errors.append(PolymarketPayloadError("Gamma updatedAt is malformed"))
            return None
        expiry_raw = _first_present(raw, "endDate", "end_date", "endDateIso", "expirationDate")
        expiry = parse_timestamp(expiry_raw)
        if expiry_raw not in (None, "") and expiry is None:
            self._validation_errors.append(PolymarketPayloadError("Gamma endDate is malformed"))
            return None
        question = _text(raw.get("question"))
        if question is None:
            return None
        rules_field = _first_present_key(raw, "resolutionCriteria", "resolution_criteria", "rules", "description")
        criteria = _text(raw.get(rules_field)) if rules_field else ""
        active = _optional_bool_value(raw.get("active")) if "active" in raw else None
        closed = _optional_bool_value(raw.get("closed")) if "closed" in raw else None
        archived = _optional_bool_value(raw.get("archived")) if "archived" in raw else None
        accepting_orders = _optional_bool_value(raw.get("acceptingOrders", raw.get("accepting_orders"))) if (
            "acceptingOrders" in raw or "accepting_orders" in raw
        ) else None
        enable_order_book = _optional_bool_value(raw.get("enableOrderBook", raw.get("enable_order_book"))) if (
            "enableOrderBook" in raw or "enable_order_book" in raw
        ) else None
        if any(
            value is _INVALID_BOOL
            for value in (active, closed, archived, accepting_orders, enable_order_book)
        ):
            self._validation_errors.append(PolymarketPayloadError("Gamma lifecycle field is malformed"))
            return None
        try:
            _optional_number(raw, "orderMinSize", "order_min_size", positive=True)
            _optional_number(
                raw, "orderPriceMinTickSize", "order_price_min_tick_size", "tickSize", "tick_size", positive=True
            )
        except ValueError as exc:
            self._validation_errors.append(PolymarketPayloadError(str(exc)))
            return None
        snapshot = PredictionMarketSnapshot(
            timestamp=timestamp or _EPOCH,
            market_id=identifier,
            question=question,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            yes_mid=yes_mid,
            no_bid=no_bid,
            no_ask=no_ask,
            no_mid=no_mid,
            volume=_nonnegative(_first_float(raw, "volume", "volumeNum", "volume_num")),
            liquidity=_nonnegative(_first_float(raw, "liquidity", "liquidityNum", "liquidity_num")),
            expiry=expiry,
            settlement=_settlement(raw),
            resolution_criteria=criteria or "",
            category=_text(raw.get("category")),
            tags=_tags(raw.get("tags", raw.get("tag", []))),
            source=self.provider_name,
            yes_token_id=token_mapping.get("yes"),
            no_token_id=token_mapping.get("no"),
            condition_id=condition_id,
            slug=_text(raw.get("slug")),
            provider_timestamp=timestamp,
            active=None if active is _INVALID_BOOL else active,
            closed=None if closed is _INVALID_BOOL else closed,
            archived=None if archived is _INVALID_BOOL else archived,
            accepting_orders=None if accepting_orders is _INVALID_BOOL else accepting_orders,
            enable_order_book=None if enable_order_book is _INVALID_BOOL else enable_order_book,
        )
        self._provider_timestamps[("market", identifier)] = timestamp
        return snapshot


def _trade_source_identity(raw: Mapping[str, Any]) -> dict[str, str | None]:
    transaction_hash = _text(raw.get("transactionHash")) or _text(raw.get("transaction_hash"))
    fill_id = (
        _text(raw.get("fillId"))
        or _text(raw.get("fill_id"))
        or _text(raw.get("tradeId"))
        or _text(raw.get("trade_id"))
        or _text(raw.get("id"))
    )
    return {
        "transaction_hash": transaction_hash,
        "fill_id": fill_id,
    }


def _trade_identity(
    *,
    condition_id: str,
    raw: Mapping[str, Any],
    timestamp: datetime,
    token_id: str | None,
    side: str,
    price: float,
    size: float,
) -> str:
    """Build a durable identity for one public fill, not only its transaction.

    A single on-chain transaction can settle several fills.  The Data API does
    not promise a transaction hash is a row identity, so the canonical key
    includes the source/fill identifiers and every economic field that
    distinguishes one print from another.  Replayed rows with the same fields
    hash to the same key while same-transaction fills remain separate.
    """
    source_identity = _trade_source_identity(raw)
    canonical = {
        "condition_id": condition_id,
        "source": source_identity,
        "token_id": token_id,
        "side": side,
        "timestamp": timestamp.isoformat(),
        "price": format(price, ".17g"),
        "size": format(size, ".17g"),
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"polymarket:{digest}"


def _trade_page_fingerprint(identities: Sequence[str]) -> str:
    return hashlib.sha256(
        json.dumps(list(identities), separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _normalized_number(value: Any, name: str, *, minimum: float | None = None) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number) or (minimum is not None and number < minimum):
        raise ValueError(f"{name} must be a finite number >= {minimum:g}")
    return format(number, ".15g")


def _normalized_date_filter(value: Any, name: str) -> str:
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty ISO-8601 string or datetime")
    return value.strip()


def _jsonable_query(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable_query(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable_query(item) for item in value]
    return value


def _market_query_fingerprint(request_path: str, query: Mapping[str, Any]) -> str:
    fingerprint_query = {
        str(key): value for key, value in query.items() if str(key) != "after_cursor"
    }
    canonical = json.dumps(
        {"request_path": str(request_path), "query": _jsonable_query(fingerprint_query)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_INVALID_BOOL = object()


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, (bool, Mapping, list, tuple, set)):
        return None
    text = str(value).strip()
    return text or None


def _first_present(raw: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw:
            return raw[key]
    return None


def _first_present_key(raw: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        if key in raw and raw[key] not in (None, ""):
            return key
    return None


def _present_text(raw: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        if key in raw:
            value = _text(raw[key])
            if value is None:
                raise PolymarketBookIdentityError(f"CLOB {key} is malformed")
            return value
    return None


def _market_identity(raw: Mapping[str, Any]) -> tuple[str, str]:
    declared_id = _text(raw.get("id")) if "id" in raw else None
    alias_id = _text(raw.get("market_id")) if "market_id" in raw else None
    if "id" in raw and declared_id is None:
        raise PolymarketIdentityError("Gamma market id is malformed")
    if "market_id" in raw and alias_id is None:
        raise PolymarketIdentityError("Gamma market_id alias is malformed")
    market_id = declared_id or alias_id
    condition_id = _text(raw.get("conditionId", raw.get("condition_id")))
    if market_id is None:
        raise PolymarketIdentityError("Gamma market id is required")
    if declared_id is not None and alias_id is not None and declared_id != alias_id:
        raise PolymarketIdentityError("Gamma market id aliases disagree")
    if condition_id is None:
        raise PolymarketIdentityError("Gamma conditionId is required")
    if market_id == condition_id:
        raise PolymarketIdentityError("Gamma market id and conditionId must be distinct")
    return market_id, condition_id


def _token_mapping(raw: Mapping[str, Any]) -> dict[str, str]:
    outcomes = decode_jsonish(raw.get("outcomes"))
    tokens = decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids")))
    if isinstance(outcomes, Mapping) and isinstance(tokens, Mapping):
        outcome_labels = {str(key).strip().lower() for key in outcomes}
        token_by_label = {str(key).strip().lower(): value for key, value in tokens.items()}
        if outcome_labels != {"yes", "no"} or set(token_by_label) != {"yes", "no"}:
            raise PolymarketTokenMappingError("Gamma outcome/token mappings must contain exactly YES and NO")
        result = {
            label: _text(token_by_label.get(label)) or ""
            for label in ("yes", "no")
        }
        if not all(result.values()) or len(set(result.values())) != 2:
            raise PolymarketTokenMappingError("Gamma outcome/token mappings contain invalid token ids")
        return result
    if not isinstance(outcomes, (list, tuple)) or not isinstance(tokens, (list, tuple)):
        raise PolymarketTokenMappingError("Gamma outcomes and clobTokenIds must be aligned arrays")
    if len(outcomes) != 2 or len(tokens) != 2:
        raise PolymarketTokenMappingError("Gamma outcomes and clobTokenIds must each contain YES and NO")
    result: dict[str, str] = {}
    for outcome, token in zip(outcomes, tokens):
        label = _text(outcome)
        normalized = label.lower() if label is not None else ""
        token_text = _text(token)
        if normalized not in {"yes", "no"} or token_text is None or normalized in result:
            raise PolymarketTokenMappingError("Gamma outcomes and clobTokenIds are not aligned YES/NO pairs")
        result[normalized] = token_text
    if set(result) != {"yes", "no"} or len(set(result.values())) != 2:
        raise PolymarketTokenMappingError("Gamma outcomes and clobTokenIds are not aligned YES/NO pairs")
    return result


def _outcome_indexes(outcomes: Any) -> dict[str, int]:
    outcomes = decode_jsonish(outcomes)
    if isinstance(outcomes, Mapping):
        labels = {str(key).strip().lower() for key in outcomes}
        if labels == {"yes", "no"}:
            return {"yes": 0, "no": 1}
    if isinstance(outcomes, (list, tuple)) and len(outcomes) == 2:
        result: dict[str, int] = {}
        for index, outcome in enumerate(outcomes):
            label = _text(outcome)
            normalized = label.lower() if label is not None else ""
            if normalized not in {"yes", "no"} or normalized in result:
                break
            result[normalized] = index
        if set(result) == {"yes", "no"}:
            return result
    raise PolymarketTokenMappingError("Gamma outcomes are not an aligned YES/NO pair")


def _optional_bool_value(value: Any) -> bool | None | object:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isfinite(float(value)) and value in (0, 1):
            return bool(value)
        return _INVALID_BOOL
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return _INVALID_BOOL


def _optional_bool(raw: Mapping[str, Any], *keys: str) -> bool | None:
    for key in keys:
        if key in raw:
            value = _optional_bool_value(raw[key])
            if value is _INVALID_BOOL:
                raise ValueError(f"{key} must be a boolean")
            return value
    return None


def _optional_number(
    raw: Mapping[str, Any], *keys: str, positive: bool = False
) -> float | None:
    for key in keys:
        if key not in raw or raw[key] in (None, ""):
            continue
        if isinstance(raw[key], bool):
            raise ValueError(f"{key} must be finite and {'positive' if positive else 'non-negative'}")
        value = as_float(raw[key])
        if value is None or (positive and value <= 0) or (not positive and value < 0):
            raise ValueError(f"{key} must be finite and {'positive' if positive else 'non-negative'}")
        return value
    return None




def _first_float(raw: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in raw and not isinstance(raw.get(key), bool):
            value = as_float(raw.get(key))
            if value is not None:
                return value
    return None


def _probability(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) and 0.0 <= value <= 1.0 else None


def _nonnegative(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) and value >= 0 else None


def _indexed_float(values: Any, index: int) -> float | None:
    if isinstance(values, (list, tuple)) and 0 <= index < len(values):
        return None if isinstance(values[index], bool) else as_float(values[index])
    if isinstance(values, Mapping):
        key = "yes" if index == 0 else "no"
        value = values.get(key)
        return None if isinstance(value, bool) else as_float(value)
    return None


def _tags(value: Any) -> tuple[str, ...]:
    value = decode_jsonish(value)
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        value = [value]
    result: list[str] = []
    for item in value:
        if isinstance(item, Mapping):
            item = item.get("label", item.get("name", item.get("slug", "")))
        text = str(item).strip()
        if text and text not in result:
            result.append(text)
    return tuple(result)


def _settlement(raw: Mapping[str, Any]) -> SettlementState:
    outcome = _first_present(
        raw,
        "resolvedOutcome",
        "resolved_outcome",
        "winningOutcome",
        "winner",
        "resolution",
        "resolutionOutcome",
        "resolution_outcome",
        "umaResolutionOutcome",
        "uma_resolution_outcome",
    )
    text = _text(outcome)
    normalized = text.lower() if text is not None else ""
    if normalized in {"void", "invalid", "cancelled", "canceled", "null"}:
        return SettlementState.VOID
    if normalized in {"yes", "y", "1", "true", "resolved_yes"}:
        return SettlementState.RESOLVED_YES
    if normalized in {"no", "n", "0", "false", "resolved_no"}:
        return SettlementState.RESOLVED_NO
    closed = _optional_bool_value(raw.get("closed")) if "closed" in raw else None
    active = _optional_bool_value(raw.get("active")) if "active" in raw else None
    resolved = _optional_bool_value(raw.get("resolved")) if "resolved" in raw else None
    if closed is False or active is True or resolved is False:
        return SettlementState.OPEN
    return SettlementState.UNKNOWN


__all__ = [
    "MarketDiscoveryPage",
    "PolymarketAdapter",
    "PolymarketPayloadError",
    "PolymarketIdentityError",
    "PolymarketTokenMappingError",
    "PolymarketBookIdentityError",
]
