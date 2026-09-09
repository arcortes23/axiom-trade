"""Read-only Polymarket Gamma and CLOB adapter.

Gamma supplies market identity, question, rules, tags and lifecycle fields;
the CLOB supplies token prices, price history and displayed depth.  The
adapter never submits orders.  Public endpoints may be unavailable from an
offline environment, in which case empty/``None`` values are returned without
fabricating probabilities or settlement outcomes.
"""
from __future__ import annotations

import math
import urllib.parse

from datetime import datetime, timezone
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
        clob_url: str = "https://clob.polymarket.com",
        base_url: str | None = None,
        timeout: float = 10.0,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        timeout_value = float(timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        # base_url is a convenient alias for Gamma's origin.
        self.gamma_url = str(base_url or gamma_url).rstrip("/")
        self.clob_url = str(clob_url).rstrip("/")
        self.timeout = timeout_value
        self._opener = opener
        self._raw_cache: dict[str, Mapping[str, Any]] = {}
        self._token_context: dict[str, tuple[str, str]] = {}
        self._validation_errors: list[PolymarketPayloadError] = []
        self._transport_errors: list[HTTPFetchError] = []
        self._provider_timestamps: dict[tuple[str, str], datetime | None] = {}
        self._book_provider_timestamps: dict[str, datetime | None] = {}
        self._last_trades_complete = True
        self._last_trade_cursor: str | None = None

    def isolated_worker_factory(self) -> Callable[[], "PolymarketAdapter"]:
        """Return a factory for workers with independent mutable adapter state."""
        gamma_url, clob_url, timeout, opener = self.gamma_url, self.clob_url, self.timeout, self._opener

        def create() -> "PolymarketAdapter":
            return PolymarketAdapter(
                gamma_url=gamma_url,
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

    def markets(self, active: bool = True, *, limit: int | None = None) -> Sequence[PredictionMarketSnapshot]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer")
        if limit == 0:
            return []
        page_size = min(100, limit) if limit is not None else 100
        params: dict[str, Any] = {"active": str(bool(active)).lower(), "limit": page_size}
        if not active:
            params["closed"] = "true"
            params["order"] = "createdAt"
            params["ascending"] = "false"
        result: list[PredictionMarketSnapshot] = []
        seen_ids: set[str] = set()
        for page_number in range(100):
            params["offset"] = page_number * page_size
            payload = self._gamma_get("/markets", **params)
            if isinstance(payload, Mapping):
                payload = payload.get("markets", payload.get("data", []))
            if not isinstance(payload, list):
                break
            page_added = 0
            for record in payload:
                if not isinstance(record, Mapping):
                    continue
                snapshot = self._snapshot(record)
                if snapshot is not None and snapshot.market_id not in seen_ids:
                    seen_ids.add(snapshot.market_id)
                    result.append(snapshot)
                    page_added += 1
                    if limit is not None and len(result) >= int(limit):
                        return result
            if len(payload) < page_size or page_added == 0:
                break
        return result

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
        """CLOB ``GET /trades`` is authenticated in the current V2 API."""
        return False

    @property
    def last_trade_cursor(self) -> str | None:
        return None

    def trades(
        self,
        market_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        *,
        max_pages: int = 100,
        cursor: str | None = None,
    ) -> Sequence[TradePrint]:
        """Return no rows rather than calling the authenticated account endpoint.

        Public price history and last-trade streams remain available, but they
        are not equivalent to authenticated account trade history.
        """
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
        self._last_trades_complete = True
        self._last_trade_cursor = None
        return ()

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
    "PolymarketAdapter",
    "PolymarketPayloadError",
    "PolymarketIdentityError",
    "PolymarketTokenMappingError",
    "PolymarketBookIdentityError",
]
