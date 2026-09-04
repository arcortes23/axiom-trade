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

from datetime import datetime
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
    utc_now,
)
from ._http import HTTPFetchError, as_float, decode_jsonish, fetch_json_strict, parse_timestamp, query_url
from .interfaces import PredictionMarketDataProvider


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
        self._transport_errors: list[HTTPFetchError] = []
        self._last_trades_complete = True
        self._last_trade_cursor: str | None = None

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
        identifier = str(market_id)
        if not identifier:
            return None
        payload = self._gamma_get("/markets/" + urllib.parse.quote(identifier, safe=""))
        if not isinstance(payload, Mapping):
            return None
        self._remember(payload)
        return self._snapshot(payload)

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
            payload = payload.get("history", payload.get("data", []))
        if not isinstance(payload, list):
            return []
        result: list[dict[str, Any]] = []
        for point in payload:
            if isinstance(point, Mapping):
                stamp = parse_timestamp(point.get("t", point.get("timestamp", point.get("time"))))
                price = as_float(point.get("p", point.get("price", point.get("value"))))
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                stamp, price = parse_timestamp(point[0]), as_float(point[1])
            else:
                continue
            if stamp is not None and price is not None and 0.0 <= price <= 1.0:
                result.append({"timestamp": stamp, "price": price, "token_id": token_id})
        result.sort(key=lambda item: item["timestamp"])
        return result

    def token_ids(self, market_id: str) -> Mapping[str, str]:
        """Return normalized YES/NO CLOB token identifiers from Gamma metadata."""
        identifier = str(market_id)
        raw = self._raw_cache.get(identifier)
        if raw is None:
            self.market(identifier)
            raw = self._raw_cache.get(identifier, {})
        tokens = decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids", raw.get("tokens", []))))
        outcomes = decode_jsonish(raw.get("outcomes", []))
        if isinstance(tokens, Mapping):
            return {
                str(outcome).strip().lower(): str(token)
                for outcome, token in tokens.items()
                if str(outcome).strip().lower() in {"yes", "no"} and token
            }
        if not isinstance(tokens, (list, tuple)):
            return {}
        result: dict[str, str] = {}
        if isinstance(outcomes, (list, tuple)):
            for index, outcome in enumerate(outcomes):
                if index < len(tokens) and str(outcome).strip().lower() in {"yes", "no"} and tokens[index]:
                    result[str(outcome).strip().lower()] = str(tokens[index])
        if len(tokens) >= 2:
            result.setdefault("yes", str(tokens[0]))
            result.setdefault("no", str(tokens[1]))
        return result

    def order_book_for_token(self, token_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        payload = self._clob_get("/book", token_id=str(token_id))
        if not isinstance(payload, Mapping):
            return None
        bids = self._levels(payload.get("bids"), reverse=True, depth=depth)
        asks = self._levels(payload.get("asks"), reverse=False, depth=depth)
        if not bids and not asks:
            return None
        timestamp = parse_timestamp(
            payload.get("timestamp", payload.get("ts", payload.get("time")))
        ) or utc_now()
        try:
            return OrderBookSnapshot(timestamp=timestamp, bids=tuple(bids), asks=tuple(asks), token_id=str(token_id))
        except ValueError:
            return None

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
    def last_trade_cursor(self) -> str | None:
        return self._last_trade_cursor

    def trades(
        self,
        market_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        *,
        max_pages: int = 100,
        cursor: str | None = None,
    ) -> Sequence[TradePrint]:
        """Fetch bounded public CLOB trades without advancing incomplete watermarks."""
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
        initial_cursor = str(cursor).strip() if cursor is not None and str(cursor).strip() else None
        self._last_trades_complete = True
        self._last_trade_cursor = initial_cursor
        params: dict[str, Any] = {"market": str(market_id), "limit": 100}
        if initial_cursor is not None:
            params["cursor"] = initial_cursor
        seen: set[str] = set()
        result: list[TradePrint] = []
        for page_number in range(int(max_pages)):
            previous_cursor = params.get("cursor")
            payload = self._clob_get("/trades", **params)
            if payload is None:
                self._last_trades_complete = False
                break
            cursor = None
            if isinstance(payload, Mapping):
                rows = payload.get("data", payload.get("trades", []))
                cursor = payload.get("next_cursor", payload.get("nextCursor"))
            else:
                rows = payload
            if not isinstance(rows, list):
                self._last_trades_complete = False
                break
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                stamp = parse_timestamp(row.get("timestamp", row.get("ts", row.get("time"))))
                price = as_float(row.get("price", row.get("p")))
                size = as_float(row.get("size", row.get("amount", row.get("q"))))
                if stamp is None or price is None or size is None or price <= 0 or price > 1 or size <= 0:
                    continue
                if start is not None and stamp < ensure_utc(start):
                    continue
                if end is not None and stamp > ensure_utc(end):
                    continue
                token = row.get("asset_id", row.get("token_id", row.get("assetId")))
                trade_id = row.get("id", row.get("trade_id", row.get("tradeId")))
                identity = str(trade_id or f"{stamp.isoformat()}|{price}|{size}|{token or ''}")
                if identity in seen:
                    continue
                seen.add(identity)
                side_value = str(row.get("side", "")).strip().lower()
                side = Side(side_value) if side_value in {"buy", "sell"} else None
                result.append(
                    TradePrint(
                        stamp,
                        price,
                        size,
                        side,
                        trade_id=identity,
                        market_id=str(market_id),
                        token_id=str(token) if token is not None else None,
                    )
                )
            if not cursor or not rows:
                break
            if str(cursor) == str(previous_cursor):
                self._last_trade_cursor = str(previous_cursor) if previous_cursor is not None else None
                self._last_trades_complete = False
                break
            params["cursor"] = str(cursor)
            self._last_trade_cursor = str(cursor)
            if page_number + 1 >= int(max_pages):
                self._last_trades_complete = False
        result.sort(key=lambda item: item.timestamp)
        return result

    @staticmethod
    def _levels(value: Any, *, reverse: bool, depth: int) -> list[OrderBookLevel]:
        levels: list[OrderBookLevel] = []
        if not isinstance(value, list):
            return levels
        for point in value:
            if not isinstance(point, Mapping):
                if isinstance(point, (list, tuple)) and len(point) >= 2:
                    price, size = as_float(point[0]), as_float(point[1])
                else:
                    continue
            else:
                price = as_float(point.get("price", point.get("p")))
                size = as_float(point.get("size", point.get("quantity", point.get("q"))))
            if price is not None and size is not None and 0.0 <= price <= 1.0 and size > 0:
                levels.append(OrderBookLevel(price=price, size=size))
        levels.sort(key=lambda level: level.price, reverse=reverse)
        return levels[:depth]

    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        snapshot = self.market(market_id)
        if snapshot is None:
            return None
        raw = self._raw_cache.get(str(market_id), {})
        return InstrumentMetadata(
            symbol=str(raw.get("slug") or snapshot.market_id),
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            currency="USD",
            market_id=snapshot.market_id,
            question=snapshot.question,
            resolution_criteria=snapshot.resolution_criteria,
            category=snapshot.category,
            tags=snapshot.tags,
            expiry=snapshot.expiry,
            extra={
                "settlement": snapshot.settlement.value,
                "volume": snapshot.volume,
                "liquidity": snapshot.liquidity,
                "closed": _boolish(raw.get("closed")),
                "active": _boolish(raw.get("active")) if raw.get("active") is not None else (
                    not _boolish(raw.get("closed"))
                    and snapshot.settlement not in {SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO, SettlementState.VOID}
                ),
                "start": raw.get("startDate", raw.get("start_date", raw.get("createdAt", raw.get("created_at")))),
                "end": raw.get("endDate", raw.get("end_date", raw.get("endDateIso", raw.get("expirationDate")))),
                "rules": snapshot.resolution_criteria,
                "outcomes": decode_jsonish(raw.get("outcomes", [])),
                "clob_token_ids": decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids", []))),
                "condition_id": raw.get("conditionId", raw.get("condition_id")),
                "yes_token_id": self.token_ids(snapshot.market_id).get("yes"),
                "no_token_id": self.token_ids(snapshot.market_id).get("no"),
            },
        )

    def _remember(self, payload: Mapping[str, Any]) -> None:
        identifiers = (
            payload.get("id"),
            payload.get("market_id"),
            payload.get("conditionId"),
            payload.get("condition_id"),
        )
        for identifier in identifiers:
            if identifier is not None and str(identifier):
                self._raw_cache[str(identifier)] = payload

    def _yes_token(self, market_id: str) -> str | None:
        tokens = self.token_ids(market_id)
        if tokens.get("yes"):
            return tokens["yes"]
        # Some fixture payloads use the token itself as the market id.
        return str(market_id) if str(market_id) else None

    def _snapshot(self, raw: Mapping[str, Any]) -> PredictionMarketSnapshot | None:
        identifier = raw.get("id", raw.get("market_id", raw.get("conditionId")))
        if identifier is None:
            return None
        identifier = str(identifier)
        self._remember(raw)
        outcomes = decode_jsonish(raw.get("outcomes", []))
        prices = decode_jsonish(raw.get("outcomePrices", raw.get("outcome_prices", [])))
        yes_index = 0
        if isinstance(outcomes, (list, tuple)):
            for index, outcome in enumerate(outcomes):
                if str(outcome).strip().lower() == "yes":
                    yes_index = index
                    break
        yes_mid = _probability(_indexed_float(prices, yes_index))
        # Gamma may expose current CLOB quote fields directly.
        yes_bid = _probability(_first_float(raw, "yesBid", "yes_bid", "bestBid", "best_bid"))
        yes_ask = _probability(_first_float(raw, "yesAsk", "yes_ask", "bestAsk", "best_ask"))
        if yes_mid is None:
            last = _probability(_first_float(raw, "lastTradePrice", "last_price"))
            yes_mid = last
        no_index = 1 if yes_index == 0 else 0
        no_mid = _probability(_indexed_float(prices, no_index))
        if no_mid is None and yes_mid is not None:
            no_mid = 1.0 - yes_mid
        no_bid = _probability(_first_float(raw, "noBid", "no_bid"))
        no_ask = _probability(_first_float(raw, "noAsk", "no_ask"))
        if no_bid is None and yes_ask is not None:
            no_bid = 1.0 - yes_ask
        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
        if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
            yes_bid = yes_ask = None
        if no_bid is not None and no_ask is not None and no_bid > no_ask:
            no_bid = no_ask = None
        timestamp = parse_timestamp(raw.get("updatedAt", raw.get("updated_at"))) or utc_now()
        expiry = parse_timestamp(
            raw.get("endDate", raw.get("end_date", raw.get("endDateIso", raw.get("expirationDate"))))
        )
        question = str(raw.get("question", raw.get("title", "")))
        if not question.strip():
            return None
        criteria = raw.get(
            "resolutionCriteria",
            raw.get("resolution_criteria", raw.get("rules", raw.get("description", ""))),
        )
        tokens = self.token_ids(identifier)
        return PredictionMarketSnapshot(
            timestamp=timestamp,
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
            settlement=_settlement(raw, prices, yes_index),
            resolution_criteria=str(criteria or ""),
            category=str(raw["category"]) if raw.get("category") is not None else None,
            tags=_tags(raw.get("tags", raw.get("tag", []))),
            source=self.provider_name,
            yes_token_id=tokens.get("yes"),
            no_token_id=tokens.get("no"),
        )


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "closed", "resolved"}
    return bool(value)

def _first_float(raw: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in raw:
            value = as_float(raw.get(key))
            if value is not None:
                return value
    return None

def _probability(value: float | None) -> float | None:
    return value if value is not None and 0.0 <= value <= 1.0 else None


def _nonnegative(value: float | None) -> float | None:
    return value if value is not None and value >= 0 else None


def _indexed_float(values: Any, index: int) -> float | None:
    if isinstance(values, (list, tuple)) and index < len(values):
        return as_float(values[index])
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


def _settlement(raw: Mapping[str, Any], prices: Any, yes_index: int) -> SettlementState:
    outcome = raw.get(
        "resolvedOutcome",
        raw.get("resolved_outcome", raw.get("winningOutcome", raw.get("winner", raw.get("resolution")))),
    )
    text = str(outcome).strip().lower() if outcome is not None else ""
    if text in {"void", "invalid", "cancelled", "canceled", "null"}:
        return SettlementState.VOID
    if text in {"yes", "y", "1", "true", "resolved_yes"}:
        return SettlementState.RESOLVED_YES
    if text in {"no", "n", "0", "false", "resolved_no"}:
        return SettlementState.RESOLVED_NO
    if not _boolish(raw.get("closed", raw.get("resolved", False))):
        return SettlementState.OPEN
    yes = _probability(_indexed_float(prices, yes_index))
    if yes is not None and yes >= 0.999:
        return SettlementState.RESOLVED_YES
    if yes is not None and yes <= 0.001:
        return SettlementState.RESOLVED_NO
    return SettlementState.UNKNOWN


__all__ = ["PolymarketAdapter"]
