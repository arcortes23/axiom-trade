"""Read-only Polymarket Gamma and CLOB adapter.

Gamma supplies market identity, question, rules, tags and lifecycle fields;
the CLOB supplies token prices, price history and displayed depth.  The
adapter never submits orders.  Public endpoints may be unavailable from an
offline environment, in which case empty/``None`` values are returned without
fabricating probabilities or settlement outcomes.
"""
from __future__ import annotations

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
    ensure_utc,
    utc_now,
)
from ._http import as_float, decode_jsonish, fetch_json, parse_timestamp, query_url
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
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        # base_url is a convenient alias for Gamma's origin.
        self.gamma_url = (base_url or gamma_url).rstrip("/")
        self.clob_url = clob_url.rstrip("/")
        self.timeout = float(timeout)
        self._opener = opener
        self._raw_cache: dict[str, Mapping[str, Any]] = {}

    def _gamma_get(self, path: str, **params: Any) -> Any | None:
        return fetch_json(query_url(self.gamma_url, path, params), self.timeout, self._opener)

    def _clob_get(self, path: str, **params: Any) -> Any | None:
        return fetch_json(query_url(self.clob_url, path, params), self.timeout, self._opener)

    def markets(self, active: bool = True) -> Sequence[PredictionMarketSnapshot]:
        params: dict[str, Any] = {"active": str(bool(active)).lower(), "limit": 100}
        if not active:
            # Gamma's ``active=false`` still returns open markets; ``closed``
            # is the explicit historical/settled query.
            params["closed"] = "true"
            params["order"] = "createdAt"
            params["ascending"] = "false"
        result: list[PredictionMarketSnapshot] = []
        seen_ids: set[str] = set()
        for page_number in range(100):
            params["offset"] = page_number * 100
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
            if len(payload) < 100 or page_added == 0:
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
            if stamp is not None and price is not None:
                result.append({"timestamp": stamp, "price": price})
        result.sort(key=lambda item: item["timestamp"])
        return result

    def order_book(self, market_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        if depth <= 0:
            raise ValueError("depth must be positive")
        token_id = self._yes_token(market_id)
        if token_id is None:
            return None
        payload = self._clob_get("/book", token_id=token_id)
        if not isinstance(payload, Mapping):
            return None
        bids = self._levels(payload.get("bids"), reverse=True, depth=depth)
        asks = self._levels(payload.get("asks"), reverse=False, depth=depth)
        if not bids and not asks:
            return None
        timestamp = parse_timestamp(
            payload.get("timestamp", payload.get("ts", payload.get("time")))
        ) or utc_now()
        return OrderBookSnapshot(timestamp=timestamp, bids=tuple(bids), asks=tuple(asks))

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
                "outcomes": decode_jsonish(raw.get("outcomes", [])),
                "clob_token_ids": decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids", []))),
                "condition_id": raw.get("conditionId", raw.get("condition_id")),
            },
        )

    def _remember(self, payload: Mapping[str, Any]) -> None:
        identifier = payload.get("id", payload.get("market_id"))
        if identifier is not None:
            self._raw_cache[str(identifier)] = payload

    def _yes_token(self, market_id: str) -> str | None:
        raw = self._raw_cache.get(str(market_id))
        if raw is None:
            snapshot = self.market(market_id)
            if snapshot is None:
                return None
            raw = self._raw_cache.get(str(market_id), {})
        tokens = decode_jsonish(raw.get("clobTokenIds", raw.get("clob_token_ids", raw.get("tokens", []))))
        outcomes = decode_jsonish(raw.get("outcomes", []))
        if isinstance(tokens, Mapping):
            token = tokens.get("Yes", tokens.get("yes"))
            return str(token) if token else None
        if not isinstance(tokens, (list, tuple)) or not tokens:
            # Some fixture payloads use the token itself as the market id.
            return str(market_id) if str(market_id) else None
        if isinstance(outcomes, (list, tuple)):
            for index, outcome in enumerate(outcomes):
                if str(outcome).strip().lower() == "yes" and index < len(tokens):
                    return str(tokens[index])
        return str(tokens[0])

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
