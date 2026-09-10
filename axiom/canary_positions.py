"""Durable canary position, exit, and reconciliation orchestration.

The entry ledger remains the authority for entry intents.  This module owns only
an auditable projection of AXIOM-owned lots and exit requests, while all risk
capacity and exchange fill identities are delegated to the canonical canary
risk APIs on :class:`AxiomStore`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import inspect
import json
import re
import sqlite3
import threading
import uuid
from typing import Any, Callable, Mapping, Sequence

RECOVERY_ACTION = "canary.recover_entry"
RECOVERY_CONFIRMATION = "RECOVER UNKNOWN ENTRY"
RECOVERY_ATTACHED = "RECOVERY_ATTACHED"
UNKNOWN_ENTRY_STATUSES = frozenset({"UNKNOWN", "SUBMITTING"})
_RECOVERY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")


class RecoveryProfileError(ValueError):
    """A malformed or non-production execution profile."""

    def __init__(self, code: str) -> None:
        self.code = str(code).strip() or "RECOVERY_PROFILE_INVALID"
        super().__init__(self.code)


def _profile_value(profile: Any, name: str, default: Any = None) -> Any:
    if isinstance(profile, Mapping):
        return profile.get(name, default)
    return getattr(profile, name, default) if profile is not None else default


def normalize_recovery_profile(profile: Any = None) -> dict[str, Any]:
    """Return a bounded profile projection, accepting production only."""
    if profile is None:
        return {"environment": "PRODUCTION", "allow_environment": False}
    environment = _profile_value(profile, "environment", None)
    allow_environment = _profile_value(profile, "allow_environment", False)
    if environment is None or isinstance(allow_environment, (dict, list, tuple, set)):
        raise RecoveryProfileError("RECOVERY_PROFILE_INVALID")
    environment = str(getattr(environment, "value", environment) or "").strip().upper()
    if environment not in {"PRODUCTION", "POLYMARKET_PRODUCTION", "LIVE"}:
        raise RecoveryProfileError("RECOVERY_PRODUCTION_PROFILE_REQUIRED")
    if not isinstance(allow_environment, bool) or allow_environment:
        raise RecoveryProfileError("RECOVERY_PRODUCTION_PROFILE_REQUIRED")
    return {"environment": "PRODUCTION", "allow_environment": False}


def recovery_identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text or _RECOVERY_ID.fullmatch(text) is None:
        raise ValueError(f"invalid {field}")
    return text


def decimal_value(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"invalid {field}")
    try:
        parsed = Decimal(str(value).strip())
    except (ArithmeticError, TypeError, ValueError):
        raise ValueError(f"invalid {field}") from None
    if not parsed.is_finite() or (positive and parsed <= 0) or (not positive and parsed < 0):
        raise ValueError(f"invalid {field}")
    return parsed


def timestamp_value(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        stamp = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"invalid {field}")
        try:
            stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            raise ValueError(f"invalid {field}") from None
    if stamp.tzinfo is None:
        raise ValueError(f"invalid {field}")
    return stamp.astimezone(timezone.utc)


def mapping_value(source: Any, *names: str) -> Any:
    for name in names:
        if isinstance(source, Mapping) and name in source:
            return source[name]
        if source is not None and hasattr(source, name):
            return getattr(source, name)
    return None


def response_order_id(order: Any) -> str | None:
    value = mapping_value(order, "order_id", "orderId", "id", "exchange_order_id")
    text = str(value or "").strip()
    return text or None


def response_side(order: Any) -> str:
    return str(mapping_value(order, "side", "order_side") or "").strip().upper()


def response_token(order: Any) -> str | None:
    value = mapping_value(order, "token_id", "tokenId", "asset_id", "assetId", "asset")
    text = str(value or "").strip()
    return text or None


def response_market(order: Any) -> str | None:
    value = mapping_value(order, "market_id", "market", "condition_id", "conditionId")
    text = str(value or "").strip()
    return text or None


def response_price(order: Any) -> Decimal:
    return decimal_value(mapping_value(order, "price", "limit_price", "limitPrice"), "order price", positive=True)


def response_quantity(order: Any) -> Decimal:
    value = mapping_value(order, "original_size", "originalSize", "size", "quantity", "order_size")
    return decimal_value(value, "order quantity", positive=True)


def response_status(order: Any) -> str:
    return str(mapping_value(order, "status", "state", "order_status") or "").strip().upper()


def response_timestamp(order: Any) -> datetime | None:
    value = mapping_value(order, "submitted_at", "submittedAt", "created_at", "createdAt", "timestamp", "time")
    if value in (None, ""):
        return None
    return timestamp_value(value, "order timestamp")


def trade_order_id(trade: Any) -> str | None:
    value = mapping_value(
        trade,
        "order_id",
        "orderId",
        "taker_order_id",
        "takerOrderId",
        "exchange_order_id",
    )
    text = str(value or "").strip()
    return text or None


def trade_token(trade: Any) -> str | None:
    value = mapping_value(trade, "token_id", "tokenId", "asset_id", "assetId", "asset")
    text = str(value or "").strip()
    return text or None


def trade_side(trade: Any) -> str:
    return str(mapping_value(trade, "side", "order_side") or "").strip().upper()


def trade_price(trade: Any) -> Decimal:
    return decimal_value(mapping_value(trade, "price", "execution_price", "executionPrice"), "trade price", positive=True)


def trade_quantity(trade: Any) -> Decimal:
    return decimal_value(mapping_value(trade, "size", "quantity", "qty", "matched_size", "matchedSize"), "trade quantity", positive=True)


def trade_timestamp(trade: Any) -> datetime | None:
    value = mapping_value(
        trade,
        "timestamp",
        "time",
        "trade_time",
        "tradeTime",
        "matched_at",
        "matchedAt",
        "match_time",
        "matchTime",
        "created_at",
        "createdAt",
    )
    if value in (None, ""):
        return None
    return timestamp_value(value, "trade timestamp")

from .canary import (
    CANARY_SUBMISSION_TIMEOUT_SECONDS,
    CanaryBlocked,
    CanaryService,
    PolymarketClobV2Venue,
    _call_with_timeout,
    _canonical_exchange_order_id,
)
from .domain import ensure_utc, parse_timestamp, utc_now

UTC = timezone.utc
ZERO = Decimal("0")
DUST = Decimal("0.00000001")
_MAX_ENTRY_RECONCILIATION = 100
_MAX_ACTIVE_ENTRY_RECONCILIATION = 80

_PENDING = frozenset({
    "PREPARED", "SUBMITTING", "SUBMITTED", "ACKNOWLEDGED", "OPEN", "UNKNOWN",
    "MATCHED", "FILLED", "PARTIAL", "PARTIALLY_FILLED", "EXIT_REQUESTED",
    "RECONCILE_PENDING",
})
_TERMINAL = frozenset({
    "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED", "ERROR",
    "SETTLED", "FINAL", "CLOSED", "COMPLETED",
})
# A venue order being matched/filled proves execution, not settlement.  Only
# explicit settlement/resolution states may release the risk reservation.
_FINAL_SETTLEMENT = frozenset({
    "SETTLED", "FINAL", "CLOSED", "COMPLETED", "RESOLVED",
})
_OWNED_ENTRY_STATUSES = frozenset({
    "CONFIRMED",
    "TRADE_STATUS_CONFIRMED",
    "SETTLED",
    "TRADE_STATUS_SETTLED",
})
_FAILED_ORDER = frozenset({
    "REJECTED", "FAILED", "ERROR",
})
_CANCELED_ORDER = frozenset({
    "CANCELED", "CANCELLED", "EXPIRED",
})
_ACCEPTED_ORDER_STATUSES = frozenset({
    "ACCEPTED",
    "SUBMITTED",
    "MATCHED",
    "FILLED",
    "PARTIAL",
    "PARTIALLY_FILLED",
    "OPEN",
    "LIVE",
    "DELAYED",
    "ACKNOWLEDGED",
    "SETTLED",
    "RESOLVED",
})
_UNKNOWN_RESPONSE_CODES = frozenset({
    "",
    "UNKNOWN",
    "UNSPECIFIED",
    "NONE",
    "NULL",
})


def _explicit_rejection_code(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.strip().upper() not in _UNKNOWN_RESPONSE_CODES
    )




def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError, ArithmeticError):
        return default
    return result if result.is_finite() else default


def _stamp(value: Any, fallback: datetime | None = None) -> datetime:
    parsed = parse_timestamp(value)
    if parsed is not None:
        return parsed
    return ensure_utc(fallback or utc_now())
def _confirmed_trade_time(trade: Mapping[str, Any]) -> datetime:
    raw_time = _value(
        trade,
        "match_time",
        "matchTime",
        "matched_at",
        "matchedAt",
        "executed_at",
        "execution_time",
        "timestamp",
        "time",
        default=None,
    )
    parsed = parse_timestamp(raw_time)
    if parsed is None:
        raise CanaryBlocked("CANARY_TRADE_TIME_UNAVAILABLE")
    return parsed

def _iso(value: Any, fallback: datetime | None = None) -> str:
    return _stamp(value, fallback).isoformat()


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return "{}"


def _decode(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _value(row: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(row, Mapping) and name in row:
            value = row.get(name)
        else:
            value = getattr(row, name, None)
        if value is not None:
            return value
    return default


def _method(target: Any, name: str) -> Callable[..., Any]:
    """Return one required transport method, failing explicitly if absent."""
    method = getattr(target, name, None)
    if not callable(method):
        raise CanaryBlocked(f"CANARY_VENUE_{name.upper()}_UNAVAILABLE")
    return method


def _call(method: Callable[..., Any], **kwargs: Any) -> Any:
    """Call a transport with its declared keyword contract only.

    This is intentionally not a best-effort fallback: an unsupported transport
    is an actionable blocker, never an implicit no-op or blind retry.
    """
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(**kwargs)
    params = signature.parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return method(**kwargs)
    accepted = {key: value for key, value in kwargs.items() if key in params}
    missing = [
        name for name, param in params.items()
        if name not in accepted
        and param.default is inspect.Parameter.empty
        and param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ]
    if missing:
        raise CanaryBlocked("CANARY_VENUE_METHOD_CONTRACT_INVALID")
    if any(params[name].kind is inspect.Parameter.POSITIONAL_ONLY for name in accepted):
        raise CanaryBlocked("CANARY_VENUE_METHOD_CONTRACT_INVALID")
    return method(**accepted)


def _service(value: Any) -> CanaryService:
    if isinstance(value, CanaryService):
        return value
    if hasattr(value, "connection") and hasattr(value, "_lock"):
        return CanaryService(value)
    raise TypeError("service must be CanaryService or AxiomStore")


def _connection(service: CanaryService) -> Any:
    return service.store.connection


def _ensure_schema(service: CanaryService) -> None:
    connection = _connection(service)
    with service.store._lock, connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS canary_position_lots (
              position_id TEXT PRIMARY KEY,
              reservation_id TEXT,
              event_id TEXT,
              venue TEXT NOT NULL,
              market_id TEXT NOT NULL,
              token_id TEXT NOT NULL,
              candidate_id TEXT,
              strategy_id TEXT,
              strategy_version TEXT,
              strategy_hash TEXT,
              model_hash TEXT,
              config_id TEXT,
              config_generation INTEGER,
              exit_policy_json TEXT NOT NULL DEFAULT '{}',
              quantity TEXT NOT NULL DEFAULT '0',
              sold_quantity TEXT NOT NULL DEFAULT '0',
              cost_basis TEXT NOT NULL DEFAULT '0',
              fees TEXT NOT NULL DEFAULT '0',
              gross_proceeds TEXT NOT NULL DEFAULT '0',
              exit_fees TEXT NOT NULL DEFAULT '0',
              realized_pnl TEXT NOT NULL DEFAULT '0',
              pending_exit_quantity TEXT NOT NULL DEFAULT '0',
              status TEXT NOT NULL DEFAULT 'OPEN',
              opened_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_canary_position_lots_market
              ON canary_position_lots(market_id, token_id, status);
            CREATE TABLE IF NOT EXISTS canary_position_requests (
              request_id TEXT PRIMARY KEY,
              position_id TEXT NOT NULL,
              reservation_id TEXT,
              event_id TEXT,
              venue TEXT NOT NULL,
              market_id TEXT NOT NULL,
              token_id TEXT NOT NULL,
              side TEXT NOT NULL,
              order_id TEXT,
              requested_quantity TEXT NOT NULL,
              filled_quantity TEXT NOT NULL DEFAULT '0',
              average_price TEXT,
              fees TEXT NOT NULL DEFAULT '0',
              status TEXT NOT NULL,
              expected_generation INTEGER NOT NULL,
              config_id TEXT NOT NULL,
              submitted_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              last_error TEXT,
              settlement_status TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_canary_position_requests_order
              ON canary_position_requests(order_id) WHERE order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_canary_position_requests_pending
              ON canary_position_requests(status, updated_at);
            CREATE TABLE IF NOT EXISTS canary_position_reconciliation (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              active_timestamp TEXT NOT NULL DEFAULT '',
              active_event_id TEXT NOT NULL DEFAULT '',
              terminal_timestamp TEXT NOT NULL DEFAULT '',
              terminal_event_id TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canary_position_fills (
              fill_id TEXT PRIMARY KEY,
              request_id TEXT NOT NULL,
              position_id TEXT NOT NULL,
              quantity TEXT NOT NULL,
              price TEXT NOT NULL,
              fee TEXT NOT NULL DEFAULT '0',
              status TEXT NOT NULL,
              filled_at TEXT NOT NULL,
              detail_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_canary_position_fills_request
              ON canary_position_fills(request_id, filled_at, fill_id);
            """
        )
        columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(canary_position_lots)"
            )
        }
        for name in ("gross_proceeds", "exit_fees", "realized_pnl"):
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE canary_position_lots "
                    f"ADD COLUMN {name} TEXT NOT NULL DEFAULT '0'"
                )
        reconciliation_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(canary_position_reconciliation)"
            )
        }
        for name in ("active_timestamp", "active_event_id"):
            if name not in reconciliation_columns:
                connection.execute(
                    f"ALTER TABLE canary_position_reconciliation "
                    f"ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )
        connection.execute(
            "INSERT OR IGNORE INTO canary_position_reconciliation("
            "singleton,active_timestamp,active_event_id,"
            "terminal_timestamp,terminal_event_id,updated_at) "
            "VALUES(1,'','','','',?)",
            (_iso(utc_now()),),
        )

def _settings_fence(service: CanaryService, expected_generation: Any, config_id: Any) -> dict[str, Any]:
    if isinstance(expected_generation, bool):
        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
    try:
        generation = int(expected_generation)
    except (TypeError, ValueError, OverflowError):
        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED") from None
    identifier = str(config_id or "").strip()
    if generation < 1 or not identifier:
        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
    settings = service.settings
    if settings is None:
        raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
    snapshot = settings.snapshot(now=ensure_utc(service.clock()))
    if int(snapshot.get("generation", 0) or 0) != generation or str(snapshot.get("config_id") or "") != identifier:
        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
    return snapshot


def _control_state(service: CanaryService) -> str:
    try:
        return str(service.authoritative_status().get("micro_live_canary") or "DISARMED").upper()
    except Exception as exc:
        raise CanaryBlocked("CANARY_CONTROL_UNAVAILABLE") from exc

def _check_venue(venue: Any, *, allow_test_venue: bool) -> None:
    if type(venue) is not PolymarketClobV2Venue and not allow_test_venue:
        raise CanaryBlocked("CANARY_TEST_VENUE_NOT_ALLOWED")


def _control_generation(service: CanaryService) -> int | None:
    with service.store._lock:
        try:
            row = _connection(service).execute("SELECT control_generation FROM canary_control WHERE singleton=1").fetchone()
        except sqlite3.OperationalError:
            row = None
    if row is None:
        return None
    try:
        return int(row["control_generation"])
    except (TypeError, ValueError, OverflowError):
        return None

def _extract_book_price(context: Mapping[str, Any], *, side: str) -> Decimal:
    side = side.upper()
    names = ("best_bid", "bid", "sell_price", "price") if side == "SELL" else ("best_ask", "ask", "buy_price", "price")
    for name in names:
        result = _decimal(context.get(name), ZERO)
        if result > ZERO:
            return result
    books = context.get("order_book", context.get("book"))
    if isinstance(books, Mapping):
        rows = books.get("bids" if side == "SELL" else "asks")
        if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
            prices = [_decimal(_value(row, "price"), ZERO) for row in rows]
            prices = [item for item in prices if item > ZERO]
            if prices:
                return max(prices) if side == "SELL" else min(prices)
    direct_rows = context.get("bids" if side == "SELL" else "asks")
    if isinstance(direct_rows, Sequence) and not isinstance(direct_rows, (str, bytes)):
        prices = [_decimal(_value(row, "price"), ZERO) for row in direct_rows]
        prices = [item for item in prices if item > ZERO]
        if prices:
            return max(prices) if side == "SELL" else min(prices)
    raise CanaryBlocked("CANARY_EXIT_PRICE_UNAVAILABLE")


def _mark_fee(context: Mapping[str, Any], *, quantity: Decimal, price: Decimal) -> Decimal:
    """Derive an explicit fee for a current-book equity mark."""
    raw_fee = _value(context, "mark_fee", "fee", "fee_amount", default=None)
    if raw_fee is not None:
        fee = _decimal(raw_fee, Decimal("-1"))
        if fee < ZERO:
            raise CanaryBlocked("CANARY_EQUITY_FEE_UNAVAILABLE")
        return fee
    raw_bps = _value(context, "fee_bps", "fee_rate_bps", default=None)
    if raw_bps is not None:
        rate_bps = _decimal(raw_bps, Decimal("-1"))
        if rate_bps < ZERO:
            raise CanaryBlocked("CANARY_EQUITY_FEE_UNAVAILABLE")
        return quantity * price * rate_bps / Decimal("10000")
    raw_rate = _value(context, "fee_rate", "feeRate", default=None)
    if raw_rate is None:
        raise CanaryBlocked("CANARY_EQUITY_FEE_UNAVAILABLE")
    rate = _decimal(raw_rate, Decimal("-1"))
    if rate < ZERO:
        raise CanaryBlocked("CANARY_EQUITY_FEE_UNAVAILABLE")
    return quantity * price * rate


def _mark_owned_equity(
    service: CanaryService,
    venue: Any,
    now: datetime,
) -> dict[str, Any]:
    """Persist fee-aware marks for every currently owned open lot.

    The venue context is read-only and queried for each exact market/token
    identity.  Missing price or fee evidence is reported as UNKNOWN rather
    than being converted into a zero-valued mark.
    """
    record_mark = getattr(service.store, "record_canary_equity_mark", None)
    if not callable(record_mark):
        return {
            "status": "UNKNOWN",
            "marks": [],
            "blocked": [{"reason": "CANARY_EQUITY_MARK_UNAVAILABLE"}],
        }
    with service.store._lock:
        lots = [
            dict(row)
            for row in _connection(service).execute(
                "SELECT * FROM canary_position_lots "
                "WHERE status IN ('OPEN','EXIT_PENDING','MANAGEMENT_BLOCKED') "
                "ORDER BY opened_at,position_id"
            ).fetchall()
        ]
    marks: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    assessment_config_id: str | None = None
    assessment_config_generation: int | None = None
    settings = getattr(service, "settings", None)
    if settings is not None:
        try:
            snapshot = settings.snapshot(now=now)
        except Exception:
            snapshot = {}
        if isinstance(snapshot, Mapping):
            assessment_config_id = str(snapshot.get("config_id") or "").strip() or None
            raw_generation = snapshot.get("generation")
            if raw_generation is not None:
                try:
                    assessment_config_generation = int(raw_generation)
                except (TypeError, ValueError, OverflowError):
                    assessment_config_generation = None
    for lot in lots:
        position_id = str(lot.get("position_id") or "").strip()
        total_quantity = max(ZERO, _decimal(lot.get("quantity"), ZERO))
        sold_quantity = max(ZERO, _decimal(lot.get("sold_quantity"), ZERO))
        quantity = max(ZERO, total_quantity - sold_quantity)
        if quantity <= DUST:
            continue
        market_id = str(lot.get("market_id") or "").strip()
        token_id = str(lot.get("token_id") or "").strip()
        if not position_id or not market_id or not token_id:
            blocked.append({
                "position_id": position_id or None,
                "reason": "CANARY_EQUITY_IDENTITY_UNAVAILABLE",
            })
            continue
        try:
            context = _call(
                _method(venue, "market_context"),
                market_id=market_id,
                token_id=token_id,
            )
            if not isinstance(context, Mapping):
                raise CanaryBlocked("CANARY_EQUITY_MARKET_CONTEXT_INVALID")
            price = _extract_book_price(context, side="SELL")
            mark_fee = _mark_fee(context, quantity=quantity, price=price)
            cost_basis = max(ZERO, _decimal(lot.get("cost_basis"), ZERO))
            if total_quantity <= DUST:
                raise CanaryBlocked("CANARY_EQUITY_COST_BASIS_UNAVAILABLE")
            cost_basis = cost_basis * quantity / total_quantity
            # Entry lot bindings remain immutable; valuation snapshots bind
            # the currently active assessment generation instead.
            config_id = assessment_config_id
            config_generation = assessment_config_generation
            control_generation = _control_generation(service)
            mark_id = (
                "equity:"
                + position_id
                + ":"
                + now.isoformat()
                + ":"
                + str(price)
                + ":"
                + str(mark_fee)
                + ":"
                + str(cost_basis)
                + ":"
                + str(config_id or "")
                + ":"
                + str(config_generation or "")
                + ":"
                + str(control_generation or "")
            )
            mark = record_mark(
                mark_id=mark_id,
                observed_at=now,
                market_id=market_id,
                token_id=token_id,
                side="SELL",
                quantity=quantity,
                mark_price=price,
                cost_basis_usd=cost_basis,
                mark_fee=mark_fee,
                source="POLYMARKET_ORDER_BOOK",
                config_id=config_id,
                config_generation=config_generation,
                control_generation=control_generation,
                detail={
                    "position_id": position_id,
                    "venue": str(lot.get("venue") or "POLYMARKET"),
                    "quantity_source": "CANARY_POSITION_LOT",
                },
            )
            marks.append({
                "position_id": position_id,
                "market_id": market_id,
                "token_id": token_id,
                "quantity": str(quantity),
                "mark_price": str(price),
                "mark_fee": str(mark_fee),
                "cost_basis_usd": str(cost_basis),
                "record": dict(mark) if isinstance(mark, Mapping) else mark,
            })
        except CanaryBlocked as exc:
            blocked.append({"position_id": position_id, "reason": str(exc)})
        except Exception as exc:
            blocked.append({"position_id": position_id, "reason": type(exc).__name__})
    if blocked:
        return {"status": "UNKNOWN", "marks": marks, "blocked": blocked}
    if not lots:
        return {"status": "FLAT", "marks": [], "blocked": []}
    return {"status": "KNOWN", "marks": marks, "blocked": []}


def _parse_trades(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("trades", "fills", "data", "items"):
            rows = payload.get(key)
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
                return [row for row in rows if isinstance(row, Mapping)]
        if any(key in payload for key in ("trade_id", "tradeId", "fill_id", "id")):
            return [payload]
        return []
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        return [row for row in payload if isinstance(row, Mapping)]
    return []
def _trade_id(trade: Mapping[str, Any], order_id: str, index: int) -> str:
    value = _value(trade, "trade_id", "tradeId", "fill_id", "id", default=None)
    if value in (None, ""):
        # An array position is not a durable exchange identity: pagination,
        # reconnects, and out-of-order trade reads can change it.  Refuse to
        # account a fill whose venue did not provide its stable ID.
        raise CanaryBlocked("CANARY_TRADE_ID_UNAVAILABLE")
    return str(value)


def _trade_quantity(trade: Mapping[str, Any]) -> Decimal:
    return max(ZERO, _decimal(_value(
        trade,
        "quantity",
        "size",
        "amount",
        "filled_quantity",
        "filledQty",
        "match_size",
        "size_matched",
    ), ZERO))


def _trade_price(trade: Mapping[str, Any], order: Mapping[str, Any]) -> Decimal:
    del order
    # An individual fill must carry its own execution price.  An aggregate
    # order price cannot be allocated to a stable trade identity.
    raw_price = _value(
        trade,
        "price",
        "execution_price",
        "match_price",
        "fill_price",
        default=None,
    )
    price = _decimal(raw_price, Decimal("-1"))
    if price <= ZERO or price >= Decimal("1"):
        raise CanaryBlocked("CANARY_TRADE_PRICE_UNAVAILABLE")
    return price


def _trade_fee(trade: Mapping[str, Any], order: Mapping[str, Any]) -> Decimal:
    del order
    # Fees are likewise sourced from the individual fill, or derived from
    # that fill's explicit rate.  Never borrow an order-level aggregate fee.
    raw_fee = _value(trade, "fee", "fees", "fee_amount", "commission", default=None)
    if raw_fee is not None:
        parsed = _decimal(raw_fee, Decimal("-1"))
        if parsed < ZERO:
            raise CanaryBlocked("CANARY_TRADE_FEE_UNAVAILABLE")
        return parsed
    rate = _value(
        trade,
        "fee_rate_bps",
        "feeRateBps",
        "fee_rate",
        "feeRate",
        default=None,
    )
    if rate is None:
        raise CanaryBlocked("CANARY_TRADE_FEE_UNAVAILABLE")
    quantity = _trade_quantity(trade)
    price = _trade_price(trade, {})
    parsed_rate = _decimal(rate, Decimal("-1"))
    if parsed_rate < ZERO:
        raise CanaryBlocked("CANARY_TRADE_FEE_UNAVAILABLE")
    # The SDK's ClobTrade exposes fee_rate_bps.  Only derive an absolute fee
    # when quantity, price, and the explicit rate are all available.
    return quantity * price * parsed_rate / Decimal("10000")


def _order_status(order: Mapping[str, Any]) -> str:
    status = str(_value(order, "status", "state", "order_status", default="UNKNOWN") or "UNKNOWN").upper()
    if status in {"MATCHED", "MATCH", "EXECUTED"}:
        # MATCHED is execution evidence, not authoritative settlement.
        return "MATCHED"
    if status in {"PARTIAL", "PARTIALLY_FILLED", "PARTIALLYFILLED"}:
        return "PARTIALLY_FILLED"
    if status in {"RESOLVED", "SETTLED_FULL", "SETTLED_PARTIAL"}:
        return "SETTLED"
    return status





def _authoritative_settlement(order: Mapping[str, Any], status: str) -> str | None:
    """Return an explicit settlement state, never inferred from fills."""
    if status in _FINAL_SETTLEMENT:
        return status
    raw = _value(
        order,
        "settlement_status",
        "settlementState",
        "settlement",
        "resolution_status",
        "resolutionState",
        default=None,
    )
    normalized = str(raw or "").strip().upper()
    if normalized in _FINAL_SETTLEMENT:
        return normalized
    settled = _value(order, "settled", "is_settled", "final", "is_final", default=None)
    if settled is True:
        return "SETTLED"
    return None


def _submission_fence(
    service: CanaryService,
    *,
    expected_generation: int,
    config_id: str,
    expected_control_generation: int,
) -> None:
    """Re-read control/settings immediately before an exit transmission."""
    _settings_fence(service, expected_generation, config_id)
    state = _control_state(service)
    if state == "KILLED":
        raise CanaryBlocked("CANARY_KILLED")
    if state not in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "ENTRY_PAUSED", "PAUSED"}:
        raise CanaryBlocked("CANARY_NOT_ARMED")
    current = _control_generation(service)
    if current is None or current <= 0:
        raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
    if current != int(expected_control_generation):
        raise CanaryBlocked("CANARY_CONTROL_CHANGED")


def _request_rows(service: CanaryService, position_id: str | None = None) -> list[dict[str, Any]]:
    query = "SELECT * FROM canary_position_requests WHERE status IN (" + ",".join("?" for _ in _PENDING) + ")"
    params: list[Any] = list(_PENDING)
    if position_id is not None:
        query += " AND position_id=?"
        params.append(str(position_id))
    query += " ORDER BY submitted_at,request_id"
    with service.store._lock:
        return [dict(row) for row in _connection(service).execute(query, params).fetchall()]

def _lot_row(service: CanaryService, position_id: str) -> dict[str, Any] | None:
    with service.store._lock:
        row = _connection(service).execute(
            "SELECT * FROM canary_position_lots WHERE position_id=? OR event_id=?",
            (str(position_id), str(position_id)),
        ).fetchone()
    return dict(row) if row is not None else None


def _sync_entry_lots(service: CanaryService, now: datetime) -> int:
    """Project authoritative BUY fills into owned lots without mutating entries."""
    connection = _connection(service)
    rows: list[dict[str, Any]] = []
    with service.store._lock:
        try:
            rows = [dict(row) for row in connection.execute("SELECT * FROM canary_ledger WHERE UPPER(side)='BUY' AND CAST(COALESCE(fill_quantity,'0') AS NUMERIC)>0").fetchall()]
        except sqlite3.OperationalError:
            rows = []
    created = 0
    for row in rows:
        event_id = str(row.get("event_id") or "").strip()
        ledger_status = str(row.get("status") or "").upper()
        if ledger_status not in _OWNED_ENTRY_STATUSES:
            # Venue matched/filled acknowledgements are provisional.  Only a
            # stable confirmed trade (projected to a settled/confirmed ledger
            # state) can establish owned inventory.
            continue
        if not event_id:
            continue
        reservation_id: str | None = None
        reservation_config_id: str | None = None
        reservation_config_generation: int | None = None
        canonical_fill = None
        with service.store._lock:
            try:
                reservation_row = connection.execute(
                    "SELECT reservation_id,config_id,config_generation "
                    "FROM canary_risk_reservations "
                    "WHERE event_id=? AND UPPER(side)='BUY' "
                    "ORDER BY created_at DESC,reservation_id DESC LIMIT 1",
                    (event_id,),
                ).fetchone()
                if reservation_row is not None:
                    canonical_fill = connection.execute(
                        "SELECT 1 FROM canary_risk_fills "
                        "WHERE reservation_id=? AND CAST(quantity AS NUMERIC)>0 LIMIT 1",
                        (reservation_row["reservation_id"],),
                    ).fetchone()
            except sqlite3.OperationalError:
                reservation_row = None
        if reservation_row is None or canonical_fill is None:
            # A legacy ledger acknowledgement without a canonical BUY
            # reservation/fill cannot establish owned inventory.
            continue
        reservation_id = str(reservation_row["reservation_id"] or "") or None
        reservation_config_id = str(reservation_row["config_id"] or "") or None
        reservation_config_generation = reservation_row["config_generation"]
        position_id = "position:" + event_id
        quantity = max(ZERO, _decimal(row.get("fill_quantity"), ZERO))
        raw_price = row.get("actual_average_price")
        raw_fees = row.get("fees")
        if raw_price in (None, "") or raw_fees in (None, ""):
            continue
        price = _decimal(raw_price, Decimal("-1"))
        fees = _decimal(raw_fees, Decimal("-1"))
        if quantity <= DUST or price <= ZERO or fees < ZERO:
            continue
        evidence = _decode(row.get("evidence_json"))
        execution_asset_id = str(
            evidence.get("resolved_asset_id") or row.get("token_id") or ""
        ).strip()
        signal_id = str(row.get("signal_id") or "")
        with service.store._lock:
            signal_row = connection.execute("SELECT strategy_hash,model_hash,config_hash FROM canary_signals WHERE signal_id=?", (signal_id,)).fetchone() if signal_id else None
        if signal_row is not None:
            evidence.setdefault("strategy_hash", signal_row["strategy_hash"])
            evidence.setdefault("model_hash", signal_row["model_hash"])
        lifecycle = service.store.load_candidate_lifecycle(str(row.get("candidate_id") or ""))
        lifecycle_payload = service._merged_lifecycle_payload(lifecycle) if isinstance(lifecycle, Mapping) else {}
        if isinstance(lifecycle_payload, Mapping):
            for name in ("strategy_id", "strategy_version", "exit_policy", "exit"):
                if name in lifecycle_payload and name not in evidence:
                    evidence[name] = lifecycle_payload[name]
        policy = evidence.get("exit_policy", evidence.get("exit"))
        policy_status = "OPEN" if _valid_exit_policy(policy) else "MANAGEMENT_BLOCKED"
        candidate_id = str(row.get("candidate_id") or "")
        strategy_hash = str(evidence.get("strategy_hash") or "")
        opened = _iso(row.get("timestamp"), now)
        with service.store._lock, connection:
            existing = connection.execute("SELECT quantity,cost_basis,fees,status FROM canary_position_lots WHERE position_id=?", (position_id,)).fetchone()
            if existing is not None:
                # Reconnect/reordered reads must never reduce owned inventory
                # or rewrite a lot's immutable policy/version binding.  A
                # later confirmed acquisition may reopen a previously closed
                # projection, but only its incremental quantity is available
                # after prior SELL accounting.
                prior_quantity = max(ZERO, _decimal(existing["quantity"], ZERO))
                prior_status = str(existing["status"] or "").upper()
                if quantity <= prior_quantity + DUST:
                    if prior_status == "MANAGEMENT_BLOCKED" and policy_status == "OPEN":
                        connection.execute(
                            "UPDATE canary_position_lots SET exit_policy_json=?,status='OPEN',updated_at=? WHERE position_id=?",
                            (_json(policy), _iso(now), position_id),
                        )
                    continue
                prior_cost = max(ZERO, _decimal(existing["cost_basis"], ZERO))
                prior_fees = max(ZERO, _decimal(existing["fees"], ZERO))
                projected_cost = quantity * price + fees
                next_quantity = max(prior_quantity, quantity)
                next_cost = max(prior_cost, projected_cost)
                next_fees = max(prior_fees, fees)
                prior_status = str(existing["status"] or "").upper()
                next_status = (
                    policy_status
                    if prior_status in {"CLOSED", "DUST", "MANAGEMENT_BLOCKED"}
                    else prior_status
                )
                connection.execute(
                    "UPDATE canary_position_lots SET quantity=?,cost_basis=?,fees=?,status=?,updated_at=? WHERE position_id=?",
                    (str(next_quantity), str(next_cost), str(next_fees), next_status, _iso(now), position_id),
                )
                continue
            connection.execute(
                "INSERT INTO canary_position_lots(position_id,reservation_id,event_id,venue,market_id,token_id,candidate_id,strategy_id,strategy_version,strategy_hash,model_hash,config_id,config_generation,exit_policy_json,quantity,sold_quantity,cost_basis,fees,pending_exit_quantity,status,opened_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (position_id, reservation_id, event_id, str(row.get("venue") or "polymarket"), str(row.get("market_id") or ""), execution_asset_id, candidate_id, evidence.get("strategy_id"), evidence.get("strategy_version"), strategy_hash, evidence.get("model_hash"), reservation_config_id, reservation_config_generation, _json(policy), str(quantity), "0", str(quantity * price + fees), str(fees), "0", policy_status, opened, _iso(now)),
            )
            created += 1
    return created


def _reconcile_entry_ledger(service: CanaryService, venue: Any, now: datetime) -> list[dict[str, Any]]:
    """Refresh pending BUY entries without posting or inventing aggregate fills."""
    connection = _connection(service)
    active_statuses = (
        "(UPPER(status) IN "
        "('ACCEPTED','SUBMITTED','SUBMITTING','UNKNOWN','PARTIAL',"
        "'PARTIALLY_FILLED','MATCHED','FILLED','OPEN') "
        "OR (UPPER(status) IN ('CONFIRMED','TRADE_STATUS_CONFIRMED',"
        "'SETTLED','TRADE_STATUS_SETTLED') "
        "AND COALESCE(settlement,'')='' "
        "AND CAST(COALESCE(submitted_quantity,'0') AS NUMERIC) "
        "> CAST(COALESCE(fill_quantity,'0') AS NUMERIC)) "
        "OR COALESCE(settlement,'')='PENDING')"
    )
    eligible_entries = (
        "SELECT * FROM canary_ledger WHERE UPPER(side)='BUY' "
        "AND exchange_order_id IS NOT NULL "
        "AND ("
        + active_statuses
        + " OR (UPPER(status) IN ('CANCELED','CANCELLED','EXPIRED','REJECTED','FAILED','ERROR') "
        "AND COALESCE(settlement,'')='')"
        " OR (UPPER(status) IN ('CANCELED','CANCELLED','EXPIRED','REJECTED','FAILED','ERROR') "
        "AND COALESCE(settlement,'')='TERMINAL')"
        " OR (UPPER(status) IN ('CONFIRMED','TRADE_STATUS_CONFIRMED',"
        "'SETTLED','TRADE_STATUS_SETTLED') "
        "AND COALESCE(settlement,'')='TERMINAL')"
        ") "
    )
    terminal_filter = "AND NOT (" + active_statuses + ") "
    with service.store._lock:
        try:
            cursor = connection.execute(
                "SELECT active_timestamp,active_event_id,"
                "terminal_timestamp,terminal_event_id "
                "FROM canary_position_reconciliation WHERE singleton=1"
            ).fetchone()
            active_cursor_timestamp = (
                str(cursor["active_timestamp"])
                if cursor is not None
                else ""
            )
            active_cursor_event_id = (
                str(cursor["active_event_id"])
                if cursor is not None
                else ""
            )
            terminal_cursor_timestamp = (
                str(cursor["terminal_timestamp"])
                if cursor is not None
                else ""
            )
            terminal_cursor_event_id = (
                str(cursor["terminal_event_id"])
                if cursor is not None
                else ""
            )

            def page(query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
                return [dict(row) for row in connection.execute(query, params).fetchall()]

            if active_cursor_timestamp:
                active_rows = page(
                    eligible_entries
                    + "AND "
                    + active_statuses
                    + "AND (timestamp > ? OR "
                    "(timestamp=? AND event_id>?)) "
                    "ORDER BY timestamp,event_id LIMIT ?",
                    (
                        active_cursor_timestamp,
                        active_cursor_timestamp,
                        active_cursor_event_id,
                        _MAX_ACTIVE_ENTRY_RECONCILIATION,
                    ),
                )
                if len(active_rows) < _MAX_ACTIVE_ENTRY_RECONCILIATION:
                    active_rows.extend(
                        page(
                            eligible_entries
                            + "AND "
                            + active_statuses
                            + "AND (timestamp < ? OR "
                            "(timestamp=? AND event_id<=?)) "
                            "ORDER BY timestamp,event_id LIMIT ?",
                            (
                                active_cursor_timestamp,
                                active_cursor_timestamp,
                                active_cursor_event_id,
                                _MAX_ACTIVE_ENTRY_RECONCILIATION - len(active_rows),
                            ),
                        )
                    )
            else:
                active_rows = page(
                    eligible_entries
                    + "AND "
                    + active_statuses
                    + "ORDER BY timestamp,event_id LIMIT ?",
                    (_MAX_ACTIVE_ENTRY_RECONCILIATION,),
                )

            terminal_limit = max(
                0,
                _MAX_ENTRY_RECONCILIATION - len(active_rows),
            )
            terminal_rows: list[dict[str, Any]] = []
            if terminal_limit:
                if terminal_cursor_timestamp:
                    terminal_rows = page(
                        eligible_entries
                        + terminal_filter
                        + "AND (timestamp > ? OR "
                        "(timestamp=? AND event_id>?)) "
                        "ORDER BY timestamp,event_id LIMIT ?",
                        (
                            terminal_cursor_timestamp,
                            terminal_cursor_timestamp,
                            terminal_cursor_event_id,
                            terminal_limit,
                        ),
                    )
                    if len(terminal_rows) < terminal_limit:
                        terminal_rows.extend(
                            page(
                                eligible_entries
                                + terminal_filter
                                + "AND (timestamp < ? OR "
                                "(timestamp=? AND event_id<=?)) "
                                "ORDER BY timestamp,event_id LIMIT ?",
                                (
                                    terminal_cursor_timestamp,
                                    terminal_cursor_timestamp,
                                    terminal_cursor_event_id,
                                    terminal_limit - len(terminal_rows),
                                ),
                            )
                        )
                else:
                    terminal_rows = page(
                        eligible_entries
                        + terminal_filter
                        + "ORDER BY timestamp,event_id LIMIT ?",
                        (terminal_limit,),
                    )
            rows = active_rows + terminal_rows
        except sqlite3.OperationalError:
            rows = []
            active_rows = []
            terminal_rows = []
    if not rows:
        return []
    get_order = _method(venue, "get_order")
    list_trades = _method(venue, "list_account_trades")
    active_keys = {
        (str(row.get("timestamp") or ""), str(row.get("event_id") or ""))
        for row in active_rows
    }
    terminal_keys = {
        (str(row.get("timestamp") or ""), str(row.get("event_id") or ""))
        for row in terminal_rows
    }
    active_cursor_candidate: tuple[str, str] | None = None
    terminal_cursor_candidate: tuple[str, str] | None = None
    results: list[dict[str, Any]] = []
    for row in rows:
        event_id = str(row.get("event_id") or "")
        order_id = str(row.get("exchange_order_id") or "")
        row_key = (
            str(row.get("timestamp") or ""),
            str(row.get("event_id") or ""),
        )
        if row_key in active_keys:
            # Advance the durable page cursor on every attempted poll so an
            # unchanged oldest obligation cannot starve later active work.
            active_cursor_candidate = row_key
        if row_key in terminal_keys:
            # Failed terminal rows remain UNKNOWN and are retried after the
            # keyset page wraps, while later terminal rows still get service.
            terminal_cursor_candidate = row_key
        try:
            entry_evidence = _decode(row.get("evidence_json"))
            execution_asset_id = str(
                entry_evidence.get("resolved_asset_id")
                or row.get("token_id")
                or ""
            ).strip()
            order = _call(get_order, order_id=order_id)
            if not isinstance(order, Mapping):
                raise CanaryBlocked("CANARY_ORDER_RESPONSE_INVALID")
            status = _order_status(order)
            raw_trades = _parse_trades(
                _call(
                    list_trades,
                    order_id=order_id,
                    token_id=execution_asset_id or None,
                    market=str(row.get("market_id") or "") or None,
                )
            )
            failed_trade = any(
                str(_value(trade, "status", "state", default="")).upper()
                in {"FAILED", "TRADE_STATUS_FAILED"}
                for trade in raw_trades
            )
            # A FAILED order cannot establish new ownership.  A FAILED
            # sibling trade is excluded, but must not erase unrelated stable
            # confirmations returned for the same order.
            trades = [] if status in _FAILED_ORDER else [
                trade for trade in raw_trades
                if str(_value(trade, "status", "state", default="")).upper()
                not in {"FAILED", "TRADE_STATUS_FAILED"}
            ]
            confirmed_states = frozenset({"CONFIRMED", "TRADE_STATUS_CONFIRMED"})
            confirmed_evidence = [
                trade
                for trade in trades
                if str(_value(trade, "status", "state", default="")).upper()
                in confirmed_states
            ]
            if any(_trade_quantity(trade) <= ZERO for trade in confirmed_evidence):
                raise CanaryBlocked("CANARY_TRADE_QUANTITY_UNAVAILABLE")
            confirmed_trades = [
                trade
                for trade in confirmed_evidence
                if _trade_quantity(trade) > ZERO
            ]
            positive_trades = [
                trade for trade in raw_trades if _trade_quantity(trade) > ZERO
            ]
            provisional_trades = [
                trade
                for trade in positive_trades
                if str(_value(trade, "status", "state", default="")).upper()
                not in {
                    "CONFIRMED",
                    "TRADE_STATUS_CONFIRMED",
                    "FAILED",
                    "TRADE_STATUS_FAILED",
                }
            ]
            terminal_order = status in _FAILED_ORDER or status in _CANCELED_ORDER
            # Validate every stable venue fill before writing any canonical
            # fill.  This prevents a later malformed row from leaving a
            # partially projected reconciliation.
            validated_trades: list[
                tuple[Mapping[str, Any], str, Decimal, Decimal, Decimal, datetime]
            ] = []
            seen_evidence: dict[str, tuple[Decimal, Decimal, Decimal, datetime]] = {}
            for index, trade in enumerate(confirmed_trades):
                fill_id = _trade_id(trade, order_id, index)
                fill_quantity = _trade_quantity(trade)
                fill_price = _trade_price(trade, dict(order))
                order_limit = _decimal(
                    _value(order, "price", "limit_price", default=None),
                    ZERO,
                )
                if order_limit > ZERO and fill_price > order_limit + DUST:
                    raise CanaryBlocked("CANARY_TRADE_PRICE_OUT_OF_BOUNDS")
                fill_fee = _trade_fee(trade, dict(order))
                matched_at = _confirmed_trade_time(trade)
                evidence_signature = (
                    fill_quantity,
                    fill_price,
                    fill_fee,
                    matched_at,
                )
                prior_evidence = seen_evidence.get(fill_id)
                if prior_evidence is not None:
                    if prior_evidence != evidence_signature:
                        raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
                    continue
                seen_evidence[fill_id] = evidence_signature
                validated_trades.append(
                    (
                        trade,
                        fill_id,
                        fill_quantity,
                        fill_price,
                        fill_fee,
                        matched_at,
                    )
                )
            quantity = ZERO
            cost = ZERO
            fees = ZERO
            risk_fill = getattr(service.store, "record_canary_fill", None)
            if validated_trades and not callable(risk_fill):
                raise CanaryBlocked("CANARY_RISK_FILL_UNAVAILABLE")
            risk_reservation_id = event_id
            with service.store._lock:
                try:
                    reservation_row = connection.execute(
                        "SELECT reservation_id FROM canary_risk_reservations "
                        "WHERE event_id=? AND UPPER(side)='BUY' "
                        "ORDER BY created_at DESC,reservation_id DESC LIMIT 1",
                        (event_id,),
                    ).fetchone()
                except sqlite3.OperationalError:
                    reservation_row = None
            if reservation_row is not None:
                risk_reservation_id = str(reservation_row["reservation_id"])
            else:
                adopt = getattr(service.store, "adopt_canary_legacy_reservation", None)
                if not callable(adopt):
                    raise CanaryBlocked("CANARY_LEGACY_ADOPTION_UNAVAILABLE")
                legacy_evidence = _decode(row.get("evidence_json"))
                requested_legacy_quantity = max(
                    _decimal(row.get("submitted_quantity"), ZERO),
                    _decimal(row.get("fill_quantity"), ZERO),
                )
                adoption = adopt(
                    event_id=event_id,
                    intent_id="legacy-intent:" + event_id,
                    reservation_id=event_id,
                    side="BUY",
                    market_id=str(row.get("market_id") or ""),
                    token_id=str(row.get("token_id") or ""),
                    requested_cost=row.get("requested_notional"),
                    quantity=requested_legacy_quantity,
                    timestamp=_stamp(row.get("timestamp"), now),
                    evidence=legacy_evidence,
                    legacy_detail={
                        "signal_id": str(row.get("signal_id") or ""),
                        "exchange_order_id": order_id,
                        "fill_quantity": row.get("fill_quantity"),
                        "actual_average_price": row.get("actual_average_price"),
                        "fees": row.get("fees"),
                    },
                    fee_reserve=legacy_evidence.get("fee_reserve", "0"),
                    legacy_status=str(row.get("status") or ""),
                    venue=str(row.get("venue") or "polymarket"),
                    candidate_id=str(row.get("candidate_id") or "legacy"),
                )
                risk_reservation_id = str(adoption.get("reservation_id") or event_id)
            with service.store._lock:
                try:
                    existing_fills = connection.execute(
                        "SELECT fill_id,quantity,price,fee,filled_at "
                        "FROM canary_risk_fills WHERE reservation_id=?",
                        (risk_reservation_id,),
                    ).fetchall()
                except sqlite3.OperationalError:
                    existing_fills = []
            existing_evidence: dict[str, tuple[Decimal, Decimal, Decimal, datetime]] = {}
            for existing in existing_fills:
                existing_time = parse_timestamp(existing["filled_at"])
                if existing_time is None:
                    raise CanaryBlocked("CANARY_TRADE_TIME_UNAVAILABLE")
                existing_evidence[str(existing["fill_id"])] = (
                    _decimal(existing["quantity"], Decimal("-1")),
                    _decimal(existing["price"], Decimal("-1")),
                    _decimal(existing["fee"], Decimal("-1")),
                    existing_time,
                )
                existing_quantity = _decimal(existing["quantity"], ZERO)
                existing_price = _decimal(existing["price"], ZERO)
                existing_fee = _decimal(existing["fee"], ZERO)
                if existing_quantity <= ZERO or existing_price <= ZERO or existing_fee < ZERO:
                    raise CanaryBlocked("CANARY_TRADE_EVIDENCE_UNAVAILABLE")
                quantity += existing_quantity
                cost += existing_quantity * existing_price
                fees += existing_fee
            for (
                _trade,
                fill_id,
                _fill_quantity,
                _fill_price,
                _fill_fee,
                _matched_at,
            ) in validated_trades:
                with service.store._lock:
                    global_fill = connection.execute(
                        "SELECT reservation_id FROM canary_risk_fills WHERE fill_id=?",
                        (fill_id,),
                    ).fetchone()
                if (
                    global_fill is not None
                    and str(global_fill["reservation_id"]) != risk_reservation_id
                ):
                    raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
            for (
                _trade,
                fill_id,
                fill_quantity,
                fill_price,
                fill_fee,
                matched_at,
            ) in validated_trades:
                prior_evidence = existing_evidence.get(fill_id)
                evidence_signature = (
                    fill_quantity,
                    fill_price,
                    fill_fee,
                    matched_at,
                )
                if prior_evidence is not None:
                    if prior_evidence != evidence_signature:
                        raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
                    continue
                quantity += fill_quantity
                cost += fill_quantity * fill_price
                fees += fill_fee
                if not callable(risk_fill):
                    raise CanaryBlocked("CANARY_RISK_FILL_UNAVAILABLE")
                risk_fill(
                    fill_id=fill_id,
                    reservation_id=risk_reservation_id,
                    quantity=fill_quantity,
                    price=fill_price,
                    cost=fill_quantity * fill_price + fill_fee,
                    fee=fill_fee,
                    filled_at=matched_at,
                    detail={
                        "event_id": event_id,
                        "side": "BUY",
                        "settlement_status": "CONFIRMED",
                        "order_status": status,
                    },
                )
            requested_quantity = _decimal(row.get("submitted_quantity"), ZERO)
            fill_over_plan = (
                requested_quantity > ZERO
                and quantity > requested_quantity + DUST
            )
            if fill_over_plan:
                with service.store._lock:
                    reservation_row = connection.execute(
                        "SELECT detail_json FROM canary_risk_reservations "
                        "WHERE reservation_id=?",
                        (risk_reservation_id,),
                    ).fetchone()
                    try:
                        reservation_detail = json.loads(
                            reservation_row["detail_json"] or "{}"
                        ) if reservation_row is not None else {}
                    except (TypeError, ValueError, json.JSONDecodeError):
                        reservation_detail = {}
                    reservation_detail["risk_breaker"] = "ACTUAL_FILL_OVER_PLAN"
                    reservation_detail["actual_quantity"] = str(quantity)
                    reservation_detail["submitted_quantity"] = str(requested_quantity)
                    connection.execute(
                        "UPDATE canary_risk_reservations SET detail_json=?,"
                        "status='UNKNOWN',released_at=NULL WHERE reservation_id=?",
                        (_json(reservation_detail), risk_reservation_id),
                    )
            has_confirmed_fill = bool(existing_fills or confirmed_trades)
            prior_quantity = _decimal(row.get("fill_quantity"), ZERO)
            if quantity <= ZERO:
                prior_status = str(row.get("status") or "").upper()
                prior_price = _decimal(row.get("actual_average_price"), Decimal("-1"))
                prior_fees = _decimal(row.get("fees"), Decimal("-1"))
                if (
                    prior_status in _OWNED_ENTRY_STATUSES
                    and prior_quantity > ZERO
                    and prior_price > ZERO
                    and prior_fees >= ZERO
                ):
                    quantity = prior_quantity
                    cost = prior_quantity * prior_price
                    fees = prior_fees
                else:
                    quantity = ZERO
                    cost = ZERO
                    fees = ZERO
            settlement = _authoritative_settlement(order, status)
            requested_quantity = _decimal(row.get("submitted_quantity"), ZERO)
            confirmed_quantity_complete = (
                has_confirmed_fill
                and not provisional_trades
                and requested_quantity > ZERO
                and quantity + DUST >= requested_quantity
            )
            terminal_outcomes_complete = (
                (terminal_order or confirmed_quantity_complete)
                and not provisional_trades
                and not fill_over_plan
            )
            if fill_over_plan:
                ledger_status = "UNKNOWN"
            elif has_confirmed_fill:
                # Canonical, individually confirmed fill evidence establishes
                # ownership but never synthesizes venue settlement.
                ledger_status = settlement or "CONFIRMED"
            elif status in _FAILED_ORDER:
                ledger_status = status
            elif status in _CANCELED_ORDER:
                ledger_status = status
            elif status in {"MATCHED", "FILLED"}:
                # Preserve non-final venue state and the reservation while no
                # stable confirmed trade has established owned inventory.
                ledger_status = status
            elif settlement is not None:
                ledger_status = status
            elif quantity > ZERO and requested_quantity > ZERO and quantity + DUST >= requested_quantity:
                ledger_status = "FILLED"
            elif quantity > ZERO:
                ledger_status = "PARTIALLY_FILLED"
            else:
                ledger_status = status
            settlement_marker = (
                None
                if fill_over_plan
                else "TERMINAL"
                if terminal_outcomes_complete
                else "PROVISIONAL"
                if provisional_trades
                else settlement
            )
            if terminal_outcomes_complete:
                release = getattr(service.store, "release_canary_capacity", None)
                if not callable(release):
                    raise CanaryBlocked("CANARY_RISK_RELEASE_UNAVAILABLE")
                release_detail = None
                if not has_confirmed_fill and not provisional_trades:
                    release_detail = {
                        "no_fill_confirmed": True,
                        "terminal_status": status,
                        "filled_quantity": "0",
                        "source": "POLYMARKET_ORDER_STATUS",
                        "trade_count": "0",
                        "trade_ids": [],
                    }
                release(risk_reservation_id, status="RELEASED", timestamp=now, detail=release_detail)
            average = cost / quantity if quantity > ZERO else None
            with service.store._lock, connection:
                connection.execute(
                    "UPDATE canary_ledger SET status=?,fill_quantity=?,actual_average_price=?,fees=?,exchange_order_id=?,settlement=? WHERE event_id=?",
                    (
                        ledger_status,
                        str(quantity),
                        str(average) if average is not None else row.get("actual_average_price"),
                        str(fees),
                        order_id,
                        settlement_marker,
                        event_id,
                    ),
                )
            results.append({"event_id": event_id, "status": ledger_status, "fill_quantity": str(quantity)})
        except CanaryBlocked as exc:
            with service.store._lock, connection:
                connection.execute("UPDATE canary_ledger SET status='UNKNOWN' WHERE event_id=?", (event_id,))
            results.append({"event_id": event_id, "status": "UNKNOWN", "reason": str(exc)})
        except Exception as exc:
            with service.store._lock, connection:
                connection.execute("UPDATE canary_ledger SET status='UNKNOWN' WHERE event_id=?", (event_id,))
            results.append({"event_id": event_id, "status": "UNKNOWN", "reason": type(exc).__name__})
    if active_cursor_candidate is not None or terminal_cursor_candidate is not None:
        active_cursor = active_cursor_candidate or (
            active_cursor_timestamp,
            active_cursor_event_id,
        )
        terminal_cursor = terminal_cursor_candidate or (
            terminal_cursor_timestamp,
            terminal_cursor_event_id,
        )
        with service.store._lock, connection:
            connection.execute(
                "UPDATE canary_position_reconciliation "
                "SET active_timestamp=?,active_event_id=?,"
                "terminal_timestamp=?,terminal_event_id=?,updated_at=? "
                "WHERE singleton=1",
                (
                    active_cursor[0],
                    active_cursor[1],
                    terminal_cursor[0],
                    terminal_cursor[1],
                    _iso(now),
                ),
            )
    return results


def _reserve_exit(service: CanaryService, *, request_id: str, lot: Mapping[str, Any], quantity: Decimal, config: Mapping[str, Any], now: datetime) -> Mapping[str, Any]:
    reserve = getattr(service.store, "reserve_canary_capacity", None)
    if not callable(reserve):
        raise CanaryBlocked("CANARY_RISK_RESERVATION_UNAVAILABLE")
    try:
        control_generation = int(config.get("control_generation", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        control_generation = 0
    if control_generation <= 0:
        control_generation = _control_generation(service) or 0
    if control_generation <= 0:
        raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
    identifier = str(config.get("config_id") or "").strip()
    config_hash = str(config.get("config_hash") or "").strip()
    generation = int(config.get("generation", 0) or 0)
    if not identifier or generation <= 0 or not config_hash:
        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
    # ``event_id`` is the canonical reservation identity.  Exit requests use
    # their own event ID so they cannot collide with the originating BUY.
    return reserve(
        intent_id=request_id,
        reservation_id=request_id,
        side="SELL",
        requested_cost="0",
        fee_reserve="0",
        quantity=str(quantity),
        market_id=str(lot.get("market_id") or ""),
        event_id=request_id,
        config_id=identifier,
        config_generation=generation,
        config_hash=config_hash,
        control_generation=control_generation,
        detail={
            "position_id": str(lot.get("position_id") or ""),
            "market_id": str(lot.get("market_id") or ""),
            "token_id": str(lot.get("token_id") or ""),
            "event_id": request_id,
            "settlement_status": "PENDING",
        },
    )


def submit_exit(service: CanaryService, position_id: str, venue: Any, *, expected_generation: int, config_id: str, allow_test_venue: bool = False) -> Mapping[str, Any]:
    """Submit one bounded SELL for an AXIOM-owned, reconciled lot."""
    service = _service(service)
    service.require_current_credential_binding()
    _check_venue(venue, allow_test_venue=allow_test_venue)
    _ensure_schema(service)
    now = ensure_utc(service.clock())
    config = _settings_fence(service, expected_generation, config_id)
    state = _control_state(service)
    if state == "KILLED":
        raise CanaryBlocked("CANARY_KILLED")
    if state not in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "ENTRY_PAUSED", "PAUSED"}:
        # DISARMED is read/reconcile-only; it must never create a new
        # transmission, including an authorized exit.
        raise CanaryBlocked("CANARY_NOT_ARMED")
    lot = _lot_row(service, str(position_id))
    if lot is None:
        _sync_entry_lots(service, now)
        lot = _lot_row(service, str(position_id))
    if lot is None:
        raise CanaryBlocked("CANARY_POSITION_NOT_FOUND")
    position_key = str(lot.get("position_id") or position_id)
    total_quantity = _decimal(lot.get("quantity"), ZERO)
    sold_quantity = _decimal(lot.get("sold_quantity"), ZERO)
    pending_quantity = _decimal(lot.get("pending_exit_quantity"), ZERO)
    quantity = max(ZERO, total_quantity - sold_quantity - pending_quantity)
    lot_status = str(lot.get("status") or "").upper()
    if lot_status == "MANAGEMENT_BLOCKED":
        raise CanaryBlocked("CANARY_POSITION_MANAGEMENT_BLOCKED")
    if quantity <= DUST or lot_status in {"CLOSED", "DUST", "DISPUTED"}:
        raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")
    if not _valid_exit_policy(_decode(lot.get("exit_policy_json"))):
        raise CanaryBlocked("CANARY_POSITION_MANAGEMENT_BLOCKED")
    existing = _request_rows(service, position_key)
    if any(str(item.get("side") or "").upper() == "SELL" for item in existing):
        raise CanaryBlocked("DUPLICATE_EXIT_REQUEST")
    market_id = str(lot.get("market_id") or "").strip()
    token_id = str(lot.get("token_id") or "").strip()
    context = _call(_method(venue, "market_context"), market_id=market_id, token_id=token_id)
    if not isinstance(context, Mapping):
        raise CanaryBlocked("CANARY_EXIT_MARKET_CONTEXT_INVALID")
    if context.get("accepting_orders") is False:
        raise CanaryBlocked("CANARY_MARKET_NOT_ACCEPTING_ORDERS")
    min_size = _decimal(context.get("min_order_size"), ZERO)
    if min_size > ZERO and quantity + DUST < min_size:
        raise CanaryBlocked("CANARY_EXIT_BELOW_MINIMUM")
    price = _extract_book_price(context, side="SELL")
    # Validate a test transport before reserving capacity so a missing method
    # is a deterministic preflight blocker, not an UNKNOWN post-boundary state.
    test_submit = _method(venue, "submit_limit_order") if allow_test_venue else None
    request_id = "exit:" + position_key + ":" + str(expected_generation) + ":" + uuid.uuid4().hex
    reservation = _reserve_exit(service, request_id=request_id, lot=lot, quantity=quantity, config=config, now=now)
    reservation_id = str(reservation.get("reservation_id") or "").strip()
    if reservation_id != request_id:
        raise CanaryBlocked("CANARY_RISK_RESERVATION_ID_INVALID")
    expected_pending = pending_quantity
    release = getattr(service.store, "release_canary_capacity", None)
    if not callable(release):
        raise CanaryBlocked("CANARY_RISK_RELEASE_UNAVAILABLE")

    def reject_prepared(reason: str) -> None:
        release(reservation_id, status="RELEASED", timestamp=ensure_utc(service.clock()))
        with service.store._lock, _connection(service):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status='REJECTED',last_error=?,updated_at=? WHERE request_id=?",
                (reason, _iso(ensure_utc(service.clock())), request_id),
            )
            _connection(service).execute(
                "UPDATE canary_position_lots SET status='OPEN',pending_exit_quantity=?,updated_at=? WHERE position_id=?",
                (str(expected_pending), _iso(ensure_utc(service.clock())), position_key),
            )

    try:
        with service.store._lock, _connection(service):
            updated = _connection(service).execute(
                "UPDATE canary_position_lots SET pending_exit_quantity=?,status='EXIT_PENDING',updated_at=? WHERE position_id=? AND status IN ('OPEN','EXIT_PENDING')",
                (str(expected_pending + quantity), _iso(now), position_key),
            )
            if int(updated.rowcount or 0) != 1:
                raise CanaryBlocked("DUPLICATE_EXIT_REQUEST")
            _connection(service).execute(
                "INSERT INTO canary_position_requests(request_id,position_id,reservation_id,event_id,venue,market_id,token_id,side,requested_quantity,status,expected_generation,config_id,submitted_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (request_id, position_key, reservation_id, request_id, lot.get("venue"), market_id, token_id, "SELL", str(quantity), "PREPARED", int(expected_generation), str(config_id), _iso(now), _iso(now)),
            )
    except (CanaryBlocked, sqlite3.IntegrityError) as exc:
        reject_prepared(str(exc))
        if isinstance(exc, CanaryBlocked):
            raise
        raise CanaryBlocked("DUPLICATE_EXIT_REQUEST") from exc

    attempt = getattr(service.store, "record_canary_submission_attempt", None)
    if not callable(attempt):
        reject_prepared("CANARY_RISK_ATTEMPT_UNAVAILABLE")
        raise CanaryBlocked("CANARY_RISK_ATTEMPT_UNAVAILABLE")
    try:
        control_generation = int(config.get("control_generation", 0) or 0)
        if control_generation <= 0:
            raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
        attempt(
            attempt_id=request_id + ":attempt",
            intent_id=request_id,
            side="SELL",
            attempted_at=now,
            status="ATTEMPTED",
            config_id=str(config.get("config_id") or ""),
            config_generation=int(expected_generation),
            config_hash=str(config.get("config_hash") or ""),
            control_generation=control_generation,
            detail={"position_id": position_key, "quantity": str(quantity), "venue": str(lot.get("venue") or "polymarket")},
        )
        with service.store._lock, _connection(service):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status='SUBMITTING',updated_at=? WHERE request_id=? AND status='PREPARED'",
                (_iso(ensure_utc(service.clock())), request_id),
            )
    except Exception as exc:
        reject_prepared(type(exc).__name__)
        raise CanaryBlocked("CANARY_SUBMISSION_BLOCKED") from exc

    def before_post() -> None:
        _submission_fence(
            service,
            expected_generation=int(expected_generation),
            config_id=str(config_id),
            expected_control_generation=control_generation,
        )
        with service.store._lock:
            row = _connection(service).execute(
                "SELECT status,requested_quantity FROM canary_position_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            lot_check = _connection(service).execute(
                "SELECT quantity,sold_quantity,status FROM canary_position_lots WHERE position_id=?",
                (position_key,),
            ).fetchone()
        if row is None or str(row["status"] or "").upper() != "SUBMITTING":
            raise CanaryBlocked("CANARY_SUBMISSION_PHASE_CHANGED")
        if lot_check is None or str(lot_check["status"] or "").upper() not in {"OPEN", "EXIT_PENDING"}:
            raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")
        owned = max(ZERO, _decimal(lot_check["quantity"], ZERO) - _decimal(lot_check["sold_quantity"], ZERO))
        if owned + DUST < quantity:
            raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")

    transport_state_lock = threading.Lock()
    submission_cancelled = threading.Event()
    network_send_started = False

    def on_send_started() -> None:
        nonlocal network_send_started
        # The timeout path and the last pre-POST boundary are serialized:
        # either transmission is durably uncertain, or a timed-out worker
        # is prevented from posting later.
        with transport_state_lock:
            if submission_cancelled.is_set():
                raise CanaryBlocked("CANARY_SUBMISSION_TIMEOUT")
            network_send_started = True
        with service.store._lock, _connection(service):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status='SUBMITTING',updated_at=? WHERE request_id=?",
                (_iso(ensure_utc(service.clock())), request_id),
            )

    def external_submission() -> Any:
        if allow_test_venue:
            _submission_fence(
                service,
                expected_generation=int(expected_generation),
                config_id=str(config_id),
                expected_control_generation=control_generation,
            )
            on_send_started()
            return _call(
                test_submit,
                token_id=token_id,
                side="SELL",
                price=price,
                size=quantity,
            )
        return service.submit_position_order(
            market_version=context.get("market_version"),
            neg_risk=context.get("neg_risk"),
            asset_id=str(
                context.get("asset_id") or context.get("position_id") or token_id
            ),
            side="SELL",
            price=price,
            size=quantity,
            before_post=before_post,
            on_send_started=on_send_started,
        )

    def persist_submission_failure(status: str, error: str) -> None:
        with service.store._lock, _connection(service):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status=?,last_error=?,updated_at=? WHERE request_id=?",
                (status, error, _iso(ensure_utc(service.clock())), request_id),
            )
            if status == "REJECTED":
                _connection(service).execute(
                    "UPDATE canary_position_lots SET status='OPEN',pending_exit_quantity=?,updated_at=? WHERE position_id=?",
                    (
                        str(expected_pending),
                        _iso(ensure_utc(service.clock())),
                        position_key,
                    ),
                )
            else:
                _connection(service).execute(
                    "UPDATE canary_risk_reservations SET status='UNKNOWN',released_at=NULL,updated_at=? WHERE reservation_id=?",
                    (_iso(ensure_utc(service.clock())), reservation_id),
                )
        if status == "REJECTED":
            release(
                reservation_id,
                status="RELEASED",
                timestamp=ensure_utc(service.clock()),
            )

    # PREPARED -> SUBMITTING is durable before this bounded transport call.
    try:
        response = _call_with_timeout(
            external_submission,
            CANARY_SUBMISSION_TIMEOUT_SECONDS,
        )
        if not isinstance(response, Mapping):
            raise CanaryBlocked("CANARY_ORDER_RESPONSE_INVALID")
    except TimeoutError as exc:
        with transport_state_lock:
            submission_cancelled.set()
            transport_started_before_timeout = network_send_started
        if transport_started_before_timeout:
            persist_submission_failure("UNKNOWN", type(exc).__name__)
            raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN") from exc
        persist_submission_failure("REJECTED", type(exc).__name__)
        raise CanaryBlocked("CANARY_SUBMISSION_TIMEOUT") from exc
    except Exception as exc:
        code = str(exc).upper()
        preflight_codes = {
            "CANARY_KILLED",
            "CANARY_NOT_ARMED",
            "CANARY_CONTROL_CHANGED",
            "CANARY_CONTROL_CORRUPT",
            "CANARY_SETTINGS_GENERATION_CHANGED",
            "CANARY_MARKET_NOT_ACCEPTING_ORDERS",
            "CANARY_EXIT_BELOW_MINIMUM",
            "CANARY_ALLOWANCE_INSUFFICIENT",
            "CANARY_ALLOWANCE_UNAVAILABLE",
            "CANARY_SPENDER_UNAVAILABLE",
            "CANARY_BALANCE_UNAVAILABLE",
            "INSUFFICIENT_BALANCE",
            "CREDENTIALS_NOT_CONFIGURED",
            "UNSUPPORTED_POLYMARKET_SDK",
            "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED",
            "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE",
            "ISOLATED_EXECUTION_PROFILE",
        }
        rejected = (
            any(code == item or code.endswith(":" + item) for item in preflight_codes)
            or code.startswith("CLOB_API_CREDENTIALS_")
            or code.startswith("CANARY_ALLOWANCE_")
            or code.startswith("OFFICIAL_POLYMARKET_SDK_")
        )
        status = "REJECTED" if rejected else "UNKNOWN"
        persist_submission_failure(status, type(exc).__name__)
        if status == "REJECTED":
            raise CanaryBlocked(str(exc)) from exc
        raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN") from exc

    ok_value = response.get("ok")
    status_value = response.get("status", response.get("state"))
    code_value = response.get("code", response.get("error_code"))
    malformed = (
        not isinstance(ok_value, bool)
        or (
            status_value is not None
            and not isinstance(status_value, str)
        )
        or (
            code_value is not None
            and not isinstance(code_value, str)
        )
    )
    status_text = (
        status_value.strip().upper()
        if isinstance(status_value, str)
        else ""
    )
    raw_order_id: Any = None
    for name in ("order_id", "orderId", "id", "exchange_order_id"):
        value = response.get(name)
        if raw_order_id is None and value is not None:
            raw_order_id = value
        if _canonical_exchange_order_id(value) is not None:
            raw_order_id = value
            break
    order_id = _canonical_exchange_order_id(raw_order_id)
    if malformed:
        status = "UNKNOWN"
        response_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
    elif status_text in _FAILED_ORDER or status_text in _CANCELED_ORDER:
        status = "REJECTED"
        response_error = None
    elif ok_value is True:
        if order_id is None:
            status = "UNKNOWN"
            response_error = "CANARY_EXTERNAL_IDENTITY_MISSING"
        elif status_text not in _ACCEPTED_ORDER_STATUSES:
            status = "UNKNOWN"
            response_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
        elif status_text in {"MATCHED", "FILLED", "PARTIAL", "PARTIALLY_FILLED", "OPEN", "ACKNOWLEDGED"}:
            status = _order_status(response)
            response_error = None
        else:
            status = "SUBMITTED"
            response_error = None
    elif _explicit_rejection_code(code_value):
        status = "REJECTED"
        response_error = None
    else:
        status = "UNKNOWN"
        response_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
    with service.store._lock, _connection(service):
        _connection(service).execute(
            "UPDATE canary_position_requests SET order_id=?,status=?,last_error=?,updated_at=? WHERE request_id=?",
            (order_id, status, response_error, _iso(ensure_utc(service.clock())), request_id),
        )
        if status == "REJECTED":
            _connection(service).execute(
                "UPDATE canary_position_lots SET status='OPEN',pending_exit_quantity=?,updated_at=? WHERE position_id=?",
                (str(expected_pending), _iso(ensure_utc(service.clock())), position_key),
            )
        else:
            reservation_status = "UNKNOWN" if status == "UNKNOWN" else "OPEN"
            # Persist the acknowledgement stage with the request.  This
            # transaction closes the crash window between an acknowledged
            # order and its reservation projection.
            _connection(service).execute(
                "UPDATE canary_risk_reservations SET status=?,released_at=NULL,updated_at=? WHERE reservation_id=?",
                (reservation_status, _iso(ensure_utc(service.clock())), reservation_id),
            )
    if status == "REJECTED":
        release(reservation_id, status="RELEASED", timestamp=ensure_utc(service.clock()))
    return {"request_id": request_id, "position_id": position_key, "reservation_id": reservation_id, "order_id": order_id, "status": status, "quantity": str(quantity), "price": str(price)}


def _apply_reconciled_request(service: CanaryService, request: Mapping[str, Any], order: Mapping[str, Any], trades: Sequence[Mapping[str, Any]], now: datetime) -> dict[str, Any]:
    connection = _connection(service)
    request_id = str(request.get("request_id") or "")
    position_id = str(request.get("position_id") or "")
    order_id = str(request.get("order_id") or _value(order, "order_id", "orderId", "id", default="") or "")
    order_status = _order_status(order)
    with service.store._lock:
        lot_context = connection.execute(
            "SELECT quantity,cost_basis FROM canary_position_lots WHERE position_id=?",
            (position_id,),
        ).fetchone()
    if lot_context is None:
        raise CanaryBlocked("CANARY_POSITION_NOT_FOUND")
    entry_quantity = _decimal(lot_context["quantity"], ZERO)
    entry_cost_basis = _decimal(lot_context["cost_basis"], ZERO)
    # Failed/provisional trade states are not sale evidence.  Only stable
    # CONFIRMED trade identities may mutate the owned lot or risk ledger.
    usable_trades = [] if order_status in _FAILED_ORDER else [
        trade for trade in trades
        if str(_value(trade, "status", "state", default="")).upper()
        not in {"FAILED", "TRADE_STATUS_FAILED"}
    ]
    confirmed_states = frozenset({"CONFIRMED", "TRADE_STATUS_CONFIRMED"})
    confirmed_evidence = [
        trade
        for trade in usable_trades
        if str(_value(trade, "status", "state", default="")).upper()
        in confirmed_states
    ]
    if any(_trade_quantity(trade) <= ZERO for trade in confirmed_evidence):
        raise CanaryBlocked("CANARY_TRADE_QUANTITY_UNAVAILABLE")
    positive_trades = [
        trade for trade in usable_trades if _trade_quantity(trade) > ZERO
    ]
    stable_trades = [
        trade
        for trade in positive_trades
        if str(_value(trade, "status", "state", default="")).upper()
        in confirmed_states
    ]
    provisional_trades = [
        trade for trade in positive_trades if trade not in stable_trades
    ]
    # Validate every stable venue fill before writing any canonical fill.
    validated_trades: list[
        tuple[Mapping[str, Any], str, Decimal, Decimal, Decimal, datetime]
    ] = []
    seen_evidence: dict[str, tuple[Decimal, Decimal, Decimal, datetime]] = {}
    for index, trade in enumerate(stable_trades):
        fill_id = _trade_id(trade, order_id or request_id, index)
        quantity = _trade_quantity(trade)
        price = _trade_price(trade, order)
        order_limit = _decimal(
            _value(order, "price", "limit_price", default=None),
            ZERO,
        )
        if order_limit > ZERO and price + DUST < order_limit:
            raise CanaryBlocked("CANARY_TRADE_PRICE_OUT_OF_BOUNDS")
        fee = _trade_fee(trade, order)
        matched_at = _confirmed_trade_time(trade)
        evidence_signature = (quantity, price, fee, matched_at)
        prior_evidence = seen_evidence.get(fill_id)
        if prior_evidence is not None:
            if prior_evidence != evidence_signature:
                raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
            continue
        seen_evidence[fill_id] = evidence_signature
        validated_trades.append(
            (trade, fill_id, quantity, price, fee, matched_at)
        )
    existing_local: dict[str, tuple[Decimal, Decimal, Decimal, datetime]] = {}
    for (
        _trade,
        fill_id,
        quantity,
        price,
        fee,
        matched_at,
    ) in validated_trades:
        with service.store._lock:
            prior = connection.execute(
                "SELECT quantity,price,fee,filled_at "
                "FROM canary_position_fills WHERE fill_id=?",
                (fill_id,),
            ).fetchone()
        if prior is None:
            continue
        prior_time = parse_timestamp(prior["filled_at"])
        if prior_time is None:
            raise CanaryBlocked("CANARY_TRADE_TIME_UNAVAILABLE")
        prior_evidence = (
            _decimal(prior["quantity"], Decimal("-1")),
            _decimal(prior["price"], Decimal("-1")),
            _decimal(prior["fee"], Decimal("-1")),
            prior_time,
        )
        if prior_evidence != (quantity, price, fee, matched_at):
            raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
        existing_local[fill_id] = prior_evidence
    order_settlement = _authoritative_settlement(order, order_status)
    inserted = 0
    for (
        trade,
        fill_id,
        quantity,
        price,
        fee,
        matched_at,
    ) in validated_trades:
        if fill_id in existing_local:
            continue
        risk_fill = getattr(service.store, "record_canary_fill", None)
        reservation_id = str(request.get("reservation_id") or "").strip()
        if not callable(risk_fill) or not reservation_id:
            raise CanaryBlocked("CANARY_RISK_FILL_UNAVAILABLE")
        entry_cost = (
            entry_cost_basis * quantity / entry_quantity
            if entry_quantity > ZERO
            else ZERO
        )
        net_proceeds = quantity * price - fee
        realized_pnl = net_proceeds - entry_cost
        risk_detail = {
            "position_id": position_id,
            "request_id": request_id,
            "settlement_status": "CONFIRMED",
            "entry_cost_usd": str(entry_cost),
            "cost_basis_usd": str(entry_cost),
            "proceeds_usd": str(net_proceeds),
            "realized_pnl_usd": str(realized_pnl),
            "exit_fee_usd": str(fee),
        }
        risk_fill(
            fill_id=fill_id,
            reservation_id=reservation_id,
            quantity=quantity,
            price=price,
            cost=quantity * price + fee,
            fee=fee,
            filled_at=matched_at,
            detail=risk_detail,
        )
        with service.store._lock, connection:
            connection.execute(
                "INSERT INTO canary_position_fills(fill_id,request_id,position_id,quantity,price,fee,status,filled_at,detail_json) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    fill_id,
                    request_id,
                    position_id,
                    str(quantity),
                    str(price),
                    str(fee),
                    "CONFIRMED",
                    _iso(matched_at),
                    _json(dict(trade)),
                ),
            )
            inserted += 1
    with service.store._lock:
        fill_rows = connection.execute(
            "SELECT quantity,fee,price FROM canary_position_fills WHERE request_id=? ORDER BY filled_at,fill_id",
            (request_id,),
        ).fetchall()
        existing_qty = sum((_decimal(row["quantity"], ZERO) for row in fill_rows), ZERO)
        existing_fees = sum((_decimal(row["fee"], ZERO) for row in fill_rows), ZERO)
        existing_cost = sum(
            (_decimal(row["quantity"], ZERO) * _decimal(row["price"], ZERO) for row in fill_rows),
            ZERO,
        )
        requested = _decimal(request.get("requested_quantity"), ZERO)
        prior_request_qty = _decimal(request.get("filled_quantity"), ZERO)
        prior_request_avg = _decimal(request.get("average_price"), ZERO)
        prior_request_fees = _decimal(request.get("fees"), ZERO)
        delta_qty = max(ZERO, existing_qty - prior_request_qty)
        delta_gross = max(ZERO, existing_cost - (prior_request_qty * prior_request_avg))
        delta_fees = max(ZERO, existing_fees - prior_request_fees)
        lot_row = connection.execute(
            "SELECT quantity,cost_basis,sold_quantity,pending_exit_quantity,gross_proceeds,exit_fees,realized_pnl "
            "FROM canary_position_lots WHERE position_id=?",
            (position_id,),
        ).fetchone()
        prior_sold = _decimal(lot_row["sold_quantity"], ZERO) if lot_row is not None else ZERO
        prior_pending = _decimal(lot_row["pending_exit_quantity"], ZERO) if lot_row is not None else ZERO
        prior_gross = _decimal(lot_row["gross_proceeds"], ZERO) if lot_row is not None else ZERO
        prior_exit_fees = _decimal(lot_row["exit_fees"], ZERO) if lot_row is not None else ZERO
        prior_pnl = _decimal(lot_row["realized_pnl"], ZERO) if lot_row is not None else ZERO
        lot_quantity = _decimal(lot_row["quantity"], ZERO) if lot_row is not None else ZERO
        lot_basis = _decimal(lot_row["cost_basis"], ZERO) if lot_row is not None else ZERO
        fill_over_plan = (
            existing_qty > requested + DUST
            or prior_sold + delta_qty > lot_quantity + DUST
        )
        if fill_over_plan:
            reservation_id = str(request.get("reservation_id") or "").strip()
            reservation_row = connection.execute(
                "SELECT detail_json FROM canary_risk_reservations "
                "WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            try:
                reservation_detail = json.loads(
                    reservation_row["detail_json"] or "{}"
                ) if reservation_row is not None else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                reservation_detail = {}
            reservation_detail["risk_breaker"] = "EXIT_FILL_OVER_PLAN"
            reservation_detail["actual_quantity"] = str(existing_qty)
            reservation_detail["requested_quantity"] = str(requested)
            connection.execute(
                "UPDATE canary_risk_reservations SET detail_json=?,"
                "status='UNKNOWN',released_at=NULL WHERE reservation_id=?",
                (_json(reservation_detail), reservation_id),
            )
            connection.execute(
                "UPDATE canary_position_requests SET order_id=?,filled_quantity=?,"
                "average_price=?,fees=?,status='UNKNOWN',settlement_status=NULL,"
                "updated_at=? WHERE request_id=?",
                (
                    order_id or None,
                    str(existing_qty),
                    str(existing_cost / existing_qty) if existing_qty > ZERO else None,
                    str(existing_fees),
                    _iso(now),
                    request_id,
                ),
            )
            connection.execute(
                "UPDATE canary_position_lots SET status='EXIT_PENDING',updated_at=? "
                "WHERE position_id=?",
                (_iso(now), position_id),
            )
            return {
                "request_id": request_id,
                "position_id": position_id,
                "status": "UNKNOWN",
                "filled_quantity": str(existing_qty),
                "new_fills": inserted,
                "order_id": order_id or None,
                "blocker": "EXIT_FILL_OVER_PLAN",
            }
        settlement_status = order_settlement
        terminal = (
            settlement_status is not None
            or order_status in _FAILED_ORDER
            or (order_status in _CANCELED_ORDER and not provisional_trades)
        )
        if settlement_status is not None:
            status = "SETTLED" if existing_qty + DUST >= requested else "SETTLED_PARTIAL"
        elif provisional_trades:
            # A canceled/matched order with provisional venue evidence still
            # owns an unresolved settlement obligation.  Keep the reservation
            # and pending quantity so no duplicate SELL can be admitted.
            status = "MATCHED"
        else:
            status = order_status
        pending_total = ZERO if terminal else prior_pending
        sold_total = prior_sold + delta_qty
        gross_total = prior_gross + delta_gross
        exit_fees_total = prior_exit_fees + delta_fees
        basis_delta = (delta_qty * lot_basis / lot_quantity) if lot_quantity > ZERO else ZERO
        pnl_total = prior_pnl + delta_gross - delta_fees - basis_delta
        lot_status = (
            "CLOSED"
            if settlement_status is not None and status == "SETTLED" and max(ZERO, requested - existing_qty) <= DUST
            else "OPEN"
            if terminal
            else "EXIT_PENDING"
        )
        connection.execute(
            "UPDATE canary_position_requests SET order_id=?,filled_quantity=?,average_price=?,fees=?,status=?,settlement_status=?,updated_at=? WHERE request_id=?",
            (
                order_id or None,
                str(existing_qty),
                str(existing_cost / existing_qty) if existing_qty > ZERO else None,
                str(existing_fees),
                status,
                settlement_status,
                _iso(now),
                request_id,
            ),
        )
        connection.execute(
            "UPDATE canary_position_lots SET sold_quantity=?,pending_exit_quantity=?,gross_proceeds=?,exit_fees=?,realized_pnl=?,status=?,updated_at=? WHERE position_id=?",
            (
                str(sold_total),
                str(pending_total),
                str(gross_total),
                str(exit_fees_total),
                str(pnl_total),
                lot_status,
                _iso(now),
                position_id,
            ),
        )
    if terminal:
        release = getattr(service.store, "release_canary_capacity", None)
        if not callable(release):
            raise CanaryBlocked("CANARY_RISK_RELEASE_UNAVAILABLE")
        reservation_id = str(request.get("reservation_id") or "").strip()
        if reservation_id:
            release(
                reservation_id,
                status="SETTLED" if settlement_status is not None else "RELEASED",
                timestamp=now,
            )
    return {
        "request_id": request_id,
        "position_id": position_id,
        "status": status,
        "filled_quantity": str(existing_qty),
        "new_fills": inserted,
        "order_id": order_id or None,
    }


def reconcile_pending(service: CanaryService, venue: Any, *, allow_test_venue: bool = False) -> Mapping[str, Any]:
    """Reconcile every pending/UNKNOWN request using official order/trade reads."""
    service = _service(service)
    try:
        service.require_current_credential_binding()
    except CanaryBlocked as exc:
        reason = str(exc)
        return {
            "status": "DEGRADED",
            "reconciled": 0,
            "blocked": 1,
            "requests": [{"status": "UNKNOWN", "reason": reason}],
            "entries": [],
            "equity": {
                "status": "UNKNOWN",
                "marks": [],
                "blocked": [{"reason": reason}],
            },
        }
    _check_venue(venue, allow_test_venue=allow_test_venue)
    _ensure_schema(service)
    now = ensure_utc(service.clock())
    entry_results: list[dict[str, Any]] = []
    try:
        entry_results = _reconcile_entry_ledger(service, venue, now)
    except CanaryBlocked as exc:
        # Entry reconciliation may be blocked independently of read-only
        # valuation; retain fresh marks for already-owned obligations.
        equity = _mark_owned_equity(service, venue, now)
        return {
            "status": "DEGRADED",
            "reconciled": 0,
            "blocked": 1,
            "requests": [{"status": "UNKNOWN", "reason": str(exc)}],
            "entries": [],
            "equity": equity,
        }
    _sync_entry_lots(service, now)
    equity = _mark_owned_equity(service, venue, now)
    requests = _request_rows(service)
    if not requests and not entry_results:
        return {
            "status": "IDLE",
            "reconciled": 0,
            "blocked": 0,
            "requests": [],
            "entries": [],
            "equity": equity,
        }
    results: list[dict[str, Any]] = []
    blocked = sum(1 for item in entry_results if str(item.get("status") or "").upper() == "UNKNOWN")
    with_order: list[dict[str, Any]] = []
    for request in requests:
        order_id = str(request.get("order_id") or "").strip()
        if order_id:
            with_order.append(request)
            continue
        request_id = str(request.get("request_id") or "")
        request_status = str(request.get("status") or "").upper()
        # PREPARED/legacy EXIT_REQUESTED with no recorded attempt is known
        # unsent work and may safely release its reservation after a crash.
        attempted = False
        if request_status == "EXIT_REQUESTED":
            with service.store._lock:
                attempted = _connection(service).execute(
                    "SELECT 1 FROM canary_submission_attempts WHERE intent_id=? LIMIT 1",
                    (request_id,),
                ).fetchone() is not None
        if request_status == "PREPARED" or (request_status == "EXIT_REQUESTED" and not attempted):
            reservation_id = str(request.get("reservation_id") or "").strip()
            release = getattr(service.store, "release_canary_capacity", None)
            if reservation_id and callable(release):
                release(reservation_id, status="RELEASED", timestamp=now)
            requested = _decimal(request.get("requested_quantity"), ZERO)
            position_id = str(request.get("position_id") or "")
            with service.store._lock, _connection(service):
                lot = _connection(service).execute(
                    "SELECT pending_exit_quantity FROM canary_position_lots WHERE position_id=?",
                    (position_id,),
                ).fetchone()
                pending = max(ZERO, _decimal(lot["pending_exit_quantity"], ZERO) - requested) if lot is not None else ZERO
                _connection(service).execute(
                    "UPDATE canary_position_requests SET status='REJECTED',last_error=?,updated_at=? WHERE request_id=?",
                    ("CRASH_BEFORE_SUBMISSION", _iso(now), request_id),
                )
                _connection(service).execute(
                    "UPDATE canary_position_lots SET status=?,pending_exit_quantity=?,updated_at=? WHERE position_id=?",
                    ("EXIT_PENDING" if pending > DUST else "OPEN", str(pending), _iso(now), position_id),
                )
            results.append({"request_id": request_id, "status": "REJECTED", "reason": "CRASH_BEFORE_SUBMISSION"})
        else:
            blocked += 1
            with service.store._lock, _connection(service):
                _connection(service).execute(
                    "UPDATE canary_position_requests SET status='UNKNOWN',last_error=?,updated_at=? WHERE request_id=?",
                    ("ORDER_ID_UNAVAILABLE", _iso(now), request_id),
                )
            results.append({"request_id": request_id, "status": "UNKNOWN", "reason": "ORDER_ID_UNAVAILABLE"})
    if not with_order and not entry_results:
        return {
            "status": "DEGRADED" if blocked else "RECONCILED",
            "reconciled": len(results) - blocked,
            "blocked": blocked,
            "requests": results,
            "entries": entry_results,
            "equity": equity,
        }
    get_order = _method(venue, "get_order")
    list_trades = _method(venue, "list_account_trades")
    for request in with_order:
        order_id = str(request.get("order_id") or "").strip()
        try:
            order = _call(get_order, order_id=order_id)
            if not isinstance(order, Mapping):
                raise CanaryBlocked("CANARY_ORDER_RESPONSE_INVALID")
            trades = _call(list_trades, order_id=order_id)
            results.append(_apply_reconciled_request(service, request, dict(order), _parse_trades(trades), now))
        except CanaryBlocked as exc:
            blocked += 1
            with service.store._lock, _connection(service):
                _connection(service).execute(
                    "UPDATE canary_position_requests SET status='UNKNOWN',last_error=?,updated_at=? WHERE request_id=?",
                    (str(exc), _iso(now), str(request.get("request_id") or "")),
                )
            results.append({"request_id": request.get("request_id"), "status": "UNKNOWN", "reason": str(exc)})
        except Exception as exc:
            blocked += 1
            with service.store._lock, _connection(service):
                _connection(service).execute(
                    "UPDATE canary_position_requests SET status='UNKNOWN',last_error=?,updated_at=? WHERE request_id=?",
                    (type(exc).__name__, _iso(now), str(request.get("request_id") or "")),
                )
            results.append({"request_id": request.get("request_id"), "status": "UNKNOWN", "reason": type(exc).__name__})
    return {
        "status": "DEGRADED" if blocked else "RECONCILED",
        "reconciled": len(results) + len(entry_results) - blocked,
        "blocked": blocked,
        "requests": results,
        "entries": entry_results,
        "equity": equity,
    }


def _valid_exit_policy(value: Any) -> bool:
    if not isinstance(value, Mapping) or not value:
        return False
    kind = str(value.get("type", value.get("kind", ""))).strip().lower()
    if kind in {"manual", "none", "disabled"}:
        return True
    if kind not in {"fixed_holding_period", "holding_period", "time", "duration", ""}:
        return False
    if "holding_period_seconds" in value:
        raw_period = value.get("holding_period_seconds")
    elif "max_hold_seconds" in value:
        raw_period = value.get("max_hold_seconds")
    elif "max_age_seconds" in value:
        raw_period = value.get("max_age_seconds")
    else:
        raw_period = value.get("holding_period")
    period = _decimal(raw_period, Decimal("-1"))
    return period.is_finite() and period >= ZERO

def _policy_due(lot: Mapping[str, Any], now: datetime) -> bool:
    policy = _decode(lot.get("exit_policy_json"))
    kind = str(policy.get("type", policy.get("kind", "fixed_holding_period"))).strip().lower()
    if kind in {"manual", "none", "disabled"}:
        return False
    opened = _stamp(lot.get("opened_at"), now)
    if "holding_period_seconds" in policy:
        raw_period = policy.get("holding_period_seconds")
        multiplier = 1.0
    elif "max_hold_seconds" in policy:
        raw_period = policy.get("max_hold_seconds")
        multiplier = 1.0
    else:
        raw_period = policy.get("holding_period")
        multiplier = 86400.0 if str(policy.get("unit", "days")).lower().startswith("day") else 1.0
    if raw_period is None:
        return False
    try:
        seconds = float(raw_period) * multiplier
    except (TypeError, ValueError):
        return False
    return seconds >= 0 and (now - opened).total_seconds() >= seconds


def manage_positions(service: CanaryService, venue: Any, *, allow_test_venue: bool = False) -> Mapping[str, Any]:
    """Admit only bounded exits for lots owned by AXIOM's canary ledger."""
    service = _service(service)
    try:
        service.require_current_credential_binding()
    except CanaryBlocked as exc:
        return {
            "status": "BLOCKED",
            "submitted": 0,
            "blocked": str(exc),
            "positions": [],
        }
    _ensure_schema(service)
    state = _control_state(service)
    if state == "KILLED":
        return {"status": "KILL_LATCHED", "submitted": 0, "blocked": "CANARY_KILLED", "positions": []}
    if state in {"DISARMED", "DISABLED"}:
        return {"status": "DISARMED", "submitted": 0, "blocked": [], "positions": []}
    if state not in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "ENTRY_PAUSED", "PAUSED"}:
        return {"status": "BLOCKED", "submitted": 0, "blocked": "CANARY_NOT_ARMED", "positions": []}
    now = ensure_utc(service.clock())
    with service.store._lock:
        lots = [
            dict(row)
            for row in _connection(service).execute(
                "SELECT * FROM canary_position_lots "
                "WHERE status IN ('OPEN','EXIT_PENDING','MANAGEMENT_BLOCKED') "
                "ORDER BY opened_at,position_id"
            ).fetchall()
        ]
    submitted: list[Mapping[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for lot in lots:
        if str(lot.get("status") or "").upper() == "MANAGEMENT_BLOCKED":
            blocked.append({
                "position_id": lot.get("position_id"),
                "reason": "CANARY_POSITION_MANAGEMENT_BLOCKED",
            })
            continue
        if not _valid_exit_policy(_decode(lot.get("exit_policy_json"))):
            blocked.append({
                "position_id": lot.get("position_id"),
                "reason": "CANARY_POSITION_MANAGEMENT_BLOCKED",
            })
            continue
        available = max(
            ZERO,
            _decimal(lot.get("quantity"), ZERO)
            - _decimal(lot.get("sold_quantity"), ZERO)
            - _decimal(lot.get("pending_exit_quantity"), ZERO),
        )
        if available <= DUST or not _policy_due(lot, now):
            continue
        try:
            config = service.settings.snapshot(now=now) if service.settings is not None else {}
            result = submit_exit(service, str(lot["position_id"]), venue, expected_generation=int(config.get("generation", 0)), config_id=str(config.get("config_id") or ""), allow_test_venue=allow_test_venue)
            submitted.append(result)
        except CanaryBlocked as exc:
            blocked.append({"position_id": lot.get("position_id"), "reason": str(exc)})
    return {"status": "SUBMITTED" if submitted else ("BLOCKED" if blocked else "IDLE"), "submitted": len(submitted), "blocked": blocked, "positions": submitted}


@dataclass(frozen=True, slots=True)
class OwnedPosition:
    position_id: str
    market_id: str
    token_id: str
    quantity: Decimal
    cost_basis: Decimal
    strategy_hash: str | None
    strategy_version: str | None
    exit_policy: Mapping[str, Any]
def list_positions(service: CanaryService, *, venue: str | None = None, include_closed: bool = False) -> list[OwnedPosition]:
    service = _service(service)
    _ensure_schema(service)
    clauses: list[str] = []
    values: list[Any] = []
    if venue is not None:
        clauses.append("venue=?")
        values.append(str(venue))
    if not include_closed:
        clauses.append("status NOT IN ('CLOSED','DUST')")
    query = "SELECT * FROM canary_position_lots"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY opened_at,position_id"
    with service.store._lock:
        rows = _connection(service).execute(query, values).fetchall()
    return [
        OwnedPosition(
            position_id=str(row["position_id"]),
            market_id=str(row["market_id"]),
            token_id=str(row["token_id"]),
            quantity=max(ZERO, _decimal(row["quantity"]) - _decimal(row["sold_quantity"])),
            cost_basis=_decimal(row["cost_basis"]),
            strategy_hash=str(row["strategy_hash"]) if row["strategy_hash"] else None,
            strategy_version=str(row["strategy_version"]) if row["strategy_version"] else None,
            exit_policy=_decode(row["exit_policy_json"]),
        )
        for row in rows
    ]


# Names kept intentionally boring for callers that prefer an object boundary.
class CanaryPositionManager:
    def __init__(self, service: CanaryService):
        self.service = _service(service)

    def submit_exit(self, position_id: str, venue: Any, *, expected_generation: int, config_id: str, allow_test_venue: bool = False) -> Mapping[str, Any]:
        return submit_exit(self.service, position_id, venue, expected_generation=expected_generation, config_id=config_id, allow_test_venue=allow_test_venue)

    def reconcile_pending(self, venue: Any, *, allow_test_venue: bool = False) -> Mapping[str, Any]:
        return reconcile_pending(self.service, venue, allow_test_venue=allow_test_venue)

    def manage_positions(self, venue: Any, *, allow_test_venue: bool = False) -> Mapping[str, Any]:
        return manage_positions(self.service, venue, allow_test_venue=allow_test_venue)


PositionManager = CanaryPositionManager

__all__ = [
    "RECOVERY_ACTION",
    "RECOVERY_CONFIRMATION",
    "RECOVERY_ATTACHED",
    "UNKNOWN_ENTRY_STATUSES",
    "RecoveryProfileError",
    "normalize_recovery_profile",
    "recovery_identifier",
    "decimal_value",
    "timestamp_value",
    "mapping_value",
    "response_order_id",
    "response_side",
    "response_token",
    "response_market",
    "response_price",
    "response_quantity",
    "response_status",
    "response_timestamp",
    "trade_order_id",
    "trade_token",
    "trade_side",
    "trade_price",
    "trade_quantity",
    "trade_timestamp",
    "CanaryPositionManager", "PositionManager", "OwnedPosition", "list_positions",
    "submit_exit", "reconcile_pending", "manage_positions",
]
