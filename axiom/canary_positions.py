"""Durable canary position, exit, and reconciliation orchestration.

The entry ledger remains the authority for entry intents.  This module owns only
an auditable projection of AXIOM-owned lots and exit requests, while all risk
capacity and exchange fill identities are delegated to the canonical canary
risk APIs on :class:`AxiomStore`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
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
    parsed = decimal_value(
        mapping_value(order, "price", "limit_price", "limitPrice"),
        "order price",
        positive=True,
    )
    if parsed >= Decimal("1"):
        raise ValueError("invalid order price")
    return parsed


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
def trade_market(trade: Any) -> str | None:
    value = mapping_value(trade, "market_id", "market", "condition_id", "conditionId")
    text = str(value or "").strip()
    return text or None


def _required_identity(value: Any, reason: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CanaryBlocked(reason)
    return text


def _canonical_order_identity(value: Any, reason: str) -> str:
    canonical = _canonical_exchange_order_id(value)
    if canonical is None:
        raise CanaryBlocked(reason)
    return canonical



def trade_side(trade: Any) -> str:
    return str(mapping_value(trade, "side", "order_side") or "").strip().upper()


def trade_price(trade: Any) -> Decimal:
    parsed = decimal_value(
        mapping_value(trade, "price", "execution_price", "executionPrice"),
        "trade price",
        positive=True,
    )
    if parsed >= Decimal("1"):
        raise ValueError("invalid trade price")
    return parsed


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

from .polymarket_rules import (
    PolymarketRuleError,
    assess_selected_token_depth,
    parse_polymarket_rules,
)
from .canary import (
    CANARY_SUBMISSION_TIMEOUT_SECONDS,
    CanaryBlocked,
    CanaryService,
    PolymarketClobV2Venue,
    _account_trade_binding,
    _call_with_timeout,
    _canonical_exchange_order_id,
    _final_venue_identity_fence,
    _require_controller_lease,
    _require_execution_authorization,
    _validate_trade_history_coverage,
)
from .domain import ensure_utc, parse_timestamp, utc_now

UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
DUST = Decimal("0.00000001")
_MAX_ENTRY_RECONCILIATION = 100
_MAX_ACTIVE_ENTRY_RECONCILIATION = 80
_POSITION_LINEAGE_FIELDS = (
    "strategy_version_id",
    "research_trial_id",
    "portfolio_selection_id",
    "admission_policy_id",
    "admission_policy_version",
    "risk_config_id",
    "risk_config_generation",
    "risk_config_hash",
)
_POSITION_AUTHORITY_FIELDS = (
    "execution_authorization_id",
    "execution_authorization_mode",
    "controller_owner_id",
    "controller_generation",
)
_LEGACY_LINEAGE_TYPE = "LEGACY_FINITE_CAMPAIGN"
_ROLLING_LINEAGE_TYPE = "ROLLING_PORTFOLIO"

def _lineage_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return only lineage explicitly established by a rolling opening."""
    raw_type = (
        row["lineage_type"]
        if "lineage_type" in row.keys()
        else _LEGACY_LINEAGE_TYPE
    )
    lineage_type = str(raw_type or _LEGACY_LINEAGE_TYPE).strip().upper()
    authority = {
        name: row[name] if name in row.keys() else None
        for name in _POSITION_AUTHORITY_FIELDS
    }
    if lineage_type != _ROLLING_LINEAGE_TYPE:
        return {
            "lineage_type": _LEGACY_LINEAGE_TYPE,
            **{name: None for name in _POSITION_LINEAGE_FIELDS},
            **authority,
        }
    result = {
        name: row[name] if name in row.keys() else None
        for name in _POSITION_LINEAGE_FIELDS
    }
    result.update(authority)
    result["lineage_type"] = _ROLLING_LINEAGE_TYPE
    if result["risk_config_generation"] is not None:
        result["risk_config_generation"] = int(result["risk_config_generation"])
    return result


def _opening_authority(
    service: CanaryService,
    lot: Mapping[str, Any],
) -> dict[str, Any]:
    """Recover immutable opening authority from the lot and canonical BUY evidence."""
    authority = {
        name: lot.get(name)
        for name in _POSITION_AUTHORITY_FIELDS
        if lot.get(name) not in (None, "")
    }
    reservation_id = str(lot.get("reservation_id") or "").strip()
    if reservation_id:
        with service.store._lock:
            sources: list[Mapping[str, Any]] = []
            try:
                reservation = _connection(service).execute(
                    "SELECT execution_authorization_id,controller_owner_id,"
                    "controller_generation,detail_json "
                    "FROM canary_risk_reservations WHERE reservation_id=?",
                    (reservation_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                reservation = None
            if reservation is not None:
                sources.append(dict(reservation))
            try:
                fill = _connection(service).execute(
                    "SELECT execution_authorization_id,controller_owner_id,"
                    "controller_generation,detail_json "
                    "FROM canary_risk_fills WHERE reservation_id=? "
                    "ORDER BY filled_at DESC LIMIT 1",
                    (reservation_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                fill = None
            if fill is not None:
                sources.append(dict(fill))
        for source in sources:
            detail = _decode(source.get("detail_json"))
            for name in (
                "execution_authorization_id",
                "controller_owner_id",
                "controller_generation",
            ):
                if authority.get(name) in (None, "") and source.get(name) not in (
                    None,
                    "",
                ):
                    authority[name] = source.get(name)
            if authority.get("execution_authorization_mode") in (None, ""):
                mode = detail.get("execution_authorization_mode")
                if mode not in (None, ""):
                    authority["execution_authorization_mode"] = mode
    if authority.get("controller_generation") not in (None, ""):
        authority["controller_lease_generation"] = authority["controller_generation"]
    return authority


def _opening_lot_lineage(service: CanaryService, lot: Mapping[str, Any]) -> dict[str, Any]:
    """Return immutable lot lineage, including its opening allocation."""
    lineage = _lineage_from_row(lot)
    lineage.update(_opening_authority(service, lot))
    rolling = lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
    candidate_id = str(lot.get("candidate_id") or "").strip()
    lineage["candidate_id"] = (candidate_id or None) if rolling else None
    if not rolling:
        lineage["allocation"] = None
        return lineage
    allocation = lot.get("allocation")
    if allocation in (None, ""):
        opening_reservation_id = str(lot.get("reservation_id") or "").strip()
        if opening_reservation_id:
            with service.store._lock:
                try:
                    row = _connection(service).execute(
                        "SELECT allocation FROM canary_risk_reservations "
                        "WHERE reservation_id=? AND UPPER(side)='BUY'",
                        (opening_reservation_id,),
                    ).fetchone()
                except sqlite3.OperationalError:
                    row = None
            if row is not None:
                allocation = row["allocation"]
    lineage["allocation"] = allocation
    return lineage


def _entry_lineage(
    service: CanaryService,
    row: Mapping[str, Any],
    reservation_id: str,
) -> dict[str, Any]:
    """Return the exact persisted BUY lineage for risk-fill writes."""
    lineage = _lineage_from_row(row)
    rolling = lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
    candidate_id = str(row.get("candidate_id") or "").strip()
    lineage["candidate_id"] = (candidate_id or None) if rolling else None
    lineage["allocation"] = row.get("allocation") if rolling else None
    if rolling and lineage["allocation"] in (None, ""):
        with service.store._lock:
            try:
                reservation = _connection(service).execute(
                    "SELECT allocation FROM canary_risk_reservations "
                    "WHERE reservation_id=?",
                    (str(reservation_id),),
                ).fetchone()
            except sqlite3.OperationalError:
                reservation = None
        if reservation is not None:
            lineage["allocation"] = reservation["allocation"]
    return lineage


_PENDING = frozenset({
    "PREPARED", "SUBMITTING", "SUBMITTED", "ACCEPTED", "ACKNOWLEDGED",
    "OPEN", "LIVE", "DELAYED", "UNKNOWN", "MATCHED", "FILLED", "PARTIAL",
    "PARTIALLY_FILLED", "EXIT_REQUESTED", "RECONCILE_PENDING",
})
LEGACY_ENTRY_NON_RESUMABLE = "LEGACY_IDENTITY_UNRESOLVED"
LEGACY_REQUEST_NON_RESUMABLE = "LEGACY_PRICE_UNRESOLVED"
_TERMINAL = frozenset({
    "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED", "ERROR",
    "SETTLED", "SETTLED_PARTIAL", "FINAL", "CLOSED", "COMPLETED",
    LEGACY_ENTRY_NON_RESUMABLE, LEGACY_REQUEST_NON_RESUMABLE,
})
# A venue order being matched/filled proves execution, not settlement.  Only
# explicit settlement/resolution states may release the risk reservation.
_FINAL_SETTLEMENT = frozenset({
    "SETTLED", "SETTLED_PARTIAL", "FINAL", "CLOSED", "COMPLETED", "RESOLVED",
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
    "SETTLED_PARTIAL",
    "RESOLVED",
})
_ORDER_PENDING_STATUSES = frozenset({
    "ACCEPTED",
    "SUBMITTED",
    "SUBMITTING",
    "ACKNOWLEDGED",
    "OPEN",
    "MATCHED",
    "FILLED",
    "PARTIAL",
    "PARTIALLY_FILLED",
    "CONFIRMED",
    "TRADE_STATUS_CONFIRMED",
})
_KNOWN_ORDER_STATUSES = _ACCEPTED_ORDER_STATUSES | _TERMINAL | {"UNKNOWN"}
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


def _canonical_clob_price(value: Any, reason: str) -> Decimal:
    """Parse one executable prediction-market probability price."""
    result = _decimal(value, Decimal("-1"))
    if not ZERO < result < ONE:
        raise CanaryBlocked(reason)
    return result


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


def _identity_values(source: Mapping[str, Any], names: Sequence[str]) -> list[str]:
    values: list[str] = []
    for name in names:
        raw = source.get(name)
        text = str(raw or "").strip()
        if text:
            values.append(text)
    return values


def _strict_outcome_index(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if type(value) is int and value in (0, 1):
        return value
    if isinstance(value, str) and value in {"0", "1"}:
        return int(value)
    raise ValueError("outcome index must be canonical integer 0 or 1")

def _legacy_entry_identity(
    row: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize identity from persisted submission evidence, never a venue read."""
    token = str(row.get("token_id") or "").strip()
    if not token:
        return None, "CANARY_POSITION_IDENTITY_UNAVAILABLE"
    version_values = [
        value.lower()
        for value in _identity_values(evidence, ("market_version", "marketVersion"))
    ]
    version_hints = _identity_values(
        evidence,
        (
            "market_version", "marketVersion", "selected_token_id",
            "selectedTokenId", "resolved_asset_id", "asset_id", "assetId",
            "selected_position_id", "selectedPositionId", "position_id",
            "positionId", "outcome_index", "identity_bindings",
        ),
    )
    if len(set(version_values)) > 1:
        return None, "CANARY_POSITION_IDENTITY_CONFLICT"
    if not version_hints:
        version = "v1"
    elif not version_values:
        return None, "CANARY_POSITION_IDENTITY_UNAVAILABLE"
    else:
        version = version_values[0]
    if version not in {"v1", "v2"}:
        return None, "CANARY_POSITION_IDENTITY_CONFLICT"

    token_values = _identity_values(
        evidence,
        ("selected_token_id", "selectedTokenId", "token_id", "tokenId"),
    )
    if len(set(token_values)) > 1 or (token_values and token_values[0] != token):
        return None, "CANARY_POSITION_IDENTITY_CONFLICT"
    selected_token = token_values[0] if token_values else token
    asset_values = _identity_values(
        evidence,
        (
            "resolved_asset_id", "asset_id", "assetId",
            "selected_position_id", "selectedPositionId",
            "position_id", "positionId",
        ),
    )
    if len(set(asset_values)) > 1:
        return None, "CANARY_POSITION_IDENTITY_CONFLICT"
    if version == "v1":
        if asset_values and asset_values[0] != selected_token:
            return None, "CANARY_POSITION_IDENTITY_CONFLICT"
        normalized = dict(evidence)
        normalized.pop("marketVersion", None)
        for alias in (
            "asset_id",
            "assetId",
            "selected_position_id",
            "selectedPositionId",
            "position_id",
            "positionId",
        ):
            normalized.pop(alias, None)
        normalized.update(
            {
                "market_version": "v1",
                "selected_token_id": selected_token,
                "resolved_asset_id": selected_token,
            }
        )
        return normalized, None
    if not asset_values:
        return None, "CANARY_POSITION_IDENTITY_UNAVAILABLE"
    selected_asset = asset_values[0]
    try:
        outcome_index = _strict_outcome_index(evidence.get("outcome_index"))
    except ValueError:
        return None, "CANARY_POSITION_IDENTITY_CONFLICT"
    outcome = str(
        evidence.get("outcome", evidence.get("selected_outcome", "")) or ""
    ).strip().lower()
    if outcome in {"yes", "no"}:
        outcome_value = 0 if outcome == "yes" else 1
        if outcome_index is not None and outcome_index != outcome_value:
            return None, "CANARY_POSITION_IDENTITY_CONFLICT"
        outcome_index = outcome_value

    raw_bindings = evidence.get("identity_bindings")
    normalized_bindings: list[dict[str, Any]] = []
    if raw_bindings not in (None, ""):
        if not isinstance(raw_bindings, Sequence) or isinstance(
            raw_bindings, (str, bytes, bytearray)
        ):
            return None, "CANARY_POSITION_IDENTITY_CONFLICT"
        seen_indexes: set[int] = set()
        seen_tokens: set[str] = set()
        seen_assets: set[str] = set()
        for binding in raw_bindings:
            if not isinstance(binding, Mapping):
                return None, "CANARY_POSITION_IDENTITY_CONFLICT"
            bound_token = str(binding.get("token_id") or "").strip()
            bound_asset = str(binding.get("position_id") or "").strip()
            if not bound_token or not bound_asset:
                return None, "CANARY_POSITION_IDENTITY_CONFLICT"
            try:
                index = _strict_outcome_index(binding.get("index"))
            except ValueError:
                return None, "CANARY_POSITION_IDENTITY_CONFLICT"
            if (
                index is not None and index in seen_indexes
            ) or bound_token in seen_tokens or bound_asset in seen_assets:
                return None, "CANARY_POSITION_IDENTITY_CONFLICT"
            if index is not None:
                seen_indexes.add(index)
            seen_tokens.add(bound_token)
            seen_assets.add(bound_asset)
            normalized_bindings.append(
                {
                    key: binding[key]
                    for key in ("index", "outcome", "token_id", "position_id")
                    if key in binding
                }
            )
        matching = [
            item for item in normalized_bindings
            if item.get("token_id") == selected_token
        ]
        if len(matching) != 1 or matching[0].get("position_id") != selected_asset:
            return None, "CANARY_POSITION_IDENTITY_CONFLICT"
        bound_index = matching[0].get("index")
        if outcome_index is not None and bound_index is None:
            return None, "CANARY_POSITION_IDENTITY_CONFLICT"
        if bound_index is not None:
            if outcome_index is not None and int(bound_index) != outcome_index:
                return None, "CANARY_POSITION_IDENTITY_CONFLICT"
            outcome_index = int(bound_index)
        elif outcome_index is None:
            # Legacy evidence may list several outcomes without indices, but
            # the durable selected token/position identifies exactly one
            # authenticated binding.  Keep only that binding so downstream
            # legacy validation remains fail-closed and resumable.
            normalized_bindings = [matching[0]]
    if not normalized_bindings:
        normalized_bindings = [
            {
                "index": outcome_index,
                "token_id": selected_token,
                "position_id": selected_asset,
            }
        ]
    normalized = dict(evidence)
    for alias in (
        "asset_id",
        "assetId",
        "selected_position_id",
        "selectedPositionId",
        "position_id",
        "positionId",
    ):
        normalized.pop(alias, None)
    normalized.pop("marketVersion", None)
    normalized.update(
        {
            "market_version": "v2",
            "selected_token_id": selected_token,
            "selected_position_id": selected_asset,
            "resolved_asset_id": selected_asset,
            "outcome_index": outcome_index,
            "identity_bindings": normalized_bindings,
        }
    )
    if outcome_index is None:
        normalized["legacy_identity_binding"] = True
        normalized["identity_binding_source"] = "DURABLE_SUBMISSION_EVIDENCE"
    return normalized, None


def _migrate_legacy_entry_rows(service: CanaryService, connection: Any) -> None:
    """Backfill old BUY rows and terminalize unauthenticated identity."""
    try:
        rows = connection.execute(
            "SELECT * FROM canary_ledger WHERE UPPER(side)='BUY'"
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for source in rows:
        row = dict(source)
        evidence = _decode(row.get("evidence_json"))
        current_settlement = str(row.get("settlement") or "").strip().upper()
        terminal_record = current_settlement == "TERMINAL"
        normalized, reason = _legacy_entry_identity(row, evidence)
        if normalized is None:
            if terminal_record:
                continue
            marker = dict(evidence)
            marker["legacy_migration"] = {
                "status": "NON_RESUMABLE",
                "reason": reason or "CANARY_POSITION_IDENTITY_UNAVAILABLE",
                "source": "DURABLE_SUBMISSION_EVIDENCE_ONLY",
            }
            marker["non_resumable"] = True
            connection.execute(
                "UPDATE canary_ledger SET status=?,settlement=?,evidence_json=? "
                "WHERE event_id=? AND UPPER(COALESCE(status,'')) NOT IN "
                "('SETTLED','SETTLED_PARTIAL','FINAL','CLOSED','COMPLETED')",
                (
                    LEGACY_ENTRY_NON_RESUMABLE,
                    "MANUAL_RESOLUTION_REQUIRED",
                    _json(marker),
                    str(row.get("event_id") or ""),
                ),
            )
            continue
        if normalized != evidence:
            connection.execute(
                "UPDATE canary_ledger SET evidence_json=? WHERE event_id=?",
                (_json(normalized), str(row.get("event_id") or "")),
            )


def _migrate_legacy_lots(connection: Any) -> None:
    """Align pre-v2 lot token columns with authenticated BUY evidence."""
    try:
        rows = connection.execute("SELECT * FROM canary_position_lots").fetchall()
    except sqlite3.OperationalError:
        return
    for source in rows:
        lot = dict(source)
        event_id = str(lot.get("event_id") or "").strip()
        if not event_id:
            continue
        try:
            ledger = connection.execute(
                "SELECT token_id,evidence_json FROM canary_ledger WHERE event_id=?",
                (event_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue
        if ledger is None:
            continue
        normalized, reason = _legacy_entry_identity(
            {"token_id": ledger["token_id"]},
            _decode(ledger["evidence_json"]),
        )
        if normalized is None:
            if str(lot.get("status") or "").upper() not in {"CLOSED", "DUST"}:
                connection.execute(
                    "UPDATE canary_position_lots SET status='LEGACY_IDENTITY_UNRESOLVED',"
                    "updated_at=? WHERE position_id=?",
                    (
                        _iso(utc_now()),
                        str(lot.get("position_id") or ""),
                    ),
                )
            continue
        version = str(normalized.get("market_version") or "").lower()
        selected_token = str(normalized.get("selected_token_id") or "").strip()
        asset_id = str(normalized.get("resolved_asset_id") or "").strip()
        lot_token = str(lot.get("token_id") or "").strip()
        if (
            not selected_token
            or not asset_id
            or (
                lot_token != selected_token
                and not (version == "v2" and lot_token == asset_id)
            )
        ):
            if str(lot.get("status") or "").upper() not in {"CLOSED", "DUST"}:
                connection.execute(
                    "UPDATE canary_position_lots SET status='LEGACY_IDENTITY_UNRESOLVED',"
                    "updated_at=? WHERE position_id=?",
                    (_iso(utc_now()), str(lot.get("position_id") or "")),
                )
            continue
        connection.execute(
            "UPDATE canary_position_lots SET token_id=?,asset_id=?,market_version=? "
            "WHERE position_id=?",
            (
                selected_token,
                asset_id,
                version,
                str(lot.get("position_id") or ""),
            ),
        )


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
              asset_id TEXT,
              market_version TEXT,
              candidate_id TEXT,
              strategy_id TEXT,
              strategy_version TEXT,
              strategy_hash TEXT,
              model_hash TEXT,
              config_id TEXT,
              config_generation INTEGER,
              strategy_version_id TEXT,
              research_trial_id TEXT,
              portfolio_selection_id TEXT,
              admission_policy_id TEXT,
              admission_policy_version TEXT,
              risk_config_id TEXT,
              risk_config_generation INTEGER,
              risk_config_hash TEXT,
              lineage_type TEXT NOT NULL DEFAULT 'LEGACY_FINITE_CAMPAIGN',
              execution_authorization_id TEXT,
              execution_authorization_mode TEXT,
              controller_owner_id TEXT,
              controller_generation INTEGER,
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
              asset_id TEXT,
              market_version TEXT,
              side TEXT NOT NULL,
              strategy_version_id TEXT,
              research_trial_id TEXT,
              portfolio_selection_id TEXT,
              admission_policy_id TEXT,
              admission_policy_version TEXT,
              risk_config_id TEXT,
              risk_config_generation INTEGER,
              risk_config_hash TEXT,
              execution_authorization_id TEXT,
              controller_owner_id TEXT,
              controller_generation INTEGER,
              lineage_type TEXT NOT NULL DEFAULT 'LEGACY_FINITE_CAMPAIGN',
              order_id TEXT,
              requested_quantity TEXT NOT NULL,
              requested_price TEXT,
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
              strategy_version_id TEXT,
              research_trial_id TEXT,
              portfolio_selection_id TEXT,
              admission_policy_id TEXT,
              admission_policy_version TEXT,
              risk_config_id TEXT,
              risk_config_generation INTEGER,
              risk_config_hash TEXT,
              lineage_type TEXT NOT NULL DEFAULT 'LEGACY_FINITE_CAMPAIGN',
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
        for name in ("asset_id", "market_version"):
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE canary_position_lots ADD COLUMN {name} TEXT"
                )
        lineage_fields = (
            "strategy_version_id",
            "research_trial_id",
            "portfolio_selection_id",
            "admission_policy_id",
            "admission_policy_version",
            "risk_config_id",
            "risk_config_generation",
            "risk_config_hash",
        )
        lot_authority_fields = (
            "execution_authorization_id",
            "execution_authorization_mode",
            "controller_owner_id",
            "controller_generation",
        )
        lot_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(canary_position_lots)"
            )
        }
        for name in lot_authority_fields:
            if name not in lot_columns:
                declaration = "INTEGER" if name == "controller_generation" else "TEXT"
                connection.execute(
                    f"ALTER TABLE canary_position_lots ADD COLUMN {name} {declaration}"
                )
        for table in (
            "canary_position_lots",
            "canary_position_requests",
            "canary_position_fills",
        ):
            table_columns = {
                str(row["name"])
                for row in connection.execute(f"PRAGMA table_info({table})")
            }
            for name in lineage_fields:
                if name not in table_columns:
                    declaration = "INTEGER" if name == "risk_config_generation" else "TEXT"
                    connection.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                    )
            if "lineage_type" not in table_columns:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN lineage_type TEXT "
                    "NOT NULL DEFAULT 'LEGACY_FINITE_CAMPAIGN'"
                )
            connection.execute(
                f"UPDATE {table} SET lineage_type='LEGACY_FINITE_CAMPAIGN' "
                "WHERE lineage_type IS NULL OR TRIM(lineage_type)=''"
            )
        request_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(canary_position_requests)"
            )
        }
        for name in ("asset_id", "market_version", "requested_price"):
            if name not in request_columns:
                connection.execute(
                    f"ALTER TABLE canary_position_requests ADD COLUMN {name} TEXT"
                )
        for name, declaration in (
            ("execution_authorization_id", "TEXT"),
            ("controller_owner_id", "TEXT"),
            ("controller_generation", "INTEGER"),
        ):
            if name not in request_columns:
                connection.execute(
                    f"ALTER TABLE canary_position_requests ADD COLUMN {name} {declaration}"
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
        allowed_request_statuses = tuple(sorted(_PENDING | _TERMINAL))
        connection.execute(
            "UPDATE canary_position_requests SET status='UNKNOWN' "
            "WHERE UPPER(COALESCE(status,'')) NOT IN ("
            + ",".join("?" for _ in allowed_request_statuses)
            + ")",
            allowed_request_statuses,
        )
        _migrate_legacy_entry_rows(service, connection)
        _migrate_legacy_lots(connection)
        pending_request_statuses = tuple(sorted(_PENDING))
        pending_price_rows = connection.execute(
            "SELECT request_id,requested_price FROM canary_position_requests "
            "WHERE UPPER(side)='SELL' AND UPPER(COALESCE(status,'')) IN ("
            + ",".join("?" for _ in pending_request_statuses)
            + ")",
            pending_request_statuses,
        ).fetchall()
        for pending_price_row in pending_price_rows:
            try:
                _canonical_clob_price(
                    pending_price_row["requested_price"],
                    "CANARY_EXIT_PRICE_UNAVAILABLE",
                )
            except CanaryBlocked:
                connection.execute(
                    "UPDATE canary_position_requests SET status=?,last_error=? "
                    "WHERE request_id=? AND UPPER(COALESCE(status,'')) IN ("
                    + ",".join("?" for _ in _PENDING)
                    + ")",
                    (
                        LEGACY_REQUEST_NON_RESUMABLE,
                        "LEGACY_REQUEST_PRICE_UNAVAILABLE",
                        pending_price_row["request_id"],
                        *_PENDING,
                    ),
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
    names = (
        ("best_bid", "bid", "sell_price", "price")
        if side == "SELL"
        else ("best_ask", "ask", "buy_price", "price")
    )
    side_rows_name = "bids" if side == "SELL" else "asks"

    # Every supplied price candidate is evidence from the remote CLOB.  Do
    # not skip an invalid preferred field and fall back to a different quote:
    # that would let malformed data select a mark or exit price.
    direct_prices: dict[str, Decimal] = {}
    for name in names:
        if name not in context:
            continue
        raw = context[name]
        if raw in (None, ""):
            raise CanaryBlocked("CANARY_EXIT_PRICE_UNAVAILABLE")
        direct_prices[name] = _canonical_clob_price(
            raw,
            "CANARY_EXIT_PRICE_UNAVAILABLE",
        )

    def parse_rows(raw_rows: Any) -> list[Decimal]:
        if raw_rows in (None, ""):
            return []
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
            raise CanaryBlocked("CANARY_EXIT_PRICE_UNAVAILABLE")
        return [
            _canonical_clob_price(
                _value(row, "price", default=None),
                "CANARY_EXIT_PRICE_UNAVAILABLE",
            )
            for row in raw_rows
        ]

    nested_rows: list[Decimal] = []
    books = context.get("order_book", context.get("book"))
    if isinstance(books, Mapping) and side_rows_name in books:
        nested_rows = parse_rows(books.get(side_rows_name))

    direct_rows: list[Decimal] = []
    if side_rows_name in context:
        direct_rows = parse_rows(context.get(side_rows_name))

    for name in names:
        if name in direct_prices:
            return direct_prices[name]
    if nested_rows:
        return max(nested_rows) if side == "SELL" else min(nested_rows)
    if direct_rows:
        return max(direct_rows) if side == "SELL" else min(direct_rows)
    raise CanaryBlocked("CANARY_EXIT_PRICE_UNAVAILABLE")
def _required_market_decimal(
    context: Mapping[str, Any],
    names: Sequence[str],
    *,
    reason: str,
) -> Decimal:
    raw: Any = None
    found = False
    sources: list[Mapping[str, Any]] = [context]
    for name in ("market_rules", "rules", "order_book", "book"):
        nested = context.get(name)
        if isinstance(nested, Mapping):
            sources.append(nested)
    for source in sources:
        for name in names:
            if name in source and source.get(name) not in (None, ""):
                raw = source.get(name)
                found = True
                break
        if found:
            break
    if not found:
        raise CanaryBlocked(reason)
    try:
        value = Decimal(str(raw))
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise CanaryBlocked(reason) from exc
    if not value.is_finite() or value <= ZERO:
        raise CanaryBlocked(reason)
    return value
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
    record_mark = getattr(service, "_record_canary_equity_mark", None)
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
    assessment_config_hash: str | None = None
    try:
        (
            assessment_config_id,
            assessment_config_generation,
            assessment_config_hash,
        ) = service._settings_identity()
    except CanaryBlocked:
        pass
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
        try:
            lot_lineage = _opening_lot_lineage(service, lot)
            context = _call(
                _method(venue, "market_context"),
                market_id=market_id,
                token_id=token_id,
            )
            if not isinstance(context, Mapping):
                raise CanaryBlocked("CANARY_EQUITY_MARKET_CONTEXT_INVALID")
            market_version, asset_id = _validate_market_context_identity(
                context,
                token_id,
                missing_reason="CANARY_EQUITY_IDENTITY_UNAVAILABLE",
                conflict_reason="CANARY_EQUITY_IDENTITY_CONFLICT",
            )
            persisted_identity = _lot_persisted_identity(service, lot)
            if persisted_identity is None:
                if market_version != "v1" or asset_id != token_id:
                    raise CanaryBlocked("CANARY_EQUITY_IDENTITY_UNAVAILABLE")
            elif persisted_identity != (market_version, asset_id):
                raise CanaryBlocked("CANARY_EQUITY_IDENTITY_CONFLICT")
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
                config_id=assessment_config_id,
                config_generation=assessment_config_generation,
                config_hash=assessment_config_hash,
                control_generation=control_generation,
                position_id=position_id,
                lineage=lot_lineage,
                detail={
                    "position_id": position_id,
                    "venue": str(lot.get("venue") or "POLYMARKET"),
                    "quantity_source": "CANARY_POSITION_LOT",
                    **{
                        name: lot_lineage[name]
                        for name in _POSITION_LINEAGE_FIELDS
                    },
                    "lineage_type": lot_lineage["lineage_type"],
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
    _validate_trade_history_coverage(payload)
    rows: Any = None
    if isinstance(payload, Mapping):
        for key in ("trades", "fills", "data", "items"):
            if key not in payload:
                continue
            candidate = payload[key]
            if not isinstance(candidate, Sequence) or isinstance(
                candidate, (str, bytes)
            ):
                raise CanaryBlocked("CANARY_TRADE_RESPONSE_INVALID")
            rows = candidate
            break
        if rows is None:
            if any(key in payload for key in ("trade_id", "tradeId", "fill_id", "id")):
                return [payload]
            raise CanaryBlocked("CANARY_TRADE_RESPONSE_INVALID")
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        rows = payload
    else:
        raise CanaryBlocked("CANARY_TRADE_RESPONSE_INVALID")
    if any(not isinstance(row, Mapping) for row in rows):
        raise CanaryBlocked("CANARY_TRADE_RESPONSE_INVALID")
    return [row for row in rows if isinstance(row, Mapping)]
def _trade_id(trade: Mapping[str, Any], order_id: str, index: int) -> str:
    value = _value(trade, "trade_id", "tradeId", "fill_id", "id", default=None)
    if value in (None, ""):
        # An array position is not a durable exchange identity: pagination,
        # reconnects, and out-of-order trade reads can change it.  Refuse to
        # account a fill whose venue did not provide its stable ID.
        raise CanaryBlocked("CANARY_TRADE_ID_UNAVAILABLE")
    return str(value)


def _account_trade_projection(
    trade: Mapping[str, Any],
    expected_order_id: Any,
) -> Mapping[str, Any]:
    """Project one fill onto the exact account order role and economics."""
    binding = _account_trade_binding(trade, expected_order_id)
    if binding is None:
        raise CanaryBlocked("CANARY_TRADE_IDENTITY_CONFLICT")
    projected = dict(trade)
    projected["account_order_side"] = binding["account_order_side"]
    maker_order = binding.get("maker_order")
    if maker_order is not None:
        for field in ("asset_id", "price", "size", "fee_rate_bps", "fee"):
            if field in maker_order:
                projected[field] = maker_order[field]
            else:
                projected.pop(field, None)
        projected["quantity"] = maker_order["size"]
    return projected

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
    return _canonical_clob_price(raw_price, "CANARY_TRADE_PRICE_UNAVAILABLE")


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
    """Normalize venue order states to the bounded reconciliation vocabulary."""
    raw_status = _value(order, "status", "state", "order_status", default=None)
    status = str(raw_status or "UNKNOWN").strip().upper() or "UNKNOWN"
    if status in {"MATCHED", "MATCH", "EXECUTED"}:
        # MATCHED is execution evidence, not authoritative settlement.
        return "MATCHED"
    if status in {"PARTIAL", "PARTIALLY_FILLED", "PARTIALLYFILLED"}:
        return "PARTIALLY_FILLED"
    if status in {"RESOLVED", "SETTLED_FULL"}:
        return "SETTLED"
    if status == "SETTLED_PARTIAL":
        return "SETTLED_PARTIAL"
    if status in _FAILED_ORDER or status in _CANCELED_ORDER:
        return status
    if status in _FINAL_SETTLEMENT:
        return status
    # Venue-specific LIVE/DELAYED/PROCESSING values and any unrecognized
    # response are pollable uncertainty, not durable state names.
    if status in _ORDER_PENDING_STATUSES:
        return status
    return "UNKNOWN"





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


def _identity_alias(
    source: Any,
    names: Sequence[str],
    *,
    missing: str,
    conflict: str,
) -> str:
    values: list[str] = []
    for name in names:
        raw = mapping_value(source, name)
        text = str(raw or "").strip()
        if text:
            values.append(text)
    if not values:
        raise CanaryBlocked(missing)
    if len(set(values)) != 1:
        raise CanaryBlocked(conflict)
    return values[0]
def _validate_market_context_identity(
    context: Any,
    expected_token_id: Any,
    *,
    missing_reason: str,
    conflict_reason: str,
) -> tuple[str, str]:
    """Return the canonical version and asset bound to one expected outcome."""
    if not isinstance(context, Mapping):
        raise CanaryBlocked(missing_reason)
    expected_token = _required_identity(expected_token_id, missing_reason)
    market_version = _identity_alias(
        context,
        ("market_version", "marketVersion"),
        missing=missing_reason,
        conflict=conflict_reason,
    ).lower()
    if market_version not in {"v1", "v2"}:
        raise CanaryBlocked(conflict_reason)
    selected_token = _identity_alias(
        context,
        ("token_id", "tokenId", "selected_token_id", "selectedTokenId"),
        missing=missing_reason,
        conflict=conflict_reason,
    )
    asset_id = _identity_alias(
        context,
        ("asset_id", "assetId"),
        missing=missing_reason,
        conflict=conflict_reason,
    )
    if selected_token != expected_token:
        raise CanaryBlocked(conflict_reason)
    if market_version == "v1":
        if asset_id != selected_token:
            raise CanaryBlocked(conflict_reason)
    else:
        position_id = _identity_alias(
            context,
            ("position_id", "positionId", "selected_position_id", "selectedPositionId"),
            missing=missing_reason,
            conflict=conflict_reason,
        )
        if asset_id != position_id:
            raise CanaryBlocked(conflict_reason)
    _validate_context_outcome_binding(
        context,
        token_id=selected_token,
        asset_id=asset_id,
        market_version=market_version,
        missing_reason=missing_reason,
        conflict_reason=conflict_reason,
    )
    return market_version, asset_id
def _entry_context_identity(
    row: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    missing_reason: str,
    conflict_reason: str,
) -> tuple[str, str]:
    """Resolve the owned token and exchange asset for one persisted BUY."""
    token_id = _required_identity(row.get("token_id"), missing_reason)
    identity_values = (
        evidence.get("market_version"),
        evidence.get("marketVersion"),
        evidence.get("selected_token_id"),
        evidence.get("selectedTokenId"),
        evidence.get("token_id"),
        evidence.get("tokenId"),
        evidence.get("selected_position_id"),
        evidence.get("selectedPositionId"),
        evidence.get("position_id"),
        evidence.get("positionId"),
        evidence.get("resolved_asset_id"),
        evidence.get("asset_id"),
        evidence.get("assetId"),
    )
    if not any(str(value or "").strip() for value in identity_values):
        # Rows written before identity evidence was persisted can only be
        # accepted as the v1 token/asset identity.  A v2 observation must
        # carry its explicit context and therefore cannot be guessed here.
        return "v1", token_id
    context = dict(evidence)
    context.setdefault(
        "token_id",
        evidence.get("selected_token_id")
        or evidence.get("selectedTokenId")
        or evidence.get("tokenId"),
    )
    context.setdefault(
        "position_id",
        evidence.get("selected_position_id")
        or evidence.get("selectedPositionId")
        or evidence.get("positionId"),
    )
    context.setdefault(
        "asset_id",
        evidence.get("resolved_asset_id")
        or evidence.get("assetId"),
    )
    market_version, asset_id = _validate_market_context_identity(
        context,
        token_id,
        missing_reason=missing_reason,
        conflict_reason=conflict_reason,
    )
    return market_version, asset_id
def _validate_context_outcome_binding(
    context: Mapping[str, Any],
    *,
    token_id: str,
    asset_id: str,
    market_version: str,
    missing_reason: str,
    conflict_reason: str,
) -> None:
    if market_version != "v2":
        return
    raw_index = context.get("outcome_index")
    legacy_binding = context.get("legacy_identity_binding") is True
    bindings = context.get("identity_bindings")
    if legacy_binding and raw_index in (None, ""):
        if not isinstance(bindings, Sequence) or isinstance(
            bindings, (str, bytes, bytearray)
        ) or len(bindings) != 1:
            raise CanaryBlocked(conflict_reason)
        binding = bindings[0]
        if not isinstance(binding, Mapping):
            raise CanaryBlocked(conflict_reason)
        bound_token = _required_identity(binding.get("token_id"), missing_reason)
        bound_asset = _required_identity(binding.get("position_id"), missing_reason)
        if bound_token != token_id or bound_asset != asset_id:
            raise CanaryBlocked(conflict_reason)
        return
    if isinstance(raw_index, bool) or raw_index in (None, ""):
        raise CanaryBlocked(missing_reason)
    try:
        outcome_index = int(raw_index)
    except (TypeError, ValueError, OverflowError):
        raise CanaryBlocked(conflict_reason) from None
    if not isinstance(bindings, Sequence) or isinstance(
        bindings, (str, bytes, bytearray)
    ):
        raise CanaryBlocked(missing_reason)
    matched: list[tuple[int, str, str]] = []
    seen_indexes: set[int] = set()
    seen_tokens: set[str] = set()
    seen_assets: set[str] = set()
    for binding in bindings:
        if not isinstance(binding, Mapping):
            raise CanaryBlocked(conflict_reason)
        try:
            index_value = binding.get("index")
            if isinstance(index_value, bool):
                raise ValueError
            index = int(index_value)
            bound_token = _required_identity(
                binding.get("token_id"),
                missing_reason,
            )
            bound_asset = _required_identity(
                binding.get("position_id"),
                missing_reason,
            )
        except (TypeError, ValueError, OverflowError):
            raise CanaryBlocked(conflict_reason) from None
        if (
            index in seen_indexes
            or bound_token in seen_tokens
            or bound_asset in seen_assets
        ):
            raise CanaryBlocked(conflict_reason)
        seen_indexes.add(index)
        seen_tokens.add(bound_token)
        seen_assets.add(bound_asset)
        if bound_token == token_id:
            matched.append((index, bound_token, bound_asset))
    if len(matched) != 1:
        raise CanaryBlocked(conflict_reason)
    bound_index, _, bound_asset = matched[0]
    if bound_index != outcome_index or bound_asset != asset_id:
        raise CanaryBlocked(conflict_reason)
def _lot_persisted_identity(
    service: CanaryService,
    lot: Mapping[str, Any],
) -> tuple[str, str] | None:
    """Read the immutable BUY context that established an owned lot."""
    event_id = str(lot.get("event_id") or "").strip()
    if not event_id:
        return None
    with service.store._lock:
        row = _connection(service).execute(
            "SELECT token_id,evidence_json FROM canary_ledger WHERE event_id=?",
            (event_id,),
        ).fetchone()
    if row is None:
        return None
    evidence = _decode(row["evidence_json"])
    return _entry_context_identity(
        {"token_id": row["token_id"]},
        evidence,
        missing_reason="CANARY_EQUITY_IDENTITY_UNAVAILABLE",
        conflict_reason="CANARY_EQUITY_IDENTITY_CONFLICT",
    )



def _validate_venue_identity(
    *,
    order: Mapping[str, Any],
    trades: Sequence[Mapping[str, Any]],
    expected_order_id: Any,
    expected_side: Any,
    expected_market_id: Any,
    expected_token_id: Any,
    expected_quantity: Any,
    expected_asset_id: Any = None,
    expected_market_version: Any = None,
    expected_price: Any = None,
    lot: Mapping[str, Any] | None = None,
    prior_request_quantity: Any = ZERO,
) -> tuple[str, Decimal]:
    """Require an exact persisted order/request identity before any writes."""
    expected_order = _canonical_order_identity(
        expected_order_id,
        "CANARY_ORDER_IDENTITY_UNAVAILABLE",
    )
    side = _identity_alias(
        {"side": expected_side},
        ("side",),
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    ).upper()
    market = _identity_alias(
        {"market_id": expected_market_id},
        ("market_id",),
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    )
    token = _identity_alias(
        {"token_id": expected_token_id},
        ("token_id",),
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    )
    try:
        market_version = _required_identity(
            expected_market_version,
            "CANARY_ORDER_IDENTITY_UNAVAILABLE",
        ).lower()
        expected_asset = _required_identity(
            expected_asset_id,
            "CANARY_ORDER_IDENTITY_UNAVAILABLE",
        )
    except CanaryBlocked:
        raise
    if market_version not in {"v1", "v2"}:
        raise CanaryBlocked("CANARY_ORDER_IDENTITY_CONFLICT")
    if market_version == "v1" and expected_asset != token:
        raise CanaryBlocked("CANARY_ORDER_IDENTITY_CONFLICT")

    def observed_identity(
        source: Mapping[str, Any],
        *,
        missing: str,
        conflict: str,
    ) -> tuple[str | None, str | None]:
        token_values = _identity_values(
            source, ("token_id", "tokenId", "token")
        )
        if len(set(token_values)) > 1 or (
            token_values and token_values[0] != token
        ):
            raise CanaryBlocked(conflict)
        asset_values = _identity_values(source, ("asset_id", "assetId"))
        if len(set(asset_values)) > 1:
            raise CanaryBlocked(conflict)
        generic_values = _identity_values(source, ("asset",))
        if len(set(generic_values)) > 1:
            raise CanaryBlocked(conflict)
        observed_token = token_values[0] if token_values else None
        observed_asset = asset_values[0] if asset_values else None
        if generic_values:
            if observed_asset is not None and generic_values[0] != observed_asset:
                raise CanaryBlocked(conflict)
            observed_asset = generic_values[0]
        if market_version == "v2" and observed_asset is None:
            raise CanaryBlocked(missing)
        if market_version == "v1" and observed_asset is None:
            observed_asset = observed_token
        if observed_asset is None:
            raise CanaryBlocked(missing)
        return observed_token, observed_asset
    try:
        requested = decimal_value(
            expected_quantity,
            "requested quantity",
            positive=True,
        )
    except ValueError:
        raise CanaryBlocked("CANARY_ORDER_QUANTITY_UNAVAILABLE") from None
    if side not in {"BUY", "SELL"}:
        raise CanaryBlocked("CANARY_ORDER_IDENTITY_UNAVAILABLE")
    if lot is not None:
        lot_market = _required_identity(
            lot.get("market_id"),
            "CANARY_POSITION_IDENTITY_UNAVAILABLE",
        )
        lot_token = _required_identity(
            lot.get("token_id"),
            "CANARY_POSITION_IDENTITY_UNAVAILABLE",
        )
        if lot_market != market or lot_token != token:
            raise CanaryBlocked("CANARY_POSITION_IDENTITY_CONFLICT")
        prior_filled = _decimal(prior_request_quantity, Decimal("-1"))
        if prior_filled < ZERO or prior_filled > requested + DUST:
            raise CanaryBlocked("CANARY_ORDER_QUANTITY_CONFLICT")
        available = max(
            ZERO,
            _decimal(lot.get("quantity"), ZERO)
            - _decimal(lot.get("sold_quantity"), ZERO)
            + prior_filled,
        )
        if requested > available + DUST:
            raise CanaryBlocked("CANARY_ORDER_QUANTITY_OVER_POSITION")

    observed_order = _canonical_order_identity(
        _identity_alias(
            order,
            ("order_id", "orderId", "id", "exchange_order_id"),
            missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
            conflict="CANARY_ORDER_IDENTITY_CONFLICT",
        ),
        "CANARY_ORDER_IDENTITY_UNAVAILABLE",
    )
    if observed_order != expected_order:
        raise CanaryBlocked("CANARY_ORDER_IDENTITY_CONFLICT")
    observed_side = _identity_alias(
        order,
        ("side", "order_side"),
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    ).upper()
    observed_order_token, observed_order_asset = observed_identity(
        order,
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    )
    observed_market = _identity_alias(
        order,
        ("market_id", "market", "condition_id", "conditionId"),
        missing="CANARY_ORDER_IDENTITY_UNAVAILABLE",
        conflict="CANARY_ORDER_IDENTITY_CONFLICT",
    )
    if (
        observed_side != side
        or (observed_order_token is not None and observed_order_token != token)
        or observed_order_asset != expected_asset
        or observed_market != market
    ):
        raise CanaryBlocked("CANARY_ORDER_IDENTITY_CONFLICT")
    try:
        order_price = response_price(order)
    except ValueError:
        raise CanaryBlocked("CANARY_ORDER_PRICE_UNAVAILABLE") from None
    if not ZERO < order_price < ONE:
        raise CanaryBlocked("CANARY_ORDER_PRICE_UNAVAILABLE")
    try:
        expected_limit_price = decimal_value(
            expected_price,
            "expected limit price",
            positive=True,
        )
    except ValueError:
        raise CanaryBlocked("CANARY_ORDER_PRICE_UNAVAILABLE") from None
    if not ZERO < expected_limit_price < ONE:
        raise CanaryBlocked("CANARY_ORDER_PRICE_UNAVAILABLE")
    if order_price != expected_limit_price:
        raise CanaryBlocked("CANARY_ORDER_PRICE_CONFLICT")
    try:
        order_quantity = response_quantity(order)
    except ValueError:
        raise CanaryBlocked("CANARY_ORDER_QUANTITY_UNAVAILABLE") from None
    if abs(order_quantity - requested) > DUST:
        raise CanaryBlocked("CANARY_ORDER_QUANTITY_CONFLICT")

    aggregate_quantity = ZERO
    for raw_trade in trades:
        if not isinstance(raw_trade, Mapping):
            raise CanaryBlocked("CANARY_TRADE_RESPONSE_INVALID")
        trade = _account_trade_projection(raw_trade, expected_order)
        binding = _account_trade_binding(trade, expected_order)
        if binding is None:
            raise CanaryBlocked("CANARY_TRADE_IDENTITY_CONFLICT")
        direct_order_values = binding["direct_order_ids"]
        taker_order_values = binding["taker_order_ids"]
        maker_order_values = binding["maker_order_ids"]
        if not (
            expected_order in direct_order_values
            or expected_order in taker_order_values
            or expected_order in maker_order_values
        ):
            raise CanaryBlocked("CANARY_TRADE_IDENTITY_CONFLICT")
        observed_trade_side = binding["account_order_side"]
        observed_trade_token, observed_trade_asset = observed_identity(
            trade,
            missing="CANARY_TRADE_IDENTITY_UNAVAILABLE",
            conflict="CANARY_TRADE_IDENTITY_CONFLICT",
        )
        observed_trade_market = _identity_alias(
            trade,
            ("market_id", "market", "condition_id", "conditionId"),
            missing="CANARY_TRADE_IDENTITY_UNAVAILABLE",
            conflict="CANARY_TRADE_IDENTITY_CONFLICT",
        )
        if (
            observed_trade_side != side
            or (
                observed_trade_token is not None
                and observed_trade_token != token
            )
            or observed_trade_asset != expected_asset
            or observed_trade_market != market
        ):
            raise CanaryBlocked("CANARY_TRADE_IDENTITY_CONFLICT")
        quantity = _trade_quantity(trade)
        if quantity <= ZERO:
            raise CanaryBlocked("CANARY_TRADE_QUANTITY_UNAVAILABLE")
        aggregate_quantity += quantity
        if aggregate_quantity > requested + DUST:
            raise CanaryBlocked("CANARY_TRADE_QUANTITY_OVER_PLAN")
    return expected_order, requested

def _submission_fence(
    service: CanaryService,
    *,
    expected_generation: int,
    config_id: str,
    expected_control_generation: int,
    expected_credential_fingerprint: str | None = None,
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
    if expected_credential_fingerprint is not None:
        service.require_current_credential_binding(expected_credential_fingerprint)
_REQUEST_NONDEGRADABLE = frozenset(
    {
        "FILLED",
        "SETTLED",
        "SETTLED_PARTIAL",
        "CANCELED",
        "CANCELLED",
        "EXPIRED",
        "REJECTED",
        "FAILED",
        "ERROR",
    }
)


def _normalized_request_status(value: Any) -> str:
    status = str(value or "").strip().upper()
    return status if status in (_PENDING | _TERMINAL) else "UNKNOWN"


def _monotonic_request_status(current: Any, proposed: Any) -> str:
    """Apply only forward request transitions to a persisted request."""
    prior = _normalized_request_status(current)
    next_status = _normalized_request_status(proposed)
    if prior == "SETTLED":
        return prior
    if prior == "SETTLED_PARTIAL":
        return prior
    if prior in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED", "ERROR"}:
        return prior
    if prior == "FILLED" and next_status not in {"SETTLED", "SETTLED_PARTIAL"}:
        return prior
    return next_status


def _request_terminal(value: Any) -> bool:
    return _normalized_request_status(value) in _REQUEST_NONDEGRADABLE


def _mark_request_unknown(
    service: CanaryService,
    request_id: Any,
    reason: Any,
) -> str:
    """Keep terminal requests intact while making failures retryable."""
    identifier = str(request_id or "").strip()
    if not identifier:
        return "UNKNOWN"
    error = str(reason or "UNKNOWN")
    with service.store._lock, _connection(service):
        row = _connection(service).execute(
            "SELECT status FROM canary_position_requests WHERE request_id=?",
            (identifier,),
        ).fetchone()
        if row is None:
            return "UNKNOWN"
        prior = _normalized_request_status(row["status"])
        if not _request_terminal(prior):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status='UNKNOWN',"
                "last_error=?,updated_at=? WHERE request_id=?",
                (error, _iso(ensure_utc(service.clock())), identifier),
            )
            return "UNKNOWN"
        return prior


def _request_snapshot_result(request: Mapping[str, Any], status: Any) -> dict[str, Any]:
    return {
        "request_id": str(request.get("request_id") or ""),
        "position_id": str(request.get("position_id") or ""),
        "status": _normalized_request_status(status),
        "filled_quantity": str(request.get("filled_quantity") or "0"),
        "new_fills": 0,
        "order_id": str(request.get("order_id") or "") or None,
    }



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


def _flag_lot_identity(service: CanaryService, position_id: str) -> None:
    with service.store._lock, _connection(service):
        _connection(service).execute(
            "UPDATE canary_position_lots SET status=?,updated_at=? "
            "WHERE position_id=? AND UPPER(COALESCE(status,'')) "
            "NOT IN ('CLOSED','DUST')",
            (
                LEGACY_ENTRY_NON_RESUMABLE,
                _iso(ensure_utc(service.clock())),
                position_id,
            ),
        )

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
                    "SELECT reservation_id,config_id,config_generation,"
                    "execution_authorization_id,controller_owner_id,"
                    "controller_generation,detail_json "
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
        if quantity <= DUST or not ZERO < price < ONE or fees < ZERO:
            continue
        evidence = _decode(row.get("evidence_json"))
        try:
            market_version, execution_asset_id = _entry_context_identity(
                row,
                evidence,
                missing_reason="CANARY_POSITION_IDENTITY_UNAVAILABLE",
                conflict_reason="CANARY_POSITION_IDENTITY_CONFLICT",
            )
        except CanaryBlocked:
            # Invalid or incomplete identity evidence must not establish
            # ownership, and must not prevent unrelated entries projecting.
            continue
        signal_id = str(row.get("signal_id") or "")
        with service.store._lock:
            signal_row = connection.execute(
                "SELECT strategy_hash,model_hash,config_hash,"
                "strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,lineage_type "
                "FROM canary_signals WHERE signal_id=?",
                (signal_id,),
            ).fetchone() if signal_id else None
        lineage = _lineage_from_row(row)
        opening_authority = _opening_authority(
            service,
            {
                "reservation_id": reservation_id,
                **lineage,
            },
        )
        lineage.update(opening_authority)
        for name in _POSITION_AUTHORITY_FIELDS:
            if evidence.get(name) not in (None, ""):
                lineage[name] = evidence[name]
        if signal_row is not None:
            signal_lineage = _lineage_from_row(signal_row)
            # A legacy BUY may point at a signal that later acquired rolling
            # metadata.  That signal is not proof that the opening was
            # rolling, so never promote the lot (or its risk fill) here.
            if (
                lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
                and signal_lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
            ):
                for name in _POSITION_LINEAGE_FIELDS:
                    if lineage.get(name) in (None, ""):
                        lineage[name] = signal_lineage.get(name)
            evidence.setdefault("strategy_hash", signal_row["strategy_hash"])
            evidence.setdefault("model_hash", signal_row["model_hash"])
            for name in _POSITION_LINEAGE_FIELDS:
                if lineage.get(name) is not None:
                    evidence.setdefault(name, lineage[name])
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
        with service.store._lock, connection:
            existing = connection.execute(
                "SELECT token_id,quantity,cost_basis,fees,status,"
                "strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,lineage_type,"
                "execution_authorization_id,execution_authorization_mode,"
                "controller_owner_id,controller_generation "
                "FROM canary_position_lots WHERE position_id=?",
                (position_id,),
            ).fetchone()
            if existing is not None:
                existing_token = str(existing["token_id"] or "").strip()
                if not existing_token or existing_token != str(row.get("token_id") or "").strip():
                    continue
                existing_lineage = _lineage_from_row(existing)
                if (
                    str(existing_lineage.get("lineage_type") or _LEGACY_LINEAGE_TYPE)
                    == _ROLLING_LINEAGE_TYPE
                    and any(
                        existing_lineage.get(name) != lineage.get(name)
                        for name in _POSITION_LINEAGE_FIELDS
                    )
                ):
                    continue
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
            opened = _iso(row.get("timestamp"), now)
            connection.execute(
                "INSERT INTO canary_position_lots("
                "position_id,reservation_id,event_id,venue,market_id,token_id,"
                "asset_id,market_version,candidate_id,strategy_id,strategy_version,"
                "strategy_hash,model_hash,config_id,config_generation,"
                "strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,lineage_type,"
                "execution_authorization_id,execution_authorization_mode,"
                "controller_owner_id,controller_generation,exit_policy_json,"
                "quantity,sold_quantity,cost_basis,fees,pending_exit_quantity,status,"
                "opened_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    position_id,
                    reservation_id,
                    event_id,
                    str(row.get("venue") or "polymarket"),
                    str(row.get("market_id") or ""),
                    str(row.get("token_id") or "").strip(),
                    execution_asset_id,
                    market_version,
                    candidate_id,
                    evidence.get("strategy_id"),
                    evidence.get("strategy_version"),
                    strategy_hash,
                    evidence.get("model_hash"),
                    reservation_config_id,
                    reservation_config_generation,
                    *(lineage.get(name) for name in _POSITION_LINEAGE_FIELDS),
                    lineage.get("lineage_type") or _LEGACY_LINEAGE_TYPE,
                    *(lineage.get(name) for name in _POSITION_AUTHORITY_FIELDS),
                    _json(policy),
                    str(quantity),
                    "0",
                    str(quantity * price + fees),
                    str(fees),
                    "0",
                    policy_status,
                    opened,
                    _iso(now),
                ),
            )
            created += 1
    return created


def _reconcile_entry_ledger(
    service: CanaryService,
    venue: Any,
    now: datetime,
    *,
    target_event_id: str | None = None,
) -> list[dict[str, Any]]:
    """Refresh pending BUY entries without posting or inventing aggregate fills."""
    connection = _connection(service)
    target_key = str(target_event_id or "").strip()
    active_statuses = (
        "(UPPER(status) IN "
        "('ACCEPTED','SUBMITTED','SUBMITTING','ACKNOWLEDGED','LIVE','DELAYED',"
        "'UNKNOWN','PARTIAL','PARTIALLY_FILLED','MATCHED','FILLED','OPEN') "
        "OR (UPPER(status) IN ('CONFIRMED','TRADE_STATUS_CONFIRMED',"
        "'SETTLED','TRADE_STATUS_SETTLED') "
        "AND COALESCE(settlement,'')='') "
        "OR COALESCE(settlement,'')='PENDING')"
    )
    eligible_entries = (
        "SELECT * FROM canary_ledger WHERE UPPER(side)='BUY' "
        "AND exchange_order_id IS NOT NULL "
        + ("AND event_id=? " if target_key else "")
        + "AND ("
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
    with service.store._lock, connection:
        known_entry_statuses = tuple(
            sorted(_KNOWN_ORDER_STATUSES | _OWNED_ENTRY_STATUSES)
        )
        if not target_key:
            try:
                connection.execute(
                    "UPDATE canary_ledger SET status='UNKNOWN' "
                    "WHERE UPPER(side)='BUY' AND exchange_order_id IS NOT NULL "
                    "AND UPPER(COALESCE(status,'')) NOT IN ("
                    + ",".join("?" for _ in known_entry_statuses)
                    + ")",
                    known_entry_statuses,
                )
            except sqlite3.OperationalError:
                pass
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
                if target_key:
                    params = (target_key, *params)
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
            market_version, execution_asset_id = _entry_context_identity(
                row,
                entry_evidence,
                missing_reason="CANARY_ORDER_IDENTITY_UNAVAILABLE",
                conflict_reason="CANARY_ORDER_IDENTITY_CONFLICT",
            )
            order = _call(get_order, order_id=order_id)
            if not isinstance(order, Mapping):
                raise CanaryBlocked("CANARY_ORDER_RESPONSE_INVALID")
            status = _order_status(order)
            trade_query = {
                "order_id": order_id,
                "market": str(row.get("market_id") or "") or None,
                "asset_id": execution_asset_id if market_version == "v2" else None,
                "token_id": execution_asset_id if market_version == "v1" else None,
            }
            raw_trades = _parse_trades(
                _call(list_trades, **trade_query)
            )
            _validate_venue_identity(
                order=order,
                trades=raw_trades,
                expected_order_id=order_id,
                expected_side=row.get("side"),
                expected_market_id=row.get("market_id"),
                expected_token_id=row.get("token_id"),
                expected_quantity=row.get("submitted_quantity"),
                expected_asset_id=execution_asset_id,
                expected_market_version=market_version,
                expected_price=row.get("max_price"),
            )
            raw_trades = [
                _account_trade_projection(trade, order_id)
                for trade in raw_trades
            ]
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
            reservation_status = ""
            with service.store._lock:
                try:
                    reservation_row = connection.execute(
                        "SELECT reservation_id,status FROM canary_risk_reservations "
                        "WHERE event_id=? AND UPPER(side)='BUY' "
                        "ORDER BY created_at DESC,reservation_id DESC LIMIT 1",
                        (event_id,),
                    ).fetchone()
                except sqlite3.OperationalError:
                    reservation_row = None
            if reservation_row is not None:
                risk_reservation_id = str(reservation_row["reservation_id"])
                reservation_status = str(reservation_row["status"] or "").strip().upper()
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
                reservation_status = str(adoption.get("status") or "").strip().upper()
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
                if (
                    existing_quantity <= ZERO
                    or not ZERO < existing_price < ONE
                    or existing_fee < ZERO
                ):
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
            entry_lineage = _entry_lineage(
                service,
                row,
                risk_reservation_id,
            )
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
                    strategy_version_id=entry_lineage["strategy_version_id"],
                    research_trial_id=entry_lineage["research_trial_id"],
                    candidate_id=entry_lineage.get("candidate_id"),
                    portfolio_selection_id=entry_lineage["portfolio_selection_id"],
                    admission_policy_id=entry_lineage["admission_policy_id"],
                    admission_policy_version=entry_lineage["admission_policy_version"],
                    risk_config_id=entry_lineage["risk_config_id"],
                    risk_config_generation=entry_lineage["risk_config_generation"],
                    risk_config_hash=entry_lineage["risk_config_hash"],
                    allocation=entry_lineage.get("allocation"),
                    detail={
                        "event_id": event_id,
                        "order_id": order_id,
                        "market_id": str(row.get("market_id") or ""),
                        "token_id": execution_asset_id,
                        "candidate_id": str(row.get("candidate_id") or ""),
                        "side": "BUY",
                        "settlement_status": "CONFIRMED",
                        "order_status": status,
                        **{
                            name: entry_lineage.get(name)
                            for name in _POSITION_LINEAGE_FIELDS
                            if entry_lineage.get(name) is not None
                        },
                        "lineage_type": entry_lineage["lineage_type"],
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
            prior_status = str(row.get("status") or "").upper()
            prior_price = _decimal(row.get("actual_average_price"), Decimal("-1"))
            prior_fees = _decimal(row.get("fees"), Decimal("-1"))
            prior_owned_evidence = (
                prior_status in _OWNED_ENTRY_STATUSES
                and prior_quantity > ZERO
                and ZERO < prior_price < ONE
                and prior_fees >= ZERO
            )
            preserved_prior_ownership = False
            if quantity <= ZERO:
                if prior_owned_evidence:
                    # A read with no current trade rows cannot revoke durable
                    # ownership already acknowledged by the ledger.  Keep the
                    # prior fill/cost basis and its risk reservation until a
                    # contradictory, independently authenticated fill is
                    # observed.
                    quantity = prior_quantity
                    cost = prior_quantity * prior_price
                    fees = prior_fees
                    preserved_prior_ownership = True
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
                (
                    terminal_order
                    or (confirmed_quantity_complete and settlement is not None)
                )
                and not provisional_trades
                and not fill_over_plan
                and not preserved_prior_ownership
            )
            if fill_over_plan:
                ledger_status = "UNKNOWN"
            elif has_confirmed_fill:
                # Canonical, individually confirmed fill evidence establishes
                # ownership but never synthesizes venue settlement.
                ledger_status = settlement or "CONFIRMED"
            elif preserved_prior_ownership:
                # Preserve the durable ledger acknowledgement when this poll
                # has no current fills; the absence of rows is not a
                # revocation of already-owned inventory.
                ledger_status = prior_status
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
                row.get("settlement")
                if preserved_prior_ownership
                else (
                    None
                    if fill_over_plan
                    else "TERMINAL"
                    if terminal_outcomes_complete
                    else "PROVISIONAL"
                    if provisional_trades
                    else settlement
                )
            )
            if terminal_outcomes_complete:
                release_detail = {
                    "no_fill_confirmed": True,
                    "terminal_status": status,
                    "filled_quantity": "0",
                    "source": "POLYMARKET_ORDER_STATUS",
                    "trade_count": "0",
                    "trade_ids": [],
                } if not has_confirmed_fill and not provisional_trades else {}
                _release_capacity(
                    service,
                    risk_reservation_id,
                    status="RELEASED",
                    timestamp=now,
                    lineage=entry_lineage,
                    candidate_id=row.get("candidate_id"),
                    detail=release_detail,
                )
            average = cost / quantity if quantity > ZERO else None
            with service.store._lock, connection:
                connection.execute(
                    "UPDATE canary_ledger SET status=?,fill_quantity=?,actual_average_price=?,fees=?,exchange_order_id=?,settlement=? "
                    "WHERE event_id=? AND ("
                    "NOT (UPPER(status) IN "
                    "('SETTLED','FINAL','CLOSED','COMPLETED') OR UPPER(COALESCE(settlement,''))='TERMINAL') "
                    "OR (UPPER(?)='RELEASED' AND CAST(COALESCE(fill_quantity,'0') AS NUMERIC) < CAST(? AS NUMERIC))"
                    ")",
                    (
                        ledger_status,
                        str(quantity),
                        str(average) if average is not None else row.get("actual_average_price"),
                        str(fees),
                        order_id,
                        settlement_marker,
                        event_id,
                        reservation_status,
                        str(quantity),
                    ),
                )
            results.append({"event_id": event_id, "status": ledger_status, "fill_quantity": str(quantity)})
        except CanaryBlocked as exc:
            with service.store._lock, connection:
                connection.execute(
                    "UPDATE canary_ledger SET status='UNKNOWN' WHERE event_id=? "
                    "AND NOT (UPPER(status) IN ('SETTLED','FINAL','CLOSED','COMPLETED') "
                    "OR UPPER(COALESCE(settlement,''))='TERMINAL')",
                    (event_id,),
                )
            results.append({"event_id": event_id, "status": "UNKNOWN", "reason": str(exc)})
        except Exception as exc:
            with service.store._lock, connection:
                connection.execute(
                    "UPDATE canary_ledger SET status='UNKNOWN' WHERE event_id=? "
                    "AND NOT (UPPER(status) IN ('SETTLED','FINAL','CLOSED','COMPLETED') "
                    "OR UPPER(COALESCE(settlement,''))='TERMINAL')",
                    (event_id,),
                )
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


def _release_capacity(
    service: CanaryService,
    reservation_id: str,
    *,
    status: str,
    timestamp: datetime,
    lineage: Mapping[str, Any],
    candidate_id: Any = None,
    detail: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Release/update a reservation with its exact opening lineage."""
    release = getattr(service.store, "release_canary_capacity", None)
    if not callable(release):
        raise CanaryBlocked("CANARY_RISK_RELEASE_UNAVAILABLE")
    opening_lineage = dict(lineage)
    candidate_value = str(
        candidate_id
        if candidate_id is not None
        else opening_lineage.get("candidate_id")
        or ""
    ).strip() or None
    candidate = (
        candidate_value
        if opening_lineage.get("lineage_type") == _ROLLING_LINEAGE_TYPE
        else None
    )
    if opening_lineage.get("allocation") in (None, ""):
        with service.store._lock:
            try:
                reservation = _connection(service).execute(
                    "SELECT allocation FROM canary_risk_reservations "
                    "WHERE reservation_id=?",
                    (str(reservation_id),),
                ).fetchone()
            except sqlite3.OperationalError:
                reservation = None
        if reservation is not None:
            opening_lineage["allocation"] = reservation["allocation"]
    release_detail = dict(detail or {})
    release_detail["candidate_id"] = candidate_value or ""
    for name in _POSITION_LINEAGE_FIELDS:
        release_detail[name] = opening_lineage.get(name)
    release_detail["lineage_type"] = (
        opening_lineage.get("lineage_type") or _LEGACY_LINEAGE_TYPE
    )
    return release(
        str(reservation_id),
        status=status,
        timestamp=timestamp,
        strategy_version_id=opening_lineage.get("strategy_version_id"),
        research_trial_id=opening_lineage.get("research_trial_id"),
        candidate_id=candidate,
        portfolio_selection_id=opening_lineage.get("portfolio_selection_id"),
        admission_policy_id=opening_lineage.get("admission_policy_id"),
        admission_policy_version=opening_lineage.get("admission_policy_version"),
        risk_config_id=opening_lineage.get("risk_config_id"),
        risk_config_generation=opening_lineage.get("risk_config_generation"),
        risk_config_hash=opening_lineage.get("risk_config_hash"),
        allocation=opening_lineage.get("allocation"),
        detail=release_detail,
    )
def reconcile_immediate_entry(
    service: CanaryService,
    venue: Any,
    *,
    event_id: str,
    now: datetime | None = None,
) -> Mapping[str, Any] | None:
    """Reconcile one just-submitted BUY before the next worker tick."""
    service = _service(service)
    _ensure_schema(service)
    stamp = ensure_utc(now or service.clock())
    event_key = str(event_id or "").strip()
    if not event_key:
        return None
    with service.store._lock:
        row = _connection(service).execute(
            "SELECT * FROM canary_ledger WHERE event_id=?",
            (event_key,),
        ).fetchone()
    if row is None or str(row["side"] or "").strip().upper() != "BUY":
        return None
    status = str(row["status"] or "").strip().upper()
    if status in _FAILED_ORDER or status in _CANCELED_ORDER:
        return None
    results = _reconcile_entry_ledger(
        service,
        venue,
        stamp,
        target_event_id=event_key,
    )
    _sync_entry_lots(service, stamp)
    for result in results:
        if str(result.get("event_id") or "").strip() == event_key:
            return result
    return None

def _reserve_exit(
    service: CanaryService,
    *,
    request_id: str,
    lot: Mapping[str, Any],
    quantity: Decimal,
    config: Mapping[str, Any],
    now: datetime,
    lineage: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    # The persisted lot is the sole authority for opening lineage.  A caller
    # supplied mapping is retained for compatibility but must not be allowed
    # to turn a legacy lot into a rolling reservation.
    del lineage
    opening_lineage = _opening_lot_lineage(service, lot)
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
        execution_authorization_id=config.get("execution_authorization_id"),
        controller_owner_id=config.get("controller_owner_id"),
        controller_generation=config.get("controller_generation"),
        config_generation=generation,
        config_hash=config_hash,
        control_generation=control_generation,
        strategy_version_id=opening_lineage["strategy_version_id"],
        research_trial_id=opening_lineage["research_trial_id"],
        candidate_id=(
            opening_lineage.get("candidate_id")
            if opening_lineage.get("lineage_type") == _ROLLING_LINEAGE_TYPE
            else None
        ),
        portfolio_selection_id=opening_lineage["portfolio_selection_id"],
        admission_policy_id=opening_lineage["admission_policy_id"],
        admission_policy_version=opening_lineage["admission_policy_version"],
        risk_config_id=opening_lineage["risk_config_id"],
        risk_config_generation=opening_lineage["risk_config_generation"],
        risk_config_hash=opening_lineage["risk_config_hash"],
        allocation=opening_lineage["allocation"],
        detail={
            "position_id": str(lot.get("position_id") or ""),
            "market_id": str(lot.get("market_id") or ""),
            "token_id": str(lot.get("token_id") or ""),
            "candidate_id": str(lot.get("candidate_id") or ""),
            "event_id": request_id,
            "settlement_status": "PENDING",
            "execution_authorization_id": config.get("execution_authorization_id"),
            "execution_authorization_mode": config.get("execution_authorization_mode"),
            "controller_owner_id": config.get("controller_owner_id"),
            "controller_generation": config.get("controller_generation"),
            **{
                name: opening_lineage[name]
                for name in _POSITION_LINEAGE_FIELDS
            },
        },
        timestamp=now,
    )


def submit_exit(
    service: CanaryService,
    position_id: str,
    venue: Any,
    *,
    expected_generation: int,
    config_id: str,
    allow_test_venue: bool = False,
    rolling_context: Mapping[str, Any] | None = None,
    force_exit: bool = False,
    strategy_version_id: str | None = None,
    research_trial_id: str | None = None,
    portfolio_selection_id: str | None = None,
    admission_policy_id: str | None = None,
    admission_policy_version: str | None = None,
    risk_config_id: str | None = None,
    risk_config_generation: int | None = None,
    risk_config_hash: str | None = None,
) -> Mapping[str, Any]:
    """Submit one bounded SELL for an AXIOM-owned, reconciled lot."""
    service = _service(service)
    expected_credential_fingerprint = service.require_current_credential_binding()
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
    lot_lineage = _opening_lot_lineage(service, lot)
    controller_lease = _require_controller_lease(
        service,
        context=rolling_context,
        now=now,
    )
    execution_authorization = _require_execution_authorization(
        service,
        signal=None,
        lineage=lot_lineage,
        context=rolling_context,
        now=now,
    )
    authorization_mode = execution_authorization.get("execution_authorization_mode")
    config = {
        **dict(config),
        "execution_authorization_id": execution_authorization.get(
            "authorization_id"
        ),
        "execution_authorization_mode": authorization_mode,
        "controller_owner_id": controller_lease.get("owner_id"),
        "controller_generation": controller_lease.get("generation"),
    }
    lot_lineage = {
        **lot_lineage,
        "execution_authorization_id": execution_authorization.get(
            "authorization_id"
        ),
        "execution_authorization_mode": authorization_mode,
        "controller_owner_id": controller_lease.get("owner_id"),
        "controller_generation": controller_lease.get("generation"),
        "controller_lease_generation": controller_lease.get("generation"),
    }
    authority_context = {
        **(dict(rolling_context) if isinstance(rolling_context, Mapping) else {}),
        "controller_owner_id": controller_lease.get("owner_id"),
        "controller_generation": controller_lease.get("generation"),
        "controller_lease_generation": controller_lease.get("generation"),
        "execution_authorization_id": execution_authorization.get(
            "authorization_id"
        ),
        "execution_authorization_mode": authorization_mode,
    }
    requested_lineage = dict(rolling_context or {})
    requested_lineage.update(
        {
            name: value
            for name, value in {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
            }.items()
            if value is not None
        }
    )
    requested_lineage_type = requested_lineage.get("lineage_type")
    if requested_lineage_type is not None and str(requested_lineage_type) != str(lot_lineage["lineage_type"]):
        raise CanaryBlocked("ROLLING_OPENING_LINEAGE_MISMATCH")
    if lot_lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE:
        lot_candidate = str(lot.get("candidate_id") or "").strip()
        if not lot_candidate:
            raise CanaryBlocked("ROLLING_OPENING_LINEAGE_INCOMPLETE")
        if "candidate_id" not in requested_lineage:
            requested_lineage["candidate_id"] = lot_candidate
        requested_candidate = str(requested_lineage.get("candidate_id") or "").strip()
        if requested_candidate != lot_candidate:
            raise CanaryBlocked("ROLLING_OPENING_LINEAGE_MISMATCH")
    for name in _POSITION_LINEAGE_FIELDS:
        if name in requested_lineage and requested_lineage[name] not in (None, ""):
            if str(requested_lineage[name]) != str(lot_lineage.get(name)):
                raise CanaryBlocked("ROLLING_OPENING_LINEAGE_MISMATCH")
    total_quantity = _decimal(lot.get("quantity"), ZERO)
    sold_quantity = _decimal(lot.get("sold_quantity"), ZERO)
    pending_quantity = _decimal(lot.get("pending_exit_quantity"), ZERO)
    quantity = max(ZERO, total_quantity - sold_quantity - pending_quantity)
    # Polymarket CLOB orders carry quantities at two decimal places.  Floor
    quantity = quantity.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if quantity:
        quantity = quantity.normalize()
    lot_status = str(lot.get("status") or "").upper()
    if lot_status == "MANAGEMENT_BLOCKED" and not force_exit:
        raise CanaryBlocked("CANARY_POSITION_MANAGEMENT_BLOCKED")
    if quantity <= DUST or lot_status in {"CLOSED", "DUST", "DISPUTED"}:
        raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")
    if not force_exit and not _valid_exit_policy(_decode(lot.get("exit_policy_json"))):
        raise CanaryBlocked("CANARY_POSITION_MANAGEMENT_BLOCKED")
    if (
        lot_lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
        and any(
            lot_lineage.get(name) in (None, "")
            for name in _POSITION_LINEAGE_FIELDS
        )
    ):
        raise CanaryBlocked("ROLLING_OPENING_LINEAGE_INCOMPLETE")
    existing = _request_rows(service, position_key)
    if any(str(item.get("side") or "").upper() == "SELL" for item in existing):
        raise CanaryBlocked("DUPLICATE_EXIT_REQUEST")
    market_id = str(lot.get("market_id") or "").strip()
    token_id = str(lot.get("token_id") or "").strip()
    context = _call(_method(venue, "market_context"), market_id=market_id, token_id=token_id)
    if not isinstance(context, Mapping):
        raise CanaryBlocked("CANARY_EXIT_MARKET_CONTEXT_INVALID")
    # Price is the first market preflight so malformed or missing bid data
    # cannot be masked by a later market-rule failure.
    price = _extract_book_price(context, side="SELL")
    market_version, asset_id = _validate_market_context_identity(
        context,
        token_id,
        missing_reason="CANARY_EXIT_MARKET_CONTEXT_INVALID",
        conflict_reason="CANARY_EXIT_MARKET_CONTEXT_INVALID",
    )
    try:
        persisted_identity = _lot_persisted_identity(service, lot)
    except CanaryBlocked:
        _flag_lot_identity(service, position_key)
        raise
    if persisted_identity is None:
        if market_version != "v1" or asset_id != token_id:
            _flag_lot_identity(service, position_key)
            raise CanaryBlocked("CANARY_POSITION_IDENTITY_UNAVAILABLE")
    elif persisted_identity != (market_version, asset_id):
        _flag_lot_identity(service, position_key)
        raise CanaryBlocked("CANARY_POSITION_IDENTITY_CONFLICT")
    accepting_orders = context.get("accepting_orders")
    if type(accepting_orders) is not bool or not accepting_orders:
        raise CanaryBlocked("CANARY_MARKET_NOT_ACCEPTING_ORDERS")
    neg_risk_key = "neg_risk" if "neg_risk" in context else "negRisk"
    if neg_risk_key not in context or type(context.get(neg_risk_key)) is not bool:
        raise CanaryBlocked("CANARY_EXIT_MARKET_RULES_UNAVAILABLE")
    try:
        rules = parse_polymarket_rules(context)
    except PolymarketRuleError as exc:
        raise CanaryBlocked("CANARY_EXIT_MARKET_RULES_UNAVAILABLE") from exc
    if quantity + DUST < rules.min_order_size:
        raise CanaryBlocked("CANARY_EXIT_BELOW_MINIMUM")
    # A managed SELL must use a canonical tick-aligned quote.
    remainder = price % rules.tick_size
    if remainder > DUST and rules.tick_size - remainder > DUST:
        raise CanaryBlocked("CANARY_EXIT_PRICE_TICK_INVALID")
    try:
        depth = assess_selected_token_depth(
            context,
            rules,
            side="SELL",
            quantity=quantity,
        )
    except PolymarketRuleError as exc:
        raise CanaryBlocked("CANARY_EXIT_DEPTH_UNAVAILABLE") from exc
    if not depth.suitable:
        if depth.reason in {"NO_DEPTH", "INSUFFICIENT_DEPTH"}:
            raise CanaryBlocked("CANARY_EXIT_INSUFFICIENT_DEPTH")
        raise CanaryBlocked("CANARY_EXIT_DEPTH_UNAVAILABLE")
    # Validate a test transport before reserving capacity so a missing method
    # is a deterministic preflight blocker, not an UNKNOWN post-boundary state.
    test_submit = _method(venue, "submit_limit_order") if allow_test_venue else None
    request_id = "exit:" + position_key + ":" + str(expected_generation) + ":" + uuid.uuid4().hex
    reservation = _reserve_exit(
        service,
        request_id=request_id,
        lot=lot,
        quantity=quantity,
        config=config,
        now=now,
        lineage=lot_lineage,
    )
    reservation_id = str(reservation.get("reservation_id") or "").strip()
    if reservation_id != request_id:
        raise CanaryBlocked("CANARY_RISK_RESERVATION_ID_INVALID")
    expected_pending = pending_quantity
    release = getattr(service.store, "release_canary_capacity", None)
    if not callable(release):
        raise CanaryBlocked("CANARY_RISK_RELEASE_UNAVAILABLE")

    def reject_prepared(reason: str) -> None:
        _release_capacity(
            service,
            reservation_id,
            status="RELEASED",
            timestamp=ensure_utc(service.clock()),
            lineage=lot_lineage,
            candidate_id=lot_lineage.get("candidate_id"),
        )
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
                "UPDATE canary_position_lots SET pending_exit_quantity=?,"
                "status='EXIT_PENDING',updated_at=? WHERE position_id=? "
                "AND status IN ('OPEN','EXIT_PENDING','MANAGEMENT_BLOCKED')",
                (str(expected_pending + quantity), _iso(now), position_key),
            )
            if int(updated.rowcount or 0) != 1:
                raise CanaryBlocked("DUPLICATE_EXIT_REQUEST")
            _connection(service).execute(
                "INSERT INTO canary_position_requests("
                "request_id,position_id,reservation_id,event_id,venue,market_id,"
                "token_id,asset_id,market_version,strategy_version_id,research_trial_id,"
                "portfolio_selection_id,admission_policy_id,admission_policy_version,"
                "risk_config_id,risk_config_generation,risk_config_hash,"
                "execution_authorization_id,controller_owner_id,controller_generation,"
                "lineage_type,side,requested_quantity,requested_price,status,expected_generation,"
                "config_id,submitted_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    request_id,
                    position_key,
                    reservation_id,
                    request_id,
                    lot.get("venue"),
                    market_id,
                    token_id,
                    asset_id,
                    market_version,
                    *(lot_lineage.get(name) for name in _POSITION_LINEAGE_FIELDS),
                    config.get("execution_authorization_id"),
                    config.get("controller_owner_id"),
                    config.get("controller_generation"),
                    lot_lineage["lineage_type"],
                    "SELL",
                    str(quantity),
                    str(price),
                    "PREPARED",
                    int(expected_generation),
                    str(config_id),
                    _iso(now),
                    _iso(now),
                ),
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
            execution_authorization_id=config.get("execution_authorization_id"),
            controller_owner_id=config.get("controller_owner_id"),
            controller_generation=config.get("controller_generation"),
            strategy_version_id=lot_lineage["strategy_version_id"],
            research_trial_id=lot_lineage["research_trial_id"],
            portfolio_selection_id=lot_lineage["portfolio_selection_id"],
            admission_policy_id=lot_lineage["admission_policy_id"],
            admission_policy_version=lot_lineage["admission_policy_version"],
            risk_config_id=lot_lineage["risk_config_id"],
            risk_config_generation=lot_lineage["risk_config_generation"],
            risk_config_hash=lot_lineage["risk_config_hash"],
            allocation=lot_lineage["allocation"],
            detail={
                "position_id": position_key,
                "quantity": str(quantity),
                "venue": str(lot.get("venue") or "polymarket"),
                "candidate_id": str(lot.get("candidate_id") or ""),
                **{
                    name: lot_lineage[name]
                    for name in _POSITION_LINEAGE_FIELDS
                    if lot_lineage.get(name) is not None
                },
                "execution_authorization_id": config.get("execution_authorization_id"),
                "controller_owner_id": config.get("controller_owner_id"),
                "controller_generation": config.get("controller_generation"),
                "lineage_type": lot_lineage["lineage_type"],
            },
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
            expected_credential_fingerprint=expected_credential_fingerprint,
        )
        with service.store._lock:
            row = _connection(service).execute(
                "SELECT status,requested_quantity,execution_authorization_id,"
                "controller_owner_id,controller_generation "
                "FROM canary_position_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            lot_check = _connection(service).execute(
                "SELECT quantity,sold_quantity,status,candidate_id,"
                "strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,lineage_type "
                "FROM canary_position_lots WHERE position_id=?",
                (position_key,),
            ).fetchone()
        if row is None or str(row["status"] or "").upper() != "SUBMITTING":
            raise CanaryBlocked("CANARY_SUBMISSION_PHASE_CHANGED")
        expected_authority = (
            str(config.get("execution_authorization_id") or "").strip(),
            str(config.get("controller_owner_id") or "").strip(),
            str(config.get("controller_generation") or "").strip(),
        )
        observed_authority = (
            str(row["execution_authorization_id"] or "").strip(),
            str(row["controller_owner_id"] or "").strip(),
            str(row["controller_generation"] or "").strip(),
        )
        if not all(expected_authority) or observed_authority != expected_authority:
            raise CanaryBlocked("CANARY_RESERVATION_INVALID")
        allowed_lot_statuses = {"OPEN", "EXIT_PENDING"}
        if force_exit:
            allowed_lot_statuses.add("MANAGEMENT_BLOCKED")
        if lot_check is None or str(lot_check["status"] or "").upper() not in allowed_lot_statuses:
            raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")
        owned = max(ZERO, _decimal(lot_check["quantity"], ZERO) - _decimal(lot_check["sold_quantity"], ZERO))
        if owned + DUST < quantity:
            raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")
        current_lineage = _lineage_from_row(lot_check) if lot_check is not None else {}
        if (
            lot_lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE
            and (
                lot_check is None
                or any(
                    current_lineage.get(name) != lot_lineage.get(name)
                    for name in (*_POSITION_LINEAGE_FIELDS, "lineage_type")
                )
            )
        ):
            raise CanaryBlocked("ROLLING_OPENING_LINEAGE_MISMATCH")
        if lot_lineage["lineage_type"] == _ROLLING_LINEAGE_TYPE and (
            str(lot_check["candidate_id"] or "").strip()
            != str(requested_lineage.get("candidate_id") or "").strip()
        ):
            raise CanaryBlocked("ROLLING_OPENING_LINEAGE_MISMATCH")

        _require_controller_lease(
            service,
            context=authority_context,
            now=ensure_utc(service.clock()),
        )
        _require_execution_authorization(
            service,
            signal=None,
            lineage=lot_lineage,
            context=authority_context,
            now=ensure_utc(service.clock()),
        )
        _final_venue_identity_fence(
            service,
            venue,
            side="SELL",
            market_id=market_id,
            token_id=token_id,
            asset_id=asset_id,
            context={**context, **authority_context},
            close_only_allowed=True,
        )
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
                expected_credential_fingerprint=expected_credential_fingerprint,
            )
            on_send_started()
            service.require_current_credential_binding(expected_credential_fingerprint)
            return _call(
                test_submit,
                token_id=asset_id,
                side="SELL",
                price=price,
                size=quantity,
            )
        return service._submit_position_order(
            market_version=market_version,
            neg_risk=context.get("neg_risk"),
            asset_id=asset_id,
            side="SELL",
            price=price,
            size=quantity,
            before_post=before_post,
            on_send_started=on_send_started,
            expected_credential_fingerprint=expected_credential_fingerprint,
            reservation_id=reservation_id,
            control_generation=control_generation,
            market_id=market_id,
            token_id=token_id,
            candidate_id=str(lot_lineage.get("candidate_id") or ""),
            lineage=lot_lineage,
        )

    def persist_submission_failure(status: str, error: str) -> None:
        with service.store._lock, _connection(service):
            _connection(service).execute(
                "UPDATE canary_position_requests SET status=?,last_error=?,updated_at=? "
                "WHERE request_id=? AND UPPER(COALESCE(status,'')) NOT IN "
                "('SETTLED','SETTLED_PARTIAL','FINAL','CLOSED','COMPLETED',"
                "'CANCELED','CANCELLED','EXPIRED','REJECTED','FAILED','ERROR','FILLED')",
                (status, error, _iso(ensure_utc(service.clock())), request_id),
            )
        release_status = "RELEASED" if status == "REJECTED" else "UNKNOWN"
        _release_capacity(
            service,
            reservation_id,
            status=release_status,
            timestamp=ensure_utc(service.clock()),
            lineage=lot_lineage,
            candidate_id=lot_lineage.get("candidate_id"),
            detail={"request_status": status},
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
            "CANARY_EXIT_MARKET_RULES_UNAVAILABLE",
            "CANARY_EXIT_PRICE_TICK_INVALID",
            "CANARY_EXIT_INSUFFICIENT_DEPTH",
            "CANARY_EXIT_DEPTH_UNAVAILABLE",
            "CANARY_SUBMISSION_CONTEXT_INVALID",
            "CANARY_CONTROLLER_LEASE_REQUIRED",
            "CANARY_CONTROLLER_LEASE_UNAVAILABLE",
            "CANARY_CONTROLLER_LEASE_INVALID",
            "CANARY_CONTROLLER_LEASE_CHANGED",
            "CANARY_CONTROLLER_LEASE_OWNER_MISMATCH",
            "CANARY_CONTROLLER_LEASE_EXPIRED",
            "EXECUTION_AUTHORIZATION_REQUIRED",
            "EXECUTION_AUTHORIZATION_EXPIRED",
            "EXECUTION_AUTHORIZATION_SETTINGS_MISMATCH",
            "EXECUTION_AUTHORIZATION_CHANGED",
            "GEOGRAPHICALLY_BLOCKED",
            "GEOBLOCK_CLOSE_ONLY",
            "GEOBLOCK_CHECK_FAILED",
            "GEOBLOCK_RESPONSE_INVALID",
            "ACCOUNT_CHECK_UNAVAILABLE",
            "ACCOUNT_CHECK_FAILED",
            "ACCOUNT_NOT_AUTHENTICATED",
            "SIGNER_MISMATCH",
            "FUNDER_MISMATCH",
            "OWNER_MISMATCH",
            "ACCOUNT_OWNER_MISMATCH",
            "SELECTED_TOKEN_MISMATCH",
            "MARKET_OUTCOME_ID_UNAVAILABLE",
            "MARKET_CONTEXT_FAILED",
            "EXCHANGE_SPENDER_MISMATCH",
            "EXCHANGE_SPENDER_UNAVAILABLE",
            "CANARY_RESERVATION_INVALID",
            "ROLLING_LINEAGE_INCOMPLETE",
            "ROLLING_LINEAGE_CONFLICT",
            "CANARY_ALLOWANCE_INSUFFICIENT",
            "CANARY_ALLOWANCE_UNAVAILABLE",
            "CANARY_SPENDER_UNAVAILABLE",
            "CANARY_BALANCE_UNAVAILABLE",
            "INSUFFICIENT_BALANCE",
            "CREDENTIALS_NOT_CONFIGURED",
            "CREDENTIAL_BINDING_MISSING",
            "CREDENTIAL_BINDING_MISMATCH",
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
        status = status_text
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
            "UPDATE canary_position_requests SET order_id=?,status=?,last_error=?,updated_at=? "
            "WHERE request_id=? AND UPPER(COALESCE(status,'')) NOT IN "
            "('SETTLED','SETTLED_PARTIAL','FINAL','CLOSED','COMPLETED',"
            "'CANCELED','CANCELLED','EXPIRED','REJECTED','FAILED','ERROR','FILLED')",
            (order_id, status, response_error, _iso(ensure_utc(service.clock())), request_id),
        )
        if status in _FAILED_ORDER or status in _CANCELED_ORDER:
            _connection(service).execute(
                "UPDATE canary_position_lots SET status='OPEN',pending_exit_quantity=?,updated_at=? WHERE position_id=? "
                "AND UPPER(COALESCE(status,'')) NOT IN ('CLOSED','DUST')",
                (str(expected_pending), _iso(ensure_utc(service.clock())), position_key),
            )
    reservation_status = (
        "RELEASED"
        if status in _FAILED_ORDER or status in _CANCELED_ORDER
        else "UNKNOWN"
        if status == "UNKNOWN"
        else "OPEN"
    )
    _release_capacity(
        service,
        reservation_id,
        status=reservation_status,
        timestamp=ensure_utc(service.clock()),
        lineage=lot_lineage,
        candidate_id=lot_lineage.get("candidate_id"),
        detail={"request_status": status},
    )
    return {
        "request_id": request_id,
        "position_id": position_key,
        "reservation_id": reservation_id,
        "order_id": order_id,
        "status": status,
        "quantity": str(quantity),
        "price": str(price),
        "lineage": lot_lineage,
    }


def _apply_reconciled_request(service: CanaryService, request: Mapping[str, Any], order: Mapping[str, Any], trades: Sequence[Mapping[str, Any]], now: datetime) -> dict[str, Any]:
    connection = _connection(service)
    request_id = str(request.get("request_id") or "").strip()
    position_id = str(request.get("position_id") or "").strip()
    order_id = str(request.get("order_id") or "").strip()
    with service.store._lock:
        current_request_row = connection.execute(
            "SELECT * FROM canary_position_requests WHERE request_id=?",
            (request_id,),
        ).fetchone()
        current_reservation_row = connection.execute(
            "SELECT status FROM canary_risk_reservations WHERE reservation_id=?",
            (str(request.get("reservation_id") or "").strip(),),
        ).fetchone()
        lot_row = connection.execute(
            "SELECT * FROM canary_position_lots WHERE position_id=?",
            (position_id,),
        ).fetchone()
    current_request = (
        dict(current_request_row) if current_request_row is not None else dict(request)
    )
    current_request_status = _normalized_request_status(
        current_request.get("status")
    )
    # A request that already reached a terminal outcome must not be reopened
    # by a stale reconciliation worker.  FILLED remains pollable for the
    # explicit settlement transition, so it is deliberately handled below.
    if current_request_status in _TERMINAL:
        return _request_snapshot_result(current_request, current_request_status)
    reservation_status = (
        str(current_reservation_row["status"] or "").strip().upper()
        if current_reservation_row is not None
        else ""
    )
    # RELEASED is final for a reservation.  No late venue observation may
    # mutate the sold quantity, P&L, or released accounting basis.
    if reservation_status == "RELEASED":
        return _request_snapshot_result(current_request, current_request_status)
    if lot_row is None:
        raise CanaryBlocked("CANARY_POSITION_NOT_FOUND")
    lot_context = dict(lot_row)
    opening_lineage = _opening_lot_lineage(service, lot_context)
    _validate_venue_identity(
        order=order,
        trades=trades,
        expected_order_id=order_id,
        expected_side=current_request.get("side"),
        expected_market_id=current_request.get("market_id"),
        expected_token_id=current_request.get("token_id"),
        expected_quantity=current_request.get("requested_quantity"),
        expected_asset_id=current_request.get("asset_id"),
        expected_market_version=current_request.get("market_version"),
        expected_price=current_request.get("requested_price"),
        lot=lot_context,
        prior_request_quantity=current_request.get("filled_quantity"),
    )
    trades = [
        _account_trade_projection(trade, order_id)
        for trade in trades
    ]
    order_status = _order_status(order)
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
    reservation_id = str(request.get("reservation_id") or "").strip()
    if not reservation_id:
        raise CanaryBlocked("CANARY_RISK_RESERVATION_UNAVAILABLE")
    release_reservation_id = reservation_id
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
                "SELECT request_id,position_id,quantity,price,fee,filled_at "
                "FROM canary_position_fills WHERE fill_id=?",
                (fill_id,),
            ).fetchone()
            global_fill = connection.execute(
                "SELECT reservation_id FROM canary_risk_fills WHERE fill_id=?",
                (fill_id,),
            ).fetchone()
        if global_fill is not None and str(global_fill["reservation_id"]) != reservation_id:
            raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
        if prior is None:
            continue
        if (
            str(prior["request_id"] or "") != request_id
            or str(prior["position_id"] or "") != position_id
        ):
            raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
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
    with service.store.transaction(immediate=True):
        # Keep each canonical risk fill and its position projection in the
        # same outer transaction.  ``record_canary_fill`` uses a nested
        # savepoint when called here, so it cannot commit independently of
        # the matching local fill insert.
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
            if not callable(risk_fill):
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
                "order_id": order_id,
                "market_id": str(request.get("market_id") or ""),
                "token_id": str(request.get("token_id") or ""),
                "side": "SELL",
                "requested_quantity": str(request.get("requested_quantity") or ""),
                "settlement_status": "CONFIRMED",
                "entry_cost_usd": str(entry_cost),
                "cost_basis_usd": str(entry_cost),
                "proceeds_usd": str(net_proceeds),
                "realized_pnl_usd": str(realized_pnl),
                "exit_fee_usd": str(fee),
                **{
                    name: opening_lineage.get(name)
                    for name in _POSITION_LINEAGE_FIELDS
                    if opening_lineage.get(name) is not None
                },
                "lineage_type": opening_lineage["lineage_type"],
            }
            risk_fill(
                fill_id=fill_id,
                reservation_id=reservation_id,
                quantity=quantity,
                price=price,
                cost=quantity * price + fee,
                fee=fee,
                filled_at=matched_at,
                strategy_version_id=opening_lineage["strategy_version_id"],
                research_trial_id=opening_lineage["research_trial_id"],
                candidate_id=opening_lineage.get("candidate_id"),
                portfolio_selection_id=opening_lineage["portfolio_selection_id"],
                admission_policy_id=opening_lineage["admission_policy_id"],
                admission_policy_version=opening_lineage["admission_policy_version"],
                risk_config_id=opening_lineage["risk_config_id"],
                risk_config_generation=opening_lineage["risk_config_generation"],
                risk_config_hash=opening_lineage["risk_config_hash"],
                allocation=opening_lineage.get("allocation"),
                detail=risk_detail,
            )
            inserted_row = connection.execute(
                "INSERT INTO canary_position_fills("
                "fill_id,request_id,position_id,quantity,price,fee,status,filled_at,"
                "strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,lineage_type,detail_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(fill_id) DO NOTHING",
                (
                    fill_id,
                    request_id,
                    position_id,
                    str(quantity),
                    str(price),
                    str(fee),
                    "CONFIRMED",
                    _iso(matched_at),
                    *(opening_lineage.get(name) for name in _POSITION_LINEAGE_FIELDS),
                    opening_lineage["lineage_type"],
                    _json(dict(trade)),
                ),
            )
            if int(inserted_row.rowcount or 0) == 1:
                inserted += 1
            else:
                # Two reconcilers can both observe the fill before either
                # inserts the canonical row.  The losing insert is
                # idempotent only when its evidence is byte-for-byte equal.
                prior = connection.execute(
                    "SELECT request_id,position_id,quantity,price,fee,filled_at "
                    "FROM canary_position_fills WHERE fill_id=?",
                    (fill_id,),
                ).fetchone()
                if prior is None:
                    raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
                prior_time = parse_timestamp(prior["filled_at"])
                if prior_time is None or (
                    str(prior["request_id"] or "") != request_id
                    or str(prior["position_id"] or "") != position_id
                    or _decimal(prior["quantity"], Decimal("-1")) != quantity
                    or _decimal(prior["price"], Decimal("-1")) != price
                    or _decimal(prior["fee"], Decimal("-1")) != fee
                    or prior_time != matched_at
                ):
                    raise CanaryBlocked("CANARY_TRADE_ID_CONFLICT")
    class _ReconciliationCASMiss(Exception):
        pass

    result: dict[str, Any] | None = None
    terminal = False
    settlement_status: str | None = None
    status = "UNKNOWN"
    for _attempt in range(2):
        try:
            with service.store._lock, connection:
                current_request_row = connection.execute(
                    "SELECT * FROM canary_position_requests WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                if current_request_row is None:
                    raise CanaryBlocked("CANARY_REQUEST_NOT_FOUND")
                current_request = dict(current_request_row)
                current_request_status = _normalized_request_status(
                    current_request.get("status")
                )
                # A stale replay must never reopen or re-account a request
                # whose terminal state is already durable.
                if current_request_status in _TERMINAL:
                    return _request_snapshot_result(
                        current_request,
                        current_request_status,
                    )
                current_reservation_id = str(
                    current_request.get("reservation_id") or reservation_id
                ).strip()
                reservation_row = connection.execute(
                    "SELECT status FROM canary_risk_reservations "
                    "WHERE reservation_id=?",
                    (current_reservation_id,),
                ).fetchone()
                if (
                    reservation_row is not None
                    and str(reservation_row["status"] or "").strip().upper()
                    in {
                        "SETTLED",
                        "CANCELED",
                        "CANCELLED",
                        "REJECTED",
                        "RELEASED",
                    }
                ):
                    release_reservation_id = current_reservation_id
                    return _request_snapshot_result(
                        current_request,
                        current_request_status,
                    )
                lot_row = connection.execute(
                    "SELECT * FROM canary_position_lots WHERE position_id=?",
                    (position_id,),
                ).fetchone()
                if lot_row is None:
                    raise CanaryBlocked("CANARY_POSITION_NOT_FOUND")
                lot_context = dict(lot_row)
                current_order_id = str(
                    current_request.get("order_id") or order_id
                ).strip()
                _validate_venue_identity(
                    order=order,
                    trades=trades,
                    expected_order_id=current_order_id,
                    expected_side=current_request.get("side"),
                    expected_market_id=current_request.get("market_id"),
                    expected_token_id=current_request.get("token_id"),
                    expected_quantity=current_request.get("requested_quantity"),
                    expected_asset_id=current_request.get("asset_id"),
                    expected_market_version=current_request.get("market_version"),
                    expected_price=current_request.get("requested_price"),
                    lot=lot_context,
                    prior_request_quantity=current_request.get("filled_quantity"),
                )
                trades = [
                    _account_trade_projection(trade, current_order_id)
                    for trade in trades
                ]
                request_status_before = str(
                    current_request.get("status") or ""
                ).strip().upper()
                request_filled_before = current_request.get("filled_quantity")
                request_average_before = current_request.get("average_price")
                request_fees_before = current_request.get("fees")
                request_settlement_before = current_request.get(
                    "settlement_status"
                )
                lot_quantity_before = lot_row["quantity"]
                lot_basis_before = lot_row["cost_basis"]
                lot_sold_before = lot_row["sold_quantity"]
                lot_pending_before = lot_row["pending_exit_quantity"]
                lot_gross_before = lot_row["gross_proceeds"]
                lot_exit_fees_before = lot_row["exit_fees"]
                lot_pnl_before = lot_row["realized_pnl"]
                lot_status_before = str(lot_row["status"] or "").strip().upper()
                if lot_status_before in {"CLOSED", "DUST"}:
                    raise CanaryBlocked("CANARY_POSITION_UNAVAILABLE")

                fill_rows = connection.execute(
                    "SELECT quantity,fee,price FROM canary_position_fills "
                    "WHERE request_id=? ORDER BY filled_at,fill_id",
                    (request_id,),
                ).fetchall()
                existing_qty = sum(
                    (_decimal(row["quantity"], ZERO) for row in fill_rows),
                    ZERO,
                )
                existing_fees = sum(
                    (_decimal(row["fee"], ZERO) for row in fill_rows),
                    ZERO,
                )
                existing_cost = sum(
                    (
                        _decimal(row["quantity"], ZERO)
                        * _decimal(row["price"], ZERO)
                        for row in fill_rows
                    ),
                    ZERO,
                )
                requested = _decimal(
                    current_request.get("requested_quantity"),
                    ZERO,
                )
                prior_request_qty = _decimal(
                    request_filled_before,
                    ZERO,
                )
                prior_request_avg = _decimal(
                    request_average_before,
                    ZERO,
                )
                prior_request_fees = _decimal(
                    request_fees_before,
                    ZERO,
                )
                # These are deliberately computed from the just-read request
                # and lot rows, never from the pre-venue snapshot.
                delta_qty = max(ZERO, existing_qty - prior_request_qty)
                delta_gross = max(
                    ZERO,
                    existing_cost - (prior_request_qty * prior_request_avg),
                )
                delta_fees = max(ZERO, existing_fees - prior_request_fees)
                prior_sold = _decimal(lot_sold_before, ZERO)
                prior_pending = _decimal(lot_pending_before, ZERO)
                prior_gross = _decimal(lot_gross_before, ZERO)
                prior_exit_fees = _decimal(lot_exit_fees_before, ZERO)
                prior_pnl = _decimal(lot_pnl_before, ZERO)
                lot_quantity = _decimal(lot_quantity_before, ZERO)
                lot_basis = _decimal(lot_basis_before, ZERO)
                fill_over_plan = (
                    existing_qty > requested + DUST
                    or prior_sold + delta_qty > lot_quantity + DUST
                )
                average_price = (
                    str(existing_cost / existing_qty)
                    if existing_qty > ZERO
                    else None
                )

                def update_request(
                    *,
                    next_status: str,
                    next_settlement: str | None,
                ) -> Any:
                    return connection.execute(
                        "UPDATE canary_position_requests SET order_id=?,"
                        "filled_quantity=?,average_price=?,fees=?,status=?,"
                        "settlement_status=?,updated_at=? WHERE request_id=? "
                        "AND UPPER(COALESCE(status,''))=? "
                        "AND filled_quantity IS ? AND average_price IS ? "
                        "AND fees IS ? AND settlement_status IS ?",
                        (
                            current_order_id or None,
                            str(existing_qty),
                            average_price,
                            str(existing_fees),
                            next_status,
                            next_settlement,
                            _iso(now),
                            request_id,
                            request_status_before,
                            request_filled_before,
                            request_average_before,
                            request_fees_before,
                            request_settlement_before,
                        ),
                    )

                def update_lot(
                    *,
                    sold: Decimal,
                    pending: Decimal,
                    gross: Decimal,
                    exit_fees: Decimal,
                    pnl: Decimal,
                    next_status: str,
                ) -> Any:
                    return connection.execute(
                        "UPDATE canary_position_lots SET sold_quantity=?,"
                        "pending_exit_quantity=?,gross_proceeds=?,"
                        "exit_fees=?,realized_pnl=?,status=?,updated_at=? "
                        "WHERE position_id=? AND quantity IS ? "
                        "AND cost_basis IS ? AND sold_quantity IS ? "
                        "AND pending_exit_quantity IS ? AND gross_proceeds IS ? "
                        "AND exit_fees IS ? AND realized_pnl IS ? "
                        "AND UPPER(COALESCE(status,''))=?",
                        (
                            str(sold),
                            str(pending),
                            str(gross),
                            str(exit_fees),
                            str(pnl),
                            next_status,
                            _iso(now),
                            position_id,
                            lot_quantity_before,
                            lot_basis_before,
                            lot_sold_before,
                            lot_pending_before,
                            lot_gross_before,
                            lot_exit_fees_before,
                            lot_pnl_before,
                            lot_status_before,
                        ),
                    )

                if fill_over_plan:
                    reservation_row = connection.execute(
                        "SELECT detail_json FROM canary_risk_reservations "
                        "WHERE reservation_id=?",
                        (current_reservation_id,),
                    ).fetchone()
                    try:
                        reservation_detail = (
                            json.loads(reservation_row["detail_json"] or "{}")
                            if reservation_row is not None
                            else {}
                        )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        reservation_detail = {}
                    reservation_detail["risk_breaker"] = "EXIT_FILL_OVER_PLAN"
                    reservation_detail["actual_quantity"] = str(existing_qty)
                    reservation_detail["requested_quantity"] = str(requested)
                    connection.execute(
                        "UPDATE canary_risk_reservations SET detail_json=?,"
                        "status='UNKNOWN',released_at=NULL WHERE reservation_id=? "
                        "AND UPPER(COALESCE(status,'')) NOT IN "
                        "('FILLED','SETTLED','RELEASED','CANCELED',"
                        "'CANCELLED','REJECTED')",
                        (_json(reservation_detail), current_reservation_id),
                    )
                    request_update = update_request(
                        next_status="UNKNOWN",
                        next_settlement=None,
                    )
                    if int(request_update.rowcount or 0) != 1:
                        raise _ReconciliationCASMiss
                    lot_update = update_lot(
                        sold=prior_sold,
                        pending=prior_pending,
                        gross=prior_gross,
                        exit_fees=prior_exit_fees,
                        pnl=prior_pnl,
                        next_status="EXIT_PENDING",
                    )
                    if int(lot_update.rowcount or 0) != 1:
                        raise _ReconciliationCASMiss
                    result = {
                        "request_id": request_id,
                        "position_id": position_id,
                        "status": "UNKNOWN",
                        "filled_quantity": str(existing_qty),
                        "new_fills": inserted,
                        "order_id": current_order_id or None,
                        "blocker": "EXIT_FILL_OVER_PLAN",
                    }
                else:
                    settlement_status = order_settlement
                    terminal = (
                        (
                            settlement_status is not None
                            and not provisional_trades
                        )
                        or order_status in _FAILED_ORDER
                        or (
                            order_status in _CANCELED_ORDER
                            and not provisional_trades
                        )
                    )
                    if settlement_status is not None and not provisional_trades:
                        proposed_status = (
                            "SETTLED"
                            if existing_qty + DUST >= requested
                            else "SETTLED_PARTIAL"
                        )
                    elif settlement_status is not None:
                        # A final marker accompanied by provisional evidence
                        # remains unresolved.
                        proposed_status = "UNKNOWN"
                    elif provisional_trades:
                        # Provisional evidence retains the pending obligation.
                        proposed_status = "MATCHED"
                    else:
                        proposed_status = order_status
                    status = _monotonic_request_status(
                        current_request_status,
                        proposed_status,
                    )
                    if (
                        current_request_status == "FILLED"
                        and status == "FILLED"
                        and settlement_status is None
                    ):
                        # FILLED remains pollable for settlement.
                        terminal = False
                    pending_total = ZERO if terminal else max(
                        ZERO,
                        prior_pending,
                    )
                    sold_total = prior_sold + delta_qty
                    gross_total = prior_gross + delta_gross
                    exit_fees_total = prior_exit_fees + delta_fees
                    basis_delta = (
                        delta_qty * lot_basis / lot_quantity
                        if lot_quantity > ZERO
                        else ZERO
                    )
                    basis_delta = min(lot_basis, basis_delta)
                    pnl_total = (
                        prior_pnl
                        + delta_gross
                        - delta_fees
                        - basis_delta
                    )
                    lot_status = (
                        "CLOSED"
                        if (
                            settlement_status is not None
                            and status == "SETTLED"
                            and max(ZERO, requested - existing_qty) <= DUST
                        )
                        else "OPEN"
                        if terminal
                        else "EXIT_PENDING"
                    )
                    request_update = update_request(
                        next_status=status,
                        next_settlement=settlement_status,
                    )
                    if int(request_update.rowcount or 0) != 1:
                        raise _ReconciliationCASMiss
                    lot_update = update_lot(
                        sold=sold_total,
                        pending=pending_total,
                        gross=gross_total,
                        exit_fees=exit_fees_total,
                        pnl=pnl_total,
                        next_status=lot_status,
                    )
                    if int(lot_update.rowcount or 0) != 1:
                        raise _ReconciliationCASMiss
                    result = {
                        "request_id": request_id,
                        "position_id": position_id,
                        "status": status,
                        "filled_quantity": str(existing_qty),
                        "new_fills": inserted,
                        "order_id": current_order_id or None,
                    }
        except _ReconciliationCASMiss:
            # The transaction context rolls back both writes.  Re-read and
            # recompute once so a stale worker can converge on the winner.
            continue
        break

    if result is None:
        persisted_status = _mark_request_unknown(
            service,
            request_id,
            "CANARY_RECONCILIATION_CAS_FAILED",
        )
        with service.store._lock:
            current_row = connection.execute(
                "SELECT * FROM canary_position_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
        if current_row is None:
            raise CanaryBlocked("CANARY_REQUEST_NOT_FOUND")
        failed_result = _request_snapshot_result(
            dict(current_row),
            persisted_status,
        )
        failed_result["blocker"] = "CANARY_RECONCILIATION_CAS_FAILED"
        return failed_result
    if terminal:
        if release_reservation_id:
            _release_capacity(
                service,
                release_reservation_id,
                status="SETTLED" if settlement_status is not None else "RELEASED",
                timestamp=now,
                lineage=_opening_lot_lineage(service, lot_context),
                candidate_id=lot_context.get("candidate_id"),
            )
    return result


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
            position_id = str(request.get("position_id") or "")
            with service.store._lock:
                lot_identity_row = _connection(service).execute(
                    "SELECT * FROM canary_position_lots WHERE position_id=?",
                    (position_id,),
                ).fetchone()
            lot_identity = dict(lot_identity_row) if lot_identity_row is not None else {}
            if reservation_id and lot_identity:
                _release_capacity(
                    service,
                    reservation_id,
                    status="RELEASED",
                    timestamp=now,
                    lineage=_opening_lot_lineage(service, lot_identity),
                    candidate_id=lot_identity.get("candidate_id"),
                )
            requested = _decimal(request.get("requested_quantity"), ZERO)
            with service.store._lock, _connection(service):
                lot = _connection(service).execute(
                    "SELECT pending_exit_quantity FROM canary_position_lots WHERE position_id=?",
                    (position_id,),
                ).fetchone()
                pending = max(
                    ZERO,
                    _decimal(lot["pending_exit_quantity"], ZERO) - requested,
                ) if lot is not None else ZERO
                _connection(service).execute(
                    "UPDATE canary_position_requests SET status='REJECTED',last_error=?,updated_at=? WHERE request_id=?",
                    ("CRASH_BEFORE_SUBMISSION", _iso(now), request_id),
                )
                _connection(service).execute(
                    "UPDATE canary_position_lots SET status=?,pending_exit_quantity=?,updated_at=? WHERE position_id=?",
                    (
                        "EXIT_PENDING" if pending > DUST else "OPEN",
                        str(pending),
                        _iso(now),
                        position_id,
                    ),
                )
            results.append({"request_id": request_id, "status": "REJECTED", "reason": "CRASH_BEFORE_SUBMISSION"})
        else:
            persisted_status = _mark_request_unknown(
                service,
                request_id,
                "ORDER_ID_UNAVAILABLE",
            )
            if persisted_status == "UNKNOWN":
                blocked += 1
            results.append({
                "request_id": request_id,
                "status": persisted_status,
                "reason": "ORDER_ID_UNAVAILABLE",
            })
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
            market_version = str(request.get("market_version") or "").strip().lower()
            expected_asset = str(request.get("asset_id") or "").strip()
            expected_token = str(request.get("token_id") or "").strip()
            order = _call(get_order, order_id=order_id)
            if not isinstance(order, Mapping):
                raise CanaryBlocked("CANARY_ORDER_RESPONSE_INVALID")
            trades = _call(
                list_trades,
                order_id=order_id,
                market=str(request.get("market_id") or "") or None,
                asset_id=expected_asset if market_version == "v2" else None,
                token_id=expected_token if market_version == "v1" else None,
            )
            results.append(
                _apply_reconciled_request(
                    service,
                    request,
                    dict(order),
                    _parse_trades(trades),
                    now,
                )
            )
        except CanaryBlocked as exc:
            persisted_status = _mark_request_unknown(
                service,
                request.get("request_id"),
                str(exc),
            )
            if persisted_status == "UNKNOWN":
                blocked += 1
            results.append({
                "request_id": request.get("request_id"),
                "status": persisted_status,
                "reason": str(exc),
            })
        except Exception as exc:
            persisted_status = _mark_request_unknown(
                service,
                request.get("request_id"),
                type(exc).__name__,
            )
            if persisted_status == "UNKNOWN":
                blocked += 1
            results.append({
                "request_id": request.get("request_id"),
                "status": persisted_status,
                "reason": type(exc).__name__,
            })
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


def _forced_exit_matches(lot: Mapping[str, Any], members: Sequence[Mapping[str, Any]]) -> bool:
    """Match only the immutable opening identity of a reducing member."""
    strategy = str(lot.get("strategy_version_id") or "").strip()
    trial = str(lot.get("research_trial_id") or "").strip()
    candidate = str(lot.get("candidate_id") or "").strip()
    if not strategy or not trial or not candidate:
        return False
    for member in members:
        if not isinstance(member, Mapping):
            continue
        if (
            str(member.get("strategy_version_id") or "").strip() == strategy
            and str(member.get("research_trial_id") or "").strip() == trial
            and str(member.get("candidate_id") or "").strip() == candidate
        ):
            return True
    return False

def manage_positions(
    service: CanaryService,
    venue: Any,
    *,
    allow_test_venue: bool = False,
    force_exit_members: Sequence[Mapping[str, Any]] = (),
) -> Mapping[str, Any]:
    """Admit bounded exits, including immediate exits for reducing members."""
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
    forced_members = tuple(
        member for member in force_exit_members if isinstance(member, Mapping)
    )
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
        forced = _forced_exit_matches(lot, forced_members)
        if not forced and str(lot.get("status") or "").upper() == "MANAGEMENT_BLOCKED":
            blocked.append({
                "position_id": lot.get("position_id"),
                "reason": "CANARY_POSITION_MANAGEMENT_BLOCKED",
            })
            continue
        if not forced and not _valid_exit_policy(_decode(lot.get("exit_policy_json"))):
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
        if available <= DUST or (not forced and not _policy_due(lot, now)):
            continue
        try:
            config = service.settings.snapshot(now=now) if service.settings is not None else {}
            result = submit_exit(
                service,
                str(lot["position_id"]),
                venue,
                expected_generation=int(config.get("generation", 0)),
                config_id=str(config.get("config_id") or ""),
                allow_test_venue=allow_test_venue,
                rolling_context=_opening_lot_lineage(service, lot),
                force_exit=forced,
            )
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
    strategy_version_id: str | None = None
    research_trial_id: str | None = None
    portfolio_selection_id: str | None = None
    admission_policy_id: str | None = None
    admission_policy_version: str | None = None
    risk_config_id: str | None = None
    risk_config_generation: int | None = None
    risk_config_hash: str | None = None
    lineage_type: str = _LEGACY_LINEAGE_TYPE
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
            strategy_version_id=str(row["strategy_version_id"]) if row["strategy_version_id"] else None,
            research_trial_id=str(row["research_trial_id"]) if row["research_trial_id"] else None,
            portfolio_selection_id=str(row["portfolio_selection_id"]) if row["portfolio_selection_id"] else None,
            admission_policy_id=str(row["admission_policy_id"]) if row["admission_policy_id"] else None,
            admission_policy_version=str(row["admission_policy_version"]) if row["admission_policy_version"] else None,
            risk_config_id=str(row["risk_config_id"]) if row["risk_config_id"] else None,
            risk_config_generation=int(row["risk_config_generation"]) if row["risk_config_generation"] is not None else None,
            risk_config_hash=str(row["risk_config_hash"]) if row["risk_config_hash"] else None,
            lineage_type=str(row["lineage_type"] or _LEGACY_LINEAGE_TYPE),
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

    def manage_positions(
        self,
        venue: Any,
        *,
        allow_test_venue: bool = False,
        force_exit_members: Sequence[Mapping[str, Any]] = (),
    ) -> Mapping[str, Any]:
        return manage_positions(
            self.service,
            venue,
            allow_test_venue=allow_test_venue,
            force_exit_members=force_exit_members,
        )


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
    "trade_market",
    "trade_price",
    "trade_quantity",
    "trade_timestamp",
    "CanaryPositionManager", "PositionManager", "OwnedPosition", "list_positions",
    "submit_exit", "reconcile_pending", "manage_positions",
]
