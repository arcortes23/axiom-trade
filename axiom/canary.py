"""Fail-closed micro-live Polymarket canary controls.

This module is deliberately separate from paper execution. It never enables the
platform's production-live flag. The official Polymarket venue is read-only;
only the gated canary service owns order-capable SDK construction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import getpass
import hashlib
import importlib.metadata
import json
import logging
import math
import os
import re
import sqlite3
import hmac
import queue
import threading
import time
import uuid
from urllib.request import Request, urlopen
from typing import Any, Callable, Mapping, Protocol, Sequence

from .domain import ensure_utc, parse_timestamp, utc_now
from .storage import AxiomStore, SQLiteBusyTimeout, sqlite_retry
from .data_quality import (
    CURRENT_ORDER_BOOK,
    CURRENT_ORDER_BOOK_REQUIRED,
    evaluate_prediction_data_quality,
    persisted_quality_fields,
)

_LOGGER = logging.getLogger(__name__)
from .strategy.signals import evaluate_model_document_probability

SUPPORTED_POLYMARKET_SDK = "0.9.0"
POLYMARKET_CHAIN_ID = 137
POLYMARKET_COLLATERAL_SYMBOL = "pUSD"
POLYMARKET_COLLATERAL_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYMARKET_COLLATERAL_DECIMALS = 6
_OFFICIAL_GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
PRODUCTION_LIVE_EXECUTION = False
AUTONOMOUS_MICRO_LIVE = "AUTONOMOUS_MICRO_LIVE"
AUTONOMOUS_CANARY_VENUE = "polymarket"
DEFAULT_TARGET_NOTIONAL_USD = Decimal("1.00")
DEFAULT_MAX_EXPOSURE_USD = Decimal("5.00")
DEFAULT_DAILY_LOSS_USD = Decimal("2.00")
DEFAULT_MAX_OPEN_POSITIONS = 3
DEFAULT_MAX_ORDERS_PER_DAY = 5
DEFAULT_MAX_SLIPPAGE_BPS = 100
AUTONOMOUS_CANARY_LIMITS = {
    "target_notional_usd": str(DEFAULT_TARGET_NOTIONAL_USD),
    "max_exposure_usd": str(DEFAULT_MAX_EXPOSURE_USD),
    "max_daily_loss_usd": str(DEFAULT_DAILY_LOSS_USD),
    "max_open_positions": DEFAULT_MAX_OPEN_POSITIONS,
    "max_orders_per_day": DEFAULT_MAX_ORDERS_PER_DAY,
    "max_slippage_bps": DEFAULT_MAX_SLIPPAGE_BPS,
}
_UNSET = object()
_AUTONOMOUS_SCAN_CYCLE_RETENTION = 8
CANARY_SIGNAL_TRANSIENT_RETENTION = 4096
CANARY_SUBMISSION_TIMEOUT_SECONDS = 15.0
CANARY_SIGNAL_TTL_SECONDS = 60.0
CANARY_SIGNAL_MAX_AGE_SECONDS = 60.0
CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS = 60.0
CANARY_EXECUTION_MARKET_CAP = 8
EXECUTION_FEASIBILITY_MARKET_CAP = "EXECUTION_FEASIBILITY_MARKET_CAP"



def _isolated_execution_profile() -> bool:
    """Return whether this process is not in the exact production profile.

    Profile parsing is centralized in ``node``.  A malformed ambient value is
    deliberately treated as isolated here so it can never open a production
    credential or account boundary.
    """
    try:
        from .node import (
            PRODUCTION_EXECUTION_PROFILE,
            normalized_execution_profile,
        )

        return (
            normalized_execution_profile(
                os.environ.get("AXIOM_EXECUTION_PROFILE"),
                default=PRODUCTION_EXECUTION_PROFILE,
            )
            != PRODUCTION_EXECUTION_PROFILE
        )
    except (ImportError, ValueError, TypeError):
        return True


def _deny_isolated_real_transport() -> None:
    if _isolated_execution_profile():
        raise CanaryBlocked("ISOLATED_EXECUTION_PROFILE")

_CANARY_ELIGIBLE_STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
_CANARY_LAST_GOOD_MISSING_REASONS = frozenset(
    {
        "MISSING",
        "INITIALIZING",
        "INVALID",
        "NO_PERSISTED",
        "READINESS_SNAPSHOT_MISSING",
        "READINESS_SNAPSHOT_INITIALIZING",
        "READINESS_SNAPSHOT_INVALID",
        "NO_PERSISTED_SNAPSHOT",
    }
)
_CANARY_LAST_GOOD_EVIDENCE_FIELDS = (
    "eligibility_raw_count",
    "eligible_count",
    "rankable_raw_count",
    "rankable_count",
    "ranking_run_id",
    "ranking_timestamp",
    "winner_rank",
    "winner_score",
    "rank",
    "score",
)


def _canary_has_last_good(
    payload: Mapping[str, Any] | None,
    *,
    reason: Any = None,
    projection_version: Any = None,
    readiness_status: Any = None,
    readiness_stale: Any = None,
) -> bool:
    """Return whether a persisted canary projection has usable prior evidence.

    The readiness reason fences heuristic evidence: initializing, missing,
    invalid, and non-persisted rows cannot become "last good" merely because
    they contain legacy selection metadata.  An explicitly CURRENT row or a
    positive projection version is already successful evidence.  Older rows
    predate the projection version column, so their persisted counts/rank/score
    remain valid evidence.
    """
    if not isinstance(payload, Mapping):
        return False
    raw_reason = payload.get("readiness_snapshot_reason") if reason is None else reason
    normalized_reason = str(raw_reason or "").strip().upper()
    raw_status = (
        payload.get("readiness_snapshot_status")
        if readiness_status is None
        else readiness_status
    )
    status = str(raw_status or "").strip().upper()
    stale = (
        payload.get("readiness_snapshot_stale")
        if readiness_stale is None
        else readiness_stale
    )
    current = status == "CURRENT" and stale in (False, 0, "0", "false", "FALSE")
    if current:
        return True

    raw_version = payload.get("readiness_snapshot_version")
    if projection_version is not None:
        try:
            parsed_external_version = (
                None
                if isinstance(projection_version, bool)
                else int(projection_version)
            )
        except (TypeError, ValueError):
            parsed_external_version = None
        raw_version = None
    try:
        parsed_version = int(raw_version) if raw_version is not None else 0
    except (TypeError, ValueError):
        parsed_version = 0
    if parsed_version > 0:
        return True

    if normalized_reason in _CANARY_LAST_GOOD_MISSING_REASONS:
        return False
    nested = payload.get("autonomous")
    sources = (payload, nested) if isinstance(nested, Mapping) else (payload,)
    return any(
        source.get(name) is not None
        for source in sources
        for name in _CANARY_LAST_GOOD_EVIDENCE_FIELDS
    )

_MANDATORY_SECRET_NAMES = ("private_key", "wallet_address")
_OPTIONAL_SECRET_NAMES = ("relayer_api_key", "relayer_api_key_address")
_CLOB_SECRET_NAMES = ("api_key", "api_secret", "api_passphrase")
_SECRET_NAMES = _MANDATORY_SECRET_NAMES + _OPTIONAL_SECRET_NAMES + _CLOB_SECRET_NAMES

_CREDENTIAL_FINGERPRINT_VERSION = "v1"
_CREDENTIAL_PROFILE = "production"
_CREDENTIAL_FINGERPRINT_RE = re.compile(
    r"^sha256:v1:[0-9a-f]{64}$"
)


def _credential_value(source: Any, name: str) -> str:
    if isinstance(source, Mapping):
        value = source.get(name)
    else:
        value = getattr(source, name, None) if source is not None else None
    return str(value) if value not in (None, "") else ""


def credential_fingerprint(
    values: Mapping[str, Any] | CredentialStore | None,
    *,
    profile: Any = _CREDENTIAL_PROFILE,
    execution_profile: Any = None,
) -> str:
    """Return a versioned digest of the configured credential identity.

    Only the seven configured fields and the exact execution profile enter the
    canonical document.  Missing optional values are represented as empty
    strings; the returned value is the only form allowed across persistence
    and operator projections.
    """
    selected_profile = profile if execution_profile is None else execution_profile
    if isinstance(selected_profile, Mapping):
        selected_profile = selected_profile.get("environment")
    profile_value = str(selected_profile or "").strip()
    body = {
        "version": _CREDENTIAL_FINGERPRINT_VERSION,
        "profile": profile_value,
        "credentials": {
            name: _credential_value(values, name)
            for name in _SECRET_NAMES
        },
    }
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "sha256:v1:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _valid_credential_fingerprint(value: Any) -> str | None:
    if not isinstance(value, str) or _CREDENTIAL_FINGERPRINT_RE.fullmatch(value) is None:
        return None
    return value

_ENV_NAMES = {
    "private_key": "POLYMARKET_PRIVATE_KEY",
    "wallet_address": "POLYMARKET_WALLET_ADDRESS",
    "relayer_api_key": "POLYMARKET_RELAYER_API_KEY",
    "relayer_api_key_address": "POLYMARKET_RELAYER_API_KEY_ADDRESS",
    "api_key": "POLYMARKET_CLOB_API_KEY",
    "api_secret": "POLYMARKET_CLOB_API_SECRET",
    "api_passphrase": "POLYMARKET_CLOB_API_PASSPHRASE",
}
_SAFE_PROBE_LOCK = threading.RLock()
_SAFE_PROBE_TTL_SECONDS = 5.0
_SAFE_PROBE_CACHE: dict[tuple[type[Any], bool], tuple[float, bool]] = {}
_SAFE_PROBE_GENERATIONS: dict[tuple[type[Any], bool], int] = {}
_SAFE_PROBE_IN_FLIGHT: tuple[tuple[type[Any], bool], threading.Event, int] | None = None


def _invalidate_safe_projection_cache(credential_type: type[Any]) -> None:
    """Forget metadata cached before a credential configuration change."""
    with _SAFE_PROBE_LOCK:
        for allow_environment in (False, True):
            key = (credential_type, allow_environment)
            _SAFE_PROBE_GENERATIONS[key] = _SAFE_PROBE_GENERATIONS.get(key, 0) + 1
            _SAFE_PROBE_CACHE.pop(key, None)


 

def _sdk_value(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)
_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
_CANARY_REJECTION_STATUSES = frozenset(
    {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED", "EXPIRED"}
)
_CANARY_ACCEPTED_STATUSES = frozenset(
    {
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
    }
)
_CANARY_UNKNOWN_RESPONSE_CODES = frozenset(
    {"", "UNKNOWN", "UNSPECIFIED", "NONE", "NULL"}
)


def _canonical_exchange_order_id(value: Any) -> str | None:
    """Return one bounded, venue-canonical order ID or ``None``."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text if _ORDER_ID_PATTERN.fullmatch(text) is not None else None


def _is_explicit_rejection_code(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.strip().upper() not in _CANARY_UNKNOWN_RESPONSE_CODES
    )




def _collateral_metadata() -> dict[str, Any]:
    """Return the protocol collateral denomination used by safe diagnostics."""
    return {
        "symbol": POLYMARKET_COLLATERAL_SYMBOL,
        "address": POLYMARKET_COLLATERAL_ADDRESS,
        "decimals": POLYMARKET_COLLATERAL_DECIMALS,
        "chain_id": POLYMARKET_CHAIN_ID,
    }

def _canary_closed_flag(value: Any) -> bool:
    """Normalize persisted closed flags like the storage market projection."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "closed"}

def _sdk_api_key_credentials(sdk: Any, values: Mapping[str, Any]) -> Any:
    """Build pre-supplied SDK credentials without invoking auth bootstrap.

    ``SecureClient._create`` bootstraps credentials when ``credentials`` is
    omitted.  The canary must never do that, so an installed SDK exposing
    ``ApiKeyCreds`` requires all three locally configured fields.
    """
    creds_type = getattr(sdk, "ApiKeyCreds", None)
    if not callable(creds_type):
        raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE")
    aliases = {
        "key": ("api_key", "clob_api_key"),
        "secret": ("api_secret", "clob_api_secret"),
        "passphrase": ("api_passphrase", "clob_api_passphrase"),
    }
    fields: dict[str, Any] = {}
    for field, names in aliases.items():
        value = next((values.get(name) for name in names if values.get(name)), None)
        if not value:
            raise CanaryBlocked("CLOB_API_CREDENTIALS_NOT_CONFIGURED")
        fields[field] = value
    try:
        return creds_type(**fields)
    except Exception as exc:
        raise CanaryBlocked("CLOB_API_CREDENTIALS_INVALID") from exc

def _derive_api_key_credentials(values: Mapping[str, Any]) -> Any:
    """Derive existing CLOB credentials with the audited GET-only endpoint.

    This intentionally imports the SDK's low-level ``derive_api_key_sync``
    operation rather than ``SecureClient.create`` or
    ``create_or_derive_api_key``: both of those bootstrap paths can POST
    ``/auth/api-key``.  The derived credentials remain in memory for this
    diagnostic/client lifetime and are never persisted or returned.
    """
    private_key = values.get("private_key")
    if not private_key:
        raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
    try:
        from eth_account import Account
        from polymarket._internal.actions.auth import derive_api_key_sync
        from polymarket._internal.environment import get_environment_config
        from polymarket._internal.l1_auth import sign_api_key_auth
        from polymarket.clients._transport import SyncTransport
        from polymarket.environments import PRODUCTION


        signer = Account.from_key(private_key)
        config = get_environment_config(PRODUCTION)
        signature = sign_api_key_auth(
            signer,
            chain_id=POLYMARKET_CHAIN_ID,
            timestamp=int(time.time()),
            nonce=0,
        )
        transport = SyncTransport(base_url=config.clob_url)
        try:
            return derive_api_key_sync(transport, signature)
        finally:
            transport.close()
    except CanaryBlocked:
        raise
    except Exception as exc:
        raise CanaryBlocked("CLOB_API_CREDENTIALS_DERIVATION_FAILED") from exc
def _build_safe_sdk_client(
    values: Mapping[str, Any],
    *,
    expected_fingerprint: str | None = None,
) -> Any:
    """Construct the audited SDK client used by every official canary path.

    The constructor is deliberately shared by read-only diagnostics and the
    fenced submission path.  It only uses the private ``_create`` seam with
    explicit in-memory API credentials and validation disabled; SDK bootstrap
    helpers can POST credentials and are never permitted here.
    """
    if (
        not isinstance(values, Mapping)
        or not all(_credential_value(values, name) for name in _MANDATORY_SECRET_NAMES)
    ):
        raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
    if expected_fingerprint is not None:
        expected = _valid_credential_fingerprint(expected_fingerprint)
        actual = credential_fingerprint(values)
        if expected is None or not hmac.compare_digest(actual, expected):
            raise CanaryBlocked("CREDENTIAL_BINDING_MISMATCH")
    try:
        import polymarket
        from polymarket import SecureClient
    except ImportError as exc:
        raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED") from exc
    if str(PolymarketClobV2Venue.installed_sdk_version() or "") != SUPPORTED_POLYMARKET_SDK:
        raise CanaryBlocked("UNSUPPORTED_POLYMARKET_SDK")
    safe_create = getattr(SecureClient, "_create", None)
    if not callable(safe_create):
        raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE")
    try:
        try:
            credentials = _sdk_api_key_credentials(polymarket, values)
        except CanaryBlocked as exc:
            if str(exc) != "CLOB_API_CREDENTIALS_NOT_CONFIGURED":
                raise
            credentials = _derive_api_key_credentials(values)
        return safe_create(
            private_key=values["private_key"],
            wallet=values["wallet_address"],
            credentials=credentials,
            validate_credentials=False,
        )
    except CanaryBlocked:
        raise
    except TypeError as exc:
        raise CanaryBlocked(
            "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE"
        ) from exc
    except Exception as exc:
        raise CanaryBlocked("AUTHENTICATED_CONNECTIVITY_FAILED") from exc



def _base_units(value: Any, *, field: str) -> int:
    """Normalize an SDK base-unit value without accepting lossy numbers."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        try:
            result = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field} must be an integer") from None
    else:
        raise ValueError(f"{field} must be an integer")
    if result < 0:
        raise ValueError(f"{field} must not be negative")
    return result


def _normalize_balance_allowance(value: Any) -> tuple[int, dict[str, int]]:
    """Normalize ``BalanceAllowance`` models and mapping test doubles."""
    raw_balance = _sdk_value(value, "balance", None)
    raw_allowances = _sdk_value(value, "allowances", None)
    if raw_balance is None or not isinstance(raw_allowances, Mapping):
        raise ValueError("balance/allowances are unavailable")
    balance = _base_units(raw_balance, field="balance")
    allowances: dict[str, int] = {}
    for spender, allowance in raw_allowances.items():
        if not isinstance(spender, str) or not spender.strip():
            raise ValueError("allowance spender must be a non-empty string")
        allowances[spender] = _base_units(
            allowance,
            field=f"allowances[{spender}]",
        )
    return balance, allowances


def _order_balance_allowance_target(
    *, side: Any, asset_id: str, market_version: Any = None
) -> tuple[str, str | None]:
    """Return the SDK balance target for the signed order's side/version."""
    normalized_side = str(_sdk_value(side, "value", side) or "").upper()
    if normalized_side == "BUY":
        return "COLLATERAL", None
    if normalized_side != "SELL":
        raise CanaryBlocked("CANARY_ALLOWANCE_UNAVAILABLE")
    version = str(_sdk_value(market_version, "value", market_version) or "").lower()
    if version == "v2":
        return "CONDITIONAL-V2", str(asset_id)
    if version == "v1":
        return "CONDITIONAL", str(asset_id)
    raise CanaryBlocked("CANARY_ALLOWANCE_UNAVAILABLE")


def _resolve_official_spender(
    client: Any, *, asset_id: str, market_version: Any, neg_risk: Any
) -> str:
    """Resolve the exact exchange spender from SDK environment metadata."""
    context = getattr(client, "_ctx", None)
    config = _sdk_value(context, "environment_config")
    if config is None:
        raise CanaryBlocked("CANARY_SPENDER_UNAVAILABLE")
    version = str(_sdk_value(market_version, "value", market_version) or "").lower()
    if version == "v2":
        spender = _sdk_value(config, "exchange_v3")
    elif version == "v1" and isinstance(neg_risk, bool):
        spender = _sdk_value(
            config,
            "neg_risk_exchange" if neg_risk else "standard_exchange",
        )
    else:
        raise CanaryBlocked("CANARY_SPENDER_UNAVAILABLE")
    if not isinstance(spender, str) or not spender.strip():
        raise CanaryBlocked("CANARY_SPENDER_UNAVAILABLE")
    return spender


def _allowance_for_spender(
    allowances: Mapping[str, int], spender: str
) -> int:
    matches = [
        amount
        for key, amount in allowances.items()
        if isinstance(key, str) and key.lower() == spender.lower()
    ]
    if len(matches) > 1:
        raise CanaryBlocked("CANARY_ALLOWANCE_UNAVAILABLE")
    if matches:
        return matches[0]
    # The SDK's own allowance helper treats an absent spender as zero.  Keep
    # that distinction from an unavailable/malformed allowance payload so the
    # canary emits the explicit insufficient-allowance rejection.
    return 0

def _call_with_timeout(operation: Any, timeout_seconds: float) -> Any:
    """Bound a synchronous external call without holding application locks.

    The worker is daemonized because Python cannot safely interrupt a blocking
    socket from the caller thread. A timeout therefore records UNKNOWN while
    the SDK call may still finish; callers must treat that request as already
    transmitted and never retry it automatically.
    """
    result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def invoke() -> None:
        try:
            result_queue.put(("ok", operation()))
        except BaseException as exc:
            result_queue.put(("error", exc))

    worker = threading.Thread(target=invoke, name="axiom-canary-submit", daemon=True)
    worker.start()
    worker.join(max(0.0, float(timeout_seconds)))
    if worker.is_alive():
        raise TimeoutError("canary external submission timed out")
    kind, value = result_queue.get_nowait()
    if kind == "error":
        raise value
    return value


def _best_ask_price(asks: Any) -> Decimal:
    """Return the lowest finite, positive ask price from an order book."""
    if not isinstance(asks, (list, tuple)) or not asks:
        raise ValueError("asks must contain executable levels")
    prices: list[Decimal] = []
    for level in asks:
        if not isinstance(level, Mapping):
            raise ValueError("ask level must be a mapping")
        raw_price = level.get("price")
        if isinstance(raw_price, bool):
            raise ValueError("ask price must not be bool")
        try:
            price = Decimal(str(raw_price))
        except (TypeError, ValueError, ArithmeticError):
            raise ValueError("ask price must be decimal") from None
        if not price.is_finite() or price <= 0:
            raise ValueError("ask price must be finite and positive")
        prices.append(price)
    return min(prices)


def _select_market_asset(
    market: Any,
    requested_id: str,
) -> tuple[str, str, str | None, str | None]:
    version_value = _sdk_value(
        _sdk_value(market, "version"),
        "value",
        _sdk_value(market, "version", ""),
    )
    version = str(version_value or "").lower()
    outcomes = _sdk_value(market, "outcomes")
    requested = str(requested_id).strip()
    selected_name = requested.lower()
    selected = None
    for name in ("yes", "no"):
        candidate = _sdk_value(outcomes, name) if outcomes is not None else None
        values = {
            name,
            str(_sdk_value(candidate, "label", "") or "").lower(),
            str(_sdk_value(candidate, "token_id", "") or ""),
            str(_sdk_value(candidate, "position_id", "") or ""),
        }
        if requested.lower() in values or requested in values:
            selected_name, selected = name, candidate
            break
    if outcomes is None:
        raise CanaryBlocked("MARKET_OUTCOMES_UNAVAILABLE")
    if selected is None:
        raise CanaryBlocked("MARKET_OUTCOME_NOT_ALLOWED")
    token_id = _sdk_value(selected, "token_id")
    position_id = _sdk_value(selected, "position_id")
    asset_id = position_id if version == "v2" else token_id
    if not asset_id:
        raise CanaryBlocked("MARKET_OUTCOME_ID_UNAVAILABLE")
    return str(asset_id), selected_name, (
        str(token_id) if token_id is not None else None
    ), (str(position_id) if position_id is not None else None)


def _read_only_operation(
    operation: str,
    values: Mapping[str, Any],
    *,
    market_id: str | None = None,
    token_id: str | None = None,
    order_id: str | None = None,
    asset_id: str | None = None,
    market: str | None = None,
    maker_address: str | None = None,
    after: str | None = None,
    before: str | None = None,
) -> Any:
    """Construct one local read client and return normalized diagnostics only."""
    _deny_isolated_real_transport()
    if operation not in {
        "account",
        "balance",
        "allowance",
        "market_context",
        "get_order",
        "list_account_trades",
    }:
        raise ValueError(f"unsupported read operation: {operation}")
    client = _build_safe_sdk_client(
        values,
        expected_fingerprint=credential_fingerprint(values),
    )

    def close_client() -> None:
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass

    if operation == "account":
        try:
            return {
                "authenticated": bool(getattr(client, "wallet", None)),
                "wallet_type": str(getattr(client, "wallet_type", "") or ""),
                "credential_fingerprint": credential_fingerprint(values),
            }
        finally:
            close_client()
    if operation == "balance":
        try:
            value = client.get_balance_allowance(asset_type="COLLATERAL")
            raw_balance = _sdk_value(value, "balance", 0)
            if isinstance(raw_balance, bool):
                raise ValueError("balance must not be bool")
            return Decimal(str(raw_balance)) / (Decimal(10) ** POLYMARKET_COLLATERAL_DECIMALS)
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("BALANCE_RESPONSE_INVALID") from exc
        finally:
            close_client()
    if operation == "allowance":
        try:
            value = client.get_balance_allowance(asset_type="COLLATERAL")
            balance, allowances = _normalize_balance_allowance(value)
            return {
                "status": "OK",
                "asset_type": "COLLATERAL",
                "collateral": _collateral_metadata(),
                "balance_base_units": str(balance),
                "spenders": sorted(allowances),
            }
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("BALANCE_RESPONSE_INVALID") from exc
        finally:
            close_client()

    def normalize_record(value: Any, fields: tuple[str, ...]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name in fields:
            item = _sdk_value(value, name)
            if item is None:
                continue
            if isinstance(item, (str, int, float, bool)):
                result[name] = item
            elif isinstance(item, Decimal):
                result[name] = str(item)
            elif isinstance(item, datetime):
                result[name] = ensure_utc(item).isoformat()
        return result

    if operation == "get_order":
        if not order_id:
            raise ValueError("get_order requires order_id")
        try:
            value = client.get_order(order_id=str(order_id))
            result = normalize_record(
                value,
                (
                    "id",
                    "order_id",
                    "status",
                    "asset_id",
                    "market",
                    "side",
                    "price",
                    "original_size",
                    "size_matched",
                    "created_at",
                    "expiration",
                    "order_type",
                    "owner",
                    "owner_address",
                    "maker",
                    "maker_address",
                    "account",
                    "account_address",
                ),
            )
            durable_id = result.get("order_id") or result.get("id") or str(order_id)
            result["order_id"] = str(durable_id)
            return result
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("ORDER_READ_FAILED") from exc
        finally:
            close_client()

    if operation == "list_account_trades":
        try:
            # The SDK/API ``id`` query addresses a trade ID, not an order ID.
            # Bound the server read by asset/market where available, then apply
            # the order relationship locally from taker/maker order IDs.
            paginator = client.list_account_trades(
                asset_id=asset_id,
                token_id=token_id,
                id=None,
                market=market,
                maker_address=maker_address,
                after=after,
                before=before,
            )
            records: list[dict[str, Any]] = []
            iterator = (
                iter(paginator)
                if not isinstance(paginator, Mapping)
                else iter((paginator,))
            )
            for sequence, value in enumerate(iterator):
                record = normalize_record(
                    value,
                    (
                        "id",
                        "trade_id",
                        "order_id",
                        "taker_order_id",
                        "condition_id",
                        "asset_id",
                        "market",
                        "side",
                        "price",
                        "size",
                        "timestamp",
                        "fee",
                        "fee_rate_bps",
                        "match_time",
                        "matched_at",
                        "updated_at",
                        "trader_side",
                        "transaction_hash",
                        "status",
                        "state",
                        "owner",
                        "owner_address",
                        "maker",
                        "maker_address",
                        "account",
                        "account_address",
                    ),
                )
                maker_orders = _sdk_value(value, "maker_orders", ())
                maker_order_ids: list[str] = []
                if isinstance(maker_orders, Sequence) and not isinstance(
                    maker_orders, (str, bytes, bytearray)
                ):
                    maker_order_ids = [
                        str(identifier)
                        for maker_order in maker_orders
                        if (
                            identifier := _sdk_value(
                                maker_order, "order_id", None
                            )
                        ) is not None
                    ]
                    if maker_order_ids:
                        record["maker_order_ids"] = maker_order_ids
                if order_id is not None:
                    requested_order_id = str(order_id)
                    direct_order_id = (
                        record.get("taker_order_id") or record.get("order_id")
                    )
                    if (
                        str(direct_order_id or "") != requested_order_id
                        and requested_order_id not in maker_order_ids
                    ):
                        if sequence >= 255:
                            break
                        continue
                durable_id = record.get("trade_id") or record.get("id")
                if durable_id is not None:
                    record["trade_id"] = str(durable_id)
                record.setdefault("sequence", sequence)
                records.append(record)
                if sequence >= 255:
                    break
            return records
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("ACCOUNT_TRADES_READ_FAILED") from exc
        finally:
            close_client()

    if not market_id or not token_id:
        close_client()
        raise ValueError("market_context requires market_id and token_id")
    try:
        market = client.get_market(id=market_id)
        asset_id, outcome, selected_token_id, selected_position_id = (
            _select_market_asset(market, token_id)
        )
        book = client.get_order_book(asset_id=asset_id)
        state = _sdk_value(market, "state")
        trading = _sdk_value(market, "trading")
        fee_schedule = _sdk_value(trading, "fee_schedule")
        context_obj = getattr(client, "_ctx", None)
        metadata_cache = _sdk_value(context_obj, "order_metadata")
        canonical = None
        fetch_current = getattr(metadata_cache, "fetch_current_market", None)
        if callable(fetch_current):
            try:
                canonical = fetch_current(context_obj, token_id=asset_id)
            except Exception as exc:
                raise CanaryBlocked("MARKET_CONTEXT_FAILED") from exc
        canonical_neg_risk = _sdk_value(canonical, "neg_risk", None)
        if not isinstance(canonical_neg_risk, bool):
            canonical_neg_risk = _sdk_value(book, "neg_risk", None)
        if not isinstance(canonical_neg_risk, bool):
            canonical_neg_risk = _sdk_value(
                state, "neg_risk", _sdk_value(market, "neg_risk")
            )
        canonical_tick = _sdk_value(canonical, "tick_size", None)
        if canonical_tick is None:
            canonical_tick = _sdk_value(book, "tick_size", "0")
        fee_info = _sdk_value(canonical, "fee_info")
        fee_rate = _sdk_value(fee_info, "rate", None)
        fee_exponent = _sdk_value(fee_info, "exponent", None)
        if fee_rate is None:
            fee_rate = _sdk_value(fee_schedule, "rate", None)
        if fee_rate is None:
            fee_rate = Decimal(str(_sdk_value(market, "fee_bps", 0) or 0)) / Decimal("10000")
        if fee_exponent is None:
            fee_exponent = _sdk_value(fee_schedule, "exponent", 1)
        market_version = "v2" if selected_position_id else "v1"
        fee_rate = Decimal(str(fee_rate))
        fee_exponent = Decimal(str(fee_exponent))
        if (
            not fee_rate.is_finite()
            or fee_rate < 0
            or not fee_exponent.is_finite()
            or fee_exponent < 0
        ):
            raise CanaryBlocked("MARKET_CONTEXT_FAILED")
        allowance = {
            "status": "OK",
            "asset_type": "COLLATERAL",
            "collateral": _collateral_metadata(),
            "spender": _resolve_official_spender(
                client,
                asset_id=asset_id,
                market_version=market_version,
                neg_risk=canonical_neg_risk,
            ),
        }
        allowance_value = client.get_balance_allowance(asset_type="COLLATERAL")
        _, allowance_map = _normalize_balance_allowance(allowance_value)
        allowance["available_base_units"] = str(
            _allowance_for_spender(allowance_map, allowance["spender"])
        )
        allowance["available_usd"] = str(
            Decimal(allowance["available_base_units"])
            / (Decimal(10) ** POLYMARKET_COLLATERAL_DECIMALS)
        )
        return {
            "market_version": market_version,
            "neg_risk": canonical_neg_risk,
            "outcome": outcome,
            "token_id": selected_token_id,
            "position_id": selected_position_id,
            "asset_id": asset_id,
            "accepting_orders": bool(
                _sdk_value(
                    state,
                    "accepting_orders",
                    _sdk_value(market, "accepting_orders", False),
                )
            ),
            "min_order_size": str(_sdk_value(book, "min_order_size", "0")),
            "tick_size": str(canonical_tick),
            "size_increment": str(
                _sdk_value(
                    book,
                    "size_increment",
                    _sdk_value(book, "step_size", "0.01"),
                )
            ),
            "bids": [
                {
                    "price": str(_sdk_value(level, "price")),
                    "size": str(_sdk_value(level, "size")),
                }
                for level in (_sdk_value(book, "bids", ()) or ())
            ],
            "asks": [
                {
                    "price": str(_sdk_value(level, "price")),
                    "size": str(_sdk_value(level, "size")),
                }
                for level in (_sdk_value(book, "asks", ()) or ())
            ],
            "min_notional": str(
                _sdk_value(
                    book,
                    "min_notional",
                    _sdk_value(market, "min_notional", "0"),
                )
            ),
            "fee_rate": str(fee_rate),
            "fee_exponent": str(fee_exponent),
            "fee_bps": str(fee_rate * Decimal("10000")),
            "allowance": allowance,
        }
    except CanaryBlocked:
        raise
    except Exception as exc:
        raise CanaryBlocked("MARKET_CONTEXT_FAILED") from exc
    finally:
        close_client()
class CanaryBlocked(RuntimeError):
    """A safe, credential-free canary rejection."""

class CanaryVenue(Protocol):
    def geoblock(self) -> Mapping[str, Any]: ...
    def connectivity_check(self) -> bool: ...
    def account(self) -> Mapping[str, Any]: ...
    def market_context(self, market_id: str, token_id: str) -> Mapping[str, Any]: ...
    def balance(self) -> Decimal: ...
    def submit_limit_order(
        self, *, token_id: str, side: str, price: Decimal, size: Decimal
    ) -> Mapping[str, Any]: ...
    def get_order(self, order_id: str) -> Mapping[str, Any]: ...
    def list_account_trades(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]: ...

@dataclass(frozen=True, slots=True)
class CanaryLimits:
    target_notional_usd: Decimal = DEFAULT_TARGET_NOTIONAL_USD
    max_exposure_usd: Decimal = DEFAULT_MAX_EXPOSURE_USD
    max_daily_loss_usd: Decimal = DEFAULT_DAILY_LOSS_USD
    max_open_positions: int = DEFAULT_MAX_OPEN_POSITIONS
    max_orders_per_day: int = DEFAULT_MAX_ORDERS_PER_DAY
    max_slippage_bps: int = DEFAULT_MAX_SLIPPAGE_BPS

    def __post_init__(self) -> None:
        for name in ("target_notional_usd", "max_exposure_usd", "max_daily_loss_usd"):
            raw_value = getattr(self, name)
            try:
                value = Decimal(raw_value)
            except (TypeError, ValueError, ArithmeticError):
                raise ValueError(f"{name} must be positive") from None
            if isinstance(raw_value, bool) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be positive")
        for name in ("max_open_positions", "max_orders_per_day"):
            raw_value = getattr(self, name)
            if isinstance(raw_value, bool):
                raise ValueError(f"{name} must be a positive integer")
            try:
                value = int(raw_value)
            except (TypeError, ValueError, ArithmeticError):
                raise ValueError(f"{name} must be a positive integer") from None
            if value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        raw_value = self.max_slippage_bps
        if isinstance(raw_value, bool):
            raise ValueError("max_slippage_bps must be a non-negative integer")
        try:
            value = int(raw_value)
        except (TypeError, ValueError, ArithmeticError):
            raise ValueError("max_slippage_bps must be a non-negative integer") from None
        if value < 0:
            raise ValueError("max_slippage_bps must be a non-negative integer")

class CredentialStore:
    """OS-keyring first; environment variables are an explicit fallback only."""
    service = "AXIOM-POLYMARKET-CANARY"

    def _assert_transport_allowed(self) -> None:
        _deny_isolated_real_transport()

    def configure(self, *, reader=getpass.getpass) -> None:
        self._assert_transport_allowed()
        try:
            import keyring
        except ImportError as exc:
            raise CanaryBlocked("OS_KEYRING_UNAVAILABLE") from exc

        mandatory = {
            "private_key": reader("Dedicated canary signer private key: "),
            "wallet_address": reader("Dedicated Polymarket wallet address: "),
        }
        if not all(mandatory.values()):
            raise CanaryBlocked("CREDENTIAL_CONFIGURATION_INCOMPLETE")
        for name, value in mandatory.items():
            keyring.set_password(self.service, name, value)

        # Relayer credentials are optional for signer-backed CLOB workflows.
        # Blank/omitted values intentionally leave any existing keyring values
        # untouched so configuring mandatory credentials is non-destructive.
        for name, prompt in (
            ("relayer_api_key", "Polymarket relayer API key (optional): "),
            (
                "relayer_api_key_address",
                "Polymarket relayer API key address (optional): ",
            ),
        ):
            try:
                value = reader(prompt)
            except (EOFError, StopIteration):
                value = ""
            if value:
                keyring.set_password(self.service, name, value)
        _invalidate_safe_projection_cache(type(self))

    def load(self, *, allow_environment: bool = False) -> dict[str, str]:
        self._assert_transport_allowed()
        values: dict[str, str] = {}
        try:
            import keyring
            for name in _SECRET_NAMES:
                value = keyring.get_password(self.service, name)
                if value:
                    values[name] = value
        except Exception:
            pass
        if allow_environment:
            for name, variable in _ENV_NAMES.items():
                if name not in values and os.environ.get(variable):
                    values[name] = os.environ[variable]
        return (
            values
            if all(name in values for name in _MANDATORY_SECRET_NAMES)
            else {}
        )

    def configured(self, *, allow_environment: bool = False) -> bool:
        self._assert_transport_allowed()
        return bool(self.load(allow_environment=allow_environment))

    @classmethod
    def cached_projection(
        cls,
        *,
        allow_environment: bool = False,
        persisted: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return credential metadata without touching any credential provider.

        A dashboard read may only use metadata produced by an earlier explicit
        credential check or by a persisted public canary projection.  A cache
        miss is deliberately represented as ``NOT CHECKED`` rather than
        triggering a keyring lookup.
        """
        key = (cls, bool(allow_environment))
        cached_configured: bool | None = None
        with _SAFE_PROBE_LOCK:
            cached = _SAFE_PROBE_CACHE.get(key)
            if cached is not None:
                try:
                    cache_age = time.monotonic() - float(cached[0])
                except (TypeError, ValueError):
                    cache_age = float("inf")
                if 0 <= cache_age < _SAFE_PROBE_TTL_SECONDS:
                    cached_configured = bool(cached[1])
        if cached_configured is not None:
            return {
                "configured": cached_configured,
                "status": (
                    "CONFIGURED" if cached_configured else "NOT CONFIGURED"
                ),
                "secret_values_exposed": False,
            }

        if isinstance(persisted, Mapping):
            sources: list[Mapping[str, Any]] = [persisted]
            for source in tuple(sources):
                for name in (
                    "credentials",
                    "credential",
                    "readiness",
                    "status_report",
                    "canary",
                    "connectivity",
                ):
                    nested = source.get(name)
                    if isinstance(nested, Mapping):
                        sources.append(nested)
            for source in sources:
                candidates: list[Mapping[str, Any]] = []
                nested_credentials = source.get("credentials")
                if isinstance(nested_credentials, Mapping):
                    candidates.append(nested_credentials)
                nested_credential = source.get("credential")
                if isinstance(nested_credential, Mapping):
                    candidates.append(nested_credential)
                if any(
                    name in source
                    for name in (
                        "credentials_configured",
                        "credential_configured",
                        "credentials_status",
                        "credential_status",
                    )
                ):
                    candidates.append(source)
                for candidate in candidates:
                    configured = candidate.get("configured")
                    if isinstance(configured, bool):
                        return {
                            "configured": configured,
                            "status": (
                                "CONFIGURED"
                                if configured
                                else "NOT CONFIGURED"
                            ),
                            "secret_values_exposed": False,
                        }
                    configured = candidate.get("credentials_configured")
                    if not isinstance(configured, bool):
                        configured = candidate.get("credential_configured")
                    if isinstance(configured, bool):
                        return {
                            "configured": configured,
                            "status": (
                                "CONFIGURED"
                                if configured
                                else "NOT CONFIGURED"
                            ),
                            "secret_values_exposed": False,
                        }
                    raw_status = candidate.get("status")
                    if not isinstance(raw_status, str):
                        raw_status = candidate.get("credentials_status")
                    if not isinstance(raw_status, str):
                        raw_status = candidate.get("credential_status")
                    status = raw_status.strip().upper() if isinstance(raw_status, str) else ""
                    if status == "CONFIGURED":
                        return {
                            "configured": True,
                            "status": "CONFIGURED",
                            "secret_values_exposed": False,
                        }
                    if status == "NOT CONFIGURED":
                        return {
                            "configured": False,
                            "status": "NOT CONFIGURED",
                            "secret_values_exposed": False,
                        }
                    if status in {"UNKNOWN", "NOT CHECKED"}:
                        return {
                            "configured": None,
                            "status": status,
                            "secret_values_exposed": False,
                        }
        return {
            "configured": None,
            "status": "NOT CHECKED",
            "secret_values_exposed": False,
        }


    def safe_projection(
        self,
        *,
        allow_environment: bool = False,
        timeout_seconds: float = 0.25,
    ) -> dict[str, Any]:
        """Return credential metadata without exposing values or blocking reads."""
        if _isolated_execution_profile():
            return {
                "configured": False,
                "status": "ISOLATED_EXECUTION_PROFILE",
                "secret_values_exposed": False,
            }
        global _SAFE_PROBE_IN_FLIGHT
        try:
            timeout = max(0.01, min(float(timeout_seconds), 2.0))
        except (TypeError, ValueError):
            timeout = 0.25

        key = (type(self), bool(allow_environment))
        start_probe = False
        event: threading.Event | None = None
        generation = 0
        with _SAFE_PROBE_LOCK:
            now = time.monotonic()
            cached = _SAFE_PROBE_CACHE.get(key)
            if cached is not None and now - cached[0] < _SAFE_PROBE_TTL_SECONDS:
                configured = bool(cached[1])
            else:
                in_flight = _SAFE_PROBE_IN_FLIGHT
                if in_flight is None:
                    event = threading.Event()
                    generation = _SAFE_PROBE_GENERATIONS.get(key, 0)
                    _SAFE_PROBE_IN_FLIGHT = (key, event, generation)
                    start_probe = True
                else:
                    # A probe for another key still occupies the one global
                    # keyring worker; wait for it, then use this key's safe
                    # cached value (or false before its first completion).
                    event = in_flight[1]
                configured = False

        if event is not None:
            if start_probe:
                def invoke() -> None:
                    global _SAFE_PROBE_IN_FLIGHT
                    try:
                        result = bool(
                            self.configured(allow_environment=allow_environment)
                        )
                    except BaseException:
                        result = False
                    finished = time.monotonic()
                    with _SAFE_PROBE_LOCK:
                        if _SAFE_PROBE_GENERATIONS.get(key, 0) == generation:
                            _SAFE_PROBE_CACHE[key] = (finished, result)
                        current = _SAFE_PROBE_IN_FLIGHT
                        if current is not None and current[1] is event:
                            _SAFE_PROBE_IN_FLIGHT = None
                        event.set()

                worker = threading.Thread(
                    target=invoke,
                    name="axiom-canary-credential-probe",
                    daemon=True,
                )
                try:
                    worker.start()
                except BaseException:
                    with _SAFE_PROBE_LOCK:
                        current = _SAFE_PROBE_IN_FLIGHT
                        if current is not None and current[1] is event:
                            _SAFE_PROBE_IN_FLIGHT = None
                        event.set()
            event.wait(timeout)
            with _SAFE_PROBE_LOCK:
                cached = _SAFE_PROBE_CACHE.get(key)
                configured = bool(cached[1]) if cached is not None else False

        return {
            "configured": configured,
            "status": "CONFIGURED" if configured else "NOT CONFIGURED",
            "secret_values_exposed": False,
        }



class PolymarketClobV2Venue:
    """Official ``polymarket-client`` 0.9 read-only venue integration.

    Each read obtains credentials from a fresh ``CredentialStore`` in local
    scope. No credential provider, secret mapping, or SDK client is retained
    on this venue instance.
    """

    __slots__ = ("_allow_environment",)

    def __init__(
        self,
        *,
        allow_environment: bool = False,
    ) -> None:
        self._allow_environment = bool(allow_environment)

    @staticmethod
    def installed_sdk_version() -> str | None:
        try:
            import polymarket
        except ImportError:
            try:
                return importlib.metadata.version("polymarket-client")
            except importlib.metadata.PackageNotFoundError:
                return None
        version = getattr(polymarket, "__version__", None)
        if version:
            return str(version)
        try:
            return importlib.metadata.version("polymarket-client")
        except importlib.metadata.PackageNotFoundError:
            return None

    def _read_operation(
        self,
        operation: str,
        *,
        market_id: str | None = None,
        token_id: str | None = None,
        order_id: str | None = None,
        asset_id: str | None = None,
        market: str | None = None,
        maker_address: str | None = None,
        after: str | None = None,
        before: str | None = None,
    ) -> Any:
        """Run one allowlisted authenticated read with a local SDK client."""
        if operation not in {
            "account",
            "balance",
            "allowance",
            "market_context",
            "get_order",
            "list_account_trades",
        }:
            raise ValueError(f"unsupported read operation: {operation}")
        try:
            values = CredentialStore().load(
                allow_environment=self._allow_environment
            )
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED") from exc
        if (
            not isinstance(values, Mapping)
            or not all(values.get(name) for name in _MANDATORY_SECRET_NAMES)
        ):
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
        return _read_only_operation(
            operation,
            values,
            market_id=market_id,
            token_id=token_id,
            order_id=order_id,
            asset_id=asset_id,
            market=market,
            maker_address=maker_address,
            after=after,
            before=before,
        )

    def geoblock(self) -> Mapping[str, Any]:
        _deny_isolated_real_transport()
        request = Request(
            _OFFICIAL_GEOBLOCK_URL,
            headers={"Accept": "application/json", "User-Agent": "AXIOM-canary/1"},
        )
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise CanaryBlocked("GEOBLOCK_RESPONSE_INVALID")
        return {
            "blocked": bool(payload.get("blocked", True)),
            "close_only": bool(payload.get("close_only", False)),
            "country": payload.get("country"),
            "region": payload.get("region"),
        }

    def account(self) -> Mapping[str, Any]:
        """Return only non-sensitive account diagnostics."""
        return self._read_operation("account")

    def connectivity_check(self) -> bool:
        return bool(self.account().get("authenticated"))
    def get_order(self, order_id: str) -> Mapping[str, Any]:
        """Read one authenticated order by its durable exchange ID."""
        return self._read_operation("get_order", order_id=str(order_id))

    def list_account_trades(
        self,
        *,
        asset_id: str | None = None,
        token_id: str | None = None,
        order_id: str | None = None,
        market: str | None = None,
        maker_address: str | None = None,
        after: str | None = None,
        before: str | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Read authenticated account trades with durable exchange IDs."""
        return self._read_operation(
            "list_account_trades",
            asset_id=asset_id,
            token_id=token_id,
            order_id=order_id,
            market=market,
            maker_address=maker_address,
            after=after,
            before=before,
        )


    def market_context(self, market_id: str, token_id: str) -> Mapping[str, Any]:
        return self._read_operation(
            "market_context",
            market_id=market_id,
            token_id=token_id,
        )

    def balance(self) -> Decimal:
        return self._read_operation("balance")

    def allowance(self) -> Mapping[str, Any]:
        """Return collateral allowance diagnostics without selecting a market."""
        return self._read_operation("allowance")


def _canary_merged_lifecycle_payload(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, Mapping):
        return None
    raw_payload = record.get("payload")
    if not isinstance(raw_payload, Mapping):
        return None
    payload = dict(raw_payload)
    forward_evidence = payload.get("forward_evidence")
    if forward_evidence is not None:
        if not isinstance(forward_evidence, Mapping):
            return None
        merged = dict(forward_evidence)
        # Lifecycle fields are authoritative; forward evidence supplies the
        # persisted forward metrics when they are nested.
        merged.update(payload)
        return merged
    return payload


def _canary_document_hash(value: Any) -> str | None:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        return None
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


_CANARY_QUALIFICATION_SCHEMA = "canary-qualification-v1"
_CANARY_QUALIFICATION_MARKERS = frozenset(
    {"schema", "schema_version", "qualification_schema", "qualification_hash"}
)
_CANARY_FORWARD_RANKING_FIELDS = frozenset(
    {
        "forward_expectancy",
        "forward_confidence_lower_bound",
        "forward_stability",
        "forward_calibration",
        "forward_duration_seconds",
        "forward_independent_resolved_bets",
        "forward_successful_order_attempts",
        "forward_regime_count",
        "forward_observations_without_signal",
        "observations_without_signal",
    }
)
_CANARY_RANKING_EVIDENCE_ALIASES: dict[str, tuple[str, ...]] = {
    "expectancy": ("validation_expectancy", "expectancy"),
    "confidence_lower_bound": (
        "validation_confidence_lower_bound",
        "confidence_lower_bound",
    ),
    "stability": ("validation_stability", "stability"),
    "calibration": ("validation_calibration", "calibration"),
    "sample_count": ("validation_sample_count", "sample_count"),
    "trade_count": ("validation_trade_count", "validation_trades", "trade_count"),
    "execution_quality": ("validation_execution_quality", "execution_quality"),
    "quality": (
        "validation_data_quality",
        "data_quality",
        "quality",
        "data_quality_passed",
    ),
    "execution_fidelity_score": ("execution_fidelity_score",),
    "max_drawdown": ("validation_max_drawdown", "max_drawdown"),
    "liquidity": ("validation_liquidity", "liquidity"),
    "forward_expectancy": ("forward_expectancy",),
}
_CANARY_RANKING_EVIDENCE_TOP_LEVEL_KEYS = frozenset(
    alias
    for aliases in _CANARY_RANKING_EVIDENCE_ALIASES.values()
    for alias in aliases
)

_CANARY_TELEMETRY_WORDS = (
    "duration",
    "observation",
    "fill",
    "trade",
    "order_attempt",
    "liquidity",
    "drawdown",
    "spread",
    "requested",
    "filled",
    "quantity",
)
_CANARY_QUALIFICATION_KEYS = frozenset(
    {
        "candidate_id",
        "market_type",
        "market",
        "market_scope",
        "market_scope_hash",
        "market_scope_version",
        "scope_hash",
        "scope_version",
        "plan_hash",
        "strategy_id",
        "timeframe",
        "source_type",
        "dataset_id",
        "dataset_version",
        "dataset_selector",
        "dataset_provenance",
        "dataset_attestation",
        "dataset_integrity_attestation",
        "historical_dataset_attestation",
        "strategy_hash",
        "model_hash",
        "config_hash",
        "frozen_hash",
        "frozen",
        "schema_validated",
        "schema_valid",
        "historical_backtest_passed",
        "backtest_complete",
        "validation_passed",
        "validation_complete",
        "robustness_passed",
        "holdout_used",
        "critical_error",
        "experiment_plan",
        "minimum_sample_check",
        "data_quality",
        "data_quality_passed",
        "forward_test_id",
        "historical_data_integrity",
        "historical_data_integrity_passed",
        "historical_execution_fidelity",
        "historical_execution_fidelity_score",
        "current_execution_evidence",
        "historical_provenance_complete",
        "historical_rows_nonempty",
        "historical_no_forward_contamination",
        "canary_data_quality_acceptable",
        "canary_data_quality_status",
        "production_evidence_status",
        "historical_dataset_row_count",
    }
)


def _canary_qualification_value(
    value: Any,
    *,
    key: str = "",
    preserve_telemetry: bool = False,
    exclude_prediction_market_fields: bool = False,
) -> Any:
    """Copy JSON evidence while removing mutable paper-forward telemetry."""
    preserve_telemetry = preserve_telemetry or key in {
        "minimum_sample_check",
        "dataset_provenance",
        "experiment_plan",
    } or key.lower().startswith(("validation_", "historical_", "robustness_"))
    if isinstance(value, Mapping):
        return {
            str(name): _canary_qualification_value(
                item,
                key=str(name),
                preserve_telemetry=preserve_telemetry,
            )
            for name, item in value.items()
            if not (
                (
                    exclude_prediction_market_fields
                    and str(name).lower() in {"market_type", "type"}
                )
                or str(name).lower().startswith("forward_")
                or (
                    not preserve_telemetry
                    and str(name).lower() not in _CANARY_QUALIFICATION_KEYS
                    and any(word in str(name).lower() for word in _CANARY_TELEMETRY_WORDS)
                )
            )
        }
    if isinstance(value, list):
        return [
            _canary_qualification_value(
                item,
                key=key,
                preserve_telemetry=preserve_telemetry,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            _canary_qualification_value(
                item,
                key=key,
                preserve_telemetry=preserve_telemetry,
            )
            for item in value
        ]
    return value


_CANARY_PREDICTION_MARKET_SOURCES = (
    "payload",
    "experiment_plan",
    "strategy",
    "forward_config",
    "market",
)
_CANARY_PREDICTION_MARKET_FIELDS = ("market_type", "type")


def _canary_prediction_market(payload: Mapping[str, Any]) -> str | None:
    """Resolve the effective market type with ranker's exact precedence."""
    body = payload if isinstance(payload, Mapping) else {}
    for source_name in _CANARY_PREDICTION_MARKET_SOURCES:
        source = body if source_name == "payload" else body.get(source_name)
        if not isinstance(source, Mapping):
            continue
        value = source.get("market_type", source.get("type"))
        if value is not None:
            return str(value).strip().lower()
    value = body.get("market_type")
    return str(value).strip().lower() if value is not None else None


def _canary_prediction_market_inputs(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Capture every market-type source and its precedence inputs."""
    body = payload if isinstance(payload, Mapping) else {}
    sources: list[dict[str, Any]] = []
    for source_name in _CANARY_PREDICTION_MARKET_SOURCES:
        source = body if source_name == "payload" else body.get(source_name)
        entry: dict[str, Any] = {
            "source": source_name,
            "present": isinstance(source, Mapping),
        }
        if isinstance(source, Mapping):
            for field in _CANARY_PREDICTION_MARKET_FIELDS:
                entry[f"{field}_present"] = field in source
                if field in source:
                    entry[field] = _canary_qualification_value(
                        source[field],
                        key=field,
                        preserve_telemetry=True,
                    )
        sources.append(entry)
    return {
        "sources": sources,
        "effective": _canary_prediction_market(body),
    }
def _canary_mapping_value(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _canary_scope_binding(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Extract the frozen plan/scope and historical selector binding.

    The lifecycle payload is the compatibility boundary: current candidates
    persist the canonical fields at top level, while normalized plans keep
    them nested under ``experiment_plan``/``market_scope``.  This helper does
    not infer a broad market universe and never turns a research-only scope
    into forward authority.
    """
    body = payload if isinstance(payload, Mapping) else {}
    plan = body.get("experiment_plan")
    plan = plan if isinstance(plan, Mapping) else {}
    scope = body.get("market_scope")
    if not isinstance(scope, Mapping):
        scope = plan.get("market_scope")
    scope = scope if isinstance(scope, Mapping) else {}
    selector = body.get("dataset_selector")
    if not isinstance(selector, Mapping):
        selector = plan.get("dataset_selector")
    selector = dict(selector) if isinstance(selector, Mapping) else {}
    plan_hash = (
        body.get("plan_hash")
        or plan.get("plan_hash")
        or plan.get("policy_hash")
    )
    scope_hash = (
        body.get("market_scope_hash")
        or body.get("scope_hash")
        or scope.get("scope_hash")
        or scope.get("policy_hash")
        or scope.get("hash")
        or plan.get("market_scope_hash")
    )
    scope_version = (
        body.get("market_scope_version")
        or body.get("scope_version")
        or scope.get("scope_version")
        or scope.get("schema_version")
        or scope.get("version")
        or plan.get("market_scope_version")
    )
    dataset_id = body.get("dataset_id") or plan.get("dataset_id") or selector.get("dataset_id")
    dataset_version = (
        body.get("dataset_version")
        or plan.get("dataset_version")
        or selector.get("dataset_version")
        or selector.get("version")
    )
    attestation = (
        body.get("dataset_attestation")
        or body.get("dataset_integrity_attestation")
        or body.get("historical_dataset_attestation")
    )
    if not isinstance(attestation, Mapping):
        attestation = {}
    return {
        "plan_hash": str(plan_hash).strip() if plan_hash is not None else None,
        "scope_hash": str(scope_hash).strip() if scope_hash is not None else None,
        "scope_version": str(scope_version).strip() if scope_version is not None else None,
        "market_scope": dict(scope),
        "dataset_selector": selector,
        "dataset_id": str(dataset_id).strip() if dataset_id is not None else None,
        "dataset_version": str(dataset_version).strip() if dataset_version is not None else None,
        "dataset_attestation": dict(attestation),
        "scope_declared": bool(scope or scope_hash or scope_version),
    }
def _execution_market_cap_evidence(
    resolved_market_ids: Sequence[str],
    declared_market_ids: Sequence[str],
) -> dict[str, Any]:
    resolved_count = len(resolved_market_ids)
    declared_count = len(declared_market_ids)
    market_count = max(resolved_count, declared_count)
    return {
        "execution_feasibility_reason_code": EXECUTION_FEASIBILITY_MARKET_CAP,
        "execution_market_cap": CANARY_EXECUTION_MARKET_CAP,
        "resolved_market_count": resolved_count,
        "declared_market_count": declared_count,
        "total_market_count": market_count,
        "resolved_market_ids": list(resolved_market_ids),
        "declared_market_ids": list(declared_market_ids),
        "execution_feasibility": {
            "reason_code": EXECUTION_FEASIBILITY_MARKET_CAP,
            "market_cap": CANARY_EXECUTION_MARKET_CAP,
            "resolved_market_count": resolved_count,
            "declared_market_count": declared_count,
            "total_market_count": market_count,
        },
    }



def _canary_resolution_attr(resolution: Any, name: str, default: Any = None) -> Any:
    value = _canary_mapping_value(resolution, name, default)
    if value is not default:
        return value
    as_dict = getattr(resolution, "as_dict", None)
    if callable(as_dict):
        try:
            encoded = as_dict()
        except Exception:
            encoded = None
        if isinstance(encoded, Mapping):
            return encoded.get(name, default)
    return default


def _canary_resolution_market(item: Any) -> dict[str, Any]:
    """Normalize one typed ``CurrentMarket`` without widening its fields."""
    if isinstance(item, Mapping):
        source = dict(item)
    else:
        source = {}
        as_dict = getattr(item, "as_dict", None)
        if callable(as_dict):
            try:
                encoded = as_dict()
            except Exception:
                encoded = None
            if isinstance(encoded, Mapping):
                source.update(encoded)
        for name in (
            "market_id",
            "yes_token_id",
            "no_token_id",
            "token_ids",
            "category",
            "instrument",
            "metadata",
        ):
            value = getattr(item, name, None)
            if value is not None:
                source.setdefault(name, value)
    market_id = source.get("market_id", source.get("id"))
    token_ids = source.get("token_ids")
    if isinstance(token_ids, Mapping):
        yes_token_id = source.get("yes_token_id") or token_ids.get("yes") or token_ids.get("YES")
        no_token_id = source.get("no_token_id") or token_ids.get("no") or token_ids.get("NO")
    elif isinstance(token_ids, (list, tuple)) and len(token_ids) >= 2:
        # Polymarket's canonical mapping is explicitly YES/NO in the
        # resolution object; do not guess from an unordered collection.
        yes_token_id = source.get("yes_token_id")
        no_token_id = source.get("no_token_id")
    else:
        yes_token_id = source.get("yes_token_id")
        no_token_id = source.get("no_token_id")
    result = dict(source)
    result["market_id"] = str(market_id).strip() if market_id is not None else ""
    result["yes_token_id"] = str(yes_token_id).strip() if yes_token_id is not None else ""
    result["no_token_id"] = str(no_token_id).strip() if no_token_id is not None else ""
    return result
def _canary_resolution_disposition(item: Any) -> dict[str, Any]:
    """Normalize one persisted scope disposition for diagnostic evidence."""
    if isinstance(item, Mapping):
        source = dict(item)
    else:
        source = {}
        as_dict = getattr(item, "as_dict", None)
        if callable(as_dict):
            try:
                encoded = as_dict()
            except Exception:
                encoded = None
            if isinstance(encoded, Mapping):
                source.update(encoded)
        for name in ("market_id", "reason", "detail", "metadata"):
            value = getattr(item, name, None)
            if value is not None:
                source.setdefault(name, value)
    source["market_id"] = str(source.get("market_id") or "").strip()
    source["reason"] = str(source.get("reason") or "UNKNOWN").strip().upper()
    source["detail"] = str(source.get("detail") or "").strip()
    metadata = source.get("metadata")
    source["metadata"] = dict(metadata) if isinstance(metadata, Mapping) else {}
    return source


def _canary_scope_disposition_failure(item: Mapping[str, Any]) -> str:
    """Map resolver dispositions to non-authorizing canary diagnostics."""
    reason = str(item.get("reason") or "").strip().upper()
    if reason in {
        "MARKET_CLOSED",
        "INACTIVE_MARKET",
        "RESOLVED_MARKET",
        "MARKET_EXPIRED",
    }:
        return "MARKET_CLOSED"
    if reason in {
        "RULE_MISMATCH",
        "CATEGORY_MISMATCH",
        "INSTRUMENT_MISMATCH",
        "VENUE_MISMATCH",
        "NON_CURRENT_MARKET",
    }:
        return "MARKET_FILTER_MISMATCH"
    if reason in {"NOT_OBSERVED", "FORWARD_SNAPSHOT_MISSING"}:
        return "NO_FORWARD_SNAPSHOT"
    return "DEFERRED_MARKETS"


_CANARY_SCOPE_DISPOSITION_FAILURE_PRECEDENCE = {
    "MARKET_CLOSED": 60,
    "MARKET_FILTER_MISMATCH": 50,
    "NO_FORWARD_SNAPSHOT": 40,
    "DEFERRED_MARKETS": 20,
}



def _canary_current_scope_resolution(
    store: AxiomStore,
    candidate_id: str,
    payload: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Load and validate the immutable persisted current market resolution."""
    binding = _canary_scope_binding(payload)
    result: dict[str, Any] = {
        "candidate_id": str(candidate_id).strip(),
        "bound": False,
        "reason_code": "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        "scope_hash": binding["scope_hash"],
        "scope_version": binding["scope_version"],
        "plan_hash": binding["plan_hash"],
        "dataset_selector": binding["dataset_selector"],
        "dataset_attestation": binding["dataset_attestation"],
        "matched_markets": [],
        "excluded_markets": [],
        "deferred_markets": [],
        "resolution": None,
    }
    scope_hash = binding["scope_hash"]
    scope_version = binding["scope_version"]
    if not binding.get("scope_declared"):
        # Legacy frozen documents are diagnostic/migration inputs only.  They
        # do not carry a canonical scope identity and therefore cannot become
        # current execution authority by inference at this boundary.
        result["legacy"] = True
        result["reason_code"] = "LEGACY_SCOPE_SUCCESSOR_REQUIRED"
        return result
    if not scope_hash or not scope_version:
        result["reason_code"] = "SCOPE_RESOLUTION_MISSING"
        return result
    loader = getattr(store, "load_market_scope_resolution", None)
    if not callable(loader):
        result["reason_code"] = "SCOPE_RESOLUTION_MISSING"
        return result
    try:
        resolution = loader(
            str(candidate_id).strip(),
            scope_hash=scope_hash,
            scope_version=scope_version,
        )
    except Exception:
        resolution = None
    if resolution is None:
        try:
            unbound = loader(str(candidate_id).strip())
        except Exception:
            unbound = None
        result["reason_code"] = (
            "SCOPE_RESOLUTION_SCOPE_MISMATCH"
            if unbound is not None
            else "SCOPE_RESOLUTION_MISSING"
        )
        return result
    result["resolution"] = resolution
    resolved_hash = _canary_resolution_attr(resolution, "scope_hash")
    resolved_version = _canary_resolution_attr(resolution, "scope_version")
    if (
        str(resolved_hash or "").strip() != scope_hash
        or str(resolved_version or "").strip() != scope_version
    ):
        result["reason_code"] = "SCOPE_RESOLUTION_SCOPE_MISMATCH"
        return result
    status = str(_canary_resolution_attr(resolution, "status", "") or "").strip().upper()
    reason = str(_canary_resolution_attr(resolution, "reason", "") or "").strip().upper()
    resolved_at = parse_timestamp(_canary_resolution_attr(resolution, "resolved_at"))
    if (
        resolved_at is None
        or resolved_at > ensure_utc(now)
        or (ensure_utc(now) - resolved_at).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS
    ):
        result["reason_code"] = "SCOPE_RESOLUTION_STALE"
        return result
    matched = _canary_resolution_attr(resolution, "matched_markets", ())
    excluded = _canary_resolution_attr(resolution, "excluded_markets", ())
    deferred = _canary_resolution_attr(resolution, "deferred_markets", ())
    matched_values = list(matched) if isinstance(matched, (list, tuple, set, frozenset)) else []
    excluded_values = list(excluded) if isinstance(excluded, (list, tuple, set, frozenset)) else []
    deferred_values = list(deferred) if isinstance(deferred, (list, tuple, set, frozenset)) else []
    markets = [_canary_resolution_market(item) for item in matched_values]
    result["matched_markets"] = markets
    result["excluded_markets"] = [
        _canary_resolution_disposition(item) for item in excluded_values
    ]
    result["deferred_markets"] = [
        _canary_resolution_disposition(item) for item in deferred_values
    ]
    if status not in {"MATCHED", "PARTIAL", "RESOLVED", "CURRENT"}:
        if status == "RESEARCH_ONLY" or reason == "RESEARCH_ONLY":
            result["reason_code"] = "RESEARCH_ONLY"
        elif status == "INVALID_POLICY" or reason == "INVALID_POLICY":
            result["reason_code"] = "INVALID_POLICY"
        elif status == "DEFERRED" or "DEFERRED" in reason:
            result["reason_code"] = "DEFERRED_MARKETS"
        else:
            result["reason_code"] = (
                "SCOPE_RESOLUTION_ZERO_MATCHES"
                if not markets or "ZERO" in reason
                else "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
            )
        return result
    if not markets:
        result["reason_code"] = "SCOPE_RESOLUTION_ZERO_MATCHES"
        return result
    if any(
        not item.get("market_id")
        or not item.get("yes_token_id")
        or not item.get("no_token_id")
        for item in markets
    ):
        result["reason_code"] = "SCOPE_RESOLUTION_TOKEN_MISMATCH"
        return result
    result["bound"] = True
    result["reason_code"] = None
    result["resolved_at"] = resolved_at.isoformat()
    return result


def _canary_qualification_projection(
    candidate_id: str,
    payload: Mapping[str, Any],
    *,
    frozen_hash: str | None = None,
    quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project lifecycle evidence onto the immutable qualification contract."""
    identifier = str(candidate_id).strip()
    body = payload if isinstance(payload, Mapping) else {}
    result: dict[str, Any] = {
        "schema": _CANARY_QUALIFICATION_SCHEMA,
        "schema_version": _CANARY_QUALIFICATION_SCHEMA,
        "qualification_schema": _CANARY_QUALIFICATION_SCHEMA,
        "candidate_id": identifier,
    }
    verified_frozen_hash = frozen_hash if isinstance(frozen_hash, str) and frozen_hash else body.get("frozen_hash")
    if isinstance(verified_frozen_hash, str) and verified_frozen_hash:
        result["frozen_hash"] = verified_frozen_hash
    for key, value in body.items():
        name = str(key)
        lower = name.lower()
        if name in _CANARY_QUALIFICATION_MARKERS or name == "forward_evidence":
            continue
        if lower.startswith("forward_"):
            continue
        if name not in _CANARY_QUALIFICATION_KEYS and not lower.startswith(
            ("validation_", "historical_", "robustness_")
        ):
            continue
        result[name] = _canary_qualification_value(
            value,
            key=name,
            exclude_prediction_market_fields=name in {
                "experiment_plan",
                "market",
            },
        )
    scope_binding = _canary_scope_binding(body)
    for scope_name, scope_value in (
        ("plan_hash", scope_binding.get("plan_hash")),
        ("market_scope_hash", scope_binding.get("scope_hash")),
        ("market_scope_version", scope_binding.get("scope_version")),
        ("dataset_selector", scope_binding.get("dataset_selector")),
        ("dataset_attestation", scope_binding.get("dataset_attestation")),
    ):
        if scope_value not in (None, "", {}, []):
            result.setdefault(
                scope_name,
                _canary_qualification_value(scope_value, key=scope_name),
            )
    if isinstance(quality, Mapping):
        derived_quality = _canary_qualification_value(
            persisted_quality_fields(quality)
        )
        if isinstance(derived_quality, Mapping):
            # Lifecycle evidence is authoritative; derived quality only fills
            # fields absent from the current immutable qualification payload.
            for name, value in derived_quality.items():
                result.setdefault(str(name), value)
    return result


def _canary_qualification_hash(projection: Mapping[str, Any]) -> str | None:
    body = dict(projection)
    body.pop("qualification_hash", None)
    return _canary_document_hash(body)


def _canary_ranking_snapshot_hash(
    candidate_id: str,
    stage: str,
    payload: Mapping[str, Any],
    *,
    qualification_hash: str | None,
    quality: Mapping[str, Any] | None = None,
) -> str | None:
    """Hash every mutable input that can affect a persisted ranking."""
    body = payload if isinstance(payload, Mapping) else {}
    ranking: dict[str, Any] = {}
    ranking["prediction_market"] = _canary_prediction_market_inputs(body)
    for key, value in body.items():
        name = str(key)
        lower = name.lower()
        if name == "forward_evidence":
            if isinstance(value, Mapping):
                forward_ranking = {
                    str(item): _canary_qualification_value(raw, key=str(item))
                    for item, raw in value.items()
                    if str(item) in _CANARY_FORWARD_RANKING_FIELDS
                }
                if forward_ranking:
                    ranking[name] = forward_ranking
            continue
        if (
            lower.startswith("validation_")
            or lower.startswith("historical_")
            or lower.startswith("robustness_")
            or lower in _CANARY_RANKING_EVIDENCE_TOP_LEVEL_KEYS
            or name in (
                {
                    "experiment_plan",
                    "minimum_sample_check",
                    "holdout_used",
                    "mutation_cluster",
                    "cluster_key",
                    "lineage",
                    "root_candidate_id",
                    "parent_id",
                    "experiment_family",
                    "family",
                    "forward_expectancy",
                    "observations_without_signal",
                }
                | _CANARY_FORWARD_RANKING_FIELDS
            )
        ):
            ranking[name] = _canary_qualification_value(value, key=name)
    nested_validation = body.get("validation")
    if isinstance(nested_validation, Mapping):
        # CandidateRanker reads validation metrics from this nested mapping;
        # bind the complete immutable mapping so any such mutation invalidates
        # a persisted ranking snapshot.
        ranking["validation"] = _canary_qualification_value(
            nested_validation,
            key="validation",
            preserve_telemetry=True,
        )
    if isinstance(quality, Mapping):
        ranking["data_quality"] = _canary_qualification_value(
            persisted_quality_fields(quality)
        )
    return _canary_document_hash(
        {
            "candidate_id": str(candidate_id),
            "stage": str(stage),
            "qualification_hash": qualification_hash,
            "ranking_inputs": ranking,
        }
    )
def _canary_lifecycle_snapshot_hashes(
    store: AxiomStore,
    record: Mapping[str, Any] | None,
    *,
    quality: Mapping[str, Any] | None | object = _UNSET,
) -> tuple[str, str | None, str | None]:
    """Return the stage, qualification hash, and ranking-input hash for a row.

    This is the shared hash boundary for lifecycle writers and rankers.  Keep
    the projection and ranking-input definitions in one place so readiness
    invalidation cannot drift from submission/ranking revalidation.
    """
    if not isinstance(record, Mapping):
        return "", None, None
    stage = str(record.get("stage") or "")
    payload = _canary_merged_lifecycle_payload(record)
    if payload is None:
        return stage, None, None
    frozen_hash = _canary_lifecycle_frozen_hash(store, record)
    if quality is _UNSET:
        quality = evaluate_prediction_data_quality(
            store,
            payload,
            verify_attestation=False,
        )
    qualification = _canary_qualification_projection(
        str(record.get("candidate_id") or ""),
        payload,
        frozen_hash=frozen_hash,
        quality=quality if isinstance(quality, Mapping) else None,
    )
    qualification_hash = _canary_qualification_hash(qualification)
    ranking_hash = _canary_ranking_snapshot_hash(
        str(record.get("candidate_id") or ""),
        stage,
        payload,
        qualification_hash=qualification_hash,
        quality=quality if isinstance(quality, Mapping) else None,
    )
    return stage, qualification_hash, ranking_hash


def _canary_lifecycle_payload_has_telemetry_change(
    before: Any,
    after: Any,
    *,
    key: str = "",
) -> bool:
    """Identify ignored signal/forward telemetry for the C evidence class."""
    if before == after:
        return False
    lower = str(key).lower()
    if (
        lower.startswith(("signal_", "telemetry_"))
        or lower in {"signal", "telemetry", "latest_signal"}
        or any(word in lower for word in _CANARY_TELEMETRY_WORDS)
    ):
        return True
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        names = {str(name) for name in before} | {str(name) for name in after}
        return any(
            _canary_lifecycle_payload_has_telemetry_change(
                before.get(name),
                after.get(name),
                key=name,
            )
            for name in names
        )
    if isinstance(before, (list, tuple)) and isinstance(after, (list, tuple)):
        if len(before) != len(after):
            return "signal" in lower or "telemetry" in lower
        return any(
            _canary_lifecycle_payload_has_telemetry_change(
                old,
                new,
                key=key,
            )
            for old, new in zip(before, after)
        )
    return False


def _canary_lifecycle_evidence_class(
    store: AxiomStore,
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
) -> str:
    """Classify one lifecycle write as qualification, ranking, telemetry, or harmless.

    ``A`` changes immutable qualification, ``B`` changes ranking evidence
    without changing qualification, ``C`` changes ignored signal telemetry,
    and ``D`` changes neither hash projection.  Hash failures are treated as
    qualification changes so an unclassifiable write fails closed.
    """
    before_stage, before_qualification, before_ranking = (
        _canary_lifecycle_snapshot_hashes(store, before)
    )
    after_stage, after_qualification, after_ranking = (
        _canary_lifecycle_snapshot_hashes(store, after)
    )
    if before_stage != after_stage:
        return "A"
    if before_qualification is None or after_qualification is None:
        return "A"
    if before_qualification != after_qualification:
        return "A"
    if before_ranking is None or after_ranking is None:
        return "A"
    if before_ranking != after_ranking:
        return "B"
    before_payload = _canary_merged_lifecycle_payload(before) or {}
    after_payload = _canary_merged_lifecycle_payload(after) or {}
    if _canary_lifecycle_payload_has_telemetry_change(before_payload, after_payload):
        return "C"
    return "D"


def _canary_lifecycle_evidence_reason(evidence_class: str) -> str | None:
    """Map the A/B lifecycle classes to precise readiness invalidations."""
    return {
        "A": "LIFECYCLE_QUALIFICATION_UPDATED",
        "B": "LIFECYCLE_RANKING_EVIDENCE_UPDATED",
    }.get(str(evidence_class).strip().upper())





def _canary_lifecycle_frozen_hash(store: AxiomStore, record: Mapping[str, Any] | None) -> str | None:
    """Return the verified frozen binding recorded by a lifecycle row."""
    if not isinstance(record, Mapping) or record.get("stage") not in _CANARY_ELIGIBLE_STAGES:
        return None
    payload = _canary_merged_lifecycle_payload(record)
    if payload is None:
        return None
    hash_parts: list[str] = []
    for key in ("strategy_hash", "model_hash", "config_hash"):
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            return None
        hash_parts.append(value)
    expected_frozen_hash = hashlib.sha256("|".join(hash_parts).encode("utf-8")).hexdigest()
    frozen_hash = payload.get("frozen_hash")
    if not isinstance(frozen_hash, str) or frozen_hash != expected_frozen_hash:
        return None

    frozen_documents = payload.get("frozen_documents")
    if frozen_documents is None:
        frozen_documents = {}
    elif not isinstance(frozen_documents, Mapping):
        return None
    strategy_document = payload.get(
        "strategy_document",
        frozen_documents.get("strategy_document", frozen_documents.get("strategy")),
    )
    model_document = payload.get(
        "model_document",
        frozen_documents.get("model_document", frozen_documents.get("model")),
    )
    forward_config = payload.get(
        "forward_config",
        frozen_documents.get("forward_config", frozen_documents.get("config")),
    )
    risk_snapshot = payload.get(
        "risk_snapshot",
        frozen_documents.get("risk_snapshot", frozen_documents.get("risk_limits")),
    )

    forward_test_id = payload.get("forward_test_id")
    if forward_test_id:
        try:
            forward_test = store.load_forward_test(str(forward_test_id))
        except (TypeError, ValueError):
            forward_test = None
        if forward_test is None:
            return None
        if (
            str(forward_test.get("strategy_hash", "")) != hash_parts[0]
            or str(forward_test.get("model_hash", "")) != hash_parts[1]
        ):
            return None
        if forward_config is None:
            forward_config = forward_test.get("config")
        if risk_snapshot is None:
            risk_snapshot = forward_test.get("risk_limits")

    if isinstance(forward_config, Mapping):
        if strategy_document is None:
            strategy_document = forward_config.get("strategy_document")
        if model_document is None:
            model_document = forward_config.get("model_document")
    if strategy_document is not None and _canary_document_hash(strategy_document) != hash_parts[0]:
        return None
    if model_document is not None and _canary_document_hash(model_document) != hash_parts[1]:
        return None
    if forward_config is not None or risk_snapshot is not None:
        if not isinstance(forward_config, Mapping) or not isinstance(risk_snapshot, Mapping):
            return None
        config_hash = _canary_document_hash(
            {"config": forward_config, "risk_limits": risk_snapshot}
        )
        if config_hash != hash_parts[2]:
            return None
    return frozen_hash


def _canary_eligibility_binding_result(
    store: AxiomStore,
    candidate_id: str,
    eligibility: Mapping[str, Any] | None,
    *,
    record: Mapping[str, Any] | None = None,
    verify_attestation: bool = True,
) -> dict[str, Any]:
    """Return a reasoned binding result without treating stored PASS as a gate."""
    identifier = str(candidate_id).strip()
    result: dict[str, Any] = {
        "candidate_id": identifier,
        "bound": False,
        "valid": False,
        "legacy": False,
        "reevaluation_required": False,
        "qualification_hash": None,
        "reason_code": "ELIGIBILITY_MISSING",
    }
    if eligibility is None:
        return result
    try:
        row = dict(eligibility)
        evidence = json.loads(str(row.get("evidence_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        result["reason_code"] = "ELIGIBILITY_INVALID"
        return result
    if not isinstance(evidence, Mapping):
        result["reason_code"] = "ELIGIBILITY_INVALID"
        return result
    if record is None:
        try:
            record = store.load_candidate_lifecycle(identifier)
        except Exception:
            record = None
    if not isinstance(record, Mapping):
        result["reason_code"] = "LIFECYCLE_REJECTED"
        return result
    stage = str(record.get("stage") or "")
    if stage == "REJECTED":
        result["reason_code"] = "LIFECYCLE_REJECTED"
        return result
    if stage not in _CANARY_ELIGIBLE_STAGES:
        result["reason_code"] = "ELIGIBILITY_INVALID"
        return result
    payload = _canary_merged_lifecycle_payload(record)
    frozen_hash = _canary_lifecycle_frozen_hash(store, record)
    stored_frozen_hash = row.get("frozen_hash")
    if (
        not isinstance(stored_frozen_hash, str)
        or not stored_frozen_hash
        or frozen_hash is None
        or stored_frozen_hash != frozen_hash
    ):
        result["reason_code"] = "QUALIFICATION_CHANGED"
        return result
    quality = evaluate_prediction_data_quality(
        store,
        payload or {},
        verify_attestation=verify_attestation,
    )
    current_projection = _canary_qualification_projection(
        identifier,
        payload or {},
        frozen_hash=frozen_hash,
        quality=quality,
    )
    expected_hash = _canary_qualification_hash(current_projection)
    result["qualification_hash"] = expected_hash
    marker_values = [
        evidence.get(name)
        for name in ("schema", "schema_version", "qualification_schema")
        if name in evidence
    ]
    if marker_values and any(value != _CANARY_QUALIFICATION_SCHEMA for value in marker_values):
        result["reason_code"] = "ELIGIBILITY_INVALID"
        return result
    stored_schema = evidence.get("schema") or evidence.get("schema_version") or evidence.get(
        "qualification_schema"
    )
    if stored_schema != _CANARY_QUALIFICATION_SCHEMA and "qualification_hash" in evidence:
        result["reason_code"] = "ELIGIBILITY_INVALID"
        return result
    if stored_schema == _CANARY_QUALIFICATION_SCHEMA:
        stored_hash = evidence.get("qualification_hash")
        if not isinstance(stored_hash, str) or stored_hash != expected_hash:
            result["reason_code"] = "QUALIFICATION_CHANGED"
            return result
        stored_projection = dict(evidence)
        stored_projection.pop("qualification_hash", None)
        if stored_projection != current_projection:
            result["reason_code"] = "QUALIFICATION_CHANGED"
            return result
        result.update({"bound": True, "valid": True, "reason_code": None})
        return result
    legacy_frozen_hash = evidence.get("frozen_hash")
    if (
        not isinstance(legacy_frozen_hash, str)
        or not legacy_frozen_hash
        or legacy_frozen_hash != frozen_hash
    ):
        result["reason_code"] = "QUALIFICATION_CHANGED"
        return result
    legacy_projection = _canary_qualification_projection(
        identifier,
        evidence,
        frozen_hash=frozen_hash,
        quality=quality,
    )
    if legacy_projection != current_projection:
        result["reason_code"] = "QUALIFICATION_CHANGED"
        return result
    result.update(
        {
            "bound": True,
            "valid": True,
            "legacy": True,
            "reevaluation_required": True,
            "reason_code": "REEVALUATION_REQUIRED",
        }
    )
    return result

def _canary_eligibility_is_bound(
    store: AxiomStore,
    candidate_id: str,
    eligibility: Mapping[str, Any] | None,
) -> bool:
    """Shared boolean wrapper around the reasoned qualification binding."""
    return bool(
        _canary_eligibility_binding_result(store, candidate_id, eligibility).get("bound")
    )

class CanaryService:
    def __init__(
        self,
        store: AxiomStore,
        *,
        credentials: CredentialStore | None = None,
        clock=utc_now,
        allow_environment: bool = False,
        initialize: bool = True,
        settings: Any | None = None,
        profile: Any | None = None,
    ) -> None:
        self.store = store
        credential_source = credentials
        if credential_source is None:
            credential_source = getattr(store, "_canary_credential_store", None)
        self.credentials = credential_source or CredentialStore()
        if credentials is not None or not hasattr(store, "_canary_credential_store"):
            setattr(store, "_canary_credential_store", self.credentials)
        self.clock = clock
        self.allow_environment = bool(allow_environment)
        self.profile = profile
        if settings is None:
            from .canary_settings import CanarySettingsService

            settings = CanarySettingsService(
                store,
                clock=clock,
                initialize=initialize,
            )
        self.settings = settings
        if initialize:
            self._initialize()

    def _initialize(self) -> None:
        with self.store._lock, self.store.connection:
            self.store.connection.executescript("""
            CREATE TABLE IF NOT EXISTS canary_control (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1), state TEXT NOT NULL,
              candidate_id TEXT, venue TEXT, armed_at TEXT, expires_at TEXT,
              limits_json TEXT NOT NULL, integrity_hash TEXT NOT NULL, updated_at TEXT NOT NULL,
              settings_config_id TEXT, settings_generation INTEGER);
            CREATE TABLE IF NOT EXISTS canary_eligibility (
              candidate_id TEXT PRIMARY KEY, eligible_at TEXT NOT NULL,
              frozen_hash TEXT NOT NULL, evidence_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS canary_ledger (
              event_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL UNIQUE, timestamp TEXT NOT NULL,
              candidate_id TEXT NOT NULL, venue TEXT NOT NULL, market_id TEXT NOT NULL, token_id TEXT NOT NULL,
              side TEXT NOT NULL, requested_notional TEXT NOT NULL, paper_expected_price TEXT NOT NULL,
              max_price TEXT NOT NULL, submitted_quantity TEXT, exchange_order_id TEXT,
              fill_quantity TEXT, actual_average_price TEXT, fees TEXT, status TEXT NOT NULL,
              latency_ms INTEGER, price_difference TEXT, fee_difference TEXT, slippage_difference TEXT,
              settlement TEXT, realized_pnl TEXT, evidence_json TEXT NOT NULL,
              state_version INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS canary_execution_events (
              execution_event_id TEXT PRIMARY KEY, canary_event_id TEXT NOT NULL,
              timestamp TEXT NOT NULL, exchange_order_id TEXT, status TEXT NOT NULL,
              fill_quantity TEXT, actual_average_price TEXT, fees TEXT,
              latency_ms INTEGER, evidence_json TEXT NOT NULL,
              FOREIGN KEY(canary_event_id) REFERENCES canary_ledger(event_id));
            CREATE INDEX IF NOT EXISTS idx_canary_execution_events_order
              ON canary_execution_events(canary_event_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_canary_execution_events_time
              ON canary_execution_events(timestamp, execution_event_id);
            CREATE INDEX IF NOT EXISTS idx_canary_ledger_time ON canary_ledger(timestamp, event_id);
            CREATE TABLE IF NOT EXISTS canary_signals (
              signal_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL,
              frozen_hash TEXT NOT NULL, strategy_hash TEXT NOT NULL,
              model_hash TEXT NOT NULL, config_hash TEXT NOT NULL,
              market_id TEXT NOT NULL, token_id TEXT NOT NULL,
              outcome TEXT NOT NULL, side TEXT NOT NULL,
              paper_expected_price TEXT NOT NULL,
              source_snapshot_id TEXT NOT NULL, source_timestamp TEXT NOT NULL,
              generated_at TEXT NOT NULL, expires_at TEXT NOT NULL,
              status TEXT NOT NULL, reason TEXT, evidence_json TEXT NOT NULL,
              updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_canary_signals_candidate_time
              ON canary_signals(candidate_id, generated_at, signal_id);
            CREATE INDEX IF NOT EXISTS idx_canary_signals_status_time
              ON canary_signals(status, generated_at, signal_id);
            CREATE TABLE IF NOT EXISTS canary_signal_evaluations (
              evaluation_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL,
              cycle_id TEXT, evaluated_at TEXT NOT NULL, reason_code TEXT NOT NULL,
              market_id TEXT, signal_id TEXT, signal_json TEXT,
              required_health_json TEXT NOT NULL, evidence_json TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_canary_signal_evaluations_candidate_time
              ON canary_signal_evaluations(candidate_id, evaluated_at, evaluation_id);
            CREATE INDEX IF NOT EXISTS idx_canary_signal_evaluations_cycle_time
              ON canary_signal_evaluations(cycle_id, evaluated_at, evaluation_id);
            CREATE TABLE IF NOT EXISTS canary_rankings (
              candidate_id TEXT PRIMARY KEY, ranking_run_id TEXT NOT NULL,
              ranking_timestamp TEXT NOT NULL, rank INTEGER NOT NULL DEFAULT 0,
              total_score REAL, component_scores_json TEXT NOT NULL,
              evidence_versions_json TEXT NOT NULL, cluster_key TEXT NOT NULL,
              cluster_representative INTEGER NOT NULL DEFAULT 0,
              selected INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_canary_rankings_rank
              ON canary_rankings(selected, rank, total_score, candidate_id);
            CREATE TABLE IF NOT EXISTS canary_selection (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              ranking_run_id TEXT NOT NULL, candidate_id TEXT,
              rank INTEGER, total_score REAL, component_scores_json TEXT NOT NULL,
              evidence_versions_json TEXT NOT NULL, reason TEXT NOT NULL,
              selected_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canary_autonomous_state (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              last_tick_at TEXT, next_decision TEXT NOT NULL,
              blocker TEXT, last_signal_id TEXT, worker_status TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              last_tick_started_at TEXT,
              last_tick_completed_at TEXT,
              last_successful_tick TEXT,
              last_error_code TEXT,
              consecutive_failures INTEGER NOT NULL DEFAULT 0,
              next_retry_at TEXT,
              candidates_evaluated INTEGER,
              signals_generated INTEGER,
              orders_attempted INTEGER,
              candidates_ranked INTEGER,
              candidates_signal_checked INTEGER,
              candidates_no_signal INTEGER,
              actionable_candidates_found INTEGER,
              selected_actionable_candidate TEXT,
              selected_actionable_rank INTEGER,
              selected_actionable_score REAL,
              signal_scan_cursor INTEGER NOT NULL DEFAULT 0,
              signal_scan_ranking_run_id TEXT,
              next_signal_scan_start_rank INTEGER,
              next_signal_scan_end_rank INTEGER,
              signal_scan_cycle_id TEXT,
              signal_scan_candidate_universe_hash TEXT,
              signal_scan_cycle_started_at TEXT,
              signal_scan_cycle_completed_at TEXT,
              signal_scan_cycle_complete INTEGER NOT NULL DEFAULT 0,
              signal_scan_checked_this_cycle INTEGER NOT NULL DEFAULT 0,
              signal_scan_remaining_this_cycle INTEGER NOT NULL DEFAULT 0,
              signal_scan_coverage_percentage REAL NOT NULL DEFAULT 0,
              signal_scan_skip_reasons_json TEXT NOT NULL DEFAULT '{}',
              signal_scan_reason_counts_json TEXT NOT NULL DEFAULT '{}',
              signal_scan_status TEXT NOT NULL DEFAULT 'UNKNOWN'
            );
            CREATE TABLE IF NOT EXISTS canary_signal_scan_checked (
              cycle_id TEXT NOT NULL,
              candidate_id TEXT NOT NULL,
              qualification_hash TEXT NOT NULL,
              checked_at TEXT NOT NULL,
              rank_at_check INTEGER,
              ranking_run_id TEXT,
              PRIMARY KEY(cycle_id,candidate_id,qualification_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_canary_signal_scan_checked_cycle
              ON canary_signal_scan_checked(cycle_id,checked_at,candidate_id);
            CREATE INDEX IF NOT EXISTS idx_canary_signal_scan_checked_candidate
              ON canary_signal_scan_checked(candidate_id,qualification_hash);
            CREATE TABLE IF NOT EXISTS canary_readiness_snapshot (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              payload_json TEXT NOT NULL,
              readiness_snapshot_status TEXT NOT NULL,
              readiness_snapshot_stale INTEGER NOT NULL DEFAULT 1,
              readiness_snapshot_reason TEXT NOT NULL,
              readiness_snapshot_updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_canary_readiness_snapshot_status
              ON canary_readiness_snapshot(readiness_snapshot_status, readiness_snapshot_updated_at);
            """)
            columns = {
                str(row["name"])
                for row in self.store.connection.execute("PRAGMA table_info(canary_control)")
            }
            if "control_generation" not in columns:
                self.store.connection.execute(
                    "ALTER TABLE canary_control ADD COLUMN control_generation "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            # Rows created by the pre-generation schema used zero as the
            # implicit value.  Generation zero is not a valid control fence.
            self.store.connection.execute(
                "UPDATE canary_control SET control_generation=1 "
                "WHERE control_generation=0"
            )
            columns = {
                str(row["name"])
                for row in self.store.connection.execute("PRAGMA table_info(canary_ledger)")
            }
            if "control_generation" not in columns:
                self.store.connection.execute(
                    "ALTER TABLE canary_ledger ADD COLUMN control_generation "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "state_version" not in columns:
                self.store.connection.execute(
                    "ALTER TABLE canary_ledger ADD COLUMN state_version "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            for table, additions in (
                (
                    "canary_rankings",
                    (
                        ("qualification_hash", "TEXT"),
                        ("ranking_snapshot_hash", "TEXT"),
                    ),
                ),
                (
                    "canary_selection",
                    (
                        ("ranking_timestamp", "TEXT"),
                        ("qualification_hash", "TEXT"),
                        ("ranking_snapshot_hash", "TEXT"),
                        ("selection_status", "TEXT NOT NULL DEFAULT 'NONE'"),
                        ("selection_valid", "INTEGER NOT NULL DEFAULT 0"),
                        ("selection_invalidation_reason", "TEXT"),
                        ("last_selected_candidate", "TEXT"),
                    ),
                ),
                (
                    "canary_autonomous_state",
                    (
                        ("last_tick_started_at", "TEXT"),
                        ("last_tick_completed_at", "TEXT"),
                        ("last_successful_tick", "TEXT"),
                        ("last_error_code", "TEXT"),
                        ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
                        ("next_retry_at", "TEXT"),
                        ("candidates_evaluated", "INTEGER"),
                        ("signals_generated", "INTEGER"),
                        ("orders_attempted", "INTEGER"),
                        ("candidates_ranked", "INTEGER"),
                        ("candidates_signal_checked", "INTEGER"),
                        ("candidates_no_signal", "INTEGER"),
                        ("actionable_candidates_found", "INTEGER"),
                        ("selected_actionable_candidate", "TEXT"),
                        ("selected_actionable_rank", "INTEGER"),
                        ("selected_actionable_score", "REAL"),
                        ("signal_scan_cursor", "INTEGER NOT NULL DEFAULT 0"),
                        ("signal_scan_ranking_run_id", "TEXT"),
                        ("next_signal_scan_start_rank", "INTEGER"),
                        ("next_signal_scan_end_rank", "INTEGER"),
                        ("signal_scan_cycle_id", "TEXT"),
                        ("signal_scan_candidate_universe_hash", "TEXT"),
                        ("signal_scan_cycle_started_at", "TEXT"),
                        ("signal_scan_cycle_completed_at", "TEXT"),
                        ("signal_scan_cycle_complete", "INTEGER NOT NULL DEFAULT 0"),
                        ("signal_scan_checked_this_cycle", "INTEGER NOT NULL DEFAULT 0"),
                        ("signal_scan_remaining_this_cycle", "INTEGER NOT NULL DEFAULT 0"),
                        ("signal_scan_coverage_percentage", "REAL NOT NULL DEFAULT 0"),
                        ("signal_scan_skip_reasons_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("signal_scan_reason_counts_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("signal_scan_status", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
                    ),
                ),
                (
                    "canary_control",
                    (
                        ("settings_config_id", "TEXT"),
                        ("settings_generation", "INTEGER"),
                        ("credential_fingerprint", "TEXT"),
                    ),
                ),
                (
                    "canary_readiness_snapshot",
                    (
                        ("source_control_generation", "INTEGER"),
                        ("projection_version", "INTEGER NOT NULL DEFAULT 0"),
                    ),
                ),
            ):
                table_columns = {
                    str(row["name"])
                    for row in self.store.connection.execute(f"PRAGMA table_info({table})")
                }
                for column, declaration in additions:
                    if column not in table_columns:
                        self.store.connection.execute(
                            f"ALTER TABLE {table} ADD COLUMN {column} {declaration}"
                        )

            # Readiness trigger definitions changed over time.  Do not leave
            # an older same-name trigger (notably the pre-stage-filter
            # lifecycle update trigger) active in a reopened database: SQLite
            # CREATE TRIGGER IF NOT EXISTS preserves that stale definition.
            self.store.connection.executescript("""
            DROP TRIGGER IF EXISTS canary_readiness_stale_lifecycle_insert;
            DROP TRIGGER IF EXISTS canary_readiness_stale_lifecycle_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_eligibility;
            DROP TRIGGER IF EXISTS canary_readiness_stale_eligibility_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_rankings;
            DROP TRIGGER IF EXISTS canary_readiness_stale_rankings_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_selection;
            DROP TRIGGER IF EXISTS canary_readiness_stale_selection_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_control;
            DROP TRIGGER IF EXISTS canary_readiness_stale_control_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_signal;
            DROP TRIGGER IF EXISTS canary_readiness_stale_signal_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_ledger;
            DROP TRIGGER IF EXISTS canary_readiness_stale_ledger_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_execution;
            DROP TRIGGER IF EXISTS canary_readiness_stale_execution_update;
            DROP TRIGGER IF EXISTS canary_readiness_stale_lifecycle_delete;
            DROP TRIGGER IF EXISTS canary_readiness_stale_eligibility_delete;
            DROP TRIGGER IF EXISTS canary_readiness_stale_rankings_delete;
            DROP TRIGGER IF EXISTS canary_readiness_stale_selection_delete;
            DROP TRIGGER IF EXISTS canary_readiness_stale_autonomous;
            DROP TRIGGER IF EXISTS canary_readiness_stale_autonomous_update;
            """)

            self.store.connection.executescript("""
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_lifecycle_insert
            AFTER INSERT ON candidate_lifecycle BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LIFECYCLE_CHANGED'
              WHERE singleton=1;
            END;
            DROP TRIGGER IF EXISTS canary_readiness_stale_lifecycle_update;
            CREATE TRIGGER canary_readiness_stale_lifecycle_update
            AFTER UPDATE OF stage ON candidate_lifecycle
            WHEN OLD.stage IS NOT NEW.stage BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LIFECYCLE_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_eligibility
            AFTER INSERT ON canary_eligibility BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='ELIGIBILITY_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_eligibility_update
            AFTER UPDATE ON canary_eligibility BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='ELIGIBILITY_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_rankings
            AFTER INSERT ON canary_rankings BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='RANKINGS_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_rankings_update
            AFTER UPDATE ON canary_rankings BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='RANKINGS_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_selection
            AFTER INSERT ON canary_selection BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='SELECTION_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_selection_update
            AFTER UPDATE ON canary_selection BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='SELECTION_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_control
            AFTER INSERT ON canary_control BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='CONTROL_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_control_update
            AFTER UPDATE ON canary_control BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='CONTROL_CHANGED'
              WHERE singleton=1;
            END;
            DROP TRIGGER IF EXISTS canary_readiness_stale_autonomous;
            DROP TRIGGER IF EXISTS canary_readiness_stale_autonomous_update;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_signal
            AFTER INSERT ON canary_signals BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='SIGNAL_STATUS_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_signal_update
            AFTER UPDATE ON canary_signals BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='SIGNAL_STATUS_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_ledger
            AFTER INSERT ON canary_ledger BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LEDGER_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_ledger_update
            AFTER UPDATE ON canary_ledger BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LEDGER_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_execution
            AFTER INSERT ON canary_execution_events BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='EXECUTION_EVENT_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_execution_update
            AFTER UPDATE ON canary_execution_events BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='EXECUTION_EVENT_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_lifecycle_delete
            AFTER DELETE ON candidate_lifecycle BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LIFECYCLE_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_eligibility_delete
            AFTER DELETE ON canary_eligibility BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='ELIGIBILITY_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_rankings_delete
            AFTER DELETE ON canary_rankings BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='RANKINGS_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_selection_delete
            AFTER DELETE ON canary_selection BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='SELECTION_CHANGED'
              WHERE singleton=1;
            END;
            """)
            initial_payload = self._initial_readiness_payload()
            initial_now = ensure_utc(self.clock()).isoformat()
            initial_generation = initial_payload.get("control_generation")
            self.store.connection.execute(
                "INSERT OR IGNORE INTO canary_readiness_snapshot("
                "singleton,payload_json,readiness_snapshot_status,"
                "readiness_snapshot_stale,readiness_snapshot_reason,"
                "readiness_snapshot_updated_at,source_control_generation,"
                "projection_version) VALUES(1,?,?,?,?,?,?,?)",
                (
                    json.dumps(
                        initial_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                    "STALE",
                    1,
                    "READINESS_SNAPSHOT_INITIALIZING",
                    initial_now,
                    initial_generation,
                    0,
                ),
            )

    @staticmethod
    def _readiness_snapshot_default() -> dict[str, Any]:
        limits: dict[str, Any] = {}
        risk: dict[str, Any] = {}
        autonomous = {
            "enabled": False,
            "selected_candidate": None,
            "last_selected_candidate": None,
            "ranking_run_id": None,
            "ranking_timestamp": None,
            "rank": None,
            "score": None,
            "selection_reason": None,
            "selection_status": "UNKNOWN",
            "selection_valid": None,
            "selection_invalidation_reason": None,
            "eligibility_raw_count": None,
            "eligible_count": None,
            "rankable_raw_count": None,
            "rankable_count": None,
            "historical_data_integrity": "UNKNOWN",
            "historical_execution_fidelity": "UNKNOWN",
            "current_execution_evidence": "UNKNOWN",
            "next_decision": None,
            "blocker": None,
            "last_tick_at": None,
            "last_tick_started_at": None,
            "last_tick_completed_at": None,
            "last_successful_tick": None,
            "last_error_code": None,
            "consecutive_failures": None,
            "next_retry_at": None,
            "candidates_evaluated": None,
            "signals_generated": None,
            "orders_attempted": None,
            "candidates_ranked": None,
            "candidates_signal_checked": None,
            "candidates_no_signal": None,
            "actionable_candidates_found": None,
            "selected_actionable_candidate": None,
            "selected_actionable_rank": None,
            "selected_actionable_score": None,
            "signal_scan_cursor": 0,
            "signal_scan_ranking_run_id": None,
            "next_signal_scan_start_rank": None,
            "next_signal_scan_end_rank": None,
            "signal_scan_cycle_id": None,
            "signal_scan_candidate_universe_hash": None,
            "signal_scan_cycle_started_at": None,
            "signal_scan_cycle_completed_at": None,
            "signal_scan_cycle_complete": 0,
            "signal_scan_checked_this_cycle": 0,
            "signal_scan_coverage_percentage": 0.0,
            "signal_scan_skip_reasons_json": "{}",
            "signal_scan_reason_counts_json": "{}",
            "signal_scan_status": "UNKNOWN",
            "signal_scan_checked_keys": [],
            "last_signal_id": None,
            "worker_status": "UNKNOWN",
        }
        return {
            "production_live_trading": "DISABLED",
            "micro_live_canary": "UNKNOWN",
            "display_state": "UNKNOWN",
            "control_state": "UNKNOWN",
            "candidate": None,
            "winner_id": None,
            "winner_rank": None,
            "winner_score": None,
            "selection_reason": None,
            "selection_status": "UNKNOWN",
            "selection_valid": None,
            "selection_invalidation_reason": None,
            "selected_candidate": None,
            "last_selected_candidate": None,
            "ranking_run_id": None,
            "ranking_timestamp": None,
            "venue": None,
            "expiry": None,
            "control_generation": None,
            "last_request_status": None,
            "today_orders": None,
            "today_realized_pnl": None,
            "total_exposure": None,
            "open_positions": None,
            "limits": limits,
            "risk_envelope": risk,
            "risk_limits": dict(risk),
            "eligibility_raw_count": None,
            "eligible_count": None,
            "rankable_raw_count": None,
            "rankable_count": None,
            "candidates_ranked": None,
            "candidates_signal_checked": None,
            "candidates_no_signal": None,
            "actionable_candidates_found": None,
            "selected_actionable_candidate": None,
            "selected_actionable_rank": None,
            "selected_actionable_score": None,
            "signal_scan_cursor": 0,
            "signal_scan_ranking_run_id": None,
            "next_signal_scan_start_rank": None,
            "next_signal_scan_end_rank": None,
            "signal_scan_cycle_id": None,
            "signal_scan_candidate_universe_hash": None,
            "signal_scan_cycle_started_at": None,
            "signal_scan_cycle_completed_at": None,
            "signal_scan_cycle_complete": 0,
            "signal_scan_checked_this_cycle": 0,
            "signal_scan_remaining_this_cycle": 0,
            "signal_scan_coverage_percentage": 0.0,
            "signal_scan_skip_reasons_json": "{}",
            "signal_scan_reason_counts_json": "{}",
            "signal_scan_status": "UNKNOWN",
            "signal_scan_checked_keys": [],
            "real_execution_events": None,
            "execution_event_count": None,
            "historical_data_integrity": "UNKNOWN",
            "historical_execution_fidelity": "UNKNOWN",
            "current_execution_evidence": "UNKNOWN",
            "daily_loss_budget_remaining": None,
            "autonomous": autonomous,
            "selected_winner": None,
            "latest_signal": None,
            "trades": [],
            "live_execution": False,
            "readiness_evaluation_error_code": None,
            "readiness_snapshot_version": None,
            "kill_semantics": "KILL_PREVENTS_NEW_SUBMISSIONS; IN_FLIGHT_REQUESTS_ARE_NOT_RETRACTED",
        }
    def _initial_readiness_payload(self) -> dict[str, Any]:
        """Project only durable control/selection metadata at startup.

        This path intentionally does not validate candidates or run ranking/data
        quality checks.  Counts and eligibility claims remain unknown until a
        bounded authoritative publication completes.
        """
        payload = self._readiness_snapshot_default()
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)

        def fetchone(query: str) -> Any:
            try:
                if lock is None:
                    return connection.execute(query).fetchone()
                with lock:
                    return connection.execute(query).fetchone()
            except sqlite3.OperationalError:
                return None

        def fetchall(query: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
            try:
                if lock is None:
                    return connection.execute(query, parameters).fetchall()
                with lock:
                    return connection.execute(query, parameters).fetchall()
            except sqlite3.OperationalError:
                return []

        control = fetchone(
            "SELECT state,candidate_id,venue,expires_at,control_generation,"
            "updated_at,limits_json,settings_config_id,settings_generation "
            "FROM canary_control WHERE singleton=1"
        )
        selection = fetchone(
            # ``canary_selection`` predates the readiness projection columns.
            # Read only the stable singleton fields so a cold-start display
            # can retain the durable candidate as history without treating
            # it as a current, validated selection.
            "SELECT candidate_id,rank,total_score,reason,selected_at "
            "FROM canary_selection WHERE singleton=1"
        )
        autonomous = fetchone(
            "SELECT last_tick_at,last_tick_started_at,last_tick_completed_at,"
            "last_successful_tick,last_error_code,consecutive_failures,next_retry_at,"
            "candidates_evaluated,signals_generated,orders_attempted,"
            "candidates_ranked,candidates_signal_checked,candidates_no_signal,"
            "actionable_candidates_found,selected_actionable_candidate,"
            "selected_actionable_rank,selected_actionable_score,signal_scan_cursor,"
            "signal_scan_ranking_run_id,next_signal_scan_start_rank,next_signal_scan_end_rank,"
            "signal_scan_cycle_id,signal_scan_candidate_universe_hash,"
            "signal_scan_cycle_started_at,signal_scan_cycle_completed_at,"
            "signal_scan_cycle_complete,signal_scan_checked_this_cycle,"
            "signal_scan_remaining_this_cycle,signal_scan_coverage_percentage,"
            "signal_scan_skip_reasons_json,signal_scan_reason_counts_json,signal_scan_status,"
            "next_decision,blocker,last_signal_id,worker_status FROM canary_autonomous_state "
            "WHERE singleton=1"
        )
        if control is not None:
            state = str(control["state"] or "UNKNOWN").upper()
            payload["micro_live_canary"] = state
            payload["control_state"] = state
            payload["display_state"] = (
                "ENABLED" if state in {"ARMED", AUTONOMOUS_MICRO_LIVE}
                else "KILLED" if state == "KILLED"
                else "DISABLED" if state in {"DISARMED", "DISABLED"}
                else "UNKNOWN"
            )
            payload["candidate"] = control["candidate_id"]
            payload["venue"] = control["venue"]
            payload["expiry"] = control["expires_at"]
            try:
                payload["control_generation"] = int(control["control_generation"])
            except (TypeError, ValueError):
                payload["control_generation"] = None
            payload["settings_config_id"] = control["settings_config_id"]
            try:
                payload["settings_generation"] = (
                    int(control["settings_generation"])
                    if control["settings_generation"] is not None
                    else None
                )
            except (TypeError, ValueError):
                payload["settings_generation"] = None
            try:
                limits = json.loads(control["limits_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                limits = None
            if isinstance(limits, Mapping):
                payload["limits"] = self._readiness_snapshot_sanitize(limits, key="limits")
        if selection is not None:
            historical_candidate = selection["candidate_id"]
            payload["selected_candidate"] = None
            payload["last_selected_candidate"] = historical_candidate
            payload["winner_id"] = None
            payload["winner_rank"] = None
            payload["winner_score"] = None
            payload["selection_reason"] = (
                str(selection["reason"] or "") or None
            )
            payload["ranking_run_id"] = None
            payload["ranking_timestamp"] = None
            payload["selection_status"] = "UNKNOWN"
            payload["selection_valid"] = None
            payload["selection_invalidation_reason"] = None
        if autonomous is not None:
            auto = payload["autonomous"]
            auto.update(
                {
                    key: autonomous[key]
                    for key in (
                        "last_tick_at",
                        "last_tick_started_at",
                        "last_tick_completed_at",
                        "last_successful_tick",
                        "last_error_code",
                        "consecutive_failures",
                        "next_retry_at",
                        "candidates_evaluated",
                        "signals_generated",
                        "orders_attempted",
                        "candidates_ranked",
                        "candidates_signal_checked",
                        "candidates_no_signal",
                        "actionable_candidates_found",
                        "selected_actionable_candidate",
                        "selected_actionable_rank",
                        "selected_actionable_score",
                        "signal_scan_cursor",
                        "signal_scan_ranking_run_id",
                        "next_signal_scan_start_rank",
                        "next_signal_scan_end_rank",
                        "signal_scan_cycle_id",
                        "signal_scan_candidate_universe_hash",
                        "signal_scan_cycle_started_at",
                        "signal_scan_cycle_completed_at",
                        "signal_scan_cycle_complete",
                        "signal_scan_checked_this_cycle",
                        "signal_scan_remaining_this_cycle",
                        "signal_scan_coverage_percentage",
                        "signal_scan_skip_reasons_json",
                        "signal_scan_reason_counts_json",
                        "signal_scan_status",
                        "next_decision",
                        "blocker",
                        "last_signal_id",
                        "worker_status",
                    )
                }
            )
        if autonomous is not None:
            cycle_fields = (
                "signal_scan_cycle_id",
                "signal_scan_candidate_universe_hash",
                "signal_scan_cycle_started_at",
                "signal_scan_cycle_completed_at",
                "signal_scan_cycle_complete",
                "signal_scan_checked_this_cycle",
                "signal_scan_remaining_this_cycle",
                "signal_scan_coverage_percentage",
                "signal_scan_skip_reasons_json",
                "signal_scan_reason_counts_json",
                "signal_scan_status",
            )
            for name in cycle_fields:
                payload[name] = autonomous[name]
            cycle_id = str(autonomous["signal_scan_cycle_id"] or "").strip()
            checked_rows = fetchall(
                "SELECT cycle_id,candidate_id,qualification_hash,checked_at,"
                "rank_at_check,ranking_run_id FROM canary_signal_scan_checked "
                "WHERE cycle_id=? ORDER BY checked_at,candidate_id,qualification_hash",
                (cycle_id,),
            ) if cycle_id else []
            payload["signal_scan_checked_keys"] = [dict(row) for row in checked_rows]
        # Keep the nested autonomous object a complete legacy projection even
        # before the first authoritative evaluation.  Selection values are
        # durable metadata and are safe to copy here; qualification counts
        # and quality fields remain their UNKNOWN/null defaults.
        auto = payload.get("autonomous")
        if isinstance(auto, Mapping):
            auto = dict(auto)
            auto.update(
                {
                    "enabled": payload["micro_live_canary"] == AUTONOMOUS_MICRO_LIVE,
                    "selected_candidate": payload["selected_candidate"],
                    "last_selected_candidate": payload["last_selected_candidate"],
                    "ranking_run_id": payload["ranking_run_id"],
                    "ranking_timestamp": payload["ranking_timestamp"],
                    "rank": payload["winner_rank"],
                    "score": payload["winner_score"],
                    "selection_reason": payload["selection_reason"],
                    "selection_status": payload["selection_status"],
                    "selection_valid": payload["selection_valid"],
                    "selection_invalidation_reason": payload[
                        "selection_invalidation_reason"
                    ],
                    "signal_scan_checked_keys": payload["signal_scan_checked_keys"],
                }
            )
            payload["autonomous"] = auto
        return payload
    def upsert_initializing_readiness_snapshot(
        self,
        *,
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Persist a truthful cold-start projection without qualification work.

        Startup may expose durable control, historical selection, and worker
        metadata, but it must not claim that selection or data-quality
        evaluation succeeded.  Resetting the projection version also fences
        any prior successful row so this initializer can never become
        last-good evidence.
        """
        when = ensure_utc(timestamp or self.clock()).isoformat()

        def operation() -> dict[str, Any]:
            with self.store.transaction(immediate=True):
                payload = self._initial_readiness_payload()
                encoded = json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                self.store.connection.execute(
                    "INSERT INTO canary_readiness_snapshot("
                    "singleton,payload_json,readiness_snapshot_status,"
                    "readiness_snapshot_stale,readiness_snapshot_reason,"
                    "readiness_snapshot_updated_at,source_control_generation,"
                    "projection_version) VALUES(1,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "payload_json=excluded.payload_json,"
                    "readiness_snapshot_status=excluded.readiness_snapshot_status,"
                    "readiness_snapshot_stale=excluded.readiness_snapshot_stale,"
                    "readiness_snapshot_reason=excluded.readiness_snapshot_reason,"
                    "readiness_snapshot_updated_at=excluded.readiness_snapshot_updated_at,"
                    "source_control_generation=excluded.source_control_generation,"
                    "projection_version=excluded.projection_version",
                    (
                        encoded,
                        "STALE",
                        1,
                        "READINESS_SNAPSHOT_INITIALIZING",
                        when,
                        payload.get("control_generation"),
                        0,
                    ),
                )
                return dict(payload)

        sqlite_retry(
            operation,
            operation_name="upsert initializing canary readiness snapshot",
        )
        return self.readiness_snapshot()

    @staticmethod
    def _readiness_snapshot_sanitize(
        value: Any,
        *,
        key: str = "",
        depth: int = 0,
        limit: int = 100,
    ) -> Any:
        """Copy bounded JSON display data while dropping secret-shaped keys."""
        lowered = str(key).lower()
        if any(secret in lowered for secret in ("secret", "credential", "private_key", "wallet", "api_key")):
            return None
        if depth > 8:
            return None
        if value is None or isinstance(value, (str, bool, int)):
            if isinstance(value, str):
                if lowered.endswith("reason_counts_json"):
                    try:
                        parsed = json.loads(value or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        return "{}"
                    if isinstance(parsed, Mapping):
                        return json.dumps(
                            parsed,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        )
                return value[:512]
            return value
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, Mapping):
            result: dict[str, Any] = {}
            for name in sorted(value, key=lambda item: str(item))[:64]:
                name_text = str(name)
                child = CanaryService._readiness_snapshot_sanitize(
                    value[name_text] if name_text in value else value[name],
                    key=name_text,
                    depth=depth + 1,
                    limit=limit,
                )
                if child is not None:
                    result[name_text[:128]] = child
            return result
        if isinstance(value, (list, tuple)):
            result = []
            for item in value[:limit]:
                child = CanaryService._readiness_snapshot_sanitize(
                    item,
                    depth=depth + 1,
                    limit=limit,
                )
                if child is not None:
                    result.append(child)
            return result
        return None

    @classmethod
    def _readiness_snapshot_payload(
        cls,
        source: Mapping[str, Any] | None,
        *,
        latest_signal: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        defaults = cls._readiness_snapshot_default()
        source = source if isinstance(source, Mapping) else {}
        fields = tuple(defaults)
        payload: dict[str, Any] = {}
        for name in fields:
            value = source.get(name, defaults[name])
            if name == "autonomous" and isinstance(value, Mapping):
                # Older snapshots may contain only the fields that happened to
                # be populated at the time.  Keep the nested legacy contract
                # complete without doing an authoritative read.
                value = {**defaults["autonomous"], **dict(value)}
            sanitized = cls._readiness_snapshot_sanitize(
                value,
                key=name,
                limit=100 if name == "trades" else 64,
            )
            if name == "autonomous":
                # ``_readiness_snapshot_sanitize`` omits null mapping values;
                # restore contract keys so initializing state remains
                # explicitly UNKNOWN/null rather than disappearing.
                sanitized = {
                    **defaults["autonomous"],
                    **(sanitized if isinstance(sanitized, Mapping) else {}),
                }
            payload[name] = defaults[name] if sanitized is None and value is not None else sanitized
        if latest_signal is None:
            latest_signal = source.get("latest_signal")
        payload["latest_signal"] = cls._readiness_snapshot_sanitize(
            latest_signal,
            key="latest_signal",
            limit=1,
        )
        trades = payload.get("trades")
        payload["trades"] = list(trades[:100]) if isinstance(trades, list) else []
        return payload
    def _patch_autonomous_snapshot(self) -> None:
        """Patch autonomous liveness fields without requalifying selection."""
        fields = (
            "last_tick_at",
            "last_tick_started_at",
            "last_tick_completed_at",
            "last_successful_tick",
            "last_error_code",
            "consecutive_failures",
            "next_retry_at",
            "candidates_evaluated",
            "signals_generated",
            "orders_attempted",
            "candidates_ranked",
            "candidates_signal_checked",
            "candidates_no_signal",
            "actionable_candidates_found",
            "selected_actionable_candidate",
            "selected_actionable_rank",
            "selected_actionable_score",
            "signal_scan_cursor",
            "signal_scan_ranking_run_id",
            "next_signal_scan_start_rank",
            "next_signal_scan_end_rank",
            "signal_scan_cycle_id",
            "signal_scan_candidate_universe_hash",
            "signal_scan_cycle_started_at",
            "signal_scan_cycle_completed_at",
            "signal_scan_cycle_complete",
            "signal_scan_checked_this_cycle",
            "signal_scan_remaining_this_cycle",
            "signal_scan_coverage_percentage",
            "signal_scan_skip_reasons_json",
            "signal_scan_reason_counts_json",
            "signal_scan_status",
            "next_decision",
            "blocker",
            "last_signal_id",
            "worker_status",
        )

        def operation() -> None:
            with self.store.transaction(immediate=True):
                state = self.store.connection.execute(
                    "SELECT "
                    + ",".join(fields)
                    + " FROM canary_autonomous_state WHERE singleton=1"
                ).fetchone()
                if state is None:
                    return
                row = self.store.connection.execute(
                    "SELECT payload_json,readiness_snapshot_status,"
                    "readiness_snapshot_stale,readiness_snapshot_reason,"
                    "readiness_snapshot_updated_at "
                    "FROM canary_readiness_snapshot WHERE singleton=1"
                ).fetchone()
                if row is None:
                    payload = self._readiness_snapshot_default()
                    status = "STALE"
                    stale = 1
                    reason = "READINESS_SNAPSHOT_INITIALIZING"
                    updated_at = ensure_utc(self.clock()).isoformat()
                    insert = True
                else:
                    try:
                        parsed = json.loads(str(row["payload_json"] or ""))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        # Never replace a malformed persisted qualification
                        # payload with an autonomous-only default.
                        return
                    if not isinstance(parsed, Mapping):
                        return
                    payload = self._readiness_snapshot_payload(parsed)
                    status = str(row["readiness_snapshot_status"] or "STALE")
                    stale = int(row["readiness_snapshot_stale"] or 0)
                    reason = str(
                        row["readiness_snapshot_reason"]
                        or "READINESS_SNAPSHOT_INITIALIZING"
                    )[:256]
                    updated_at = str(
                        row["readiness_snapshot_updated_at"]
                        or ensure_utc(self.clock()).isoformat()
                    )
                    insert = False
                auto = payload.get("autonomous")
                auto = (
                    {**self._readiness_snapshot_default()["autonomous"], **dict(auto)}
                    if isinstance(auto, Mapping)
                    else dict(self._readiness_snapshot_default()["autonomous"])
                )
                cycle_fields = (
                    "signal_scan_cycle_id",
                    "signal_scan_candidate_universe_hash",
                    "signal_scan_cycle_started_at",
                    "signal_scan_cycle_completed_at",
                    "signal_scan_cycle_complete",
                    "signal_scan_checked_this_cycle",
                    "signal_scan_remaining_this_cycle",
                    "signal_scan_coverage_percentage",
                    "signal_scan_skip_reasons_json",
                    "signal_scan_reason_counts_json",
                    "signal_scan_status",
                )
                auto.update({name: state[name] for name in fields})
                auto.update({name: state[name] for name in cycle_fields})
                cycle_id = str(state["signal_scan_cycle_id"] or "").strip()
                checked_rows = (
                    self.store.connection.execute(
                        "SELECT cycle_id,candidate_id,qualification_hash,checked_at,"
                        "rank_at_check,ranking_run_id "
                        "FROM canary_signal_scan_checked WHERE cycle_id=? "
                        "ORDER BY checked_at,candidate_id,qualification_hash",
                        (cycle_id,),
                    ).fetchall()
                    if cycle_id
                    else []
                )
                checked_keys = [dict(row) for row in checked_rows]
                auto["signal_scan_checked_keys"] = checked_keys
                payload.update(
                    {name: state[name] for name in cycle_fields}
                )
                payload["signal_scan_checked_keys"] = checked_keys
                payload["autonomous"] = auto
                encoded = json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                if insert:
                    self.store.connection.execute(
                        "INSERT INTO canary_readiness_snapshot("
                        "singleton,payload_json,readiness_snapshot_status,"
                        "readiness_snapshot_stale,readiness_snapshot_reason,"
                        "readiness_snapshot_updated_at) VALUES(1,?,?,?,?,?)",
                        (encoded, status, stale, reason, updated_at),
                    )
                else:
                    # Deliberately do not touch status, stale, reason, or
                    # timestamp: autonomous updates are not qualification.
                    self.store.connection.execute(
                        "UPDATE canary_readiness_snapshot SET payload_json=? "
                        "WHERE singleton=1",
                        (encoded,),
                    )

        try:
            sqlite_retry(
                operation,
                operation_name="patch canary autonomous readiness snapshot",
            )
        except Exception:
            _LOGGER.debug(
                "canary autonomous readiness snapshot patch failed",
                exc_info=True,
            )

    @staticmethod
    def _readiness_snapshot_has_last_good(payload: Mapping[str, Any]) -> bool:
        """Return whether ``payload`` came from a successful publication."""
        return _canary_has_last_good(payload)


    @staticmethod
    def _readiness_snapshot_stale_payload(
        payload: Mapping[str, Any],
        *,
        preserve_last_good: bool = False,
    ) -> dict[str, Any]:
        result = dict(payload)
        # A stale projection may no longer assert qualification or execute a
        # selection, but a previously successful publication is still useful
        # operator evidence.  Only an initializing/missing/invalid
        # never-evaluated payload is allowed to lose its counts.
        if not preserve_last_good:
            result["eligibility_raw_count"] = None
            result["eligible_count"] = None
            result["rankable_raw_count"] = None
            result["rankable_count"] = None
        historical_candidate = (
            result.get("last_selected_candidate")
            or result.get("selected_candidate")
            or result.get("winner_id")
        )
        current = (
            result.get("selection_valid") is True
            or result.get("selected_candidate") is not None
            or result.get("winner_id") is not None
        )
        if historical_candidate is not None:
            result["last_selected_candidate"] = historical_candidate
        if current:
            if preserve_last_good:
                result["selection_status"] = "STALE"
                result["selection_valid"] = False
                result["selection_invalidation_reason"] = (
                    result.get("selection_invalidation_reason") or "READINESS_SNAPSHOT_STALE"
                )
            else:
                result["selection_status"] = "UNKNOWN"
                result["selection_valid"] = None
                result["selection_invalidation_reason"] = result.get(
                    "selection_invalidation_reason"
                )
            result["selected_candidate"] = None
            result["winner_id"] = None
            result["winner_rank"] = None
            result["winner_score"] = None
        elif not preserve_last_good and str(result.get("selection_status") or "").upper() in {
            "CURRENT",
            "STALE",
            "NONE",
        }:
            result["selection_status"] = "UNKNOWN"
            result["selection_valid"] = None

        selected_winner = result.get("selected_winner")
        if isinstance(selected_winner, Mapping):
            winner = dict(selected_winner)
            winner["candidate_id"] = historical_candidate
            winner["selection_status"] = "STALE" if preserve_last_good else "UNKNOWN"
            winner["selection_valid"] = False if preserve_last_good else None
            winner["selected_candidate"] = None
            winner["last_selected_candidate"] = historical_candidate
            winner["winner_id"] = None
            winner["winner_rank"] = None
            winner["winner_score"] = None
            winner["selection_invalidation_reason"] = result.get(
                "selection_invalidation_reason"
            )
            result["selected_winner"] = winner
        autonomous = result.get("autonomous")
        if isinstance(autonomous, Mapping):
            auto = dict(autonomous)
            auto["selection_status"] = result.get(
                "selection_status",
                "STALE" if preserve_last_good else "UNKNOWN",
            )
            auto["selection_valid"] = False if preserve_last_good else None
            auto["selected_candidate"] = None
            auto["last_selected_candidate"] = historical_candidate
            auto["winner_id"] = None
            auto["winner_rank"] = None
            auto["winner_score"] = None
            auto["selection_invalidation_reason"] = result.get(
                "selection_invalidation_reason"
            )
            if not preserve_last_good:
                auto["eligibility_raw_count"] = None
                auto["eligible_count"] = None
                auto["rankable_raw_count"] = None
                auto["rankable_count"] = None
            result["autonomous"] = auto
        return result
    def _readiness_snapshot_failure_safe_payload(
        self,
        payload: Mapping[str, Any] | None,
        *,
        reason: str,
        updated_at: str | None = None,
        error_code: str | None = None,
        projection_version: Any = None,
        readiness_status: Any = None,
        readiness_stale: Any = None,
        readiness_reason: Any = None,
    ) -> dict[str, Any]:
        persisted_projection_version = projection_version
        persisted_readiness_status = readiness_status
        persisted_readiness_stale = readiness_stale
        persisted_readiness_reason = readiness_reason
        if (
            payload is None
            or persisted_projection_version is None
            or persisted_readiness_status is None
            or persisted_readiness_stale is None
            or persisted_readiness_reason is None
        ):
            try:
                lock = getattr(self.store, "_lock", None)
                if lock is None:
                    row = self.store.connection.execute(
                        "SELECT payload_json,projection_version,"
                        "readiness_snapshot_status,readiness_snapshot_stale,"
                        "readiness_snapshot_reason "
                        "FROM canary_readiness_snapshot WHERE singleton=1"
                    ).fetchone()
                else:
                    with lock:
                        row = self.store.connection.execute(
                            "SELECT payload_json,projection_version,"
                            "readiness_snapshot_status,readiness_snapshot_stale,"
                            "readiness_snapshot_reason "
                            "FROM canary_readiness_snapshot WHERE singleton=1"
                        ).fetchone()
                if row is not None:
                    if payload is None:
                        parsed = json.loads(str(row["payload_json"] or ""))
                        if isinstance(parsed, Mapping):
                            payload = parsed
                    if persisted_projection_version is None:
                        persisted_projection_version = row["projection_version"]
                    if persisted_readiness_status is None:
                        persisted_readiness_status = row["readiness_snapshot_status"]
                    if persisted_readiness_stale is None:
                        persisted_readiness_stale = row["readiness_snapshot_stale"]
                    if persisted_readiness_reason is None:
                        persisted_readiness_reason = row["readiness_snapshot_reason"]
            except (sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
                payload = None
        try:
            bounded = self._readiness_snapshot_payload(
                payload if isinstance(payload, Mapping) else self._readiness_snapshot_default()
            )
        except Exception:
            bounded = self._readiness_snapshot_default()
        preserve_last_good = _canary_has_last_good(
            bounded,
            reason=(
                persisted_readiness_reason
                if persisted_readiness_reason is not None
                else reason
            ),
            projection_version=persisted_projection_version,
            readiness_status=persisted_readiness_status,
            readiness_stale=persisted_readiness_stale,
        )
        result = self._readiness_snapshot_stale_payload(
            bounded,
            preserve_last_good=preserve_last_good,
        )
        result["readiness_snapshot_status"] = "STALE"
        result["readiness_snapshot_stale"] = True
        result["readiness_snapshot_reason"] = str(reason or "READINESS_SNAPSHOT_STALE")[:256]
        result["readiness_evaluation_error_code"] = (
            str(error_code)[:64] if error_code else None
        )
        result["readiness_snapshot_updated_at"] = (
            str(updated_at) if updated_at else ensure_utc(self.clock()).isoformat()
        )
        autonomous = result.get("autonomous")
        if isinstance(autonomous, Mapping):
            autonomous = dict(autonomous)
            autonomous.update(
                {
                    "readiness_snapshot_status": "STALE",
                    "readiness_snapshot_stale": True,
                    "readiness_snapshot_reason": result["readiness_snapshot_reason"],
                    "readiness_snapshot_updated_at": result["readiness_snapshot_updated_at"],
                    "readiness_evaluation_error_code": result[
                        "readiness_evaluation_error_code"
                    ],
                }
            )
            result["autonomous"] = autonomous
        return result
    def _readiness_projection_version(self) -> int:
        """Read the persisted projection fence without an authoritative scan."""
        lock = getattr(self.store, "_lock", None)
        try:
            if lock is None:
                row = self.store.connection.execute(
                    "SELECT projection_version FROM canary_readiness_snapshot "
                    "WHERE singleton=1"
                ).fetchone()
            else:
                with lock:
                    row = self.store.connection.execute(
                        "SELECT projection_version FROM canary_readiness_snapshot "
                        "WHERE singleton=1"
                    ).fetchone()
            if row is None:
                return 0
            value = row["projection_version"]
            if isinstance(value, bool):
                return 0
            return int(value or 0)
        except (sqlite3.Error, TypeError, ValueError, OverflowError):
            # An unavailable fence must never authorize a destructive failure
            # publication.  No valid projection uses a negative version.
            return -1

    def _persist_evaluation_failure(
        self,
        *,
        error_code: str,
        updated_at: str | None = None,
        expected_projection_version: int | None = None,
    ) -> dict[str, Any]:
        """Fence a failed evaluation while retaining the prior selection."""
        now = updated_at or ensure_utc(self.clock()).isoformat()

        def unchanged_payload(row: Any) -> dict[str, Any]:
            try:
                parsed = json.loads(str(row["payload_json"] or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = None
            if isinstance(parsed, Mapping):
                return dict(parsed)
            return self._readiness_snapshot_default()

        try:
            with self.store.transaction(immediate=True):
                row = self.store.connection.execute(
                    "SELECT payload_json,projection_version,"
                    "readiness_snapshot_status,readiness_snapshot_stale,"
                    "readiness_snapshot_reason "
                    "FROM canary_readiness_snapshot WHERE singleton=1"
                ).fetchone()
                if row is None:
                    current_version = 0
                else:
                    try:
                        current_version = int(row["projection_version"] or 0)
                    except (TypeError, ValueError, OverflowError):
                        current_version = -1
                expected_version = (
                    current_version
                    if expected_projection_version is None
                    else (
                        -1
                        if isinstance(expected_projection_version, bool)
                        else int(expected_projection_version)
                    )
                )
                try:
                    parsed = json.loads(str(row["payload_json"])) if row else None
                except (TypeError, ValueError, json.JSONDecodeError):
                    parsed = None
                result = self._readiness_snapshot_failure_safe_payload(
                    parsed if isinstance(parsed, Mapping) else self._initial_readiness_payload(),
                    reason="EVALUATION_FAILED",
                    updated_at=now,
                    error_code=error_code,
                    projection_version=(row["projection_version"] if row else None),
                    readiness_status=(row["readiness_snapshot_status"] if row else None),
                    readiness_stale=(row["readiness_snapshot_stale"] if row else None),
                    readiness_reason=(row["readiness_snapshot_reason"] if row else None),
                )
                encoded = json.dumps(
                    result,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                if row is None:
                    updated = None
                else:
                    updated = self.store.connection.execute(
                        "UPDATE canary_readiness_snapshot SET payload_json=?,"
                        "readiness_snapshot_status='STALE',readiness_snapshot_stale=1,"
                        "readiness_snapshot_reason=?,readiness_snapshot_updated_at=? "
                        "WHERE singleton=1 AND projection_version=?",
                        (
                            encoded,
                            "EVALUATION_FAILED",
                            now,
                            expected_version,
                        ),
                    )
                if updated is not None and updated.rowcount == 1:
                    return result
                current = self.store.connection.execute(
                    "SELECT payload_json FROM canary_readiness_snapshot "
                    "WHERE singleton=1"
                ).fetchone()
                if current is not None:
                    # A newer publication won the race.  Do not expose or
                    # persist the stale failure projection over that snapshot.
                    return unchanged_payload(current)
                return result
        except Exception:
            _LOGGER.exception("failed to persist canary evaluation failure")
            return self._readiness_snapshot_failure_safe_payload(
                None,
                reason="EVALUATION_FAILED",
                updated_at=now,
                error_code=error_code,
            )


    def publish_readiness_snapshot(
        self,
        reason: str = "AUTHORITATIVE_UPDATE",
    ) -> dict[str, Any]:
        """Persist one bounded, credential-free authoritative display snapshot.

        The authoritative read and projection write share one immediate
        transaction.  This prevents an older read from overwriting a newer
        snapshot when concurrent writers publish out of order.
        """
        snapshot_reason = str(reason or "AUTHORITATIVE_UPDATE").strip()[:256]
        if not snapshot_reason:
            snapshot_reason = "AUTHORITATIVE_UPDATE"
        payload: dict[str, Any] | None = None
        expected_projection_version = self._readiness_projection_version()
        try:
            def operation() -> dict[str, Any]:
                nonlocal payload
                with self.store.transaction(immediate=True):
                    authoritative = self.authoritative_status()
                    latest_signal = self.latest_signal()
                    projected = self._readiness_snapshot_payload(
                        authoritative,
                        latest_signal=latest_signal,
                    )
                    now = ensure_utc(self.clock()).isoformat()
                    projected["readiness_snapshot_status"] = "CURRENT"
                    projected["readiness_snapshot_stale"] = False
                    projected["readiness_snapshot_reason"] = snapshot_reason
                    projected["readiness_snapshot_updated_at"] = now
                    autonomous = projected.get("autonomous")
                    if isinstance(autonomous, Mapping):
                        autonomous = dict(autonomous)
                        autonomous.update(
                            {
                                "readiness_snapshot_status": "CURRENT",
                                "readiness_snapshot_stale": False,
                                "readiness_snapshot_reason": snapshot_reason,
                                "readiness_snapshot_updated_at": now,
                            }
                        )
                        projected["autonomous"] = autonomous
                    source_generation = projected.get("control_generation")
                    try:
                        previous_row = self.store.connection.execute(
                            "SELECT source_control_generation,projection_version,"
                            "payload_json,readiness_snapshot_status,"
                            "readiness_snapshot_stale,readiness_snapshot_reason,"
                            "readiness_snapshot_updated_at "
                            "FROM canary_readiness_snapshot WHERE singleton=1"
                        ).fetchone()
                        previous_version = (
                            int(previous_row["projection_version"] or 0)
                            if previous_row is not None else 0
                        )
                        previous_generation = (
                            int(previous_row["source_control_generation"])
                            if previous_row is not None
                            and previous_row["source_control_generation"] is not None
                            else None
                        )
                    except (sqlite3.Error, TypeError, ValueError):
                        previous_row = None
                        previous_version = 0
                        previous_generation = None
                    # Only a newer CURRENT projection fences this write.  A
                    # stale predecessor must never win over a successful
                    # authoritative publication, even when its control
                    # generation is newer.
                    if (
                        source_generation is not None
                        and previous_generation is not None
                        and int(source_generation) < previous_generation
                        and previous_row is not None
                        and str(
                            previous_row["readiness_snapshot_status"] or ""
                        ).upper()
                        == "CURRENT"
                        and str(
                            previous_row["readiness_snapshot_stale"] or ""
                        ).strip().lower()
                        in {"0", "false"}
                    ):
                        try:
                            previous_payload = json.loads(
                                str(previous_row["payload_json"] or "")
                            )
                        except (TypeError, ValueError, json.JSONDecodeError):
                            previous_payload = self._initial_readiness_payload()
                        if isinstance(previous_payload, Mapping):
                            fenced = self._readiness_snapshot_payload(previous_payload)
                            fenced["readiness_snapshot_status"] = str(
                                previous_row["readiness_snapshot_status"] or "STALE"
                            )
                            fenced["readiness_snapshot_stale"] = bool(
                                previous_row["readiness_snapshot_stale"]
                            )
                            fenced["readiness_snapshot_reason"] = str(
                                previous_row["readiness_snapshot_reason"]
                                or "READINESS_SNAPSHOT_STALE"
                            )[:256]
                            fenced["readiness_snapshot_updated_at"] = (
                                previous_row["readiness_snapshot_updated_at"]
                            )
                            fenced["readiness_snapshot_version"] = previous_version
                            payload = dict(fenced)
                            return dict(fenced)
                    projection_version = previous_version + 1
                    projected["readiness_snapshot_version"] = projection_version
                    encoded = json.dumps(
                        projected,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    self.store.connection.execute(
                        "INSERT INTO canary_readiness_snapshot("
                        "singleton,payload_json,readiness_snapshot_status,"
                        "readiness_snapshot_stale,readiness_snapshot_reason,"
                        "readiness_snapshot_updated_at,source_control_generation,"
                        "projection_version) VALUES(1,?,?,?,?,?,?,?) "
                        "ON CONFLICT(singleton) DO UPDATE SET "
                        "payload_json=excluded.payload_json,"
                        "readiness_snapshot_status=excluded.readiness_snapshot_status,"
                        "readiness_snapshot_stale=excluded.readiness_snapshot_stale,"
                        "readiness_snapshot_reason=excluded.readiness_snapshot_reason,"
                        "readiness_snapshot_updated_at=excluded.readiness_snapshot_updated_at,"
                        "source_control_generation=excluded.source_control_generation,"
                        "projection_version=excluded.projection_version",
                        (
                            encoded,
                            "CURRENT",
                            0,
                            snapshot_reason,
                            now,
                            source_generation,
                            projection_version,
                        ),
                    )
                    payload = dict(projected)
                    return dict(projected)

            return dict(
                sqlite_retry(
                    operation,
                    operation_name="publish canary readiness snapshot",
                )
            )
        except Exception as exc:
            if isinstance(exc, SQLiteBusyTimeout):
                _LOGGER.warning(
                    "canary readiness snapshot publication exhausted retries reason=%s",
                    snapshot_reason,
                )
                return self._readiness_snapshot_failure_safe_payload(
                    payload,
                    reason="READINESS_SNAPSHOT_PUBLICATION_FAILED",
                )
            _LOGGER.exception(
                "canary readiness snapshot evaluation failed reason=%s",
                snapshot_reason,
            )
            error_code = str(type(exc).__name__).upper()[:64] or "UNKNOWN"
            return self._persist_evaluation_failure(
                error_code=error_code,
                expected_projection_version=expected_projection_version,
            )


    def readiness_snapshot(self) -> dict[str, Any]:
        """Read the singleton display projection without running validation."""
        now = ensure_utc(self.clock())
        lock = getattr(self.store, "_lock", None)
        try:
            if lock is None:
                row = self.store.connection.execute(
                    "SELECT payload_json,readiness_snapshot_status,"
                    "readiness_snapshot_stale,readiness_snapshot_reason,"
                    "readiness_snapshot_updated_at,projection_version "
                    "FROM canary_readiness_snapshot WHERE singleton=1"
                ).fetchone()
            else:
                with lock:
                    row = self.store.connection.execute(
                        "SELECT payload_json,readiness_snapshot_status,"
                        "readiness_snapshot_stale,readiness_snapshot_reason,"
                        "readiness_snapshot_updated_at,projection_version "
                        "FROM canary_readiness_snapshot WHERE singleton=1"
                    ).fetchone()
        except sqlite3.OperationalError:
            row = None
        reason = "READINESS_SNAPSHOT_MISSING"
        updated_at = now.isoformat()
        payload: dict[str, Any]
        current = False
        projection_was_successful = False
        if row is not None:
            raw_payload = row["payload_json"]
            try:
                parsed = json.loads(str(raw_payload))
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = None
            if (
                isinstance(parsed, Mapping)
                and "micro_live_canary" in parsed
                and "selection_status" in parsed
            ):
                payload = self._readiness_snapshot_payload(parsed)
                try:
                    projection_version = int(row["projection_version"] or 0)
                except (TypeError, ValueError):
                    projection_version = 0
                try:
                    row_stale = int(row["readiness_snapshot_stale"] or 0)
                except (TypeError, ValueError):
                    row_stale = -1
                row_updated = row["readiness_snapshot_updated_at"]
                projection_was_successful = False
                reason = str(
                    row["readiness_snapshot_reason"] or "READINESS_SNAPSHOT_STALE"
                )[:256]
                projection_was_successful = _canary_has_last_good(
                    payload,
                    reason=reason,
                    projection_version=projection_version,
                    readiness_status=row["readiness_snapshot_status"],
                    readiness_stale=row["readiness_snapshot_stale"],
                )
                if not isinstance(row_updated, str) or not row_updated:
                    current = False
                    projection_was_successful = False
                    reason = "READINESS_SNAPSHOT_INVALID"
                else:
                    updated_at = row_updated
                    try:
                        age = (
                            now - ensure_utc(datetime.fromisoformat(updated_at))
                        ).total_seconds()
                        current = (
                            str(row["readiness_snapshot_status"] or "").upper()
                            == "CURRENT"
                            and int(row["readiness_snapshot_stale"] or 0) == 0
                            and age >= 0
                            and age <= CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS
                        )
                        if age < 0:
                            current = False
                            projection_was_successful = False
                            reason = "READINESS_SNAPSHOT_INVALID"
                        elif age > CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS:
                            reason = "READINESS_SNAPSHOT_TOO_OLD"
                    except (TypeError, ValueError, OverflowError):
                        current = False
                        projection_was_successful = False
                        reason = "READINESS_SNAPSHOT_INVALID"
            else:
                payload = self._initial_readiness_payload()
                row_updated = row["readiness_snapshot_updated_at"]
                if isinstance(row_updated, str) and row_updated:
                    updated_at = row_updated
                reason = "READINESS_SNAPSHOT_INVALID"
        else:
            payload = self._initial_readiness_payload()
        if not current:
            payload = self._readiness_snapshot_stale_payload(
                payload,
                preserve_last_good=projection_was_successful,
            )
        payload["readiness_snapshot_status"] = "CURRENT" if current else "STALE"
        payload["readiness_snapshot_stale"] = not current
        payload["readiness_snapshot_reason"] = (
            "AUTHORITATIVE_UPDATE" if current and not reason else reason
        )
        payload["readiness_snapshot_updated_at"] = updated_at
        autonomous = payload.get("autonomous")
        if isinstance(autonomous, Mapping):
            autonomous = dict(autonomous)
            autonomous.update(
                {
                    "readiness_snapshot_status": payload["readiness_snapshot_status"],
                    "readiness_snapshot_stale": payload["readiness_snapshot_stale"],
                    "readiness_snapshot_reason": payload["readiness_snapshot_reason"],
                    "readiness_snapshot_updated_at": updated_at,
                }
            )
            payload["autonomous"] = autonomous
        return payload

    def mark_readiness_snapshot_stale(self, reason: str) -> dict[str, Any]:
        """Best-effort stale projection update after a durable source transition."""
        snapshot_reason = str(reason or "AUTHORITATIVE_UPDATE").strip()[:256]
        if not snapshot_reason:
            snapshot_reason = "AUTHORITATIVE_UPDATE"
        now = ensure_utc(self.clock()).isoformat()
        try:
            def operation() -> None:
                with self.store.transaction(immediate=True):
                    row = self.store.connection.execute(
                        "SELECT payload_json,readiness_snapshot_status,"
                        "readiness_snapshot_stale FROM canary_readiness_snapshot "
                        "WHERE singleton=1"
                    ).fetchone()
                    # This callback runs after the lifecycle mutation commits.
                    # A CURRENT row does not prove that its publication
                    # included the mutation, so invalidate it unless the
                    # caller explicitly republishes after this callback.
                    if row is None:
                        payload = self._readiness_snapshot_default()
                        payload["selection_invalidation_reason"] = snapshot_reason
                        autonomous = payload.get("autonomous")
                        if isinstance(autonomous, Mapping):
                            autonomous = dict(autonomous)
                            autonomous["selection_invalidation_reason"] = snapshot_reason
                            payload["autonomous"] = autonomous
                        self.store.connection.execute(
                            "INSERT INTO canary_readiness_snapshot("
                            "singleton,payload_json,readiness_snapshot_status,"
                            "readiness_snapshot_stale,readiness_snapshot_reason,"
                            "readiness_snapshot_updated_at) VALUES(1,?,?,?,?,?)",
                            (
                                json.dumps(
                                    payload,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                    allow_nan=False,
                                ),
                                "STALE",
                                1,
                                snapshot_reason,
                                now,
                            ),
                        )
                        return
                    try:
                        parsed = json.loads(str(row["payload_json"] or ""))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        parsed = None
                    payload = (
                        dict(parsed)
                        if isinstance(parsed, Mapping)
                        else self._readiness_snapshot_default()
                    )
                    payload["selection_invalidation_reason"] = snapshot_reason
                    selected_winner = payload.get("selected_winner")
                    if isinstance(selected_winner, Mapping):
                        winner = dict(selected_winner)
                        winner["selection_invalidation_reason"] = snapshot_reason
                        payload["selected_winner"] = winner
                    autonomous = payload.get("autonomous")
                    if isinstance(autonomous, Mapping):
                        auto = dict(autonomous)
                        auto["selection_invalidation_reason"] = snapshot_reason
                        payload["autonomous"] = auto
                    self.store.connection.execute(
                        "UPDATE canary_readiness_snapshot SET "
                        "payload_json=?,"
                        "readiness_snapshot_status='STALE',"
                        "readiness_snapshot_stale=1,"
                        "readiness_snapshot_reason=? WHERE singleton=1",
                        (
                            json.dumps(
                                payload,
                                sort_keys=True,
                                separators=(",", ":"),
                                allow_nan=False,
                            ),
                            snapshot_reason,
                        ),
                    )

            sqlite_retry(
                operation,
                operation_name="mark canary readiness snapshot stale",
            )
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                return self._readiness_snapshot_failure_safe_payload(
                    None,
                    reason="READINESS_SNAPSHOT_MISSING",
                    updated_at=now,
                )
            _LOGGER.exception(
                "canary readiness snapshot stale mark failed reason=%s",
                snapshot_reason,
            )
            return self._readiness_snapshot_failure_safe_payload(
                None,
                reason="READINESS_SNAPSHOT_STALE_UPDATE_FAILED",
                updated_at=now,
            )
        except Exception as exc:
            if isinstance(exc, SQLiteBusyTimeout):
                _LOGGER.warning(
                    "canary readiness snapshot stale mark exhausted retries reason=%s",
                    snapshot_reason,
                )
            else:
                _LOGGER.exception(
                    "canary readiness snapshot stale mark failed reason=%s",
                    snapshot_reason,
                )
            return self._readiness_snapshot_failure_safe_payload(
                None,
                reason="READINESS_SNAPSHOT_STALE_UPDATE_FAILED",
                updated_at=now,
            )
        try:
            return self.readiness_snapshot()
        except Exception:
            _LOGGER.exception(
                "canary readiness snapshot stale readback failed reason=%s",
                snapshot_reason,
            )
            return self._readiness_snapshot_failure_safe_payload(
                None,
                reason="READINESS_SNAPSHOT_STALE_UPDATE_FAILED",
                updated_at=now,
            )
    def status(self) -> dict[str, Any]:
        """Return the cheap persisted readiness projection.

        The readiness row is the display authority.  A control write with a
        newer generation is overlaid so a kill/disarm remains visible even
        when publication is stale or unavailable.  A raw row mutation that
        does not advance the generation is not treated as an authorized
        current projection.
        """
        payload = self.readiness_snapshot()
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)
        try:
            if lock is None:
                row = connection.execute(
                    "SELECT state,candidate_id,venue,expires_at,control_generation,"
                    "settings_config_id,settings_generation "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
            else:
                with lock:
                    row = connection.execute(
                        "SELECT state,candidate_id,venue,expires_at,control_generation,"
                        "settings_config_id,settings_generation "
                        "FROM canary_control WHERE singleton=1"
                    ).fetchone()
        except sqlite3.Error:
            row = None
        if row is None:
            return payload
        def generation(value: Any) -> int | None:
            if isinstance(value, bool):
                return None
            try:
                parsed = int(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return parsed if parsed >= 0 else None

        control_generation = generation(row["control_generation"])
        projection_generation = generation(payload.get("control_generation"))
        if (
            control_generation is None
            or projection_generation is None
            or control_generation <= projection_generation
        ):
            return payload
        state = str(row["state"] or "DISABLED").upper()
        payload.update(
            {
                "micro_live_canary": state,
                "control_state": state,
                "display_state": (
                    "KILLED"
                    if state == "KILLED"
                    else "ENABLED"
                    if state in {"ARMED", AUTONOMOUS_MICRO_LIVE}
                    else "DISABLED"
                ),
                "candidate": row["candidate_id"],
                "venue": row["venue"],
                "expiry": row["expires_at"],
                "control_generation": row["control_generation"],
                "settings_config_id": row["settings_config_id"],
                "settings_generation": row["settings_generation"],
            }
        )
        autonomous = payload.get("autonomous")
        if isinstance(autonomous, Mapping):
            autonomous = dict(autonomous)
            autonomous.update(
                {
                    "enabled": state in {"ARMED", AUTONOMOUS_MICRO_LIVE},
                    "control_state": state,
                    "micro_live_canary": state,
                }
            )
            payload["autonomous"] = autonomous
        return payload

    def _canonical_execution_usage(self, now: datetime) -> dict[str, Any]:
        """Read the canonical PHT-bounded risk accounting for status projections."""
        try:
            usage = self.store.canary_risk_accounting(now=now)
        except Exception:
            _LOGGER.exception("canonical canary risk accounting read failed")
            return {"_canonical_error": True}
        if not isinstance(usage, Mapping):
            return {"_canonical_error": True}
        return dict(usage)

    def status_report(self) -> dict[str, Any]:
        """Return a bounded, read-only operator report without schema work.

        Unlike ``authoritative_status`` this method never validates candidates,
        ranks data, loads datasets, or mutates the database.  It is suitable
        for cold-start CLI/dashboard reads and reports unknown values honestly
        when durable state is absent.
        """
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)
        now = ensure_utc(self.clock())

        def fetchone(query: str, parameters: tuple[Any, ...] = ()) -> Any:
            try:
                if lock is None:
                    return connection.execute(query, parameters).fetchone()
                with lock:
                    return connection.execute(query, parameters).fetchone()
            except sqlite3.Error:
                return None
        def fetchall(query: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
            try:
                if lock is None:
                    return connection.execute(query, parameters).fetchall()
                with lock:
                    return connection.execute(query, parameters).fetchall()
            except sqlite3.Error:
                return []

        control_row = fetchone(
            "SELECT state,candidate_id,venue,expires_at,control_generation,"
            "settings_config_id,settings_generation,credential_fingerprint,updated_at "
            "FROM canary_control WHERE singleton=1"
        )
        if control_row is None:
            # Cold-start/read-only callers may hold a pre-migration control
            # table.  Recover the authoritative expiry from its legacy columns
            # without writing schema or state.
            control_row = fetchone(
                "SELECT state,candidate_id,venue,expires_at,updated_at "
                "FROM canary_control WHERE singleton=1"
            )
        def control_value(name: str, default: Any = None) -> Any:
            try:
                return control_row[name] if control_row is not None else default
            except (IndexError, KeyError):
                return default
        if control_row is None:
            control = {
                "state": "UNKNOWN",
                "candidate": None,
                "venue": None,
                "expires_at": None,
                "generation": None,
                "settings_config_id": None,
                "settings_generation": None,
                "credential_fingerprint": None,
                "binding_blocker": None,
                "updated_at": None,
            }
        else:
            try:
                generation = int(control_value("control_generation"))
            except (TypeError, ValueError):
                generation = None
            control = {
                "state": str(control_value("state") or "UNKNOWN").upper(),
                "candidate": control_value("candidate_id"),
                "venue": control_value("venue"),
                "expires_at": control_value("expires_at"),
                "generation": generation,
                "settings_config_id": control_value("settings_config_id"),
                "settings_generation": control_value("settings_generation"),
                "credential_fingerprint": _valid_credential_fingerprint(
                    control_value("credential_fingerprint")
                ),
                "binding_blocker": None,
                "updated_at": control_value("updated_at"),
            }
        raw_credential_fingerprint = control_value("credential_fingerprint")
        if control["state"] in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
            if raw_credential_fingerprint in (None, ""):
                control["state"] = "DISABLED"
                control["binding_blocker"] = "CREDENTIAL_BINDING_MISSING"
            elif control["credential_fingerprint"] is None:
                control["state"] = "KILLED"
                control["binding_blocker"] = "CREDENTIAL_BINDING_INVALID"
        if control["state"] == "ARMED":
            # Match the authoritative display rule without refreshing or
            # mutating the durable control row.  A persisted arm whose
            # deadline has passed is paper-only immediately at read time.
            expires_at = control.get("expires_at")
            if not expires_at:
                control["state"] = "DISARMED"
            else:
                try:
                    expired = (
                        ensure_utc(datetime.fromisoformat(str(expires_at))) <= now
                    )
                except (TypeError, ValueError, OverflowError):
                    control["state"] = "KILLED"
                else:
                    if expired:
                        control["state"] = "DISARMED"

        projection = self.readiness_snapshot()
        latest_signal = projection.get("latest_signal")
        try:
            queried_signal = self.latest_signal()
        except Exception:
            queried_signal = None
        if queried_signal is not None:
            latest_signal = queried_signal
        latest_signal = self._readiness_snapshot_sanitize(
            latest_signal,
            key="latest_signal",
            limit=1,
        )
        legacy_aliases = (
            "production_live_trading",
            "micro_live_canary",
            "display_state",
            "control_state",
            "candidate",
            "winner_id",
            "winner_rank",
            "winner_score",
            "selection_reason",
            "selection_status",
            "selection_valid",
            "selection_invalidation_reason",
            "selected_candidate",
            "last_selected_candidate",
            "ranking_run_id",
            "ranking_timestamp",
            "venue",
            "expiry",
            "control_generation",
            "last_request_status",
            "today_orders",
            "today_realized_pnl",
            "total_exposure",
            "open_positions",
            "limits",
            "risk_envelope",
            "risk_limits",
            "eligibility_raw_count",
            "eligible_count",
            "rankable_raw_count",
            "rankable_count",
            "real_execution_events",
            "execution_event_count",
            "historical_data_integrity",
            "historical_execution_fidelity",
            "current_execution_evidence",
            "daily_loss_budget_remaining",
            "autonomous",
            "selected_winner",
            "latest_signal",
            "trades",
            "rank",
            "score",
            "candidates_ranked",
            "candidates_signal_checked",
            "candidates_no_signal",
            "actionable_candidates_found",
            "selected_actionable_candidate",
            "selected_actionable_rank",
            "selected_actionable_score",
            "signal_scan_cursor",
            "signal_scan_ranking_run_id",
            "next_signal_scan_start_rank",
            "next_signal_scan_end_rank",
            "signal_scan_cycle_id",
            "signal_scan_candidate_universe_hash",
            "signal_scan_cycle_started_at",
            "signal_scan_cycle_completed_at",
            "signal_scan_cycle_complete",
            "signal_scan_checked_this_cycle",
            "signal_scan_remaining_this_cycle",
            "signal_scan_coverage_percentage",
            "signal_scan_skip_reasons_json",
            "signal_scan_reason_counts_json",
            "signal_scan_status",
            "signal_scan_checked_keys",
            "next_decision",
            "blocker",
            "last_signal_id",
            "worker_status",
            "consecutive_failures",
            "live_execution",
            "readiness_evaluation_error_code",
            "readiness_snapshot_version",
            "readiness_snapshot_status",
            "readiness_snapshot_stale",
            "readiness_snapshot_reason",
            "readiness_snapshot_updated_at",
            "kill_semantics",
        )
        autonomous_projection = projection.get("autonomous")
        autonomous_projection = (
            autonomous_projection
            if isinstance(autonomous_projection, Mapping)
            else {}
        )
        legacy_projection = {
            name: (
                projection[name]
                if name in projection
                else autonomous_projection.get(name)
            )
            for name in legacy_aliases
            if name in projection or name in autonomous_projection
        }
        legacy_projection["latest_signal"] = latest_signal
        readiness = {
            "status": projection.get("readiness_snapshot_status", "STALE"),
            "stale": bool(projection.get("readiness_snapshot_stale", True)),
            "reason": str(
                projection.get("readiness_snapshot_reason")
                or "READINESS_SNAPSHOT_MISSING"
            )[:256],
            "updated_at": projection.get("readiness_snapshot_updated_at"),
            "version": projection.get("readiness_snapshot_version"),
            "micro_live_canary": projection.get("micro_live_canary", "UNKNOWN"),
            "control_state": projection.get("control_state", "UNKNOWN"),
            "candidate": projection.get("candidate"),
            "selected_candidate": projection.get("selected_candidate"),
            "last_selected_candidate": projection.get("last_selected_candidate"),
            "selection_status": projection.get("selection_status", "UNKNOWN"),
            "selection_valid": projection.get("selection_valid"),
            "selection_invalidation_reason": projection.get(
                "selection_invalidation_reason"
            ),
            "eligibility_raw_count": projection.get("eligibility_raw_count"),
            "eligible_count": projection.get("eligible_count"),
            "rankable_raw_count": projection.get("rankable_raw_count"),
            "rankable_count": projection.get("rankable_count"),
            "latest_signal": latest_signal,
        }
        readiness.update(legacy_projection)
        readiness["latest_signal"] = latest_signal
        readiness["readiness_snapshot_version"] = projection.get(
            "readiness_snapshot_version"
        )

        worker_row = fetchone(
            "SELECT last_tick_at,last_tick_started_at,last_tick_completed_at,"
            "last_successful_tick,last_error_code,consecutive_failures,next_retry_at,"
            "candidates_evaluated,signals_generated,orders_attempted,"
            "candidates_ranked,candidates_signal_checked,candidates_no_signal,"
            "actionable_candidates_found,selected_actionable_candidate,"
            "selected_actionable_rank,selected_actionable_score,signal_scan_cursor,"
            "signal_scan_ranking_run_id,next_signal_scan_start_rank,next_signal_scan_end_rank,"
            "signal_scan_cycle_id,signal_scan_candidate_universe_hash,"
            "signal_scan_cycle_started_at,signal_scan_cycle_completed_at,"
            "signal_scan_cycle_complete,signal_scan_checked_this_cycle,"
            "signal_scan_remaining_this_cycle,signal_scan_coverage_percentage,"
            "signal_scan_skip_reasons_json,signal_scan_reason_counts_json,signal_scan_status,"
            "next_decision,blocker,last_signal_id,worker_status FROM canary_autonomous_state "
            "WHERE singleton=1"
        )
        worker_keys = (
            "last_tick_at",
            "last_tick_started_at",
            "last_tick_completed_at",
            "last_successful_tick",
            "last_error_code",
            "consecutive_failures",
            "candidates_ranked",
            "candidates_signal_checked",
            "candidates_no_signal",
            "actionable_candidates_found",
            "selected_actionable_candidate",
            "selected_actionable_rank",
            "signal_scan_cycle_id",
            "signal_scan_candidate_universe_hash",
            "signal_scan_cycle_started_at",
            "signal_scan_cycle_completed_at",
            "signal_scan_cycle_complete",
            "signal_scan_checked_this_cycle",
            "signal_scan_remaining_this_cycle",
            "signal_scan_coverage_percentage",
            "signal_scan_skip_reasons_json",
            "signal_scan_reason_counts_json",
            "signal_scan_status",
            "selected_actionable_score",
            "signal_scan_cursor",
            "signal_scan_ranking_run_id",
            "next_signal_scan_start_rank",
            "next_signal_scan_end_rank",
            "next_retry_at",
            "candidates_evaluated",
            "signals_generated",
            "orders_attempted",
            "next_decision",
            "blocker",
            "last_signal_id",
            "worker_status",
        )
        worker = (
            {key: worker_row[key] for key in worker_keys}
            if worker_row is not None
            else {key: None for key in worker_keys}
        )
        if worker_row is None:
            worker["worker_status"] = "UNKNOWN"
        worker_cycle_id = str(worker.get("signal_scan_cycle_id") or "").strip()
        worker_checked_keys = (
            [
                dict(item)
                for item in fetchall(
                    "SELECT cycle_id,candidate_id,qualification_hash,checked_at,"
                    "rank_at_check,ranking_run_id "
                    "FROM canary_signal_scan_checked WHERE cycle_id=? "
                    "ORDER BY checked_at,candidate_id,qualification_hash",
                    (worker_cycle_id,),
                )
            ]
            if worker_cycle_id
            else (
                projection.get(
                    "signal_scan_checked_keys",
                    autonomous_projection.get("signal_scan_checked_keys", []),
                )
                if worker_row is None
                else []
            )
        )
        worker["signal_scan_checked_keys"] = worker_checked_keys
        legacy_projection["signal_scan_checked_keys"] = worker_checked_keys
        readiness["signal_scan_checked_keys"] = worker_checked_keys
        cycle_projection_fields = (
            "signal_scan_cycle_id",
            "signal_scan_candidate_universe_hash",
            "signal_scan_cycle_started_at",
            "signal_scan_cycle_completed_at",
            "signal_scan_cycle_complete",
            "signal_scan_checked_this_cycle",
            "signal_scan_remaining_this_cycle",
            "signal_scan_coverage_percentage",
            "signal_scan_skip_reasons_json",
            "signal_scan_reason_counts_json",
            "signal_scan_status",
        )
        if worker_row is not None:
            report_autonomous = dict(
                legacy_projection.get("autonomous")
                if isinstance(legacy_projection.get("autonomous"), Mapping)
                else {}
            )
            for name in cycle_projection_fields:
                value = worker.get(name)
                legacy_projection[name] = value
                readiness[name] = value
                report_autonomous[name] = value
            report_autonomous["signal_scan_checked_keys"] = worker_checked_keys
            legacy_projection["autonomous"] = report_autonomous
            readiness["autonomous"] = report_autonomous
        now = ensure_utc(self.clock())
        canonical = self._canonical_execution_usage(now)
        event_count = fetchone(
            "SELECT COUNT(*) AS n FROM canary_execution_events"
        )
        latest = fetchone(
            "SELECT status FROM canary_ledger ORDER BY timestamp DESC,event_id DESC LIMIT 1"
        )
        if not canonical.get("_canonical_error"):
            execution = {
                "today_orders": int(canonical.get("submitted_orders", 0) or 0),
                "today_realized_pnl": float(
                    canonical.get("today_realized_pnl_usd", "0") or "0"
                ),
                "total_exposure": float(
                    canonical.get("aggregate_exposure_usd", "0") or "0"
                ),
                "open_positions": int(canonical.get("open_positions", 0) or 0),
            }
        else:
            execution = {
                "today_orders": None,
                "today_realized_pnl": None,
                "total_exposure": None,
                "open_positions": None,
            }
        execution.update(
            {
                "execution_event_count": int(event_count["n"]) if event_count is not None else 0,
                "last_request_status": (
                    str(latest["status"]).upper() if latest is not None else None
                ),
            }
        )
        for key, value in worker.items():
            if key in legacy_aliases:
                legacy_projection[key] = value
        for key, value in execution.items():
            if key in legacy_aliases:
                legacy_projection[key] = value
        control_state = str(control.get("state") or "UNKNOWN").upper()
        display_state = (
            "ENABLED"
            if control_state in {"ARMED", AUTONOMOUS_MICRO_LIVE}
            else "KILLED"
            if control_state == "KILLED"
            else "DISABLED"
            if control_state in {"DISARMED", "DISABLED"}
            else "UNKNOWN"
        )
        legacy_projection.update(
            {
                "micro_live_canary": control_state,
                "control_state": control_state,
                "display_state": display_state,
                "credential_fingerprint": control.get("credential_fingerprint"),
                "binding_blocker": control.get("binding_blocker"),
                "candidate": control.get("candidate"),
                "venue": control.get("venue"),
                "expiry": control.get("expires_at"),
                "control_generation": control.get("generation"),
            }
        )
        autonomous = legacy_projection.get("autonomous")
        autonomous = dict(autonomous) if isinstance(autonomous, Mapping) else {}
        autonomous.update(
            {
                "enabled": control_state in {"ARMED", AUTONOMOUS_MICRO_LIVE},
                "control_state": control_state,
                "micro_live_canary": control_state,
                "credential_fingerprint": control.get("credential_fingerprint"),
                "binding_blocker": control.get("binding_blocker"),
            }
        )
        legacy_projection["autonomous"] = autonomous
        readiness.update(legacy_projection)
        readiness["latest_signal"] = latest_signal
        return {
            **legacy_projection,
            "latest_signal": latest_signal,
            "control": control,
            "authoritative_control": dict(control),
            "readiness": readiness,
            "worker": worker,
            "execution": execution,
        }


    @staticmethod
    def _integrity(
        candidate: str,
        venue: str,
        expires: str,
        limits: Mapping[str, Any],
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "candidate": candidate,
                    "venue": venue,
                    "expires": expires,
                    "limits": limits,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def _limits_record(self, limits: CanaryLimits) -> dict[str, Any]:
        return {
            "target_notional_usd": str(limits.target_notional_usd),
            "max_exposure_usd": str(limits.max_exposure_usd),
            "max_daily_loss_usd": str(limits.max_daily_loss_usd),
            "max_open_positions": limits.max_open_positions,
            "max_orders_per_day": limits.max_orders_per_day,
            "max_slippage_bps": limits.max_slippage_bps,
        }
    def _active_settings_values(self) -> Mapping[str, Any]:
        if self.settings is None:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        try:
            values = self.settings.active_limits()
        except Exception as exc:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE") from exc
        if not isinstance(values, Mapping):
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        return values

    def _effective_limits(self, candidate: CanaryLimits | None = None) -> CanaryLimits:
        base = self.autonomous_limits()
        if candidate is None:
            return CanaryLimits(
                target_notional_usd=Decimal(base["target_notional_usd"]),
                max_exposure_usd=Decimal(base["max_exposure_usd"]),
                max_daily_loss_usd=Decimal(base["max_daily_loss_usd"]),
                max_open_positions=int(base["max_open_positions"]),
                max_orders_per_day=int(base["max_orders_per_day"]),
                max_slippage_bps=int(base["max_slippage_bps"]),
            )
        values = self._limits_record(candidate)
        return CanaryLimits(
            target_notional_usd=min(
                Decimal(base["target_notional_usd"]),
                Decimal(values["target_notional_usd"]),
            ),
            max_exposure_usd=min(
                Decimal(base["max_exposure_usd"]),
                Decimal(values["max_exposure_usd"]),
            ),
            max_daily_loss_usd=min(
                Decimal(base["max_daily_loss_usd"]),
                Decimal(values["max_daily_loss_usd"]),
            ),
            max_open_positions=min(
                int(base["max_open_positions"]), int(values["max_open_positions"])
            ),
            max_orders_per_day=min(
                int(base["max_orders_per_day"]), int(values["max_orders_per_day"])
            ),
            max_slippage_bps=min(
                int(base["max_slippage_bps"]), int(values["max_slippage_bps"])
            ),
        )
    @staticmethod
    def _merged_lifecycle_payload(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
        return _canary_merged_lifecycle_payload(record)

    @staticmethod
    def _document_hash(value: Any) -> str | None:
        return _canary_document_hash(value)

    def _lifecycle_frozen_hash(self, record: Mapping[str, Any] | None) -> str | None:
        return _canary_lifecycle_frozen_hash(self.store, record)

    def _eligibility_binding_result(
        self,
        candidate_id: str,
        eligibility: Mapping[str, Any] | None,
        *,
        record: Mapping[str, Any] | None = None,
        verify_attestation: bool = True,
    ) -> dict[str, Any]:
        return _canary_eligibility_binding_result(
            self.store,
            candidate_id,
            eligibility,
            record=record,
            verify_attestation=verify_attestation,
        )

    def _eligibility_is_bound(self, candidate_id: str, eligibility: Mapping[str, Any] | None) -> bool:
        return _canary_eligibility_is_bound(self.store, candidate_id, eligibility)
    @staticmethod
    def _minimum_sample_check_passed(payload: Mapping[str, Any]) -> bool:
        evidence = payload.get("minimum_sample_check")
        if not isinstance(evidence, Mapping) or evidence.get("passed") is not True:
            return False
        checks = evidence.get("checks")
        if not isinstance(checks, Mapping) or not checks or not all(value is True for value in checks.values()):
            return False

        def integer(name: str, default: int | None = None) -> int | None:
            value = evidence.get(name, default)
            if isinstance(value, bool):
                return None
            if isinstance(value, float) and not value.is_integer():
                return None
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                return None
            return parsed if parsed >= 0 else None
        count = integer("count")
        trades = integer("trades")
        minimum_observations = integer("min_observations", 30)
        minimum_trades = integer("min_trades", 0)
        if (
            count is None
            or trades is None
            or minimum_observations is None
            or minimum_trades is None
            or count < minimum_observations
            or trades < minimum_trades
        ):
            return False
        plan = payload.get("experiment_plan")
        if isinstance(plan, Mapping):
            for name, fallback in (
                ("min_independent_samples", "min_samples"),
                ("min_trades", None),
            ):
                expected = plan.get(name)
                if expected is None and fallback is not None:
                    expected = plan.get(fallback)
                if expected is None:
                    continue
                if isinstance(expected, bool):
                    return False
                if isinstance(expected, float) and not expected.is_integer():
                    return False
                try:
                    expected_int = int(expected)
                except (TypeError, ValueError):
                    return False
                if expected_int < 0 or (count if name != "min_trades" else trades) < expected_int:
                    return False
        return True
    def autonomous_limits(self) -> dict[str, Any]:
        """Return the active shared risk envelope as JSON-safe values.

        The constants remain the cold-start defaults only.  Once the risk
        owner publishes active settings, every autonomous gate reads that
        snapshot; no additional hidden one-dollar/five-dollar cap is applied.
        """
        if self.settings is None:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        try:
            snapshot = self._settings_snapshot()
            raw = snapshot.get("effective_limits")
            if not isinstance(raw, Mapping):
                raw = self.settings.active_limits()
        except Exception as exc:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE") from exc
        if isinstance(raw, Mapping):
            source = raw
        elif raw is not None:
            source = {
                name: getattr(raw, name, None)
                for name in (
                    "target_notional_usd",
                    "max_exposure_usd",
                    "max_daily_loss_usd",
                    "max_open_positions",
                    "max_orders_per_day",
                    "max_slippage_bps",
                )
            }
        else:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        try:
            limits = CanaryLimits(
                target_notional_usd=Decimal(
                    str(source.get("target_notional_usd", source.get("max_order_notional", DEFAULT_TARGET_NOTIONAL_USD)))
                ),
                max_exposure_usd=Decimal(
                    str(source.get("max_exposure_usd", source.get("max_account_exposure", DEFAULT_MAX_EXPOSURE_USD)))
                ),
                max_daily_loss_usd=Decimal(
                    str(source.get("max_daily_loss_usd", source.get("max_daily_loss", DEFAULT_DAILY_LOSS_USD)))
                ),
                max_open_positions=int(source.get("max_open_positions", DEFAULT_MAX_OPEN_POSITIONS)),
                max_orders_per_day=int(source.get("max_orders_per_day", DEFAULT_MAX_ORDERS_PER_DAY)),
                max_slippage_bps=int(source.get("max_slippage_bps", DEFAULT_MAX_SLIPPAGE_BPS)),
            )
        except (TypeError, ValueError, ArithmeticError) as exc:
            raise CanaryBlocked("CANARY_SETTINGS_INVALID") from exc
        return self._limits_record(limits)

    def _settings_snapshot(self) -> Mapping[str, Any]:
        if self.settings is None:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        try:
            snapshot = self.settings.snapshot(now=self.clock())
        except Exception as exc:
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE") from exc
        if not isinstance(snapshot, Mapping):
            raise CanaryBlocked("CANARY_SETTINGS_UNAVAILABLE")
        return snapshot

    def _settings_identity(self) -> tuple[str | None, int | None, str | None]:
        snapshot = self._settings_snapshot()
        sources = [snapshot]
        for name in ("active", "config", "settings"):
            nested = snapshot.get(name)
            if isinstance(nested, Mapping):
                sources.append(nested)
        config_id: str | None = None
        generation: int | None = None
        config_hash: str | None = None
        for source in sources:
            if config_id is None:
                for key in ("config_id", "active_config_id", "settings_config_id", "id"):
                    value = source.get(key)
                    if value is not None and str(value).strip():
                        config_id = str(value).strip()
                        break
            if config_hash is None:
                for key in ("config_hash", "active_config_hash", "settings_hash"):
                    value = source.get(key)
                    if value is not None and str(value).strip():
                        config_hash = str(value).strip()
                        break
            if generation is None:
                for key in ("generation", "config_generation", "settings_generation", "active_generation"):
                    value = source.get(key)
                    if isinstance(value, bool):
                        continue
                    try:
                        parsed = int(value)
                    except (TypeError, ValueError):
                        continue
                    if parsed >= 0:
                        generation = parsed
                        break
        return config_id, generation, config_hash

    def _settings_binding(self) -> tuple[str | None, int | None]:
        config_id, generation, _ = self._settings_identity()
        return config_id, generation

    def invalidate_eligibility(
        self,
        candidate_id: str,
        reason: str = "",
        *,
        publish_readiness: bool = True,
    ) -> None:
        """Remove a stale eligibility binding; never changes lifecycle state."""
        with self.store._lock:
            with self.store.connection:
                self.store.connection.execute(
                    "DELETE FROM canary_eligibility WHERE candidate_id=?",
                    (str(candidate_id),),
                )
        if publish_readiness:
            self.publish_readiness_snapshot(
                reason=reason or "ELIGIBILITY_INVALIDATED"
            )
    def _selection_record(self) -> dict[str, Any] | None:
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)
        try:
            if lock is None:
                row = connection.execute(
                    "SELECT * FROM canary_selection WHERE singleton=1"
                ).fetchone()
            else:
                # The shared store connection is also used by node writers.
                # Keep this read under its lock so sqlite calls cannot race
                # even while WAL permits readers and writers to overlap.
                with lock:
                    row = connection.execute(
                        "SELECT * FROM canary_selection WHERE singleton=1"
                    ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            return None
        if row is None:
            return None
        result = dict(row)
        for key in ("component_scores_json", "evidence_versions_json"):
            try:
                parsed = json.loads(result.get(key) or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = {}
            result[key.removesuffix("_json")] = parsed if isinstance(parsed, Mapping) else {}
        return result

    def record_autonomous_decision(
        self,
        *,
        next_decision: str,
        blocker: str | None = None,
        signal_id: str | None = None,
        worker_status: str = "IDLE",
        timestamp: datetime | None = None,
        last_tick_started_at: str | None | object = _UNSET,
        last_tick_completed_at: str | None | object = _UNSET,
        last_successful_tick: str | None | object = _UNSET,
        last_error_code: str | None | object = _UNSET,
        consecutive_failures: int | None | object = _UNSET,
        next_retry_at: str | None | object = _UNSET,
        candidates_evaluated: int | None | object = _UNSET,
        signals_generated: int | None | object = _UNSET,
        orders_attempted: int | None | object = _UNSET,
        candidates_ranked: int | None | object = _UNSET,
        candidates_signal_checked: int | None | object = _UNSET,
        candidates_no_signal: int | None | object = _UNSET,
        actionable_candidates_found: int | None | object = _UNSET,
        selected_actionable_candidate: str | None | object = _UNSET,
        selected_actionable_rank: int | None | object = _UNSET,
        selected_actionable_score: float | None | object = _UNSET,
        signal_scan_cursor: int | None | object = _UNSET,
        signal_scan_ranking_run_id: str | None | object = _UNSET,
        next_signal_scan_start_rank: int | None | object = _UNSET,
        next_signal_scan_end_rank: int | None | object = _UNSET,
        signal_scan_cycle_id: str | None | object = _UNSET,
        signal_scan_candidate_universe_hash: str | None | object = _UNSET,
        signal_scan_cycle_started_at: str | None | object = _UNSET,
        signal_scan_cycle_completed_at: str | None | object = _UNSET,
        signal_scan_cycle_complete: int | bool | None | object = _UNSET,
        signal_scan_checked_this_cycle: int | None | object = _UNSET,
        signal_scan_remaining_this_cycle: int | None | object = _UNSET,
        signal_scan_coverage_percentage: float | None | object = _UNSET,
        signal_scan_skip_reasons_json: str | None | object = _UNSET,
        signal_scan_reason_counts_json: str | None | object = _UNSET,
        signal_scan_status: str | None | object = _UNSET,
        signal_scan_checked_keys: list[Mapping[str, Any]] | None = None,
        publish: bool = False,
    ) -> None:
        when = ensure_utc(timestamp or self.clock()).isoformat()
        metadata = {
            "last_tick_started_at": last_tick_started_at,
            "last_tick_completed_at": last_tick_completed_at,
            "last_successful_tick": last_successful_tick,
            "last_error_code": last_error_code,
            "consecutive_failures": consecutive_failures,
            "next_retry_at": next_retry_at,
            "candidates_evaluated": candidates_evaluated,
            "signals_generated": signals_generated,
            "orders_attempted": orders_attempted,
            "candidates_ranked": candidates_ranked,
            "candidates_signal_checked": candidates_signal_checked,
            "candidates_no_signal": candidates_no_signal,
            "actionable_candidates_found": actionable_candidates_found,
            "selected_actionable_candidate": selected_actionable_candidate,
            "signal_scan_cycle_id": signal_scan_cycle_id,
            "signal_scan_candidate_universe_hash": signal_scan_candidate_universe_hash,
            "signal_scan_cycle_started_at": signal_scan_cycle_started_at,
            "signal_scan_cycle_completed_at": signal_scan_cycle_completed_at,
            "signal_scan_cycle_complete": signal_scan_cycle_complete,
            "signal_scan_checked_this_cycle": signal_scan_checked_this_cycle,
            "signal_scan_remaining_this_cycle": signal_scan_remaining_this_cycle,
            "signal_scan_coverage_percentage": signal_scan_coverage_percentage,
            "signal_scan_skip_reasons_json": signal_scan_skip_reasons_json,
            "signal_scan_reason_counts_json": signal_scan_reason_counts_json,
            "signal_scan_status": signal_scan_status,
            "selected_actionable_rank": selected_actionable_rank,
            "selected_actionable_score": selected_actionable_score,
            "signal_scan_cursor": signal_scan_cursor,
            "signal_scan_ranking_run_id": signal_scan_ranking_run_id,
            "next_signal_scan_start_rank": next_signal_scan_start_rank,
            "next_signal_scan_end_rank": next_signal_scan_end_rank,
        }
        for key, value in tuple(metadata.items()):
            if value is _UNSET:
                continue
            if key.endswith("_at") or key in {
                "last_successful_tick",
                "signal_scan_ranking_run_id",
                "signal_scan_cycle_id",
                "signal_scan_candidate_universe_hash",
                "signal_scan_status",
                "selected_actionable_candidate",
            }:
                metadata[key] = str(value) if value is not None else None
            elif key == "signal_scan_reason_counts_json":
                try:
                    decoded_counts = json.loads(str(value or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    decoded_counts = {}
                bounded_counts: dict[str, int] = {}
                if isinstance(decoded_counts, Mapping):
                    for count_key in sorted(decoded_counts, key=lambda item: str(item))[:64]:
                        try:
                            count_value = int(decoded_counts[count_key])
                        except (TypeError, ValueError):
                            continue
                        if count_value >= 0:
                            bounded_counts[str(count_key)[:128]] = count_value
                encoded_counts = json.dumps(
                    bounded_counts, sort_keys=True, separators=(",", ":")
                )
                metadata[key] = encoded_counts if len(encoded_counts) <= 4096 else "{}"
            elif key == "signal_scan_skip_reasons_json":
                metadata[key] = str(value or "{}")[:4096]
            elif key == "signal_scan_coverage_percentage":
                try:
                    parsed_coverage = float(value) if value is not None else None
                except (TypeError, ValueError):
                    parsed_coverage = None
                metadata[key] = (
                    min(100.0, max(0.0, parsed_coverage))
                    if parsed_coverage is not None and math.isfinite(parsed_coverage)
                    else 0.0
                )
            elif key == "last_error_code":
                metadata[key] = str(value)[:128] if value is not None else None
            elif key == "selected_actionable_score":
                try:
                    parsed_score = float(value) if value is not None else None
                except (TypeError, ValueError):
                    parsed_score = None
                metadata[key] = parsed_score if parsed_score is not None and math.isfinite(parsed_score) else None
            else:
                try:
                    parsed = int(value) if value is not None else None
                except (TypeError, ValueError):
                    parsed = None
                metadata[key] = max(0, parsed) if parsed is not None else None
        with self.store._lock:
            with self.store.connection:
                incoming_cycle_id = metadata.get("signal_scan_cycle_id")
                if incoming_cycle_id is _UNSET:
                    incoming_cycle_id = None
                else:
                    incoming_cycle_id = str(incoming_cycle_id or "").strip() or None
                previous_state = self.store.connection.execute(
                    "SELECT signal_scan_cycle_id FROM canary_autonomous_state "
                    "WHERE singleton=1"
                ).fetchone()
                previous_cycle_id = (
                    str(previous_state["signal_scan_cycle_id"] or "").strip()
                    if previous_state is not None
                    else ""
                )
                new_cycle_transition = bool(
                    incoming_cycle_id and incoming_cycle_id != previous_cycle_id
                )
                for checked in signal_scan_checked_keys or ():
                    if not isinstance(checked, Mapping):
                        continue
                    cycle_id = str(checked.get("cycle_id") or "").strip()
                    candidate_id = str(checked.get("candidate_id") or "").strip()
                    qualification_hash = str(
                        checked.get("qualification_hash") or ""
                    ).strip()
                    if not cycle_id or not candidate_id or not qualification_hash:
                        continue
                    checked_at = str(checked.get("checked_at") or when)
                    rank_at_check = checked.get("rank_at_check")
                    try:
                        rank_at_check = (
                            int(rank_at_check) if rank_at_check is not None else None
                        )
                    except (TypeError, ValueError):
                        rank_at_check = None
                    ranking_run_id = checked.get("ranking_run_id")
                    self.store.connection.execute(
                        "INSERT OR IGNORE INTO canary_signal_scan_checked("
                        "cycle_id,candidate_id,qualification_hash,checked_at,"
                        "rank_at_check,ranking_run_id) VALUES(?,?,?,?,?,?)",
                        (
                            cycle_id,
                            candidate_id,
                            qualification_hash,
                            checked_at,
                            rank_at_check,
                            str(ranking_run_id) if ranking_run_id else None,
                        ),
                    )
                columns = [
                    "singleton",
                    "last_tick_at",
                    "next_decision",
                    "blocker",
                    "last_signal_id",
                    "worker_status",
                    "updated_at",
                ]
                values: list[Any] = [
                    1,
                    when,
                    str(next_decision)[:256],
                    str(blocker)[:256] if blocker else None,
                    str(signal_id)[:256] if signal_id else None,
                    str(worker_status)[:64],
                    when,
                ]
                for key, value in metadata.items():
                    if value is not _UNSET:
                        columns.append(key)
                        values.append(value)
                placeholders = ",".join("?" for _ in columns)
                updates = [
                    f"{key}=excluded.{key}"
                    for key in columns
                    if key != "singleton"
                ]
                self.store.connection.execute(
                    f"INSERT INTO canary_autonomous_state({','.join(columns)}) "
                    f"VALUES({placeholders}) ON CONFLICT(singleton) DO UPDATE SET "
                    + ",".join(updates),
                    tuple(values),
                )
                if new_cycle_transition:
                    cycle_rows = self.store.connection.execute(
                        "WITH cycle_times AS ("
                        "SELECT cycle_id,MAX(checked_at) AS latest_time "
                        "FROM canary_signal_scan_checked GROUP BY cycle_id "
                        "UNION ALL SELECT ?,? "
                        "), ranked_cycles AS ("
                        "SELECT cycle_id,MAX(latest_time) AS latest_time "
                        "FROM cycle_times GROUP BY cycle_id"
                        ") SELECT cycle_id FROM ranked_cycles "
                        "ORDER BY latest_time DESC,cycle_id DESC",
                        (incoming_cycle_id, when),
                    ).fetchall()
                    retained_cycle_ids = [incoming_cycle_id]
                    retained_cycle_ids.extend(
                        str(row["cycle_id"])
                        for row in cycle_rows
                        if str(row["cycle_id"]) != incoming_cycle_id
                    )
                    retained_cycle_ids = retained_cycle_ids[
                        :_AUTONOMOUS_SCAN_CYCLE_RETENTION
                    ]
                    placeholders = ",".join("?" for _ in retained_cycle_ids)
                    self.store.connection.execute(
                        "DELETE FROM canary_signal_scan_checked "
                        f"WHERE cycle_id NOT IN ({placeholders})",
                        tuple(retained_cycle_ids),
                    )
        self._patch_autonomous_snapshot()
        if publish:
            self.publish_readiness_snapshot(reason="AUTONOMOUS_DECISION")
    def _load_credential_binding(self) -> tuple[dict[str, str], str]:
        try:
            values = self.credentials.load(
                allow_environment=self.allow_environment
            )
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED") from exc
        if (
            not isinstance(values, Mapping)
            or not all(_credential_value(values, name) for name in _MANDATORY_SECRET_NAMES)
        ):
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
        normalized = {
            name: _credential_value(values, name)
            for name in _SECRET_NAMES
        }
        return normalized, credential_fingerprint(normalized)

    @staticmethod
    def _require_control_credential_binding(
        row: Mapping[str, Any] | None,
        current_fingerprint: str | None = None,
    ) -> str:
        try:
            raw = row["credential_fingerprint"] if row is not None else None
        except (IndexError, KeyError, TypeError):
            raw = None
        persisted = _valid_credential_fingerprint(raw)
        if persisted is None:
            raise CanaryBlocked("CREDENTIAL_BINDING_MISSING")
        if current_fingerprint is not None and not hmac.compare_digest(
            persisted, current_fingerprint
        ):
            raise CanaryBlocked("CREDENTIAL_BINDING_MISMATCH")
        return persisted
    def require_current_credential_binding(self) -> str:
        """Load current credentials and require the durable control binding."""
        _, current_fingerprint = self._load_credential_binding()
        try:
            with self.store._lock:
                row = self.store.connection.execute(
                    "SELECT credential_fingerprint FROM canary_control "
                    "WHERE singleton=1"
                ).fetchone()
        except Exception as exc:
            raise CanaryBlocked("CREDENTIAL_BINDING_MISSING") from exc
        return self._require_control_credential_binding(row, current_fingerprint)

    def enable_autonomous_micro_live(
        self,
        venue: str,
        config_id: str,
        expected_generation: int,
        expected_credential_fingerprint: str | None = None,
    ) -> Mapping[str, Any]:
        """Enable autonomous canary only for the reviewed active settings."""
        requested_venue = str(venue or "").strip().lower()
        if requested_venue != AUTONOMOUS_CANARY_VENUE:
            raise CanaryBlocked("UNSUPPORTED_VENUE")
        requested_config = str(config_id or "").strip()
        if not requested_config:
            raise CanaryBlocked("CANARY_SETTINGS_CONFIG_REQUIRED")
        if (
            isinstance(expected_generation, bool)
            or not isinstance(expected_generation, int)
            or expected_generation < 1
        ):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_REQUIRED")
        _, credential_digest = self._load_credential_binding()
        if expected_credential_fingerprint is not None:
            expected_digest = _valid_credential_fingerprint(
                expected_credential_fingerprint
            )
            if expected_digest is None or not hmac.compare_digest(
                expected_digest, credential_digest
            ):
                raise CanaryBlocked("CREDENTIAL_BINDING_MISMATCH")
        now = ensure_utc(self.clock())
        snapshot = self._settings_snapshot()
        settings_config_id, settings_generation, settings_hash = self._settings_identity()
        if (
            settings_config_id != requested_config
            or settings_generation != expected_generation
            or not settings_hash
        ):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
        active = snapshot.get("active")
        active_generation = None
        if isinstance(active, Mapping):
            try:
                active_generation = int(active.get("generation"))
            except (TypeError, ValueError):
                active_generation = None
        if isinstance(active, Mapping) and (
            str(active.get("state") or "").upper() not in {"ACTIVE", ""}
            or str(active.get("config_id") or "").strip() != requested_config
            or active_generation != expected_generation
        ):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
        settings_config_id = requested_config
        settings_generation = expected_generation
        values = self.autonomous_limits()
        digest = self._integrity("", AUTONOMOUS_CANARY_VENUE, "", values)
        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            with connection:
                current = connection.execute(
                    "SELECT state,control_generation,limits_json,integrity_hash,"
                    "settings_config_id,settings_generation,credential_fingerprint "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                # Re-read the active settings identity while holding the same
                # write lock used for authorization.  The earlier snapshot is
                # only a review hint; this closes the active-settings-to-enable
                # race instead of arming against a stale reviewed envelope.
                active_current = connection.execute(
                    "SELECT config_id,generation,config_hash,state "
                    "FROM canary_setting_configs WHERE state='ACTIVE' "
                    "ORDER BY generation DESC,created_at DESC,config_id DESC LIMIT 1"
                ).fetchone()
                if (
                    active_current is None
                    or str(active_current["config_id"] or "").strip() != requested_config
                    or int(active_current["generation"]) != expected_generation
                    or str(active_current["config_hash"] or "").strip() != str(settings_hash or "").strip()
                ):
                    raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
                if str(active_current["state"] or "").upper() != "ACTIVE":
                    raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
                if current is not None and str(current["state"]).upper() == "KILLED":
                    raise CanaryBlocked("CANARY_KILLED")
                if current is not None and str(current["state"]).upper() == AUTONOMOUS_MICRO_LIVE:
                    try:
                        persisted = json.loads(current["limits_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                    if persisted != values or current["integrity_hash"] != digest:
                        raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
                    self._require_control_credential_binding(
                        current, credential_digest
                    )
                    if (
                        str(current["settings_config_id"] or "") != str(settings_config_id or "")
                        or (
                            current["settings_generation"] is not None
                            and settings_generation is not None
                            and int(current["settings_generation"]) != settings_generation
                        )
                    ):
                        raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
                else:
                    try:
                        generation = int(current["control_generation"] or 0) if current else 0
                    except (TypeError, ValueError):
                        raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                    selection = connection.execute(
                        "SELECT * FROM canary_selection WHERE singleton=1"
                    ).fetchone()
                    candidate_id = None
                    if selection is not None:
                        selection_state = self._selection_validation(dict(selection))
                        if selection_state.get("selection_valid"):
                            candidate_id = selection_state.get("selected_candidate")
                    connection.execute(
                        "INSERT INTO canary_control("
                        "singleton,state,candidate_id,venue,armed_at,expires_at,"
                        "limits_json,integrity_hash,updated_at,control_generation,"
                        "settings_config_id,settings_generation,credential_fingerprint) "
                        "VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                        "state=excluded.state,candidate_id=excluded.candidate_id,"
                        "venue=excluded.venue,armed_at=excluded.armed_at,expires_at=excluded.expires_at,"
                        "limits_json=excluded.limits_json,integrity_hash=excluded.integrity_hash,"
                        "updated_at=excluded.updated_at,control_generation=excluded.control_generation,"
                        "settings_config_id=excluded.settings_config_id,"
                        "settings_generation=excluded.settings_generation,"
                        "credential_fingerprint=excluded.credential_fingerprint",
                        (
                            AUTONOMOUS_MICRO_LIVE,
                            candidate_id,
                            AUTONOMOUS_CANARY_VENUE,
                            now.isoformat(),
                            None,
                            json.dumps(values, sort_keys=True),
                            digest,
                            now.isoformat(),
                            generation + 1,
                            settings_config_id,
                            settings_generation,
                            credential_digest,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO canary_autonomous_state("
                        "singleton,last_tick_at,next_decision,blocker,last_signal_id,worker_status,updated_at) "
                        "VALUES(1,NULL,'WAITING_FOR_NEXT_DECISION',NULL,NULL,'IDLE',?) "
                        "ON CONFLICT(singleton) DO UPDATE SET "
                        "next_decision=excluded.next_decision,blocker=NULL,worker_status='IDLE',updated_at=excluded.updated_at",
                        (now.isoformat(),),
                    )
        return self.publish_readiness_snapshot(reason="AUTONOMOUS_ENABLED")
    def bind_autonomous_selection(
        self,
        candidate_id: str | None,
        *,
        publish_readiness: bool = True,
    ) -> None:
        """Fence the selected winner into autonomous control metadata."""
        identifier = str(candidate_id).strip() if candidate_id else None
        now = ensure_utc(self.clock()).isoformat()
        with self.store._lock:
            with self.store.connection:
                row = self.store.connection.execute(
                    "SELECT state,candidate_id,control_generation "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                if row is None or str(row["state"]).upper() != AUTONOMOUS_MICRO_LIVE:
                    return
                current = str(row["candidate_id"]).strip() if row["candidate_id"] else None
                if current == identifier:
                    return
                try:
                    generation = int(row["control_generation"] or 0)
                except (TypeError, ValueError):
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                self.store.connection.execute(
                    "UPDATE canary_control SET candidate_id=?,updated_at=?,"
                    "control_generation=? WHERE singleton=1 AND state=?",
                    (identifier, now, generation + 1, AUTONOMOUS_MICRO_LIVE),
                )
        if publish_readiness:
            self.publish_readiness_snapshot(reason="AUTONOMOUS_SELECTION_CHANGED")
    def bind_autonomous_actionable_candidate(
        self,
        candidate_id: str,
        *,
        ranking_run_id: str,
        signal_id: str,
        publish_readiness: bool = True,
    ) -> Mapping[str, Any]:
        """Atomically bind a current READY candidate into autonomous control.

        ``canary_selection`` remains the immutable research winner.  This
        operation only moves the candidate fence owned by ``canary_control``
        after revalidating the current ranking run and the persisted signal
        under the same writer transaction.
        """
        identifier = str(candidate_id).strip()
        run_id = str(ranking_run_id).strip()
        persisted_signal_id = str(signal_id).strip()
        if not identifier or not run_id or not persisted_signal_id:
            raise CanaryBlocked("AUTONOMOUS_ACTIONABLE_BINDING_INVALID")
        connection = self.store.connection
        now = ensure_utc(self.clock())
        now_text = now.isoformat()
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                control = connection.execute(
                    "SELECT state,candidate_id,limits_json,integrity_hash,"
                    "control_generation FROM canary_control WHERE singleton=1"
                ).fetchone()
                if control is None:
                    raise CanaryBlocked("CANARY_NOT_ARMED")
                control_state = str(control["state"] or "").upper()
                if control_state == "KILLED":
                    raise CanaryBlocked("CANARY_KILLED")
                if control_state != AUTONOMOUS_MICRO_LIVE:
                    raise CanaryBlocked("AUTONOMOUS_CANARY_DISABLED")
                try:
                    control_generation = int(control["control_generation"] or 0)
                    control_limits = json.loads(control["limits_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                if control_generation <= 0 or not isinstance(control_limits, Mapping):
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
                control_limits = dict(control_limits)
                if control_limits != self.autonomous_limits():
                    raise CanaryBlocked("AUTONOMOUS_RISK_ENVELOPE_CORRUPT")
                expected_integrity = self._integrity(
                    "",
                    AUTONOMOUS_CANARY_VENUE,
                    "",
                    control_limits,
                )
                if expected_integrity != control["integrity_hash"]:
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT")

                lifecycle = self.store.load_candidate_lifecycle(identifier)
                if (
                    not isinstance(lifecycle, Mapping)
                    or str(lifecycle.get("stage") or "") not in _CANARY_ELIGIBLE_STAGES
                ):
                    raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
                eligibility = connection.execute(
                    "SELECT candidate_id,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (identifier,),
                ).fetchone()
                binding = self._eligibility_binding_result(
                    identifier,
                    eligibility,
                    record=lifecycle,
                    verify_attestation=False,
                )
                if not binding.get("bound") or binding.get("reevaluation_required"):
                    raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
                validation = self.validate_eligibility(
                    identifier,
                    _record=lifecycle,
                    _verify_attestation=False,
                )
                if not validation.get("eligible"):
                    raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
                payload = self._merged_lifecycle_payload(lifecycle)
                if not isinstance(payload, Mapping):
                    raise CanaryBlocked("CANDIDATE_FROZEN_BINDING_INVALID")
                quality = evaluate_prediction_data_quality(
                    self.store,
                    payload if isinstance(payload, Mapping) else {},
                    verify_attestation=False,
                )
                expected_ranking_hash = _canary_ranking_snapshot_hash(
                    identifier,
                    str(lifecycle.get("stage") or ""),
                    payload if isinstance(payload, Mapping) else {},
                    qualification_hash=str(binding.get("qualification_hash") or ""),
                    quality=quality,
                )
                ranking = connection.execute(
                    "SELECT candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,"
                    "qualification_hash,ranking_snapshot_hash,cluster_representative,reason "
                    "FROM canary_rankings WHERE candidate_id=?",
                    (identifier,),
                ).fetchone()
                if ranking is None:
                    raise CanaryBlocked("AUTONOMOUS_RANKING_NOT_CURRENT")
                try:
                    ranking_timestamp = parse_timestamp(ranking["ranking_timestamp"])
                    ranking_score = float(ranking["total_score"])
                    ranking_rank = int(ranking["rank"])
                except (TypeError, ValueError, OverflowError):
                    ranking_timestamp = None
                    ranking_score = float("nan")
                    ranking_rank = None
                try:
                    ranking_representative = int(ranking["cluster_representative"])
                except (TypeError, ValueError, OverflowError):
                    ranking_representative = None
                rank_zero_follower = (
                    ranking_rank == 0
                    and ranking_representative == 0
                    and str(ranking["reason"] or "").strip()
                    == "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                )
                if (
                    str(ranking["candidate_id"] or "").strip() != identifier
                    or str(ranking["ranking_run_id"] or "").strip() != run_id
                    or ranking_timestamp is None
                    or ranking_timestamp > now
                    or (now - ranking_timestamp).total_seconds()
                    > CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS
                    or ranking_rank is None
                    or ranking_rank < 0
                    or (ranking_rank == 0 and not rank_zero_follower)
                    or not math.isfinite(ranking_score)
                    or not binding.get("qualification_hash")
                    or ranking["qualification_hash"] != binding.get("qualification_hash")
                    or ranking["ranking_snapshot_hash"] != expected_ranking_hash
                ):
                    raise CanaryBlocked("AUTONOMOUS_RANKING_NOT_CURRENT")

                signal = connection.execute(
                    "SELECT * FROM canary_signals WHERE signal_id=?",
                    (persisted_signal_id,),
                ).fetchone()
                if signal is None:
                    raise CanaryBlocked("CANARY_SIGNAL_NOT_FOUND")
                try:
                    signal_expires = parse_timestamp(signal["expires_at"])
                except (TypeError, ValueError):
                    signal_expires = None
                try:
                    signal_evidence = json.loads(signal["evidence_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    signal_evidence = None
                frozen_hash = self._lifecycle_frozen_hash(lifecycle)
                if (
                    str(signal["candidate_id"] or "").strip() != identifier
                    or str(signal["status"] or "").upper() != "READY"
                    or signal_expires is None
                    or signal_expires <= now
                    or signal["frozen_hash"] != frozen_hash
                    or signal["strategy_hash"] != payload.get("strategy_hash")
                    or signal["model_hash"] != payload.get("model_hash")
                    or signal["config_hash"] != payload.get("config_hash")
                    or not isinstance(signal_evidence, Mapping)
                    or signal_evidence.get("current_execution_evidence") != CURRENT_ORDER_BOOK
                ):
                    raise CanaryBlocked("CANARY_SIGNAL_NO_LONGER_VALID")

                current_candidate = (
                    str(control["candidate_id"]).strip()
                    if control["candidate_id"] is not None
                    else None
                )
                if current_candidate != identifier:
                    updated = connection.execute(
                        "UPDATE canary_control SET candidate_id=?,updated_at=?,"
                        "control_generation=? WHERE singleton=1 AND state=? "
                        "AND control_generation=?",
                        (
                            identifier,
                            now_text,
                            control_generation + 1,
                            AUTONOMOUS_MICRO_LIVE,
                            control_generation,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise CanaryBlocked("CANARY_CONTROL_CHANGED")
                    control_generation += 1
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        if publish_readiness:
            self.publish_readiness_snapshot(reason="AUTONOMOUS_ACTIONABLE_BOUND")
        return {
            "candidate_id": identifier,
            "ranking_run_id": run_id,
            "signal_id": persisted_signal_id,
            "control_generation": control_generation,
        }


    def _candidate_signal_binding(self, candidate_id: str) -> dict[str, Any]:
        """Load the candidate's immutable executable documents and binding."""
        identifier = str(candidate_id).strip()
        if not identifier:
            raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
        lifecycle = self.store.load_candidate_lifecycle(identifier)
        if not isinstance(lifecycle, Mapping) or lifecycle.get("stage") not in _CANARY_ELIGIBLE_STAGES:
            raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
        eligibility = self.store.connection.execute(
            "SELECT candidate_id,frozen_hash,evidence_json FROM canary_eligibility "
            "WHERE candidate_id=?",
            (identifier,),
        ).fetchone()
        binding = self._eligibility_binding_result(
            identifier,
            eligibility,
            record=lifecycle,
        )
        if not binding.get("bound"):
            raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
        if binding.get("reevaluation_required"):
            try:
                self.mark_eligible(identifier)
            except Exception as exc:
                raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE") from exc
            lifecycle = self.store.load_candidate_lifecycle(identifier)
            eligibility = self.store.connection.execute(
                "SELECT candidate_id,frozen_hash,evidence_json FROM canary_eligibility "
                "WHERE candidate_id=?",
                (identifier,),
            ).fetchone()
            if not self._eligibility_binding_result(
                identifier,
                eligibility,
                record=lifecycle,
            ).get("bound"):
                raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
        payload = self._merged_lifecycle_payload(lifecycle)
        frozen_hash = self._lifecycle_frozen_hash(lifecycle)
        if payload is None or frozen_hash is None:
            raise CanaryBlocked("CANDIDATE_FROZEN_BINDING_INVALID")
        quality = evaluate_prediction_data_quality(self.store, payload)
        validation = self.validate_eligibility(identifier, _record=lifecycle)
        if not validation.get("eligible"):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        if not quality.get("canary_data_quality_acceptable"):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        scope_binding = _canary_scope_binding(payload)
        scope_resolution = _canary_current_scope_resolution(
            self.store,
            identifier,
            payload,
            now=ensure_utc(self.clock()),
        )
        if not scope_resolution.get("bound"):
            raise CanaryBlocked(
                str(
                    scope_resolution.get("reason_code")
                    or "SCOPE_RESOLUTION_MISSING"
                )
            )

        forward_test: Mapping[str, Any] | None = None
        forward_config: Mapping[str, Any] = {}
        risk_limits: Mapping[str, Any] = {}
        forward_id = str(payload.get("forward_test_id", "")).strip()
        if forward_id:
            forward_test = self.store.load_forward_test(forward_id)
            if not isinstance(forward_test, Mapping):
                raise CanaryBlocked("CANDIDATE_FROZEN_BINDING_INVALID")
            raw_config = forward_test.get("config")
            raw_risk = forward_test.get("risk_limits")
            if isinstance(raw_config, Mapping):
                forward_config = dict(raw_config)
            if isinstance(raw_risk, Mapping):
                risk_limits = dict(raw_risk)

        frozen_documents = payload.get("frozen_documents", {})
        if frozen_documents is None:
            frozen_documents = {}
        if not isinstance(frozen_documents, Mapping):
            raise CanaryBlocked("CANDIDATE_FROZEN_BINDING_INVALID")
        plan = payload.get("experiment_plan", {})
        if not isinstance(plan, Mapping):
            plan = {}
        strategy_document = payload.get("strategy_document", payload.get("strategy"))
        if not isinstance(strategy_document, Mapping):
            strategy_document = frozen_documents.get(
                "strategy_document", frozen_documents.get("strategy")
            )
        if not isinstance(strategy_document, Mapping):
            strategy_document = forward_config.get("strategy_document")
        model_document = payload.get("model_document")
        if not isinstance(model_document, Mapping):
            model_document = frozen_documents.get("model_document", frozen_documents.get("model"))
        if not isinstance(model_document, Mapping):
            model_document = forward_config.get("model_document")
        if not isinstance(model_document, Mapping):
            model_document = plan.get("model_document")
        if not isinstance(strategy_document, Mapping) or not isinstance(model_document, Mapping):
            raise CanaryBlocked("CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE")

        try:
            from .strategy import load_strategy

            strategy = load_strategy(strategy_document)
        except Exception as exc:
            raise CanaryBlocked("CANDIDATE_EXECUTABLE_DOCUMENTS_INVALID") from exc
        if strategy.market_type.value != "prediction":
            raise CanaryBlocked("CANARY_MARKET_TYPE_UNSUPPORTED")
        expected_strategy_hash = str(payload.get("strategy_hash", "")).strip()
        expected_model_hash = str(payload.get("model_hash", "")).strip()
        expected_config_hash = str(payload.get("config_hash", "")).strip()
        strategy_hash_matches = (
            self._document_hash(strategy_document) == expected_strategy_hash
            or self._document_hash(strategy.to_dict()) == expected_strategy_hash
        )
        if (
            not expected_strategy_hash
            or not expected_model_hash
            or not expected_config_hash
            or not strategy_hash_matches
            or self._document_hash(model_document) != expected_model_hash
        ):
            raise CanaryBlocked("CANDIDATE_FROZEN_BINDING_INVALID")
        scope_binding = _canary_scope_binding(payload)
        return {
            "candidate_id": identifier,
            "lifecycle": lifecycle,
            "payload": payload,
            "qualification_hash": binding.get("qualification_hash"),
            "plan_hash": scope_binding.get("plan_hash"),
            "frozen_hash": frozen_hash,
            "strategy_hash": expected_strategy_hash,
            "model_hash": expected_model_hash,
            "config_hash": expected_config_hash,
            "strategy": strategy,
            "model_document": dict(model_document),
            "forward_test": forward_test,
            "forward_config": forward_config,
            "data_quality": quality,
            "risk_limits": risk_limits,
            "scope_resolution": scope_resolution,
            "scope_hash": scope_resolution.get("scope_hash"),
            "scope_version": scope_resolution.get("scope_version"),
        }
    @staticmethod
    def _signal_observation(row: Mapping[str, Any]) -> dict[str, Any] | None:
        raw_payload = row.get("payload")
        if not isinstance(raw_payload, Mapping):
            return None
        nested = raw_payload.get("snapshot")
        observation = dict(nested) if isinstance(nested, Mapping) else dict(raw_payload)
        observation.setdefault("market_id", row.get("market_id"))
        observation.setdefault("timestamp", row.get("source_timestamp") or row.get("observed_at"))
        observation["source_snapshot_id"] = row.get("snapshot_id")
        observation["source_timestamp"] = row.get("source_timestamp")
        observation["observed_at"] = row.get("observed_at")
        observation["source_type"] = str(
            row.get("source_type") or raw_payload.get("source_type") or ""
        ).strip().upper()
        observation.setdefault(
            "research_quality",
            row.get("quality") or raw_payload.get("research_quality") or raw_payload.get("quality"),
        )
        for key in (
            "yes_order_book",
            "no_order_book",
            "quotes",
            "depth",
            "liquidity",
            "research_quality",
            "settlement",
            "yes_token_id",
            "no_token_id",
            "token_ids",
            "active",
            "closed",
        ):
            if key in raw_payload:
                observation[key] = raw_payload[key]
        return observation

    @staticmethod
    def _current_order_book(
        observation: Mapping[str, Any],
        outcome: str,
        *,
        now: datetime,
        token_id: str | None = None,
    ) -> Mapping[str, Any] | None:
        if str(observation.get("source_type", "")).upper() != "FORWARD_COLLECTED":
            return None
        book = observation.get(f"{outcome}_order_book")
        if not isinstance(book, Mapping) or not isinstance(book.get("asks"), list) or not book["asks"]:
            return None
        book_timestamp = parse_timestamp(book.get("timestamp"))
        source_timestamp = parse_timestamp(observation.get("source_timestamp"))
        if (
            book_timestamp is None
            or source_timestamp is None
            or book_timestamp > now
            or (now - book_timestamp).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS
            or book_timestamp != source_timestamp
        ):
            return None
        book_token = str(book.get("token_id") or "").strip()
        if token_id and book_token and book_token != str(token_id).strip():
            return None
        return book

    def _forward_snapshot_rows(self, market_id: str, *, limit: int = 512) -> list[dict[str, Any]]:
        rows = self.store.load_polymarket_snapshots(
            str(market_id),
            source_type="FORWARD_COLLECTED",
            latest=True,
            limit=limit,
        )
        return list(reversed(rows))

    @staticmethod
    def _apply_signal_model(
        observations: list[dict[str, Any]], model_document: Mapping[str, Any]
    ) -> bool:
        for observation in observations:
            probability = evaluate_model_document_probability(model_document, observation)
            if probability is None:
                return False
            observation["model_probability"] = probability
        return bool(observations)

    def _current_signal_market(
        self, market_id: str, *, now: datetime
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        rows = self._forward_snapshot_rows(market_id, limit=1)
        if not rows:
            return None
        row = rows[-1]
        observation = self._signal_observation(row)
        if observation is None:
            return None
        source_timestamp = parse_timestamp(row.get("source_timestamp"))
        if source_timestamp is None or source_timestamp > now:
            return None
        if (now - source_timestamp).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS:
            return None
        settlement = str(observation.get("settlement", "open")).strip().lower()
        if (
            _canary_closed_flag(observation.get("closed", False))
            or observation.get("active") is False
            or settlement in {"resolved_yes", "resolved_no", "void", "closed"}
        ):
            return None
        return row, observation

    def _prune_canary_signals(self) -> None:
        """Bound unreferenced transient signal history in the current transaction.

        Signal rows are also execution/idempotency state.  A row is therefore
        retained when another persisted canary record names it or when it is
        the newest READY signal for its candidate.  Every other unreferenced
        row is transient history and is subject to the fixed global bound.
        """
        transient_limit = max(0, int(CANARY_SIGNAL_TRANSIENT_RETENTION))
        self.store.connection.execute(
            """
            DELETE FROM canary_signals
            WHERE signal_id IN (
                SELECT signal.signal_id
                FROM canary_signals AS signal
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM canary_signal_evaluations AS evaluation
                    WHERE evaluation.signal_id = signal.signal_id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM canary_ledger AS ledger
                    WHERE ledger.signal_id = signal.signal_id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM canary_execution_events AS execution
                    JOIN canary_ledger AS ledger
                      ON ledger.event_id = execution.canary_event_id
                    WHERE ledger.signal_id = signal.signal_id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM canary_autonomous_state AS autonomous
                    WHERE autonomous.last_signal_id = signal.signal_id
                )
                AND NOT (
                    UPPER(signal.status) = 'READY'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM canary_signals AS newer
                        WHERE newer.candidate_id = signal.candidate_id
                          AND UPPER(newer.status) = 'READY'
                          AND (
                              newer.generated_at > signal.generated_at
                              OR (
                                  newer.generated_at = signal.generated_at
                                  AND newer.signal_id > signal.signal_id
                              )
                          )
                    )
                )
                ORDER BY signal.generated_at DESC, signal.signal_id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (transient_limit,),
        )

    def _persist_signal_evaluation(self, result: Mapping[str, Any]) -> dict[str, Any]:
        """Persist one bounded, queryable outcome of signal evaluation."""
        evaluated_at = str(result.get("evaluated_at") or "").strip()
        candidate_id = str(result.get("candidate_id") or "").strip()
        reason_code = str(result.get("reason_code") or "").strip().upper()
        if not evaluated_at or not candidate_id or not reason_code:
            raise ValueError("signal evaluation is incomplete")
        cycle_id = str(result.get("cycle_id") or "").strip() or None
        market_id = str(result.get("market_id") or "").strip() or None
        signal = result.get("signal")
        signal_id = (
            str(signal.get("signal_id") or "").strip()
            if isinstance(signal, Mapping)
            else None
        )
        required_health = result.get("required_health")
        if not isinstance(required_health, Mapping):
            required_health = {}
        evidence = result.get("evidence")
        if not isinstance(evidence, Mapping):
            evidence = {}
        signal_json = (
            json.dumps(dict(signal), sort_keys=True, separators=(",", ":"), allow_nan=False)
            if isinstance(signal, Mapping)
            else None
        )
        health_json = json.dumps(
            dict(required_health), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        evidence_json = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        evaluation_id = "canary-evaluation-" + uuid.uuid4().hex
        with self.store._lock:
            self.store.connection.execute(
                "INSERT INTO canary_signal_evaluations("
                "evaluation_id,candidate_id,cycle_id,evaluated_at,reason_code,"
                "market_id,signal_id,signal_json,required_health_json,evidence_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    evaluation_id,
                    candidate_id,
                    cycle_id,
                    evaluated_at,
                    reason_code,
                    market_id,
                    signal_id,
                    signal_json,
                    health_json,
                    evidence_json,
                ),
            )
            self.store.connection.execute(
                "DELETE FROM canary_signal_evaluations "
                "WHERE evaluation_id IN ("
                "SELECT evaluation_id FROM canary_signal_evaluations "
                "ORDER BY evaluated_at DESC,evaluation_id DESC LIMIT -1 OFFSET 4096)"
            )
            self._prune_canary_signals()
            self.store.connection.commit()
        persisted = dict(result)
        persisted["evaluation_id"] = evaluation_id
        return persisted

    def list_signal_evaluations(
        self,
        candidate_id: str | None = None,
        *,
        cycle_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return recent persisted evaluations using bounded indexed queries."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        clauses: list[str] = []
        values: list[Any] = []
        if candidate_id is not None:
            clauses.append("candidate_id=?")
            values.append(str(candidate_id).strip())
        if cycle_id is not None:
            clauses.append("cycle_id=?")
            values.append(str(cycle_id).strip())
        query = "SELECT * FROM canary_signal_evaluations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY evaluated_at DESC,evaluation_id DESC LIMIT ?"
        values.append(min(int(limit), 4096))
        with self.store._lock:
            rows = self.store.connection.execute(query, values).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)
            for field in ("signal_json", "required_health_json", "evidence_json"):
                raw = record.pop(field, None)
                if field == "signal_json" and not raw:
                    decoded = None
                else:
                    try:
                        decoded = json.loads(raw or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        decoded = {}
                record[field.removesuffix("_json")] = (
                    dict(decoded) if isinstance(decoded, Mapping) else None
                )
            records.append(record)
        return records

    def _persist_ready_signal(
        self,
        binding: Mapping[str, Any],
        *,
        market_id: str,
        current_row: Mapping[str, Any],
        current_observation: Mapping[str, Any],
        current_book: Mapping[str, Any],
        outcome: str,
        score: float,
        expected_price: Decimal,
        token_id: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        source_snapshot_id = str(current_row.get("snapshot_id") or "").strip()
        source_timestamp = parse_timestamp(current_row.get("source_timestamp"))
        if not source_snapshot_id or source_timestamp is None:
            return None
        side = "BUY"
        identity = {
            "candidate_id": binding["candidate_id"],
            "frozen_hash": binding["frozen_hash"],
            "source_snapshot_id": source_snapshot_id,
            "market_id": market_id,
            "token_id": token_id,
            "outcome": outcome,
            "side": side,
            "paper_expected_price": str(expected_price),
            "score": score,
            "current_order_book_hash": _canary_document_hash(current_book),
            "scope_hash": binding.get("scope_hash"),
            "scope_version": binding.get("scope_version"),
        }
        base_signal_id = "canary-signal-" + hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()[:32]
        signal_id = base_signal_id
        expires_at = now + timedelta(seconds=CANARY_SIGNAL_TTL_SECONDS)
        existing_signal: dict[str, Any] | None = None
        expired_ready_invalidated = False
        with self.store._lock:
            if self.store.connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            matching = self.store.connection.execute(
                "SELECT * FROM canary_signals WHERE signal_id LIKE ? || '%' "
                "AND candidate_id=? AND frozen_hash=? "
                "AND strategy_hash=? AND model_hash=? AND config_hash=? "
                "AND market_id=? AND token_id=? AND outcome=? AND side=? "
                "AND paper_expected_price=? AND source_snapshot_id=? "
                "ORDER BY generated_at DESC,signal_id DESC",
                (
                    base_signal_id,
                    binding["candidate_id"],
                    binding["frozen_hash"],
                    binding["strategy_hash"],
                    binding["model_hash"],
                    binding["config_hash"],
                    market_id,
                    token_id,
                    outcome,
                    side,
                    str(expected_price),
                    source_snapshot_id,
                ),
            ).fetchall()
            protected = next(
                (
                    row
                    for row in matching
                    if str(row["status"] or "").upper()
                    in {
                        "SUBMITTED",
                        "SUBMITTING",
                        "UNKNOWN",
                        "REJECTED",
                        "ACCEPTED",
                        "MATCHED",
                        "PARTIAL",
                        "PARTIALLY_FILLED",
                        "SETTLED",
                    }
                ),
                None,
            )
            if protected is not None:
                existing_signal = self._signal_from_row(protected)
            else:
                ready = next(
                    (
                        row
                        for row in matching
                        if str(row["status"] or "").upper() == "READY"
                    ),
                    None,
                )
                if ready is not None:
                    ready_expires = parse_timestamp(ready["expires_at"])
                    if ready_expires is not None and ready_expires > now:
                        existing_signal = self._signal_from_row(ready)
                    else:
                        updated = self.store.connection.execute(
                            "UPDATE canary_signals SET status='EXPIRED',"
                            "reason='SIGNAL_EXPIRED',updated_at=? "
                            "WHERE signal_id=? AND status='READY'",
                            (now.isoformat(), ready["signal_id"]),
                        )
                        expired_ready_invalidated = updated.rowcount == 1
                if existing_signal is None and (
                    ready is None or expired_ready_invalidated
                ):
                    if matching:
                        previous = matching[0]
                        refresh_seed = (
                            str(previous["signal_id"]) + "|" + str(previous["expires_at"] or "")
                        )
                        signal_id = (
                            base_signal_id
                            + "-refresh-"
                            + hashlib.sha256(refresh_seed.encode()).hexdigest()[:16]
                        )
                    source_observed_at = current_row.get("observed_at")
                    if isinstance(source_observed_at, datetime):
                        source_observed_at = ensure_utc(source_observed_at).isoformat()
                    scope_market = next(
                        (
                            item
                            for item in binding.get("scope_resolution", {}).get(
                                "matched_markets", ()
                            )
                            if isinstance(item, Mapping)
                            and str(item.get("market_id") or "").strip() == str(market_id)
                        ),
                        {},
                    )
                    scope_provenance = (
                        scope_market.get("metadata_provenance")
                        if isinstance(scope_market.get("metadata_provenance"), Mapping)
                        else scope_market.get("metadata")
                        if isinstance(scope_market.get("metadata"), Mapping)
                        else {}
                    )
                    scope_resolution = binding.get("scope_resolution")
                    scope_resolution_row = (
                        scope_resolution.get("resolution")
                        if isinstance(scope_resolution, Mapping)
                        else scope_resolution
                    )
                    evidence = {
                        "score": score,
                        "model_probability": current_observation.get("model_probability"),
                        "market_price": str(expected_price),
                        "research_quality": current_observation.get("research_quality"),
                        "current_execution_evidence": CURRENT_ORDER_BOOK,
                        "current_order_book_timestamp": current_book.get("timestamp"),
                        "current_order_book_source": "FORWARD_COLLECTED",
                        "source_type": current_row.get("source_type"),
                        "source_observed_at": source_observed_at,
                        "source_snapshot_id": source_snapshot_id,
                        "source_timestamp": source_timestamp.isoformat(),
                        "plan_hash": _canary_scope_binding(binding.get("payload")).get("plan_hash"),
                        "market_scope_hash": binding.get("scope_hash"),
                        "market_scope_version": binding.get("scope_version"),
                        "scope_resolution_id": _canary_resolution_attr(
                            scope_resolution_row, "resolution_id"
                        ),
                        "market_metadata_provenance": dict(scope_provenance),
                    }
                    self.store.connection.execute(
                        "INSERT INTO canary_signals("
                        "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
                        "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
                        "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            signal_id,
                            binding["candidate_id"],
                            binding["frozen_hash"],
                            binding["strategy_hash"],
                            binding["model_hash"],
                            binding["config_hash"],
                            market_id,
                            token_id,
                            outcome,
                            side,
                            str(expected_price),
                            source_snapshot_id,
                            source_timestamp.isoformat(),
                            now.isoformat(),
                            expires_at.isoformat(),
                            "READY",
                            None,
                            json.dumps(evidence, sort_keys=True, allow_nan=False),
                            now.isoformat(),
                        ),
                    )
            self._prune_canary_signals()
            self.store.connection.commit()
        self.publish_readiness_snapshot(reason="SIGNAL_GENERATED")
        if existing_signal is not None:
            return existing_signal
        return self.get_signal(signal_id)

    def evaluate_signal(
        self,
        candidate_id: str,
        *,
        cycle_id: str | None = None,
    ) -> dict[str, Any]:
        """Evaluate one candidate from authorized persisted forward evidence."""
        now = ensure_utc(self.clock())
        identifier = str(candidate_id).strip()
        if not identifier:
            identifier = str(candidate_id)
        evaluated_at = now.isoformat()
        cycle = str(cycle_id).strip() if cycle_id is not None else None

        def finish(
            reason_code: str,
            *,
            market_id: str | None = None,
            signal: Mapping[str, Any] | None = None,
            required_health: Mapping[str, Any] | None = None,
            evidence: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            result = {
                "candidate_id": identifier,
                "cycle_id": cycle,
                "evaluated_at": evaluated_at,
                "reason_code": str(reason_code).strip().upper(),
                "market_id": market_id,
                "signal": dict(signal) if isinstance(signal, Mapping) else None,
                "required_health": (
                    dict(required_health) if isinstance(required_health, Mapping) else {}
                ),
                "evidence": dict(evidence) if isinstance(evidence, Mapping) else {},
            }
            return self._persist_signal_evaluation(result)

        lifecycle = self.store.load_candidate_lifecycle(identifier)
        lifecycle_payload = self._merged_lifecycle_payload(lifecycle)
        scope_binding = _canary_scope_binding(lifecycle_payload)
        scope_resolution = _canary_current_scope_resolution(
            self.store,
            identifier,
            lifecycle_payload,
            now=now,
        )
        if not scope_resolution.get("bound"):
            evidence = {
                "scope_hash": scope_resolution.get("scope_hash"),
                "scope_version": scope_resolution.get("scope_version"),
                "plan_hash": scope_binding.get("plan_hash"),
                "scope_resolution_reason": scope_resolution.get("reason_code"),
                "excluded_markets": scope_resolution.get("excluded_markets", []),
                "deferred_markets": scope_resolution.get("deferred_markets", []),
            }
            # Persisted exclusions/deferred entries are diagnostic evidence
            # only.  Project them to a precise non-actionable reason without
            # treating any excluded market as executable authority.
            disposition_failures: list[dict[str, Any]] = []
            for field in ("excluded_markets", "deferred_markets"):
                values = scope_resolution.get(field, ())
                if not isinstance(values, (list, tuple)):
                    continue
                for disposition in values:
                    if not isinstance(disposition, Mapping):
                        continue
                    market_id = str(disposition.get("market_id") or "").strip()
                    if not market_id:
                        continue
                    failure_reason = _canary_scope_disposition_failure(disposition)
                    disposition_failures.append(
                        {
                            "market_id": market_id,
                            "reason_code": failure_reason,
                            "disposition_reason": str(
                                disposition.get("reason") or ""
                            ).strip().upper(),
                            "detail": str(disposition.get("detail") or "").strip(),
                        }
                    )
            if disposition_failures:
                selected = min(
                    enumerate(disposition_failures),
                    key=lambda item: (
                        -_CANARY_SCOPE_DISPOSITION_FAILURE_PRECEDENCE.get(
                            item[1]["reason_code"], 0
                        ),
                        item[0],
                    ),
                )[1]
                evidence["market_failures"] = [
                    {
                        "market_id": item["market_id"],
                        "reason_code": item["reason_code"],
                    }
                    for item in disposition_failures
                ]
                evidence["scope_disposition_reason"] = selected[
                    "disposition_reason"
                ]
                if selected["detail"]:
                    evidence["scope_disposition_detail"] = selected["detail"]
                return finish(
                    selected["reason_code"],
                    market_id=selected["market_id"],
                    evidence=evidence,
                )
            return finish(
                str(
                    scope_resolution.get("reason_code")
                    or "SCOPE_RESOLUTION_MISSING"
                ),
                evidence=evidence,
            )
        resolved_markets = scope_resolution.get("matched_markets", [])
        market_ids = tuple(
            str(item.get("market_id") or "").strip()
            for item in resolved_markets
            if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
        )
        scope = scope_binding.get("market_scope", {})
        declared_values = (
            scope.get("market_ids", scope.get("exact_market_ids", ()))
            if isinstance(scope, Mapping)
            else ()
        )
        declared_market_ids = tuple(
            str(value).strip()
            for value in declared_values
            if str(value).strip()
        )
        cap_evidence = _execution_market_cap_evidence(
            market_ids,
            declared_market_ids,
        )
        if (
            len(market_ids) > CANARY_EXECUTION_MARKET_CAP
            or len(declared_market_ids) > CANARY_EXECUTION_MARKET_CAP
        ):
            return finish(
                EXECUTION_FEASIBILITY_MARKET_CAP,
                evidence={
                    "scope_hash": scope_binding.get("scope_hash"),
                    "scope_version": scope_binding.get("scope_version"),
                    "plan_hash": scope_binding.get("plan_hash"),
                    "scope_resolution_status": _canary_resolution_attr(
                        scope_resolution.get("resolution"), "status"
                    ),
                    **cap_evidence,
                },
            )
        authority_reason = "CANDIDATE_FORWARD_MARKET_RESOLVED"
        authority: Mapping[str, Any] = {
            "candidates": [
                {
                    "candidate_id": identifier,
                    "resolution": "RESOLVED",
                    "reason_code": authority_reason,
                    "market_ids": list(market_ids),
                    "permitted_market_ids": list(market_ids),
                    "declared_market_ids": list(declared_market_ids),
                }
            ],
            "market_ids": list(market_ids),
            "as_of": scope_resolution.get("resolved_at"),
        }
        candidate_entry: Mapping[str, Any] = authority["candidates"][0]
        required_health: Mapping[str, Any] = {}
        try:
            health = self.store.polymarket_required_health(
                requirements=authority,
                now=now,
                stale_after_seconds=CANARY_SIGNAL_MAX_AGE_SECONDS,
            )
            if isinstance(health, Mapping):
                required_health = health
        except Exception as exc:
            return finish(
                "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                required_health={},
                evidence={"health_error": type(exc).__name__},
            )
        resolved_token_bindings = {
            str(item.get("market_id") or "").strip(): {
                "yes_token_id": str(item.get("yes_token_id") or "").strip(),
                "no_token_id": str(item.get("no_token_id") or "").strip(),
            }
            for item in scope_resolution.get("matched_markets", ())
            if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
        }

        evidence: dict[str, Any] = {
            "authority": {
                "market_ids": list(market_ids),
                "candidate_reason_code": authority_reason or None,
                "as_of": authority.get("as_of"),
            },
            "required_health_reason_code": required_health.get("reason_code"),
            "required_market_count": required_health.get("required_market_count"),
        }
        evidence.update(
            {
                "plan_hash": scope_binding.get("plan_hash"),
                "scope_hash": scope_binding.get("scope_hash"),
                "scope_version": scope_binding.get("scope_version"),
                "scope_resolution_status": _canary_resolution_attr(
                    scope_resolution.get("resolution"), "status"
                )
                if scope_resolution.get("resolution") is not None
                else None,
                "scope_resolution_resolved_at": scope_resolution.get("resolved_at"),
                "resolved_market_ids": list(market_ids),
                "excluded_markets": scope_resolution.get("excluded_markets", []),
                "deferred_markets": scope_resolution.get("deferred_markets", []),
                "resolved_market_metadata_provenance": {
                    str(item.get("market_id") or "").strip(): dict(
                        item.get("metadata_provenance")
                        if isinstance(item.get("metadata_provenance"), Mapping)
                        else item.get("metadata")
                        if isinstance(item.get("metadata"), Mapping)
                        else {}
                    )
                    for item in scope_resolution.get("matched_markets", ())
                    if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
                },
            }
        )
        for key in ("fresh", "stale", "missing", "closed_candidates", "unresolved_candidates"):
            value = required_health.get(key, authority.get(key, ()))
            if isinstance(value, (list, tuple)):
                evidence[key] = list(value)

        filter_values = candidate_entry.get(
            "normalized_frozen_filters", candidate_entry.get("normalized_filters", {})
        )
        if not isinstance(filter_values, Mapping):
            filter_values = {}

        # ``candidate_forward_requirements`` deliberately omits exact targets
        # which are currently missing, closed, or outside frozen filters.  Keep
        # those declared targets in the bounded scan when they can be diagnosed
        # locally, while retaining the authority order for permitted markets.
        filter_mismatch_ids: set[str] = set()
        closed_market_ids: set[str] = set()
        missing_market_ids: set[str] = set()
        active_inventory_by_id: dict[str, Mapping[str, Any]] = {}
        all_inventory_by_id: dict[str, Mapping[str, Any]] = {}
        inventory_loaded = False
        if declared_market_ids:
            try:
                active_inventory = self.store.tracked_polymarket_markets(
                    active_only=True,
                    now=now,
                    include_payload=True,
                    limit=len(declared_market_ids),
                    market_ids=declared_market_ids,
                )
                active_inventory_by_id = {
                    str(item.get("market_id") or "").strip(): item
                    for item in active_inventory
                    if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
                }
                all_inventory = self.store.tracked_polymarket_markets(
                    active_only=False,
                    now=now,
                    include_payload=True,
                    limit=len(declared_market_ids),
                    market_ids=declared_market_ids,
                )
                all_inventory_by_id = {
                    str(item.get("market_id") or "").strip(): item
                    for item in all_inventory
                    if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
                }
                inventory_loaded = True
            except Exception:
                active_inventory_by_id = {}
                all_inventory_by_id = {}

        for declared_market_id in declared_market_ids:
            record = all_inventory_by_id.get(declared_market_id)
            if not isinstance(record, Mapping):
                continue
            payload = record.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            snapshot = payload.get("snapshot")
            snapshot = snapshot if isinstance(snapshot, Mapping) else {}
            metadata = payload.get("metadata")
            metadata = metadata if isinstance(metadata, Mapping) else {}
            settlement = str(
                snapshot.get(
                    "settlement",
                    payload.get("settlement", metadata.get("settlement", "")),
                )
                or ""
            ).strip().lower()
            closed = (
                record.get("active") is False
                or _canary_closed_flag(payload.get("closed", False))
                or _canary_closed_flag(metadata.get("closed", False))
            )
            if (
                closed
                or settlement in {"resolved_yes", "resolved_no", "void", "closed", "expired"}
            ):
                closed_market_ids.add(declared_market_id)

        if inventory_loaded:
            ignored_values = candidate_entry.get("historical_market_ids_ignored", ())
            ignored_market_ids = (
                {
                    str(value).strip()
                    for value in ignored_values
                    if str(value).strip()
                }
                if isinstance(ignored_values, (list, tuple, set, frozenset))
                else set()
            )
            missing_market_ids = {
                market_id
                for market_id in declared_market_ids
                if market_id not in all_inventory_by_id
                and market_id not in ignored_market_ids
            }

        if filter_values and declared_market_ids:
            try:
                from .experiment_plan import forward_market_matches

                permitted_set = set(market_ids)
                for declared_market_id in declared_market_ids:
                    if (
                        declared_market_id not in permitted_set
                        and declared_market_id in active_inventory_by_id
                        and not forward_market_matches(
                            active_inventory_by_id[declared_market_id],
                            filter_values,
                            now=now,
                        )
                    ):
                        filter_mismatch_ids.add(declared_market_id)
            except (TypeError, ValueError, KeyError):
                filter_mismatch_ids = set()

        # The resolver's immutable exclusions are authoritative diagnostics.
        # They are never appended to ``market_ids`` or sent through model
        # evaluation, but must remain visible when current inventory shape
        # cannot independently classify an omitted declared target.
        for field in ("excluded_markets", "deferred_markets"):
            values = scope_resolution.get(field, ())
            if not isinstance(values, (list, tuple)):
                continue
            for disposition in values:
                if not isinstance(disposition, Mapping):
                    continue
                market_id = str(disposition.get("market_id") or "").strip()
                if not market_id:
                    continue
                failure_reason = _canary_scope_disposition_failure(disposition)
                if failure_reason == "MARKET_CLOSED":
                    closed_market_ids.add(market_id)
                elif failure_reason == "MARKET_FILTER_MISMATCH":
                    filter_mismatch_ids.add(market_id)
                elif failure_reason == "NO_FORWARD_SNAPSHOT":
                    missing_market_ids.add(market_id)

        if authority_reason == "COLLECTOR_CAPACITY_INSUFFICIENT":
            return finish(
                "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                market_id=market_ids[0] if market_ids else None,
                required_health=required_health,
                evidence={**evidence, "authority_reason_code": authority_reason},
            )

        # The authority list is already bounded to eight.  Declared targets
        # omitted by requirements remain diagnostic-only: their closed,
        # filter-mismatch, and unresolved classifications were resolved above,
        # but they must never enter executable signal evaluation.
        #
        # Keep the executable loop restricted to authority-permitted market
        # ids.  In particular, do not append declared ids here: a target
        # excluded by frozen filters or target_instrument could otherwise
        # produce a READY signal despite not being authorized.

        missing = required_health.get("missing", ())
        stale = required_health.get("stale", ())
        missing_markets = (
            {str(value).strip() for value in missing if str(value).strip()}
            if isinstance(missing, (list, tuple, set, frozenset))
            else set()
        )
        stale_markets = (
            {str(value).strip() for value in stale if str(value).strip()}
            if isinstance(stale, (list, tuple, set, frozenset))
            else set()
        )
        health_blocked_markets: set[str] = set()
        for field in ("blocked", "health_blocked", "blocked_markets", "degraded"):
            values = required_health.get(field, ())
            if isinstance(values, (list, tuple, set, frozenset)):
                health_blocked_markets.update(
                    str(value).strip() for value in values if str(value).strip()
                )
        health_states: dict[str, str] = {}
        diagnostics = required_health.get("market_diagnostics", required_health.get("diagnostics", ()))
        if isinstance(diagnostics, (list, tuple)):
            for diagnostic in diagnostics:
                if not isinstance(diagnostic, Mapping):
                    continue
                diagnostic_market_id = str(diagnostic.get("market_id") or "").strip()
                state = str(diagnostic.get("collection_state") or "").strip().lower()
                if diagnostic_market_id and state:
                    health_states[diagnostic_market_id] = state
                    if state in {"blocked", "degraded", "unhealthy"}:
                        health_blocked_markets.add(diagnostic_market_id)
        for market_id in missing_markets:
            health_states.setdefault(market_id, "missing")
        for market_id in stale_markets:
            health_states.setdefault(market_id, "stale")
        fresh_values = required_health.get("fresh", ())
        fresh_markets = (
            {str(value).strip() for value in fresh_values if str(value).strip()}
            if isinstance(fresh_values, (list, tuple, set, frozenset))
            else set()
        )
        for market_id in fresh_markets:
            health_states.setdefault(market_id, "fresh")
        health_grade = str(required_health.get("grade") or "").strip().upper()
        health_reason = str(required_health.get("reason_code") or "").strip().upper()
        health_has_market_detail = bool(
            missing_markets
            or stale_markets
            or fresh_markets
            or health_blocked_markets
            or health_states
        )

        binding: Mapping[str, Any] | None = None
        if market_ids:
            try:
                binding = self._candidate_signal_binding(identifier)
            except CanaryBlocked as exc:
                evidence["binding_reason"] = str(exc)
                return finish(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id=market_ids[0],
                    required_health=required_health,
                    evidence=evidence,
                )

        # A failed market is diagnostic evidence, not a terminal evaluation.
        # Higher values are the canonical aggregate severity: terminal market
        # state, authority mismatch, missing/stale evidence, collector health,
        # then a strategy that is simply non-actionable.  Ties retain the
        # deterministic permitted-market order.
        failure_precedence = {
            "MARKET_CLOSED": 60,
            "MARKET_FILTER_MISMATCH": 50,
            "SCOPE_RESOLUTION_TOKEN_MISMATCH": 50,
            "NO_FORWARD_SNAPSHOT": 40,
            "STALE_FORWARD_EVIDENCE": 30,
            "COLLECTOR_CANDIDATE_HEALTH_BLOCKED": 20,
            "MODEL_INPUT_MISSING": 15,
            "WARMING_UP": 12,
            "INSUFFICIENT_LOOKBACK": 12,
            "STRATEGY_EVALUATED_DECLINED": 10,
            "NO_STRATEGY_SIGNAL": 10,
        }
        failures: list[dict[str, Any]] = []

        def record_failure(
            reason_code: str,
            market_id: str,
            *,
            details: Mapping[str, Any] | None = None,
        ) -> None:
            failure_reason = str(reason_code).strip().upper()
            failure_evidence = dict(evidence)
            if isinstance(details, Mapping):
                failure_evidence.update(dict(details))
            failure_evidence["market_id"] = market_id
            failure_evidence["failure_reason_code"] = failure_reason
            failures.append(
                {
                    "reason_code": failure_reason,
                    "market_id": market_id,
                    "evidence": failure_evidence,
                    "order": len(failures),
                }
            )

        for market_id in market_ids:
            if market_id in closed_market_ids:
                record_failure(
                    "MARKET_CLOSED",
                    market_id,
                    details={"signal_blocker": "MARKET_CLOSED"},
                )
                continue
            market_health_state = health_states.get(market_id, "")
            if market_id in filter_mismatch_ids:
                record_failure(
                    "MARKET_FILTER_MISMATCH",
                    market_id,
                    details={"signal_blocker": "FROZEN_MARKET_FILTER"},
                )
                continue
            if market_health_state == "missing" or market_id in missing_markets:
                record_failure(
                    "NO_FORWARD_SNAPSHOT",
                    market_id,
                    details={"signal_blocker": "FORWARD_SNAPSHOT_REQUIRED"},
                )
                continue
            if market_health_state == "stale" or market_id in stale_markets:
                record_failure(
                    "STALE_FORWARD_EVIDENCE",
                    market_id,
                    details={"signal_blocker": "FORWARD_SNAPSHOT_STALE"},
                )
                continue
            if market_id in health_blocked_markets:
                record_failure(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id,
                    details={
                        "signal_blocker": "REQUIRED_MARKET_HEALTH",
                        "health_state": market_health_state or "blocked",
                    },
                )
                continue
            if (
                health_reason not in {
                    "REQUIRED_MARKETS_FRESH",
                    "REQUIRED_MARKETS_MISSING",
                    "REQUIRED_MARKETS_STALE",
                }
                and health_grade not in {"A", "B"}
                and (
                    not health_has_market_detail
                    or (
                        health_reason == "COLLECTOR_DEGRADED"
                        and not health_blocked_markets
                    )
                )
            ):
                record_failure(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id,
                    details={
                        "signal_blocker": "REQUIRED_MARKET_HEALTH",
                        "health_state": market_health_state or None,
                    },
                )
                continue

            rows = self._forward_snapshot_rows(market_id)
            if not rows:
                record_failure(
                    "NO_FORWARD_SNAPSHOT",
                    market_id,
                    details={"signal_blocker": "FORWARD_SNAPSHOT_REQUIRED"},
                )
                continue
            current_row = rows[-1]
            current_observation = self._signal_observation(current_row)
            if current_observation is None:
                record_failure(
                    "STALE_FORWARD_EVIDENCE",
                    market_id,
                    details={"signal_blocker": "FORWARD_OBSERVATION_INVALID"},
                )
                continue
            source_timestamp = parse_timestamp(current_row.get("source_timestamp"))
            observed_at = parse_timestamp(current_row.get("observed_at"))
            if (
                source_timestamp is None
                or observed_at is None
                or source_timestamp > now
                or observed_at > now
                or (now - source_timestamp).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS
                or (now - observed_at).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS
            ):
                record_failure(
                    "STALE_FORWARD_EVIDENCE",
                    market_id,
                    details={"signal_blocker": "FORWARD_SNAPSHOT_STALE"},
                )
                continue
            settlement = str(current_observation.get("settlement", "open")).strip().lower()
            if (
                _canary_closed_flag(current_observation.get("closed", False))
                or current_observation.get("active") is False
                or settlement in {"resolved_yes", "resolved_no", "void", "closed", "expired"}
            ):
                record_failure(
                    "MARKET_CLOSED",
                    market_id,
                    details={"signal_blocker": "MARKET_CLOSED"},
                )
                continue
            observations: list[dict[str, Any]] = []
            for row in rows:
                observation = self._signal_observation(row)
                stamp = parse_timestamp(row.get("source_timestamp"))
                if observation is not None and stamp is not None and stamp <= now:
                    observations.append(observation)
            if not observations:
                record_failure(
                    "WARMING_UP",
                    market_id,
                    details={"signal_blocker": "INSUFFICIENT_FORWARD_INPUT"},
                )
                continue
            if not self._apply_signal_model(observations, binding["model_document"]):
                record_failure(
                    "MODEL_INPUT_MISSING",
                    market_id,
                    details={"signal_blocker": "MODEL_INPUT_MISSING"},
                )
                continue
            current_observation["model_probability"] = observations[-1].get(
                "model_probability"
            )
            try:
                from .strategy import evaluate_signal_record

                evaluated = evaluate_signal_record(
                    binding["strategy"], {"snapshots": tuple(observations)}
                )
                score = float(evaluated.score)
                evaluation_reason = str(
                    getattr(evaluated, "reason_code", "")
                    or "STRATEGY_EVALUATED_DECLINED"
                ).strip().upper()
            except (TypeError, ValueError, OverflowError):
                score = 0.0
                evaluated = None
                evaluation_reason = "STRATEGY_EVALUATED_DECLINED"
            if (
                not math.isfinite(score)
                or evaluated is None
                or not evaluated.actionable
            ):
                compatibility_reason = (
                    "NO_STRATEGY_SIGNAL"
                    if (
                        not scope_binding.get("scope_declared")
                        and evaluation_reason == "STRATEGY_EVALUATED_DECLINED"
                    )
                    else evaluation_reason
                )
                record_failure(
                    compatibility_reason,
                    market_id,
                    details={
                        "signal_blocker": "STRATEGY_NOT_ACTIONABLE",
                        "evaluation_reason": evaluation_reason,
                    },
                )
                continue
            outcome = "yes" if score > 0 else "no"
            token_ids = current_observation.get("token_ids")
            token_id = current_observation.get(f"{outcome}_token_id")
            if isinstance(token_ids, Mapping):
                token_id = token_id or token_ids.get(outcome)
            token_id = str(token_id or "").strip()
            expected_token = (
                resolved_token_bindings.get(market_id, {}).get(f"{outcome}_token_id")
                if resolved_token_bindings
                else None
            )
            if expected_token and token_id != expected_token:
                record_failure(
                    "SCOPE_RESOLUTION_TOKEN_MISMATCH",
                    market_id,
                    details={
                        "signal_blocker": "FROZEN_TOKEN_MAPPING_CHANGED",
                        "expected_token_id": expected_token,
                        "observed_token_id": token_id,
                    },
                )
                continue
            current_book = self._current_order_book(
                current_observation,
                outcome,
                now=now,
                token_id=token_id,
            )
            if not token_id or current_book is None:
                record_failure(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id,
                    details={"signal_blocker": "CURRENT_ORDER_BOOK_REQUIRED"},
                )
                continue
            raw_price = current_observation.get(f"{outcome}_ask")
            if raw_price is None:
                try:
                    raw_price = _best_ask_price(current_book.get("asks"))
                except (TypeError, ValueError, ArithmeticError):
                    raw_price = None
            try:
                expected_price = Decimal(str(raw_price))
            except (TypeError, ValueError, ArithmeticError):
                expected_price = Decimal("NaN")
            if not expected_price.is_finite() or not 0 < expected_price <= 1:
                record_failure(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id,
                    details={"signal_blocker": "INVALID_EXECUTABLE_QUOTE"},
                )
                continue
            signal = self._persist_ready_signal(
                binding,
                market_id=market_id,
                current_row=current_row,
                current_observation=current_observation,
                current_book=current_book,
                outcome=outcome,
                score=score,
                expected_price=expected_price,
                token_id=token_id,
                now=now,
            )
            if signal is None:
                record_failure(
                    "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    market_id,
                    details={"signal_blocker": "SIGNAL_PERSISTENCE_FAILED"},
                )
                continue
            ready_evidence = dict(evidence)
            ready_evidence.update(
                {
                    "market_id": market_id,
                    "source_type": current_row.get("source_type"),
                    "source_snapshot_id": current_row.get("snapshot_id"),
                    "source_timestamp": source_timestamp.isoformat(),
                    "observed_at": observed_at.isoformat(),
                    "current_order_book_timestamp": current_book.get("timestamp"),
                    "current_execution_evidence": CURRENT_ORDER_BOOK,
                }
            )
            return finish(
                "READY_SIGNAL",
                market_id=market_id,
                signal=signal,
                required_health=required_health,
                evidence=ready_evidence,
            )

        # Keep exact diagnostics for declared targets that requirements
        # intentionally omitted, but never run snapshot/model/strategy
        # evaluation for them.  Append these after the executable loop so
        # permitted-market order remains the tie-breaker for executable
        # failures, matching the historical bounded-scan ordering.
        for declared_market_id in declared_market_ids:
            if declared_market_id in market_ids:
                continue
            if declared_market_id in closed_market_ids:
                record_failure(
                    "MARKET_CLOSED",
                    declared_market_id,
                    details={"signal_blocker": "MARKET_CLOSED"},
                )
            elif declared_market_id in filter_mismatch_ids:
                record_failure(
                    "MARKET_FILTER_MISMATCH",
                    declared_market_id,
                    details={"signal_blocker": "FROZEN_MARKET_FILTER"},
                )
            elif declared_market_id in missing_market_ids:
                record_failure(
                    "NO_FORWARD_SNAPSHOT",
                    declared_market_id,
                    details={"signal_blocker": "FORWARD_SNAPSHOT_REQUIRED"},
                )

        if failures:
            selected = min(
                failures,
                key=lambda item: (
                    -failure_precedence.get(str(item["reason_code"]), 0),
                    int(item["order"]),
                ),
            )
            selected_evidence = dict(selected["evidence"])
            selected_evidence["market_failures"] = [
                {
                    "market_id": item["market_id"],
                    "reason_code": item["reason_code"],
                }
                for item in failures
            ]
            return finish(
                selected["reason_code"],
                market_id=selected["market_id"],
                required_health=required_health,
                evidence=selected_evidence,
            )
        if (
            not market_ids
            and authority_reason == "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        ):
            unresolved_evidence = dict(evidence)
            unresolved_evidence["authority_reason_code"] = authority_reason
            return finish(
                authority_reason,
                market_id=declared_market_ids[0] if declared_market_ids else None,
                required_health=required_health,
                evidence=unresolved_evidence,
            )
        return finish(
            "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
            market_id=market_ids[0] if market_ids else None,
            required_health=required_health,
            evidence=evidence,
        )

    def generate_signal(self, candidate_id: str) -> Mapping[str, Any] | None:
        """Compatibility wrapper around :meth:`evaluate_signal`."""
        result = self.evaluate_signal(candidate_id)
        signal = result.get("signal") if isinstance(result, Mapping) else None
        return dict(signal) if isinstance(signal, Mapping) else None

    @staticmethod
    def _signal_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        try:
            evidence = json.loads(str(result.pop("evidence_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            evidence = {}
        result["evidence"] = dict(evidence) if isinstance(evidence, Mapping) else {}
        return result

    def get_signal(self, signal_id: str) -> Mapping[str, Any] | None:
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)
        if lock is None:
            row = connection.execute(
                "SELECT * FROM canary_signals WHERE signal_id=?", (str(signal_id).strip(),)
            ).fetchone()
        else:
            with lock:
                row = connection.execute(
                    "SELECT * FROM canary_signals WHERE signal_id=?", (str(signal_id).strip(),)
                ).fetchone()
        return self._signal_from_row(row) if row is not None else None

    def latest_signal(self, candidate_id: str | None = None) -> Mapping[str, Any] | None:
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)
        query = "SELECT * FROM canary_signals"
        parameters: tuple[Any, ...] = ()
        if candidate_id is not None:
            query += " WHERE candidate_id=?"
            parameters = (str(candidate_id).strip(),)
        query += " ORDER BY generated_at DESC,signal_id DESC LIMIT 1"
        try:
            if lock is None:
                row = connection.execute(query, parameters).fetchone()
            else:
                with lock:
                    row = connection.execute(query, parameters).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            row = None
        return self._signal_from_row(row) if row is not None else None

    def _set_signal_status(
        self,
        signal_id: str,
        status: str,
        *,
        reason: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        now = ensure_utc(self.clock()).isoformat()
        with self.store._lock:
            if self.store.connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            if isinstance(evidence, Mapping):
                row = self.store.connection.execute(
                    "SELECT evidence_json FROM canary_signals WHERE signal_id=?",
                    (str(signal_id),),
                ).fetchone()
                try:
                    existing = json.loads(row["evidence_json"]) if row is not None else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    existing = {}
                merged = dict(existing) if isinstance(existing, Mapping) else {}
                merged.update(dict(evidence))
                self.store.connection.execute(
                    "UPDATE canary_signals SET status=?,reason=?,evidence_json=?,updated_at=? "
                    "WHERE signal_id=? AND status='READY'",
                    (
                        str(status).upper(),
                        reason,
                        json.dumps(merged, sort_keys=True, separators=(",", ":"), allow_nan=False),
                        now,
                        str(signal_id),
                    ),
                )
            else:
                self.store.connection.execute(
                    "UPDATE canary_signals SET status=?,reason=?,updated_at=? "
                    "WHERE signal_id=? AND status='READY'",
                    (str(status).upper(), reason, now, str(signal_id)),
                )
            self.store.connection.commit()
        self.publish_readiness_snapshot(reason="SIGNAL_STATUS_CHANGED")
    def _invalidate_signal(
        self,
        signal_id: str,
        reason: str,
        *,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        self._set_signal_status(
            signal_id,
            "NO_LONGER_VALID",
            reason=reason,
            evidence=evidence,
        )

    def submit_signal(
        self,
        signal_id: str,
        *,
        venue: CanaryVenue,
        allow_test_venue: bool = False,
        allow_environment: bool | None = None,
    ) -> dict[str, Any]:
        signal = self.get_signal(signal_id)
        if signal is None:
            raise CanaryBlocked("CANARY_SIGNAL_NOT_FOUND")
        current_status = str(signal.get("status", "")).upper()
        if current_status != "READY":
            if current_status in {
                "SUBMITTED",
                "SUBMITTING",
                "UNKNOWN",
                "REJECTED",
                "ACCEPTED",
                "MATCHED",
                "PARTIAL",
                "PARTIALLY_FILLED",
                "SETTLED",
                "RESOLVED",
            }:
                raise CanaryBlocked("DUPLICATE_SIGNAL")
            raise CanaryBlocked("CANARY_SIGNAL_NOT_SUBMITTABLE")
        now = ensure_utc(self.clock())
        try:
            expires_at = parse_timestamp(signal.get("expires_at"))
        except (TypeError, ValueError):
            expires_at = None
        if expires_at is None or expires_at <= now:
            self._set_signal_status(signal_id, "REJECTED", reason="CANARY_SIGNAL_EXPIRED")
            raise CanaryBlocked("CANARY_SIGNAL_EXPIRED")
        try:
            binding = self._candidate_signal_binding(str(signal["candidate_id"]))
        except CanaryBlocked as exc:
            self._invalidate_signal(signal_id, str(exc))
            raise
        if (
            signal.get("frozen_hash") != binding["frozen_hash"]
            or signal.get("strategy_hash") != binding["strategy_hash"]
            or signal.get("model_hash") != binding["model_hash"]
            or signal.get("config_hash") != binding["config_hash"]
        ):
            self._invalidate_signal(signal_id, "CANDIDATE_FROZEN_BINDING_CHANGED")
            raise CanaryBlocked("CANARY_SIGNAL_NO_LONGER_VALID")
        scope_resolution = _canary_current_scope_resolution(
            self.store,
            str(signal.get("candidate_id") or ""),
            binding.get("payload"),
            now=now,
        )
        if not scope_resolution.get("bound"):
            reason = str(
                scope_resolution.get("reason_code")
                or "SCOPE_RESOLUTION_STALE"
            )
            self._invalidate_signal(signal_id, reason)
            raise CanaryBlocked(reason)
        resolved_ids = tuple(
            str(item.get("market_id") or "").strip()
            for item in scope_resolution.get("matched_markets", ())
            if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
        )
        scope = _canary_scope_binding(binding.get("payload")).get("market_scope", {})
        declared_values = (
            scope.get("market_ids", scope.get("exact_market_ids", ()))
            if isinstance(scope, Mapping)
            else ()
        )
        declared_ids = tuple(
            str(value).strip()
            for value in declared_values
            if str(value).strip()
        )
        if (
            len(resolved_ids) > CANARY_EXECUTION_MARKET_CAP
            or len(declared_ids) > CANARY_EXECUTION_MARKET_CAP
        ):
            cap_evidence = _execution_market_cap_evidence(resolved_ids, declared_ids)
            cap_evidence.update(
                {
                    "scope_hash": scope_resolution.get("scope_hash"),
                    "scope_version": scope_resolution.get("scope_version"),
                    "scope_resolution_status": _canary_resolution_attr(
                        scope_resolution.get("resolution"), "status"
                    ),
                }
            )
            self._invalidate_signal(
                signal_id,
                EXECUTION_FEASIBILITY_MARKET_CAP,
                evidence=cap_evidence,
            )
            raise CanaryBlocked(EXECUTION_FEASIBILITY_MARKET_CAP)
        selected_scope_market = next(
            (
                item
                for item in scope_resolution.get("matched_markets", ())
                if isinstance(item, Mapping)
                and str(item.get("market_id") or "").strip()
                == str(signal.get("market_id") or "").strip()
            ),
            None,
        )
        expected_scope_token = (
            selected_scope_market.get(
                f"{str(signal.get('outcome') or '').strip().lower()}_token_id"
            )
            if isinstance(selected_scope_market, Mapping)
            else None
        )
        if (
            not isinstance(selected_scope_market, Mapping)
            or not expected_scope_token
            or str(expected_scope_token).strip()
            != str(signal.get("token_id") or "").strip()
        ):
            self._invalidate_signal(signal_id, "SCOPE_RESOLUTION_TOKEN_MISMATCH")
            raise CanaryBlocked("SCOPE_RESOLUTION_TOKEN_MISMATCH")
        control_snapshot = self.authoritative_status()
        if control_snapshot.get("micro_live_canary") == AUTONOMOUS_MICRO_LIVE:
            control_candidate = str(
                control_snapshot.get("control_candidate") or ""
            ).strip()
            signal_candidate = str(signal.get("candidate_id") or "").strip()
            if not control_candidate or not signal_candidate or control_candidate != signal_candidate:
                self._invalidate_signal(signal_id, "AUTO_CANARY_CANDIDATE_NOT_SELECTED")
                raise CanaryBlocked("AUTO_CANARY_CANDIDATE_NOT_SELECTED")
        requirements: Mapping[str, Any] = {
            "candidates": [
                {
                    "candidate_id": str(signal["candidate_id"]),
                    "resolution": "RESOLVED",
                    "reason_code": "CANDIDATE_FORWARD_MARKET_RESOLVED",
                    "market_ids": resolved_ids,
                    "permitted_market_ids": resolved_ids,
                }
            ],
            "market_ids": resolved_ids,
            "as_of": scope_resolution.get("resolved_at"),
        }
        try:
            required_health = self.store.polymarket_required_health(
                requirements=requirements,
                now=now,
                stale_after_seconds=CANARY_SIGNAL_MAX_AGE_SECONDS,
            )
        except Exception:
            self._set_signal_status(
                signal_id, "STALE", reason="REQUIRED_MARKET_HEALTH_BLOCKED"
            )
            raise CanaryBlocked("CANARY_SIGNAL_STALE")
        selected_market = str(signal.get("market_id") or "").strip()
        fresh_markets = required_health.get("fresh", ())
        grade = str(required_health.get("grade") or "").strip().upper()
        if (
            grade not in {"A", "B"}
            or not isinstance(fresh_markets, (list, tuple, set, frozenset))
            or selected_market not in fresh_markets
        ):
            self._set_signal_status(
                signal_id, "STALE", reason="REQUIRED_MARKET_HEALTH_BLOCKED"
            )
            raise CanaryBlocked("CANARY_SIGNAL_STALE")
        current = self._current_signal_market(str(signal["market_id"]), now=now)
        if current is None:
            self._set_signal_status(signal_id, "STALE", reason="SOURCE_OBSERVATION_STALE")
            raise CanaryBlocked("CANARY_SIGNAL_STALE")
        source_row, observation = current
        if (
            self._current_order_book(
                observation,
                str(signal.get("outcome", "")).strip().lower(),
                now=now,
                token_id=str(signal.get("token_id") or ""),
            )
            is None
        ):
            self._invalidate_signal(signal_id, "CURRENT_ORDER_BOOK_REQUIRED")
            raise CanaryBlocked("CURRENT_ORDER_BOOK_REQUIRED")
        if str(source_row.get("snapshot_id")) != str(signal.get("source_snapshot_id")):
            self._invalidate_signal(signal_id, "SOURCE_OBSERVATION_CHANGED")
            raise CanaryBlocked("CANARY_SIGNAL_NO_LONGER_VALID")
        outcome = str(signal.get("outcome", "")).strip().lower()
        current_token = observation.get(f"{outcome}_token_id")
        token_ids = observation.get("token_ids")
        if not current_token and isinstance(token_ids, Mapping):
            current_token = token_ids.get(outcome)
        current_price = observation.get(f"{outcome}_ask")
        if current_price is None:
            order_book = observation.get(f"{outcome}_order_book")
            try:
                current_price = _best_ask_price(
                    order_book.get("asks") if isinstance(order_book, Mapping) else None
                )
            except (TypeError, ValueError, ArithmeticError):
                current_price = None
        try:
            current_price_value = Decimal(str(current_price))
            signal_price_value = Decimal(str(signal["paper_expected_price"]))
        except (TypeError, ValueError, ArithmeticError):
            current_price_value = signal_price_value = Decimal("NaN")
        if (
            str(current_token or "") != str(signal.get("token_id") or "")
            or not current_price_value.is_finite()
            or current_price_value != signal_price_value
            or str(observation.get("settlement", "")).lower()
            in {"resolved_yes", "resolved_no", "void", "closed"}
        ):
            self._invalidate_signal(signal_id, "MARKET_BINDING_CHANGED")
            raise CanaryBlocked("CANARY_SIGNAL_NO_LONGER_VALID")
        try:
            final_binding = self._candidate_signal_binding(
                str(signal.get("candidate_id") or "")
            )
        except CanaryBlocked as exc:
            self._invalidate_signal(signal_id, str(exc))
            raise
        if any(
            final_binding.get(name) != binding.get(name)
            for name in (
                "frozen_hash",
                "qualification_hash",
                "plan_hash",
                "strategy_hash",
                "model_hash",
                "config_hash",
                "scope_hash",
                "scope_version",
            )
        ):
            self._invalidate_signal(signal_id, "CANDIDATE_LIFECYCLE_CHANGED")
            raise CanaryBlocked("CANDIDATE_LIFECYCLE_CHANGED")
        binding = final_binding
        # Last scope/token/lifecycle fence immediately before the only
        # submission route. Earlier checks intentionally do not substitute
        # for this point-in-time recheck.
        final_scope = _canary_current_scope_resolution(
            self.store,
            str(signal.get("candidate_id") or ""),
            binding.get("payload"),
            now=ensure_utc(self.clock()),
        )
        final_market = next(
            (
                item
                for item in final_scope.get("matched_markets", ())
                if isinstance(item, Mapping)
                and str(item.get("market_id") or "").strip()
                == str(signal.get("market_id") or "").strip()
            ),
            None,
        )
        final_expected_token = (
            final_market.get(f"{outcome}_token_id")
            if isinstance(final_market, Mapping)
            else None
        )
        if (
            not final_scope.get("bound")
            or not isinstance(final_market, Mapping)
            or str(final_expected_token or "").strip()
            != str(signal.get("token_id") or "").strip()
        ):
            reason = str(
                final_scope.get("reason_code")
                or "SCOPE_RESOLUTION_TOKEN_MISMATCH"
            )
            self._invalidate_signal(signal_id, reason)
            raise CanaryBlocked(reason)
        return self.submit(
            signal_id=str(signal["signal_id"]),
            candidate_id=str(signal["candidate_id"]),
            market_id=str(signal["market_id"]),
            token_id=str(signal["token_id"]),
            side=str(signal["side"]),
            paper_expected_price=Decimal(str(signal["paper_expected_price"])),
            venue=venue,
            allow_test_venue=allow_test_venue,
            allow_environment=allow_environment,
        )

    @staticmethod
    def _recovery_records(value: Any, *, key: str) -> list[Any]:
        if isinstance(value, Mapping):
            for field in (key, "data", "items", "results"):
                nested = value.get(field)
                if isinstance(nested, (list, tuple)):
                    return list(nested)
            return [value]
        if isinstance(value, (list, tuple)):
            return list(value)
        return []

    @staticmethod
    def _recovery_method(
        venue: Any,
        name: str,
        order_id: str,
        **read_scope: Any,
    ) -> Any:
        method = getattr(venue, name, None)
        if not callable(method):
            raise CanaryBlocked("CANARY_RECOVERY_READ_UNAVAILABLE")
        try:
            return method(order_id=order_id, **read_scope)
        except TypeError:
            try:
                return method(order_id=order_id)
            except TypeError:
                return method(order_id)

    @staticmethod
    def _validate_recovery_persisted_binding(
        ledger: Mapping[str, Any],
        signal: Mapping[str, Any],
    ) -> dict[str, Any]:
        evidence_raw = ledger.get("evidence_json")
        if not isinstance(evidence_raw, str) or not evidence_raw.strip():
            raise CanaryBlocked("CANARY_RECOVERY_BINDING_INVALID")
        try:
            evidence = json.loads(evidence_raw)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CanaryBlocked("CANARY_RECOVERY_BINDING_INVALID") from exc
        if not isinstance(evidence, Mapping):
            raise CanaryBlocked("CANARY_RECOVERY_BINDING_INVALID")
        required = (
            "control_generation", "control_state", "control_candidate",
            "control_expiry", "signal_candidate_id", "signal_market_id",
            "signal_token_id", "signal_side", "signal_frozen_hash",
            "signal_strategy_hash", "signal_model_hash", "signal_config_hash",
            "resolved_asset_id",
        )
        if any(name not in evidence for name in required):
            raise CanaryBlocked("CANARY_RECOVERY_BINDING_INVALID")
        try:
            generation = int(ledger.get("control_generation") or 0)
        except (TypeError, ValueError) as exc:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID") from exc
        if generation <= 0:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID")
        persisted_generation = evidence.get("control_generation")
        if (
            isinstance(persisted_generation, bool)
            or not isinstance(persisted_generation, int)
            or persisted_generation != generation
        ):
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID")
        candidate_id = str(ledger.get("candidate_id") or "").strip()
        control_candidate = str(evidence.get("control_candidate") or "").strip()
        control_state = str(evidence.get("control_state") or "").strip().upper()
        if not candidate_id or control_candidate != candidate_id or not control_state:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID")
        for name in ("candidate_id", "market_id", "token_id"):
            expected = str(ledger.get(name) or "").strip()
            actual = str(signal.get(name) or "").strip()
            persisted = str(evidence.get(f"signal_{name}") or "").strip()
            if not expected or actual != expected or persisted != expected:
                raise CanaryBlocked("CANARY_RECOVERY_SIGNAL_BINDING_MISMATCH")
        expected_side = str(ledger.get("side") or "").strip().upper()
        signal_side = str(signal.get("side") or "").strip().upper()
        persisted_side = str(evidence.get("signal_side") or "").strip().upper()
        if (
            expected_side != "BUY"
            or signal_side != expected_side
            or persisted_side != expected_side
        ):
            raise CanaryBlocked("CANARY_RECOVERY_SIDE_MISMATCH")
        for name in ("frozen_hash", "strategy_hash", "model_hash", "config_hash"):
            expected = str(signal.get(name) or "").strip()
            persisted = str(evidence.get(f"signal_{name}") or "").strip()
            if not expected or persisted != expected:
                raise CanaryBlocked("CANARY_RECOVERY_CONFIG_BINDING_INVALID")
        return dict(evidence)

    def _validate_recovery_observation(
        self,
        ledger: Mapping[str, Any],
        signal: Mapping[str, Any],
        order: Any,
        trades: Sequence[Any],
        exchange_order_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        binding_evidence = self._validate_recovery_persisted_binding(ledger, signal)
        if response_order_id(order) != exchange_order_id:
            raise CanaryBlocked("CANARY_RECOVERY_ORDER_ID_MISMATCH")
        if response_side(order) != "BUY":
            raise CanaryBlocked("CANARY_RECOVERY_SIDE_MISMATCH")
        expected_token = str(ledger.get("token_id") or "").strip()
        expected_asset = str(binding_evidence.get("resolved_asset_id") or "").strip()
        if (
            not expected_token
            or not expected_asset
            or response_token(order) != expected_asset
        ):
            raise CanaryBlocked("CANARY_RECOVERY_TOKEN_MISMATCH")
        returned_market = response_market(order)
        expected_market = str(ledger.get("market_id") or "").strip()
        if returned_market is not None and returned_market != expected_market:
            raise CanaryBlocked("CANARY_RECOVERY_MARKET_MISMATCH")
        try:
            order_price = response_price(order)
            order_quantity = response_quantity(order)
            max_price = decimal_value(ledger.get("max_price"), "max price", positive=True)
            submitted_quantity = decimal_value(
                ledger.get("submitted_quantity"), "submitted quantity", positive=True
            )
        except ValueError as exc:
            raise CanaryBlocked("CANARY_RECOVERY_ORDER_BINDING_INVALID") from exc
        if order_price != max_price or order_quantity > submitted_quantity:
            raise CanaryBlocked("CANARY_RECOVERY_ORDER_BOUNDS_MISMATCH")
        try:
            order_timestamp = response_timestamp(order)
            reservation_timestamp = timestamp_value(
                ledger.get("timestamp"), "reservation timestamp"
            )
        except ValueError as exc:
            raise CanaryBlocked("CANARY_RECOVERY_TIMESTAMP_MISMATCH") from exc
        if order_timestamp is not None and (
            order_timestamp < reservation_timestamp or order_timestamp > now
        ):
            raise CanaryBlocked("CANARY_RECOVERY_TIMESTAMP_MISMATCH")
        try:
            generation = int(ledger.get("control_generation") or 0)
        except (TypeError, ValueError) as exc:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID") from exc
        total_filled = Decimal("0")
        safe_trade_count = 0
        trade_provenance: list[dict[str, Any]] = []
        for trade in trades:
            direct_trade_order_id = trade_order_id(trade)
            maker_order_ids = mapping_value(
                trade, "maker_order_ids", "makerOrderIds"
            )
            bound_order_ids = {
                str(identifier).strip()
                for identifier in (
                    [direct_trade_order_id] + list(maker_order_ids)
                    if isinstance(maker_order_ids, Sequence)
                    and not isinstance(maker_order_ids, (str, bytes, bytearray))
                    else [direct_trade_order_id]
                )
                if str(identifier or "").strip()
            }
            if exchange_order_id not in bound_order_ids:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_ORDER_ID_MISMATCH")
            if trade_token(trade) != expected_asset:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_TOKEN_MISMATCH")
            returned_trade_market = response_market(trade)
            if returned_trade_market is not None and returned_trade_market != expected_market:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_MARKET_MISMATCH")
            trade_id = str(mapping_value(trade, "trade_id", "id") or "").strip()
            if not trade_id:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_ID_REQUIRED")
            trade_state = str(
                mapping_value(trade, "status", "state") or ""
            ).strip().upper()
            try:
                trade_price_value = trade_price(trade)
                trade_quantity_value = trade_quantity(trade)
                trade_time = trade_timestamp(trade)
            except ValueError as exc:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_BINDING_INVALID") from exc
            if trade_price_value > order_price:
                raise CanaryBlocked("CANARY_RECOVERY_TRADE_BOUNDS_MISMATCH")
            if trade_time is not None and (
                trade_time < reservation_timestamp or trade_time > now
            ):
                raise CanaryBlocked("CANARY_RECOVERY_TIMESTAMP_MISMATCH")
            trade_provenance.append(
                {
                    "trade_id": trade_id,
                    "order_id": exchange_order_id,
                    "timestamp": trade_time.isoformat() if trade_time is not None else None,
                    "price": str(trade_price_value),
                    "quantity": str(trade_quantity_value),
                    "market_id": returned_trade_market,
                    "token_id": expected_asset,
                    "signal_token_id": expected_token,
                    "side": "BUY",
                    "status": trade_state or None,
                    "confirmed": trade_state
                    in {"CONFIRMED", "TRADE_STATUS_CONFIRMED", "SETTLED", "TRADE_STATUS_SETTLED"},
                    "owner": mapping_value(
                        trade,
                        "owner",
                        "owner_address",
                        "maker",
                        "maker_address",
                        "account",
                        "account_address",
                    ),
                }
            )
            if trade_state not in {
                "CONFIRMED",
                "TRADE_STATUS_CONFIRMED",
                "SETTLED",
                "TRADE_STATUS_SETTLED",
            }:
                continue
            total_filled += trade_quantity_value
            safe_trade_count += 1
        if total_filled > order_quantity or total_filled > submitted_quantity:
            raise CanaryBlocked("CANARY_RECOVERY_QUANTITY_MISMATCH")
        control = self.store.connection.execute(
            "SELECT state,candidate_id,control_generation FROM canary_control WHERE singleton=1"
        ).fetchone()
        if control is None:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID")
        current_state = str(control["state"] or "").strip().upper()
        if current_state not in {
            "DISABLED", "DISARMED", "KILLED", "ARMED", AUTONOMOUS_MICRO_LIVE
        }:
            raise CanaryBlocked("CANARY_RECOVERY_CONTROL_BINDING_INVALID")
        return {
            "exchange_order_id": exchange_order_id,
            "order_price": str(order_price),
            "order_quantity": str(order_quantity),
            "filled_quantity": str(total_filled),
            "trade_count": safe_trade_count,
            "control_generation": generation,
            "control_state": current_state,
            "candidate_id": str(ledger.get("candidate_id") or "").strip(),
            "market_id": expected_market,
            "token_id": expected_token,
            "execution_asset_id": expected_asset,
            "side": "BUY",
            "order_provenance": {
                "order_id": exchange_order_id,
                "status": response_status(order),
                "timestamp": order_timestamp.isoformat() if order_timestamp is not None else None,
                "price": str(order_price),
                "quantity": str(order_quantity),
                "market_id": returned_market,
                "token_id": expected_asset,
                "signal_token_id": expected_token,
                "side": "BUY",
                "owner": mapping_value(
                    order,
                    "owner",
                    "owner_address",
                    "maker",
                    "maker_address",
                    "account",
                    "account_address",
                ),
            },
            "trade_provenance": trade_provenance,
        }

    @staticmethod
    def _recovery_status(order: Any, filled: Decimal, quantity: Decimal) -> str:
        state = response_status(order)
        if quantity > 0 and filled >= quantity:
            return "FILLED"
        if filled > 0:
            return "PARTIAL"
        if state in {"CANCELED", "CANCELLED"}:
            return "CANCELED"
        if state == "EXPIRED":
            return "EXPIRED"
        if state in {"OPEN", "LIVE", "UNMATCHED", "ACTIVE"}:
            return "OPEN"
        return "SUBMITTED"

    def reconcile_entry_intent(
        self,
        event_id: str,
        *,
        venue: Any | None = None,
        _observed: tuple[Any, Sequence[Any]] | None = None,
    ) -> dict[str, Any]:
        connection = self.store.connection
        event_key = str(event_id).strip()
        with self.store._lock:
            row = connection.execute(
                "SELECT * FROM canary_ledger WHERE event_id=?", (event_key,)
            ).fetchone()
        if row is None:
            raise CanaryBlocked("CANARY_RECOVERY_EVENT_NOT_FOUND")
        ledger = dict(row)
        exchange_order_id = str(ledger.get("exchange_order_id") or "").strip()
        if not exchange_order_id:
            raise CanaryBlocked("CANARY_RECOVERY_ORDER_ID_REQUIRED")
        signal_row = connection.execute(
            "SELECT * FROM canary_signals WHERE signal_id=?",
            (str(ledger.get("signal_id") or ""),),
        ).fetchone()
        signal = dict(signal_row) if signal_row is not None else {}
        binding_evidence = self._validate_recovery_persisted_binding(ledger, signal)
        try:
            state_version = int(ledger.get("state_version") or 0)
        except (TypeError, ValueError) as exc:
            raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH") from exc
        if _observed is None:
            if venue is None:
                raise CanaryBlocked("CANARY_RECOVERY_READ_UNAVAILABLE")
            order = self._recovery_method(venue, "get_order", exchange_order_id)
            raw_trades = self._recovery_method(
                venue,
                "list_account_trades",
                exchange_order_id,
                token_id=str(binding_evidence.get("resolved_asset_id") or "") or None,
                market=str(signal.get("market_id") or "") or None,
            )
            trades = self._recovery_records(raw_trades, key="trades")
        else:
            order, trades = _observed
            trades = list(trades)
        observed = self._validate_recovery_observation(
            ledger, signal, order, trades, exchange_order_id,
            now=ensure_utc(self.clock()),
        )
        filled = Decimal(observed["filled_quantity"])
        quantity = Decimal(observed["order_quantity"])
        status = self._recovery_status(order, filled, quantity)
        now = ensure_utc(self.clock()).isoformat()
        evidence = {
            **binding_evidence,
            "recovery": True,
            "read_only": True,
            **observed,
            "order_status": response_status(order),
        }
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT exchange_order_id,state_version FROM canary_ledger WHERE event_id=?",
                    (event_key,),
                ).fetchone()
                if (
                    current is None
                    or str(current["exchange_order_id"] or "").strip() != exchange_order_id
                    or int(current["state_version"] or 0) != state_version
                ):
                    raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH")
                updated = connection.execute(
                    "UPDATE canary_ledger SET status=?,fill_quantity=?,evidence_json=?,"
                    "state_version=state_version+1 WHERE event_id=? "
                    "AND exchange_order_id=? AND state_version=?",
                    (status, str(filled), json.dumps(evidence, sort_keys=True, separators=(",", ":")),
                     event_key, exchange_order_id, state_version),
                )
                if updated.rowcount != 1:
                    raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH")
                connection.execute(
                    "UPDATE canary_signals SET status=?,reason=NULL,updated_at=? "
                    "WHERE signal_id=? AND status IN ('UNKNOWN','SUBMITTING')",
                    (status, now, str(ledger.get("signal_id") or "")),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        self.publish_readiness_snapshot(reason="CANARY_RECOVERY_RECONCILED")
        return {
            "event_id": event_key,
            "signal_id": str(ledger.get("signal_id") or "").strip(),
            "exchange_order_id": exchange_order_id,
            "status": status,
            "fill_quantity": str(filled),
            "read_only": True,
        }

    def recover_entry_intent(
        self,
        event_id: str,
        exchange_order_id: str,
        *,
        signal_id: str | None = None,
        venue: Any | None = None,
        profile: Any | None = None,
        confirmation: str = "",
    ) -> dict[str, Any]:
        if confirmation != RECOVERY_CONFIRMATION:
            raise CanaryBlocked("EXACT_CONFIRMATION_REQUIRED")
        try:
            selected_profile = normalize_recovery_profile(
                self.profile if profile is None else profile
            )
        except RecoveryProfileError as exc:
            raise CanaryBlocked(exc.code) from exc
        if selected_profile["allow_environment"]:
            raise CanaryBlocked("RECOVERY_PRODUCTION_PROFILE_REQUIRED")
        event_key = recovery_identifier(event_id, "event ID")
        order_key = recovery_identifier(exchange_order_id, "exchange order ID")
        if signal_id is None:
            raise CanaryBlocked("CANARY_RECOVERY_SIGNAL_REQUIRED")
        signal_key = recovery_identifier(signal_id, "signal ID")
        if venue is None:
            raise CanaryBlocked("CANARY_RECOVERY_READ_UNAVAILABLE")
        connection = self.store.connection
        with self.store._lock:
            row = connection.execute(
                "SELECT * FROM canary_ledger WHERE event_id=?", (event_key,)
            ).fetchone()
            if row is None:
                raise CanaryBlocked("CANARY_RECOVERY_EVENT_NOT_FOUND")
            ledger = dict(row)
            if str(ledger.get("signal_id") or "").strip() != signal_key:
                raise CanaryBlocked("CANARY_RECOVERY_SIGNAL_MISMATCH")
            if str(ledger.get("status") or "").upper() not in UNKNOWN_ENTRY_STATUSES:
                raise CanaryBlocked("CANARY_RECOVERY_NOT_UNRESOLVED")
            if str(ledger.get("exchange_order_id") or "").strip():
                raise CanaryBlocked("CANARY_RECOVERY_ALREADY_ATTACHED")
            duplicate = connection.execute(
                "SELECT event_id FROM canary_ledger WHERE exchange_order_id=? AND event_id<>?",
                (order_key, event_key),
            ).fetchone()
            if duplicate is not None:
                raise CanaryBlocked("CANARY_RECOVERY_DUPLICATE_ORDER_ID")
            signal_row = connection.execute(
                "SELECT * FROM canary_signals WHERE signal_id=?",
                (str(ledger.get("signal_id") or ""),),
            ).fetchone()
            if signal_row is None:
                raise CanaryBlocked("CANARY_RECOVERY_SIGNAL_NOT_FOUND")
            signal = dict(signal_row)
            binding_evidence = self._validate_recovery_persisted_binding(ledger, signal)
            try:
                state_version = int(ledger.get("state_version") or 0)
            except (TypeError, ValueError) as exc:
                raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH") from exc
            expected_status = str(ledger.get("status") or "").upper()
        self.require_current_credential_binding()
        order = self._recovery_method(venue, "get_order", order_key)
        raw_trades = self._recovery_method(
            venue,
            "list_account_trades",
            order_key,
            token_id=str(binding_evidence.get("resolved_asset_id") or "") or None,
            market=str(signal.get("market_id") or "") or None,
        )
        trades = self._recovery_records(raw_trades, key="trades")
        observed = self._validate_recovery_observation(
            ledger, signal, order, trades, order_key, now=ensure_utc(self.clock())
        )
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT status,exchange_order_id,state_version FROM canary_ledger WHERE event_id=?",
                    (event_key,),
                ).fetchone()
                if (
                    current is None
                    or str(current["status"] or "").upper() != expected_status
                    or str(current["exchange_order_id"] or "").strip()
                    or int(current["state_version"] or 0) != state_version
                ):
                    raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH")
                duplicate = connection.execute(
                    "SELECT event_id FROM canary_ledger WHERE exchange_order_id=? AND event_id<>?",
                    (order_key, event_key),
                ).fetchone()
                if duplicate is not None:
                    raise CanaryBlocked("CANARY_RECOVERY_DUPLICATE_ORDER_ID")
                evidence = {
                    **binding_evidence,
                    "recovery": True,
                    "read_only": True,
                    "event_id": event_key,
                    "signal_id": str(ledger.get("signal_id") or "").strip(),
                    **observed,
                    "order_status": response_status(order),
                }
                updated = connection.execute(
                    "UPDATE canary_ledger SET status=?,exchange_order_id=?,"
                    "evidence_json=?,state_version=state_version+1 "
                    "WHERE event_id=? AND status=? AND exchange_order_id IS NULL "
                    "AND state_version=?",
                    (RECOVERY_ATTACHED, order_key,
                     json.dumps(evidence, sort_keys=True, separators=(",", ":")),
                     event_key, expected_status, state_version),
                )
                if updated.rowcount != 1:
                    raise CanaryBlocked("CANARY_RECOVERY_STALE_CONCURRENT_ATTACH")
                connection.execute(
                    "INSERT INTO canary_execution_events("
                    "execution_event_id,canary_event_id,timestamp,exchange_order_id,status,"
                    "fill_quantity,actual_average_price,fees,latency_ms,evidence_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (event_key + "-recovery", event_key, ensure_utc(self.clock()).isoformat(),
                     order_key, RECOVERY_ATTACHED, observed["filled_quantity"],
                     None, None, None,
                     json.dumps(evidence, sort_keys=True, separators=(",", ":"))),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.reconcile_entry_intent(
            event_key, venue=venue, _observed=(order, trades)
        )
        return {
            "event_id": event_key,
            "signal_id": str(ledger.get("signal_id") or "").strip(),
            "exchange_order_id": order_key,
            "status": result["status"],
            "reconciled": True,
            "read_only": True,
            "profile": selected_profile["environment"],
        }

    def validate_eligibility(
        self,
        candidate_id: str,
        *,
        _record: Mapping[str, Any] | None = None,
        _verify_attestation: bool = True,
    ) -> dict[str, Any]:
        """Return the authoritative dry-run result for canary eligibility."""
        identifier = str(candidate_id).strip()
        record = _record if _record is not None else self.store.load_candidate_lifecycle(identifier)
        if not record or record.get("stage") not in _CANARY_ELIGIBLE_STAGES:
            return {
                "candidate_id": identifier,
                "eligible": False,
                "checks": [
                    {
                        "name": "Lifecycle stage",
                        "passed": False,
                        "detail": "Candidate is not frozen or paper-promotable.",
                    }
                ],
                "reason_code": "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            }
        payload = self._merged_lifecycle_payload(record)
        if payload is None:
            return {
                "candidate_id": identifier,
                "eligible": False,
                "checks": [
                    {
                        "name": "Lifecycle evidence",
                        "passed": False,
                        "detail": "Persisted lifecycle evidence is unavailable.",
                    }
                ],
                "reason_code": "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            }
        quality = evaluate_prediction_data_quality(
            self.store,
            payload,
            verify_attestation=_verify_attestation,
        )
        gate_aliases = {
            "schema_validated": ("schema_validated", "schema_valid"),
            "historical_backtest_passed": (
                "historical_backtest_passed",
                "backtest_complete",
            ),
            "validation_passed": ("validation_passed", "validation_complete"),
            "robustness_passed": ("robustness_passed",),
        }
        checks: list[dict[str, Any]] = []
        for label, key in (
            ("Schema", "schema_validated"),
            ("Historical backtest", "historical_backtest_passed"),
            ("Validation", "validation_passed"),
            ("Robustness", "robustness_passed"),
        ):
            passed = any(payload.get(alias) is True for alias in gate_aliases[key])
            checks.append(
                {
                    "name": label,
                    "passed": passed,
                    "detail": "Passed" if passed else f"{key} is not true.",
                }
            )
        quality_applicable = quality.get("applicable") is True
        payload_dataset_id = str(payload.get("dataset_id") or "").strip()
        payload_dataset_version = str(payload.get("dataset_version") or "").strip()
        quality_dataset_id = str(quality.get("dataset_id") or "").strip()
        quality_dataset_version = str(quality.get("dataset_version") or "").strip()
        exact_dataset_quality = (
            quality_applicable
            and bool(payload_dataset_id and payload_dataset_version)
            and quality_dataset_id == payload_dataset_id
            and quality_dataset_version == payload_dataset_version
            and quality.get("historical_data_integrity_passed") is True
            and quality.get("historical_provenance_complete") is True
            and quality.get("historical_rows_nonempty") is True
            and quality.get("historical_no_forward_contamination") is True
            and quality.get("canary_data_quality_acceptable") is True
        )
        integrity_passed = exact_dataset_quality
        checks.append(
            {
                "name": "Historical data integrity",
                "passed": integrity_passed,
                "detail": "Exact immutable historical dataset is complete and non-empty."
                if integrity_passed
                else "; ".join(
                    quality.get("reasons")
                    or ["Exact historical dataset quality is unproven."]
                ),
            }
        )
        fidelity = str(quality.get("historical_execution_fidelity") or "UNKNOWN")
        lifecycle_quality_passed = payload.get("data_quality_passed") is True
        quality_passed = exact_dataset_quality and lifecycle_quality_passed
        fidelity_passed = exact_dataset_quality and fidelity in {
            "PRICE_PROXY",
            "TIMESTAMPED_DEPTH",
        }
        checks.append(
            {
                "name": "Historical execution fidelity",
                "passed": fidelity_passed,
                "detail": (
                    f"{fidelity} · LIMITED"
                    if fidelity == "PRICE_PROXY" and fidelity_passed
                    else fidelity
                    if fidelity_passed
                    else "Historical execution fidelity is unavailable."
                ),
            }
        )
        checks.append(
            {
                "name": "Canary data quality",
                "passed": quality_passed,
                "detail": str(
                    quality.get("canary_data_quality_status")
                    or "CANARY_DATA_QUALITY_UNACCEPTABLE"
                ),
            }
        )
        sample_passed = exact_dataset_quality and self._minimum_sample_check_passed(payload)
        checks.append(
            {
                "name": "Minimum samples and trades",
                "passed": sample_passed,
                "detail": (
                    "Minimum observations and trades passed."
                    if sample_passed
                    else "Explicit minimum observation/trade evidence is missing or failed."
                ),
            }
        )
        holdout_passed = payload.get("holdout_used") is False
        checks.append(
            {
                "name": "Holdout leakage",
                "passed": holdout_passed,
                "detail": (
                    "Holdout was not used."
                    if holdout_passed
                    else "Holdout evidence is missing or was used."
                ),
            }
        )
        frozen_passed = payload.get("frozen") is True
        frozen_hash = self._lifecycle_frozen_hash(record)
        hashes_passed = frozen_passed and frozen_hash is not None
        checks.append(
            {
                "name": "Frozen hashes",
                "passed": hashes_passed,
                "detail": (
                    "Frozen binding is present."
                    if hashes_passed
                    else "Frozen flag or immutable hash is missing."
                ),
            }
        )
        no_critical_error = not bool(payload.get("critical_error"))
        checks.append(
            {
                "name": "Critical errors",
                "passed": no_critical_error,
                "detail": (
                    "No critical error recorded."
                    if no_critical_error
                    else "A critical error is recorded."
                ),
            }
        )
        eligible = all(bool(item["passed"]) for item in checks)
        qualification = _canary_qualification_projection(
            identifier,
            payload,
            frozen_hash=frozen_hash if hashes_passed else None,
            quality=quality,
        )
        qualification_hash = _canary_qualification_hash(qualification)
        existing_binding = None
        try:
            existing_binding = self.store.connection.execute(
                "SELECT candidate_id,frozen_hash,evidence_json FROM canary_eligibility "
                "WHERE candidate_id=?",
                (identifier,),
            ).fetchone()
        except sqlite3.Error:
            existing_binding = None
        binding = self._eligibility_binding_result(
            identifier,
            existing_binding,
            record=record,
        )
        return {
            "candidate_id": identifier,
            "eligible": eligible,
            "checks": checks,
            "reason_code": None if eligible else "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            "frozen_hash": frozen_hash if hashes_passed else None,
            "qualification": qualification,
            "qualification_hash": qualification_hash,
            "binding": binding,
            "data_quality": quality,
            **persisted_quality_fields(quality),
        }

    def _batch_mark_eligible_prevalidated(
        self,
        entries: list[Mapping[str, Any]],
    ) -> set[str]:
        """Persist caller-attested eligibility rows in one fenced transaction.

        Ranking performs all quality and gate evaluation before entering this
        transaction.  The lifecycle fence intentionally ignores C/D telemetry
        and harmless metadata, while A/B qualification or ranking changes
        remain excluded from the write.  This keeps batch ranking from taking
        one writer lock per candidate or evaluating historical quality while
        that lock is held.
        """
        if not entries:
            return set()
        prepared: list[dict[str, Any]] = []
        for entry in entries:
            candidate_id = str(entry.get("candidate_id") or "").strip()
            initial = entry.get("record")
            validation = entry.get("validation")
            binding = (
                validation.get("binding")
                if isinstance(validation, Mapping)
                else None
            )
            binding_reason = (
                binding.get("reason_code")
                if isinstance(binding, Mapping)
                else None
            )
            qualification = (
                validation.get("qualification")
                if isinstance(validation, Mapping)
                else None
            )
            qualification_hash = (
                validation.get("qualification_hash")
                if isinstance(validation, Mapping)
                else None
            )
            frozen_hash = (
                validation.get("frozen_hash")
                if isinstance(validation, Mapping)
                else None
            )
            if (
                not candidate_id
                or not isinstance(initial, Mapping)
                or not isinstance(validation, Mapping)
                or validation.get("eligible") is not True
                or not isinstance(binding, Mapping)
                or (
                    not binding.get("bound")
                    and binding_reason != "ELIGIBILITY_MISSING"
                )
                or not isinstance(qualification, Mapping)
                or not isinstance(qualification_hash, str)
                or not qualification_hash
                or not isinstance(frozen_hash, str)
                or not frozen_hash
            ):
                continue
            evidence = {**qualification, "qualification_hash": qualification_hash}
            try:
                evidence_json = json.dumps(
                    evidence,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            except (TypeError, ValueError):
                continue
            prepared.append(
                {
                    "candidate_id": candidate_id,
                    "initial": initial,
                    "frozen_hash": frozen_hash,
                    "evidence_json": evidence_json,
                    "quality": entry.get("quality"),
                }
            )
        if not prepared:
            return set()
        connection = self.store.connection
        persisted: set[str] = set()
        eligible_at = ensure_utc(self.clock()).isoformat()
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in prepared:
                    candidate_id = str(item["candidate_id"])
                    initial = item["initial"]
                    current = self.store.load_candidate_lifecycle(candidate_id)
                    initial_quality = item.get("quality")
                    initial_stage, initial_qualification, initial_ranking = (
                        _canary_lifecycle_snapshot_hashes(
                            self.store,
                            initial,
                            quality=initial_quality,
                        )
                    )
                    current_stage, current_qualification, current_ranking = (
                        _canary_lifecycle_snapshot_hashes(
                            self.store,
                            current,
                            quality=initial_quality,
                        )
                    )
                    if (
                        not isinstance(current, Mapping)
                        or current_stage != initial_stage
                        or current_qualification != initial_qualification
                        or current_ranking != initial_ranking
                        or self._lifecycle_frozen_hash(current) != item["frozen_hash"]
                    ):
                        continue
                    connection.execute(
                        "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                        "VALUES(?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET "
                        "eligible_at=excluded.eligible_at,frozen_hash=excluded.frozen_hash,"
                        "evidence_json=excluded.evidence_json "
                        "WHERE canary_eligibility.frozen_hash IS NOT excluded.frozen_hash "
                        "OR canary_eligibility.evidence_json IS NOT excluded.evidence_json",
                        (
                            candidate_id,
                            eligible_at,
                            item["frozen_hash"],
                            item["evidence_json"],
                        ),
                    )
                    persisted.add(candidate_id)
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return persisted

    def mark_eligible(
        self,
        candidate_id: str,
        *,
        publish_readiness: bool = True,
    ) -> None:
        identifier = str(candidate_id).strip()
        initial = self.store.load_candidate_lifecycle(identifier)
        validation = self.validate_eligibility(identifier, _record=initial)
        if not validation.get("eligible"):
            raise CanaryBlocked(
                str(
                    validation.get("reason_code")
                    or "CANDIDATE_RESEARCH_GATES_INCOMPLETE"
                )
            )
        if not isinstance(initial, Mapping):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        with self.store._lock:
            connection = self.store.connection
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = self.store.load_candidate_lifecycle(identifier)
                if (
                    not isinstance(current, Mapping)
                    or current.get("stage") != initial.get("stage")
                    or current.get("payload") != initial.get("payload")
                    or current.get("updated_at") != initial.get("updated_at")
                ):
                    raise CanaryBlocked("ELIGIBILITY_SNAPSHOT_CHANGED")
                validation = self.validate_eligibility(identifier, _record=current)
                if not validation.get("eligible"):
                    raise CanaryBlocked(
                        str(
                            validation.get("reason_code")
                            or "CANDIDATE_RESEARCH_GATES_INCOMPLETE"
                        )
                    )
                payload = self._merged_lifecycle_payload(current)
                frozen_hash = validation.get("frozen_hash")
                quality = validation.get("data_quality")
                if payload is None or not isinstance(frozen_hash, str) or not frozen_hash:
                    raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
                projection = _canary_qualification_projection(
                    identifier,
                    payload,
                    frozen_hash=frozen_hash,
                    quality=quality if isinstance(quality, Mapping) else None,
                )
                qualification_hash = _canary_qualification_hash(projection)
                if not qualification_hash:
                    raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
                evidence = {
                    **projection,
                    "qualification_hash": qualification_hash,
                }
                try:
                    evidence_json = json.dumps(
                        evidence,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                except (TypeError, ValueError):
                    raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE") from None
                connection.execute(
                    "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                    "VALUES(?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET "
                    "eligible_at=excluded.eligible_at,frozen_hash=excluded.frozen_hash,"
                    "evidence_json=excluded.evidence_json",
                    (
                        identifier,
                        ensure_utc(self.clock()).isoformat(),
                        frozen_hash,
                        evidence_json,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        if publish_readiness:
            self.publish_readiness_snapshot(reason="ELIGIBILITY_CHANGED")
    def arm(
        self,
        candidate_id: str,
        *,
        venue: CanaryVenue,
        config_id: str,
        expected_generation: int,
        target_notional_usd: Decimal | None = None,
        expires_hours: Decimal = Decimal("24"),
        limits: CanaryLimits | None = None,
        credentials_configured: bool | None = None,
    ) -> Mapping[str, Any]:
        requested_config_id = str(config_id or "").strip()
        if not requested_config_id or isinstance(expected_generation, bool):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
        try:
            requested_generation = int(expected_generation)
        except (TypeError, ValueError, OverflowError):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED") from None
        if requested_generation < 1:
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
        settings_config_id, settings_generation, _ = self._settings_identity()
        if (
            settings_config_id != requested_config_id
            or settings_generation != requested_generation
        ):
            raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")
        if credentials_configured is False:
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
        _, credential_digest = self._load_credential_binding()
        geo = venue.geoblock()
        if geo.get("blocked") or geo.get("close_only"):
            raise CanaryBlocked("GEOGRAPHICALLY_BLOCKED")
        candidate_limits = limits
        if target_notional_usd is not None:
            if candidate_limits is None:
                candidate_limits = CanaryLimits(target_notional_usd=target_notional_usd)
            else:
                candidate_limits = CanaryLimits(
                    target_notional_usd=target_notional_usd,
                    max_exposure_usd=candidate_limits.max_exposure_usd,
                    max_daily_loss_usd=candidate_limits.max_daily_loss_usd,
                    max_open_positions=candidate_limits.max_open_positions,
                    max_orders_per_day=candidate_limits.max_orders_per_day,
                    max_slippage_bps=candidate_limits.max_slippage_bps,
                )
        limits = self._effective_limits(candidate_limits)
        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            now = ensure_utc(self.clock())
            existing_control = connection.execute(
                "SELECT state,control_generation FROM canary_control "
                "WHERE singleton=1"
            ).fetchone()
            try:
                current_generation = (
                    int(existing_control["control_generation"] or 0)
                    if existing_control is not None
                    else 0
                )
            except (TypeError, ValueError):
                raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
            if (
                existing_control is not None
                and str(existing_control["state"]).upper() == "KILLED"
            ):
                raise CanaryBlocked("CANARY_KILLED")
            if (
                existing_control is not None
                and str(existing_control["state"]).upper() == AUTONOMOUS_MICRO_LIVE
            ):
                raise CanaryBlocked("AUTONOMOUS_CANARY_ACTIVE")
            limits = limits or CanaryLimits(
                target_notional_usd=Decimal(target_notional_usd)
            )
            record = self.store.load_candidate_lifecycle(str(candidate_id))
            eligible = connection.execute(
                "SELECT candidate_id,frozen_hash,evidence_json "
                "FROM canary_eligibility WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            binding = self._eligibility_binding_result(
                str(candidate_id),
                eligible,
                record=record,
                verify_attestation=False,
            )
            if not binding.get("bound"):
                raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
            if binding.get("reevaluation_required"):
                self.mark_eligible(str(candidate_id))
                eligible = connection.execute(
                    "SELECT candidate_id,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
                record = self.store.load_candidate_lifecycle(str(candidate_id))
                binding = self._eligibility_binding_result(
                    str(candidate_id),
                    eligible,
                    record=record,
                    verify_attestation=False,
                )
                if not binding.get("bound") or binding.get("reevaluation_required"):
                    raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
            if not self.validate_eligibility(
                str(candidate_id),
                _record=record,
                _verify_attestation=False,
            ).get("eligible"):
                raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
            health = self.store.polymarket_health(now=now)
            if str(health.get("grade", "F")).upper() not in {"A", "B"}:
                raise CanaryBlocked("COLLECTOR_DEGRADED")
            expires = now + timedelta(hours=float(expires_hours))
            values = self._limits_record(limits)
            digest = self._integrity(
                candidate_id,
                "polymarket",
                expires.isoformat(),
                values,
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT state,control_generation FROM canary_control "
                    "WHERE singleton=1"
                ).fetchone()
                try:
                    current_generation = (
                        int(current["control_generation"] or 0)
                        if current is not None
                        else 0
                    )
                except (TypeError, ValueError):
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                if (
                    current is not None
                    and str(current["state"]).upper() == "KILLED"
                ):
                    raise CanaryBlocked("CANARY_KILLED")
                if (
                    current is not None
                    and str(current["state"]).upper() == AUTONOMOUS_MICRO_LIVE
                ):
                    raise CanaryBlocked("AUTONOMOUS_CANARY_ACTIVE")
                connection.execute(
                    "INSERT INTO canary_control("
                    "singleton,state,candidate_id,venue,armed_at,expires_at,"
                    "limits_json,integrity_hash,updated_at,control_generation,"
                    "settings_config_id,settings_generation,credential_fingerprint) "
                    "VALUES(1,'ARMED',?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "state=excluded.state,candidate_id=excluded.candidate_id,"
                    "venue=excluded.venue,armed_at=excluded.armed_at,"
                    "expires_at=excluded.expires_at,limits_json=excluded.limits_json,"
                    "integrity_hash=excluded.integrity_hash,"
                    "updated_at=excluded.updated_at,"
                    "control_generation=excluded.control_generation,"
                    "settings_config_id=excluded.settings_config_id,"
                    "settings_generation=excluded.settings_generation,"
                    "credential_fingerprint=excluded.credential_fingerprint",
                    (
                        candidate_id,
                        "polymarket",
                        now.isoformat(),
                        expires.isoformat(),
                        json.dumps(values, sort_keys=True),
                        digest,
                        now.isoformat(),
                        current_generation + 1,
                        settings_config_id,
                        settings_generation,
                        credential_digest,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.publish_readiness_snapshot(reason="CANARY_ARMED")
    def disarm(self) -> None: self._set_state("DISARMED")
    def kill(self) -> None: self._set_state("KILLED")
    def _set_state(self, state: str) -> None:
        now = ensure_utc(self.clock()).isoformat()
        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            with connection:
                current = connection.execute(
                    "SELECT state,control_generation FROM canary_control "
                    "WHERE singleton=1"
                ).fetchone()
                if current is not None and str(current["state"]).upper() == "KILLED":
                    # KILLED is a terminal latch; disarm must not make it
                    # possible to arm again.
                    return
                try:
                    generation = (
                        int(current["control_generation"] or 0)
                        if current is not None
                        else 0
                    )
                except (TypeError, ValueError):
                    raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                connection.execute(
                    "INSERT INTO canary_control("
                    "singleton,state,limits_json,integrity_hash,updated_at,"
                    "control_generation) VALUES(1,?,'{}','',?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "state=excluded.state,updated_at=excluded.updated_at,"
                    "control_generation=excluded.control_generation",
                    (state, now, generation + 1),
                )
                connection.execute(
                    "INSERT INTO canary_autonomous_state("
                    "singleton,last_tick_at,next_decision,blocker,last_signal_id,worker_status,updated_at) "
                    "VALUES(1,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                    "next_decision=excluded.next_decision,blocker=excluded.blocker,"
                    "worker_status=excluded.worker_status,updated_at=excluded.updated_at",
                    (
                        None,
                        "ENABLE AUTO CANARY" if state == "DISARMED" else "KILL_LATCHED",
                        "AUTONOMOUS_CANARY_DISABLED" if state == "DISARMED" else "CANARY_KILLED",
                        None,
                        "IDLE" if state == "DISARMED" else "KILLED",
                        now,
                    ),
                )
        self.publish_readiness_snapshot(
            reason="CANARY_KILLED" if state == "KILLED" else "CANARY_DISARMED"
        )
    def _selection_validation(
        self, selection: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        """Validate a persisted selection while holding the store read lock."""
        lock = getattr(self.store, "_lock", None)
        if lock is None:
            return self._selection_validation_locked(selection)
        with lock:
            return self._selection_validation_locked(selection)

    def _selection_validation_locked(
        self, selection: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        """Validate persisted selection against one locked lifecycle snapshot."""

        def none_state(historical_id: str | None = None) -> dict[str, Any]:
            return {
                "selection_status": "NONE",
                "selection_valid": False,
                "selection_invalidation_reason": None,
                "selected_candidate": None,
                "last_selected_candidate": historical_id,
            }

        def missing_schema(exc: sqlite3.OperationalError) -> bool:
            message = str(exc).lower()
            return "no such table" in message or "no such column" in message

        def text_value(value: Any) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def mapping_json(value: Any) -> Mapping[str, Any] | None:
            try:
                parsed = json.loads(str(value) if value is not None else "")
            except (TypeError, ValueError, json.JSONDecodeError):
                return None
            return parsed if isinstance(parsed, Mapping) and parsed else None

        if not isinstance(selection, Mapping):
            return none_state()
        current_id = text_value(selection.get("candidate_id"))
        selection_last_selected = text_value(
            selection.get("last_selected_candidate")
        )
        historical_id = selection_last_selected or current_id or None
        if not current_id:
            return {
                "selection_status": "STALE" if historical_id else "NONE",
                "selection_valid": False,
                "selection_invalidation_reason": (
                    selection.get("selection_invalidation_reason")
                    or ("REEVALUATION_REQUIRED" if historical_id else None)
                ),
                "selected_candidate": None,
                "last_selected_candidate": historical_id,
            }

        selection_run_id = text_value(selection.get("ranking_run_id"))
        selection_timestamp = text_value(selection.get("ranking_timestamp"))
        selection_selected_at = text_value(selection.get("selected_at"))
        selection_qualification_hash = text_value(
            selection.get("qualification_hash")
        )
        selection_ranking_hash = text_value(
            selection.get("ranking_snapshot_hash")
        )
        selection_metadata_valid = bool(
            current_id
            and selection_run_id
            and selection_timestamp
            and selection_qualification_hash
            and selection_ranking_hash
            and selection_selected_at
            and selection.get("selection_status") == "CURRENT"
            and selection.get("selection_valid") == 1
        )

        try:
            lifecycle = self.store.load_candidate_lifecycle(current_id)
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            lifecycle = None
        if not isinstance(lifecycle, Mapping) or str(lifecycle.get("stage")) == "REJECTED":
            reason = "LIFECYCLE_REJECTED"
        elif str(lifecycle.get("stage")) not in _CANARY_ELIGIBLE_STAGES:
            reason = "ELIGIBILITY_INVALID"
        else:
            try:
                eligibility = self.store.connection.execute(
                    "SELECT candidate_id,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (current_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                if missing_schema(exc):
                    return none_state(historical_id)
                raise
            binding = self._eligibility_binding_result(
                current_id,
                eligibility,
                record=lifecycle,
                verify_attestation=False,
            )
            if not binding.get("bound"):
                reason = str(binding.get("reason_code") or "ELIGIBILITY_INVALID")
            elif binding.get("reevaluation_required"):
                reason = "REEVALUATION_REQUIRED"
            else:
                validation = self.validate_eligibility(
                    current_id,
                    _record=lifecycle,
                    _verify_attestation=False,
                )
                if not validation.get("eligible"):
                    reason = str(
                        validation.get("reason_code")
                        or "ELIGIBILITY_INVALID"
                    ).strip().upper()
                else:
                    try:
                        row = self.store.connection.execute(
                            "SELECT * FROM canary_rankings WHERE candidate_id=?",
                            (current_id,),
                        ).fetchone()
                    except sqlite3.OperationalError as exc:
                        if missing_schema(exc):
                            return none_state(historical_id)
                        raise
                    ranking = dict(row) if row is not None else None
                    ranking_score = ranking.get("total_score") if ranking else None
                    selection_component_scores = mapping_json(
                        selection.get("component_scores_json")
                    )
                    selection_evidence_versions = mapping_json(
                        selection.get("evidence_versions_json")
                    )
                    ranking_component_scores = mapping_json(
                        ranking.get("component_scores_json") if ranking else None
                    )
                    ranking_evidence_versions = mapping_json(
                        ranking.get("evidence_versions_json") if ranking else None
                    )
                    if (
                        ranking is None
                        or ranking_score is None
                        or selection_component_scores is None
                        or selection_evidence_versions is None
                        or ranking_component_scores is None
                        or ranking_evidence_versions is None
                    ):
                        reason = "REEVALUATION_REQUIRED"
                    else:
                        payload = self._merged_lifecycle_payload(lifecycle) or {}
                        quality = evaluate_prediction_data_quality(
                            self.store,
                            payload,
                            verify_attestation=False,
                        )
                        expected_ranking_hash = _canary_ranking_snapshot_hash(
                            current_id,
                            str(lifecycle.get("stage") or ""),
                            payload,
                            qualification_hash=str(
                                binding.get("qualification_hash") or ""
                            ),
                            quality=quality,
                        )
                        ranking_candidate_id = text_value(ranking.get("candidate_id"))
                        ranking_run_id = text_value(ranking.get("ranking_run_id"))
                        ranking_timestamp = text_value(
                            ranking.get("ranking_timestamp")
                        )
                        ranking_qualification_hash = text_value(
                            ranking.get("qualification_hash")
                        )
                        ranking_snapshot_hash = text_value(
                            ranking.get("ranking_snapshot_hash")
                        )
                        if not selection_qualification_hash:
                            reason = "REEVALUATION_REQUIRED"
                        elif selection_qualification_hash != binding.get(
                            "qualification_hash"
                        ):
                            reason = "QUALIFICATION_CHANGED"
                        elif not selection_ranking_hash:
                            reason = "REEVALUATION_REQUIRED"
                        elif selection_ranking_hash != expected_ranking_hash:
                            reason = "RANKING_EVIDENCE_CHANGED"
                        elif ranking_qualification_hash != selection_qualification_hash:
                            reason = "QUALIFICATION_CHANGED"
                        elif ranking_snapshot_hash != selection_ranking_hash:
                            reason = "RANKING_EVIDENCE_CHANGED"
                        elif not selection_metadata_valid:
                            reason = "REEVALUATION_REQUIRED"
                        elif (
                            ranking.get("selected") != 1
                            or ranking_candidate_id != current_id
                            or ranking_run_id != selection_run_id
                            or ranking_timestamp != selection_timestamp
                            or selection_selected_at != selection_timestamp
                            or selection_selected_at != ranking_timestamp
                            or selection.get("rank") != ranking.get("rank")
                            or selection.get("total_score") != ranking.get("total_score")
                            or selection.get("component_scores_json")
                            != ranking.get("component_scores_json")
                            or selection.get("evidence_versions_json")
                            != ranking.get("evidence_versions_json")
                            or selection.get("reason") != "SELECTED_WINNER"
                            or ranking.get("reason") != ""
                        ):
                            reason = "REEVALUATION_REQUIRED"
                        else:
                            return {
                                "selection_status": "CURRENT",
                                "selection_valid": True,
                                "selection_invalidation_reason": None,
                                "selected_candidate": current_id,
                                "last_selected_candidate": historical_id,
                            }
        return {
            "selection_status": "STALE",
            "selection_valid": False,
            "selection_invalidation_reason": reason,
            "selected_candidate": None,
            "last_selected_candidate": historical_id,
        }


    def authoritative_status(self) -> dict[str, Any]:
        lock = getattr(self.store, "_lock", None)
        if lock is None:
            return self._authoritative_status_locked()
        with lock:
            return self._authoritative_status_locked()

    def _authoritative_status_locked(self) -> dict[str, Any]:
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)

        def _fetchone(query: str, parameters: tuple[Any, ...] = ()) -> Any:
            if lock is None:
                return connection.execute(query, parameters).fetchone()
            with lock:
                return connection.execute(query, parameters).fetchone()

        def _fetchall(query: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
            if lock is None:
                return connection.execute(query, parameters).fetchall()
            with lock:
                return connection.execute(query, parameters).fetchall()

        def _optional_fetchone(
            query: str, parameters: tuple[Any, ...] = ()
        ) -> Any:
            try:
                return _fetchone(query, parameters)
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "no such table" in message or "no such column" in message:
                    return None
                raise

        def _optional_fetchall(
            query: str, parameters: tuple[Any, ...] = ()
        ) -> list[Any]:
            try:
                return _fetchall(query, parameters)
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "no such table" in message or "no such column" in message:
                    return []
                raise

        try:
            row = _fetchone("SELECT * FROM canary_control WHERE singleton=1")
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            row = None
        now = ensure_utc(self.clock())
        if lock is None:
            selection = self._selection_record()
            selection_state = self._selection_validation(selection)
        else:
            # Keep the persisted winner row and every lifecycle/ranking read
            # used to validate it in one store-locked snapshot.
            with lock:
                selection = self._selection_record()
                selection_state = self._selection_validation(selection)

        def _safe_count(query: str) -> int:
            try:
                result = _fetchone(query)
            except sqlite3.OperationalError:
                return 0
            try:
                return int(result["n"]) if result is not None else 0
            except (KeyError, TypeError, ValueError):
                return 0

        def _validated_eligibility_rows() -> list[Any]:
            query = (
                "SELECT e.candidate_id,e.frozen_hash,e.evidence_json "
                "FROM canary_eligibility AS e "
                "JOIN candidate_lifecycle AS c ON c.candidate_id=e.candidate_id "
                "WHERE c.stage IN ('FROZEN','PAPER_FORWARD','PAPER_PROMOTABLE')"
            )
            try:
                rows = _fetchall(query)
            except sqlite3.Error:
                return []
            valid: list[Any] = []
            for eligibility in rows:
                try:
                    candidate_id = str(eligibility["candidate_id"] or "").strip()
                    record = self.store.load_candidate_lifecycle(candidate_id)
                    binding = self._eligibility_binding_result(
                        candidate_id,
                        eligibility,
                        record=record,
                        verify_attestation=False,
                    )
                    if (
                        candidate_id
                        and binding.get("bound")
                        and self.validate_eligibility(
                            candidate_id,
                            _record=record,
                            _verify_attestation=False,
                        ).get("eligible")
                    ):
                        valid.append(eligibility)
                except Exception:
                    continue
            return valid

        eligibility_raw_count = _safe_count(
            "SELECT COUNT(*) AS n FROM canary_eligibility"
        )
        eligible_count = len(_validated_eligibility_rows())
        rankable_raw_count = _safe_count(
            "SELECT COUNT(*) AS n FROM canary_rankings WHERE total_score IS NOT NULL"
        )
        rankable_rows = _optional_fetchall(
            "SELECT * FROM canary_rankings WHERE total_score IS NOT NULL"
        )
        rankable_count = 0
        for ranking_row in rankable_rows:
            try:
                candidate_id = str(ranking_row["candidate_id"] or "").strip()
                record = self.store.load_candidate_lifecycle(candidate_id)
                if not isinstance(record, Mapping):
                    continue
                binding_row = _optional_fetchone(
                    "SELECT candidate_id,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (candidate_id,),
                )
                binding = self._eligibility_binding_result(
                    candidate_id,
                    binding_row,
                    record=record,
                    verify_attestation=False,
                )
                if not binding.get("bound") or binding.get("reevaluation_required"):
                    continue
                if not self.validate_eligibility(
                    candidate_id,
                    _record=record,
                    _verify_attestation=False,
                ).get("eligible"):
                    continue
                payload = self._merged_lifecycle_payload(record) or {}
                quality = evaluate_prediction_data_quality(
                    self.store,
                    payload,
                    verify_attestation=False,
                )
                expected_hash = _canary_ranking_snapshot_hash(
                    candidate_id,
                    str(record.get("stage") or ""),
                    payload,
                    qualification_hash=str(binding.get("qualification_hash") or ""),
                    quality=quality,
                )
                if (
                    ranking_row["qualification_hash"] == binding.get("qualification_hash")
                    and ranking_row["ranking_snapshot_hash"] == expected_hash
                ):
                    rankable_count += 1
            except Exception:
                continue
        execution_event_count = _safe_count(
            "SELECT COUNT(*) AS n FROM canary_execution_events"
        )

        winner_id = selection_state["selected_candidate"]
        last_selected_candidate = selection_state["last_selected_candidate"]
        selection_status = selection_state["selection_status"]
        selection_valid = bool(selection_state["selection_valid"])
        selection_invalidation_reason = selection_state["selection_invalidation_reason"]
        quality_candidate_id = winner_id or last_selected_candidate
        winner_quality: dict[str, Any] = {
            "historical_data_integrity": "UNKNOWN",
            "historical_execution_fidelity": "UNKNOWN",
            "current_execution_evidence": "CURRENT_ORDER_BOOK_REQUIRED",
        }
        if quality_candidate_id:
            try:
                lifecycle = self.store.load_candidate_lifecycle(quality_candidate_id)
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                lifecycle = None
            payload = lifecycle.get("payload") if isinstance(lifecycle, Mapping) else {}
            payload = payload if isinstance(payload, Mapping) else {}
            components = selection.get("component_scores") if selection else {}
            components = components if isinstance(components, Mapping) else {}
            raw_quality = components.get("raw")
            raw_quality = raw_quality if isinstance(raw_quality, Mapping) else {}
            sources = (payload, raw_quality)

            def _first_quality(*names: str, default: Any = None) -> Any:
                for source in sources:
                    for name in names:
                        value = source.get(name)
                        if value is not None and value != "":
                            return value
                return default

            integrity = _first_quality(
                "historical_data_integrity",
                "historical_integrity",
                default="UNKNOWN",
            )
            if isinstance(integrity, bool):
                integrity = "PASS" if integrity else "FAIL"
            elif isinstance(integrity, (int, float)):
                integrity = "PASS" if float(integrity) >= 1.0 else "FAIL"
            fidelity = str(
                _first_quality(
                    "historical_execution_fidelity",
                    "fidelity_label",
                    default="UNKNOWN",
                )
            )
            if fidelity.upper() == "PRICE_PROXY" and "LIMITED" not in fidelity.upper():
                fidelity = "PRICE_PROXY · LIMITED"
            current_execution = _first_quality(
                "current_execution_evidence",
                "execution_evidence",
                default="CURRENT_ORDER_BOOK_REQUIRED",
            )
            winner_quality = {
                "historical_data_integrity": str(integrity),
                "historical_execution_fidelity": str(fidelity),
                "current_execution_evidence": str(current_execution),
            }
        selection_reason = (
            str(selection.get("reason") or "")
            if isinstance(selection, Mapping)
            else ""
        ) or None
        try:
            risk_envelope = self.autonomous_limits()
            settings_available = True
        except CanaryBlocked:
            risk_envelope = {}
            settings_available = False
        autonomous_state = None
        try:
            autonomous_state = _fetchone(
                "SELECT last_tick_at,last_tick_started_at,last_tick_completed_at,"
                "last_successful_tick,last_error_code,consecutive_failures,next_retry_at,"
                "candidates_evaluated,signals_generated,orders_attempted,"
                "actionable_candidates_found,selected_actionable_candidate,"
                "selected_actionable_rank,selected_actionable_score,signal_scan_cursor,"
                "signal_scan_ranking_run_id,next_signal_scan_start_rank,next_signal_scan_end_rank,"
                "signal_scan_cycle_id,signal_scan_candidate_universe_hash,"
                "signal_scan_cycle_started_at,signal_scan_cycle_completed_at,"
                "signal_scan_cycle_complete,signal_scan_checked_this_cycle,"
                "signal_scan_remaining_this_cycle,signal_scan_coverage_percentage,"
                "signal_scan_skip_reasons_json,signal_scan_reason_counts_json,signal_scan_status,"
                "next_decision,blocker,last_signal_id,worker_status "
                "FROM canary_autonomous_state WHERE singleton=1"
            )
        except sqlite3.OperationalError:
            autonomous_state = None
        checked_keys: list[dict[str, Any]] = []
        if autonomous_state is not None:
            cycle_id = str(autonomous_state["signal_scan_cycle_id"] or "").strip()
            if cycle_id:
                checked_keys = [
                    dict(item)
                    for item in _optional_fetchall(
                        "SELECT cycle_id,candidate_id,qualification_hash,checked_at,"
                        "rank_at_check,ranking_run_id "
                        "FROM canary_signal_scan_checked WHERE cycle_id=? "
                        "ORDER BY checked_at,candidate_id,qualification_hash",
                        (cycle_id,),
                    )
                ]

        ranking_run_id = selection.get("ranking_run_id") if selection else None
        ranking_timestamp = selection.get("ranking_timestamp") if selection else None
        if selection and not ranking_timestamp:
            try:
                latest_ranking = _fetchone(
                    "SELECT ranking_run_id,ranking_timestamp FROM canary_rankings "
                    "WHERE selected=1 ORDER BY ranking_timestamp DESC LIMIT 1"
                )
            except sqlite3.Error:
                latest_ranking = None
            if latest_ranking is not None:
                ranking_run_id = ranking_run_id or latest_ranking["ranking_run_id"]
                ranking_timestamp = latest_ranking["ranking_timestamp"]
        def autonomous_value(name: str, default: Any = None) -> Any:
            if autonomous_state is None:
                return default
            try:
                return autonomous_state[name]
            except (IndexError, KeyError):
                return default

        disabled_auto = {
            "enabled": False,
            "selected_candidate": winner_id,
            "last_selected_candidate": last_selected_candidate,
            "ranking_run_id": ranking_run_id,
            "ranking_timestamp": ranking_timestamp,
            "rank": selection.get("rank") if selection else None,
            "score": selection.get("total_score") if selection else None,
            "selection_reason": selection_reason,
            "selection_status": selection_status,
            "selection_valid": selection_valid,
            "selection_invalidation_reason": selection_invalidation_reason,
            "eligibility_raw_count": eligibility_raw_count,
            "eligible_count": eligible_count,
            "rankable_raw_count": rankable_raw_count,
            "rankable_count": rankable_count,
            "candidates_ranked": autonomous_value("candidates_ranked"),
            "candidates_signal_checked": autonomous_value("candidates_signal_checked"),
            "candidates_no_signal": autonomous_value("candidates_no_signal"),
            "actionable_candidates_found": autonomous_value("actionable_candidates_found"),
            "selected_actionable_candidate": autonomous_value("selected_actionable_candidate"),
            "selected_actionable_rank": autonomous_value("selected_actionable_rank"),
            "selected_actionable_score": autonomous_value("selected_actionable_score"),
            "signal_scan_cursor": autonomous_value("signal_scan_cursor", 0),
            "signal_scan_ranking_run_id": autonomous_value("signal_scan_ranking_run_id"),
            "next_signal_scan_start_rank": autonomous_value("next_signal_scan_start_rank"),
            "next_signal_scan_end_rank": autonomous_value("next_signal_scan_end_rank"),
            "signal_scan_cycle_id": autonomous_value("signal_scan_cycle_id"),
            "signal_scan_candidate_universe_hash": autonomous_value(
                "signal_scan_candidate_universe_hash"
            ),
            "signal_scan_cycle_started_at": autonomous_value(
                "signal_scan_cycle_started_at"
            ),
            "signal_scan_cycle_completed_at": autonomous_value(
                "signal_scan_cycle_completed_at"
            ),
            "signal_scan_cycle_complete": autonomous_value(
                "signal_scan_cycle_complete", 0
            ),
            "signal_scan_checked_this_cycle": autonomous_value(
                "signal_scan_checked_this_cycle", 0
            ),
            "signal_scan_remaining_this_cycle": autonomous_value(
                "signal_scan_remaining_this_cycle", 0
            ),
            "signal_scan_coverage_percentage": autonomous_value(
                "signal_scan_coverage_percentage", 0.0
            ),
            "signal_scan_skip_reasons_json": autonomous_value(
                "signal_scan_skip_reasons_json", "{}"
            ),
            "signal_scan_reason_counts_json": autonomous_value(
                "signal_scan_reason_counts_json", "{}"
            ),
            "signal_scan_status": autonomous_value("signal_scan_status", "UNKNOWN"),
            "signal_scan_checked_keys": checked_keys,
            **winner_quality,
            "next_decision": autonomous_value(
                "next_decision", "ENABLE AUTO CANARY"
            ),
            "blocker": autonomous_value("blocker", "AUTONOMOUS_CANARY_DISABLED"),
            "last_tick_at": autonomous_value("last_tick_at"),
            "last_signal_id": autonomous_value("last_signal_id"),
            "worker_status": autonomous_value("worker_status", "IDLE"),
            "last_tick_started_at": autonomous_value("last_tick_started_at"),
            "last_tick_completed_at": autonomous_value("last_tick_completed_at"),
            "last_successful_tick": autonomous_value("last_successful_tick"),
            "last_error_code": autonomous_value("last_error_code"),
            "consecutive_failures": autonomous_value("consecutive_failures"),
            "next_retry_at": autonomous_value("next_retry_at"),
            "candidates_evaluated": autonomous_value("candidates_evaluated"),
            "signals_generated": autonomous_value("signals_generated"),
            "orders_attempted": autonomous_value("orders_attempted"),
        }
        scan_projection = {
            name: disabled_auto.get(name)
            for name in (
                "candidates_ranked",
                "candidates_signal_checked",
                "candidates_no_signal",
                "actionable_candidates_found",
                "selected_actionable_candidate",
                "selected_actionable_rank",
                "selected_actionable_score",
                "signal_scan_cursor",
                "signal_scan_ranking_run_id",
                "next_signal_scan_start_rank",
                "next_signal_scan_end_rank",
                "signal_scan_cycle_id",
                "signal_scan_candidate_universe_hash",
                "signal_scan_cycle_started_at",
                "signal_scan_cycle_completed_at",
                "signal_scan_checked_this_cycle",
                "signal_scan_remaining_this_cycle",
                "signal_scan_coverage_percentage",
                "signal_scan_skip_reasons_json",
                "signal_scan_reason_counts_json",
                "signal_scan_status",
                "signal_scan_checked_keys",
            )
        }
        selection_audit = dict(selection) if isinstance(selection, Mapping) else None
        if selection_audit is not None:
            selection_audit.update(
                {
                    "candidate_id": winner_id or last_selected_candidate,
                    "selection_status": selection_status,
                    "selection_valid": selection_valid,
                    "selection_invalidation_reason": selection_invalidation_reason,
                    "selected_candidate": winner_id,
                    "last_selected_candidate": last_selected_candidate,
                }
            )

        display_state = "DISABLED"
        canonical = self._canonical_execution_usage(now)
        canonical_error = canonical.get("_canonical_error")
        cold_orders = (
            None
            if canonical_error
            else int(canonical.get("submitted_orders", 0) or 0)
        )
        cold_pnl = (
            None
            if canonical_error
            else float(canonical.get("today_realized_pnl_usd", "0") or "0")
        )
        cold_exposure = (
            None
            if canonical_error
            else float(canonical.get("aggregate_exposure_usd", "0") or "0")
        )
        cold_positions = (
            None
            if canonical_error
            else int(canonical.get("open_positions", 0) or 0)
        )
        if row is None:
            return {
                "production_live_trading": "DISABLED",
                "micro_live_canary": "DISABLED",
                "display_state": display_state,
                "control_state": "DISABLED",
                "credential_fingerprint": None,
                "binding_blocker": None,
                "candidate": winner_id,
                "control_candidate": None,
                "winner_id": winner_id,
                "winner_rank": selection.get("rank") if winner_id and selection else None,
                "winner_score": selection.get("total_score") if winner_id and selection else None,
                "selection_reason": selection_reason,
                "selection_status": selection_status,
                "selection_valid": selection_valid,
                "selection_invalidation_reason": selection_invalidation_reason,
                "selected_candidate": winner_id,
                "last_selected_candidate": last_selected_candidate,
                "ranking_run_id": ranking_run_id,
                "ranking_timestamp": ranking_timestamp,
                "eligibility_raw_count": eligibility_raw_count,
                "eligible_count": eligible_count,
                **scan_projection,
                "rankable_raw_count": rankable_raw_count,
                "rankable_count": rankable_count,
                "venue": None,
                "expiry": None,
                "control_generation": 0,
                "settings_config_id": None,
                "settings_generation": None,
                "settings_available": settings_available,
                "today_orders": cold_orders,
                "today_realized_pnl": cold_pnl,
                "total_exposure": cold_exposure,
                "open_positions": cold_positions,
                "daily_loss_budget_remaining": (
                    None
                    if cold_pnl is None
                    else max(
                        0,
                        float(risk_envelope.get("max_daily_loss_usd", "0"))
                        + cold_pnl,
                    )
                ),
                "limits": dict(risk_envelope),
                "risk_envelope": risk_envelope,
                "risk_limits": risk_envelope,
                "real_execution_events": execution_event_count,
                "execution_event_count": execution_event_count,
                **winner_quality,
                "autonomous": disabled_auto,
                "selected_winner": selection_audit,
                "trades": [],
                "live_execution": False,
                "kill_semantics": "KILL_PREVENTS_NEW_SUBMISSIONS; IN_FLIGHT_REQUESTS_ARE_NOT_RETRACTED",
            }
        data = dict(row)
        state = str(data.get("state") or "DISABLED").upper()
        persisted_credential_fingerprint = data.get("credential_fingerprint")
        credential_digest = _valid_credential_fingerprint(
            persisted_credential_fingerprint
        )
        binding_blocker: str | None = None
        if state in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
            if persisted_credential_fingerprint in (None, ""):
                binding_blocker = "CREDENTIAL_BINDING_MISSING"
                state = "DISABLED"
            elif credential_digest is None:
                binding_blocker = "CREDENTIAL_BINDING_INVALID"
                state = "KILLED"
        limits: dict[str, Any] = {}
        try:
            parsed_limits = json.loads(data.get("limits_json") or "{}")
            if not isinstance(parsed_limits, Mapping):
                raise ValueError("canary limits must be a mapping")
            limits = dict(parsed_limits)
        except (TypeError, ValueError, json.JSONDecodeError):
            state = "KILLED"
        if state == "ARMED":
            expected = self._integrity(
                str(data.get("candidate_id") or ""),
                str(data.get("venue") or ""),
                str(data.get("expires_at") or ""),
                limits,
            )
            candidate = str(data.get("candidate_id") or "")
            eligible = _optional_fetchone(
                "SELECT candidate_id,frozen_hash,evidence_json "
                "FROM canary_eligibility WHERE candidate_id=?",
                (candidate,),
            )
            lifecycle = self.store.load_candidate_lifecycle(candidate)
            bound = self._eligibility_binding_result(
                candidate,
                eligible,
                record=lifecycle,
                verify_attestation=False,
            ).get("bound")
            gates_pass = self.validate_eligibility(
                candidate,
                _record=lifecycle,
                _verify_attestation=False,
            ).get("eligible")
            if not bound or not gates_pass or expected != data.get("integrity_hash"):
                state = "KILLED"
            elif not data.get("expires_at"):
                state = "DISARMED"
            else:
                try:
                    expired = ensure_utc(datetime.fromisoformat(data["expires_at"])) <= now
                except (TypeError, ValueError):
                    state = "KILLED"
                else:
                    if expired:
                        state = "DISARMED"
        elif state == AUTONOMOUS_MICRO_LIVE:
            expected = self._integrity("", AUTONOMOUS_CANARY_VENUE, "", limits)
            if limits != self.autonomous_limits() or expected != data.get("integrity_hash"):
                state = "KILLED"
        trades = [dict(x) for x in _optional_fetchall(
            "SELECT l.timestamp,l.candidate_id,l.market_id,l.side,"
            "l.requested_notional,l.paper_expected_price,e.actual_average_price,"
            "CASE WHEN e.actual_average_price IS NOT NULL THEN "
            "CAST(e.actual_average_price AS REAL)-CAST(l.paper_expected_price AS REAL) "
            "END price_difference,COALESCE(e.status,l.status) status,"
            "l.realized_pnl FROM canary_ledger l LEFT JOIN "
            "canary_execution_events e ON e.execution_event_id=("
            "SELECT e2.execution_event_id FROM canary_execution_events e2 "
            "WHERE e2.canary_event_id=l.event_id ORDER BY e2.timestamp DESC,"
            "e2.execution_event_id DESC LIMIT 1) ORDER BY l.timestamp DESC LIMIT 100"
        )]
        last_request_status = str(trades[0].get("status") or "").upper() if trades else None
        canonical = self._canonical_execution_usage(now)
        if canonical.get("_canonical_error"):
            aggregates = {"orders": None, "exposure": None, "positions": None, "pnl": None}
        else:
            aggregates = {
                "orders": int(canonical.get("submitted_orders", 0) or 0),
                "exposure": float(canonical.get("aggregate_exposure_usd", "0") or "0"),
                "positions": int(canonical.get("open_positions", 0) or 0),
                "pnl": float(canonical.get("today_realized_pnl_usd", "0") or "0"),
            }
        try:
            control_generation = int(data.get("control_generation") or 0)
        except (TypeError, ValueError):
            state = "KILLED"
            control_generation = 0
        control_candidate = (
            str(data.get("candidate_id")).strip()
            if data.get("candidate_id")
            else None
        )
        selected_candidate = (
            winner_id if state == AUTONOMOUS_MICRO_LIVE else control_candidate
        )
        auto_enabled = state == AUTONOMOUS_MICRO_LIVE and binding_blocker is None
        if state == "KILLED":
            display_state = "KILLED"
        elif state in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
            display_state = "ENABLED"
        else:
            display_state = "DISABLED"
        auto_payload = {
            **disabled_auto,
            "enabled": auto_enabled,
            "selected_candidate": winner_id,
            "next_decision": (
                autonomous_state["next_decision"]
                if autonomous_state is not None
                else "KILL_LATCHED" if state == "KILLED" else "ENABLE AUTO CANARY"
            ),
            "blocker": (
                binding_blocker
                or (
                    autonomous_state["blocker"]
                    if autonomous_state is not None and (auto_enabled or state == "KILLED")
                    else "CANARY_KILLED" if state == "KILLED" else "AUTONOMOUS_CANARY_DISABLED"
                )
            ),
            "last_tick_at": (
                autonomous_state["last_tick_at"]
                if autonomous_state is not None
                else None
            ),
            "last_signal_id": (
                autonomous_state["last_signal_id"]
                if autonomous_state is not None
                else None
            ),
            "worker_status": (
                autonomous_state["worker_status"]
                if autonomous_state is not None
                else "IDLE"
            ),
        }
        daily_loss_budget_remaining = (
            None
            if aggregates["pnl"] is None
            else max(
                0,
                float(
                    limits.get(
                        "max_daily_loss_usd",
                        risk_envelope.get("max_daily_loss_usd", "0"),
                    )
                )
                + float(aggregates["pnl"]),
            )
        )
        return {
            "production_live_trading": "DISABLED",
            "micro_live_canary": state,
            "display_state": display_state,
            "control_state": state,
            "credential_fingerprint": credential_digest,
            "binding_blocker": binding_blocker,
            "candidate": selected_candidate,
            "control_candidate": control_candidate,
            "winner_id": winner_id,
            "winner_rank": selection.get("rank") if winner_id and selection else None,
            "winner_score": selection.get("total_score") if winner_id and selection else None,
            "selection_reason": selection_reason,
            "selection_status": selection_status,
            "selection_valid": selection_valid,
            "selection_invalidation_reason": selection_invalidation_reason,
            "selected_candidate": winner_id,
            "last_selected_candidate": last_selected_candidate,
            "eligibility_raw_count": eligibility_raw_count,
            "eligible_count": eligible_count,
            "rankable_raw_count": rankable_raw_count,
            "rankable_count": rankable_count,
            **scan_projection,
            "ranking_run_id": ranking_run_id,
            "ranking_timestamp": ranking_timestamp,
            "venue": data.get("venue"),
            "settings_config_id": data.get("settings_config_id"),
            "settings_generation": data.get("settings_generation"),
            "settings_available": settings_available,
            "expiry": data.get("expires_at"),
            "control_generation": control_generation,
            "last_request_status": last_request_status,
            "today_orders": (
                int(aggregates["orders"]) if aggregates["orders"] is not None else None
            ),
            "today_realized_pnl": (
                float(aggregates["pnl"]) if aggregates["pnl"] is not None else None
            ),
            "total_exposure": (
                float(aggregates["exposure"]) if aggregates["exposure"] is not None else None
            ),
            "open_positions": (
                int(aggregates["positions"]) if aggregates["positions"] is not None else None
            ),
            "limits": limits,
            "risk_envelope": risk_envelope,
            "daily_loss_budget_remaining": daily_loss_budget_remaining,
            "real_execution_events": execution_event_count,
            "execution_event_count": execution_event_count,
            **winner_quality,
            "autonomous": auto_payload,
            "selected_winner": selection_audit,
            "trades": trades,
            "live_execution": False,
            "kill_semantics": "KILL_PREVENTS_NEW_SUBMISSIONS; IN_FLIGHT_REQUESTS_ARE_NOT_RETRACTED",
        }

    def check(
        self,
        *,
        candidate_id: str | None = None,
        venue: CanaryVenue | None = None,
        market_id: str | None = None,
        token_id: str | None = None,
        allow_environment: bool | None = None,
        _connectivity_only: bool = False,
    ) -> dict[str, Any]:
        """Run full readiness or pre-arming read-only diagnostics."""
        failures: list[str] = []
        environment = (
            self.allow_environment
            if allow_environment is None
            else bool(allow_environment)
        )
        status = self.authoritative_status()
        if not _connectivity_only:
            if status["micro_live_canary"] not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                failures.append("CANARY_NOT_ARMED")
            if candidate_id and candidate_id != status.get("candidate"):
                failures.append("CANDIDATE_NOT_ARMED")
        credential_failure: str | None = None
        try:
            credentials_configured = bool(
                self.credentials.configured(allow_environment=environment)
            )
        except CanaryBlocked as exc:
            credentials_configured = False
            credential_failure = str(exc)
        except Exception:
            credentials_configured = False
            credential_failure = "CREDENTIALS_NOT_CONFIGURED"
        if not credentials_configured and not credential_failure:
            credential_failure = "CREDENTIALS_NOT_CONFIGURED"
        if credential_failure:
            failures.append(credential_failure)
        if venue is None:
            failures.append("VENUE_REQUIRED")
        diagnostics: dict[str, Any] = {
            "sdk_version": (
                venue.installed_sdk_version()
                if venue is not None
                and callable(getattr(venue, "installed_sdk_version", None))
                else None
            ),
            "execution_profile": (
                "ISOLATED_EXECUTION_PROFILE"
                if _isolated_execution_profile()
                else "STANDARD"
            ),
            "credentials_status": (
                credential_failure
                or ("CONFIGURED" if credentials_configured else "NOT CONFIGURED")
            ),
            "credentials_configured": credentials_configured,
            "collateral": _collateral_metadata(),
            "geoblock": {"status": "SKIPPED"},
            "authentication": {"status": "SKIPPED"},
            "account": {"status": "SKIPPED"},
            "balance": {"status": "SKIPPED"},
            "allowance": {"status": "SKIPPED"},
            "market": {"status": "SKIPPED"},
            "book": {"status": "SKIPPED"},
        }
        try:
            health = self.store.polymarket_health(now=ensure_utc(self.clock()))
            diagnostics["collector"] = dict(health)
            if not _connectivity_only and str(health.get("grade", "F")).upper() not in {"A", "B"}:
                failures.append("COLLECTOR_DEGRADED")
        except Exception:
            diagnostics["collector"] = {"grade": "F"}
            if not _connectivity_only:
                failures.append("COLLECTOR_DEGRADED")
        if venue is not None:
            try:
                geo = dict(venue.geoblock())
                diagnostics["geoblock"] = geo
                if geo.get("blocked") or geo.get("close_only"):
                    failures.append("GEOGRAPHICALLY_BLOCKED")
            except CanaryBlocked as exc:
                failures.append(str(exc))
            except Exception:
                failures.append("GEOBLOCK_CHECK_FAILED")
            if credentials_configured:
                try:
                    authenticated = bool(venue.connectivity_check())
                    diagnostics["authentication"] = {
                        "status": "OK" if authenticated else "FAILED"
                    }
                    if not authenticated:
                        failures.append("AUTHENTICATED_CONNECTIVITY_FAILED")
                except CanaryBlocked as exc:
                    diagnostics["authentication"] = {"status": "FAILED"}
                    failures.append(str(exc))
                except Exception:
                    diagnostics["authentication"] = {"status": "FAILED"}
                    failures.append("AUTHENTICATED_CONNECTIVITY_FAILED")
                if diagnostics["authentication"]["status"] == "OK":
                    account_method = getattr(venue, "account", None)
                    if callable(account_method):
                        try:
                            account = account_method()
                            if not isinstance(account, Mapping):
                                raise TypeError("account diagnostics must be a mapping")
                            # Keep this an explicit allowlist: account payloads
                            # must never expose wallet/signer addresses or
                            # arbitrary SDK response fields.
                            diagnostics["account"] = {
                                key: account[key]
                                for key in (
                                    "authenticated",
                                    "wallet_type",
                                    "credential_fingerprint",
                                )
                                if key in account
                            }
                        except CanaryBlocked as exc:
                            failures.append(str(exc))
                        except Exception:
                            failures.append("ACCOUNT_CHECK_FAILED")
                    try:
                        balance = venue.balance()
                        available = Decimal(str(balance))
                        if not available.is_finite():
                            raise ValueError("balance must be finite")
                        diagnostics["balance"] = {
                            "status": "OK",
                            "available_usd": str(balance),
                        }
                        target = Decimal(
                            str(
                                status.get("limits", {}).get(
                                    "target_notional_usd",
                                    self.autonomous_limits()["target_notional_usd"],
                                )
                            )
                        )
                        if available < target:
                            failures.append("INSUFFICIENT_BALANCE")
                    except CanaryBlocked as exc:
                        failures.append(str(exc))
                    except Exception:
                        diagnostics["balance"] = {"status": "FAILED"}
                        failures.append("BALANCE_CHECK_FAILED")
                    if not (market_id and token_id):
                        allowance_method = getattr(venue, "allowance", None)
                        if callable(allowance_method):
                            try:
                                allowance = allowance_method()
                                if not isinstance(allowance, Mapping):
                                    raise TypeError("allowance diagnostics must be a mapping")
                                diagnostics["allowance"] = dict(allowance)
                                if (
                                    not _connectivity_only
                                    and type(venue) is PolymarketClobV2Venue
                                    and allowance.get("status") != "OK"
                                ):
                                    failures.append("CANARY_ALLOWANCE_UNAVAILABLE")
                            except CanaryBlocked as exc:
                                diagnostics["allowance"] = {"status": "FAILED"}
                                failures.append(str(exc))
                            except Exception:
                                diagnostics["allowance"] = {"status": "FAILED"}
                                failures.append("CANARY_ALLOWANCE_UNAVAILABLE")
                        elif type(venue) is PolymarketClobV2Venue:
                            failures.append("CANARY_ALLOWANCE_UNAVAILABLE")
                    if market_id and token_id:
                        try:
                            context = dict(venue.market_context(market_id, token_id))
                            allowance = context.get("allowance")
                            if isinstance(allowance, Mapping):
                                diagnostics["allowance"] = dict(allowance)
                                if allowance.get("status") != "OK":
                                    failures.append("CANARY_ALLOWANCE_UNAVAILABLE")
                                else:
                                    target = Decimal(
                                        str(
                                            status.get("limits", {}).get(
                                                "target_notional_usd",
                                                self.autonomous_limits()["target_notional_usd"],
                                            )
                                        )
                                    )
                                    available_allowance = Decimal(
                                        _base_units(
                                            allowance.get("available_base_units"),
                                            field="allowance",
                                        )
                                    )
                                    if available_allowance < target * (
                                        Decimal(10) ** POLYMARKET_COLLATERAL_DECIMALS
                                    ):
                                        failures.append("CANARY_ALLOWANCE_INSUFFICIENT")
                            elif type(venue) is PolymarketClobV2Venue:
                                failures.append("CANARY_ALLOWANCE_UNAVAILABLE")
                            diagnostics["market"] = {
                                key: context[key]
                                for key in (
                                    "market_version",
                                    "outcome",
                                    "token_id",
                                    "position_id",
                                    "asset_id",
                                    "accepting_orders",
                                    "fee_bps",
                                )
                                if key in context
                            }
                            diagnostics["book"] = {
                                key: context[key]
                                for key in (
                                    "min_order_size",
                                    "tick_size",
                                    "bids",
                                    "asks",
                                )
                                if key in context
                            }
                            target = Decimal(
                                str(
                                    status.get("limits", {}).get(
                                        "target_notional_usd",
                                        self.autonomous_limits()["target_notional_usd"],
                                    )
                                )
                            )
                            best_ask = _best_ask_price(context.get("asks") or [])
                            minimum = Decimal(str(context.get("min_order_size", 0)))
                            size_increment = Decimal(
                                str(context.get("size_increment", "0.01"))
                            )
                            minimum = (
                                minimum / size_increment
                            ).to_integral_value(rounding=ROUND_UP) * size_increment
                            minimum_notional = Decimal(
                                str(
                                    context.get(
                                        "min_notional",
                                        context.get("minimum_notional", "0"),
                                    )
                                )
                            )
                            if minimum_notional > 0:
                                minimum = max(
                                    minimum,
                                    (
                                        minimum_notional / best_ask / size_increment
                                    ).to_integral_value(rounding=ROUND_UP)
                                    * size_increment,
                                )
                            fee_rate = Decimal(
                                str(
                                    context.get(
                                        "fee_rate",
                                        Decimal(str(context.get("fee_bps", 0)))
                                        / Decimal("10000"),
                                    )
                                )
                            )
                            fee_exponent = Decimal(
                                str(context.get("fee_exponent", 1))
                            )
                            curve = best_ask * (Decimal("1") - best_ask)
                            unit_fee = max(
                                fee_rate * (curve ** fee_exponent),
                                fee_rate * (Decimal("0.25") ** fee_exponent),
                            )
                            if minimum * (best_ask + unit_fee) > target:
                                failures.append("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
                            if diagnostics["balance"].get("status") == "OK":
                                try:
                                    fee_rate = Decimal(
                                        str(
                                            context.get(
                                                "fee_rate",
                                                Decimal(str(context.get("fee_bps", 0)))
                                                / Decimal("10000"),
                                            )
                                        )
                                    )
                                    fee_exponent = Decimal(
                                        str(context.get("fee_exponent", 1))
                                    )
                                    if (
                                        not fee_rate.is_finite()
                                        or fee_rate < 0
                                        or not fee_exponent.is_finite()
                                        or fee_exponent < 0
                                    ):
                                        raise ValueError("invalid fee")
                                    conservative_fee = (
                                        target
                                        * fee_rate
                                        * (Decimal("0.25") ** fee_exponent)
                                    )
                                    if available < target + conservative_fee:
                                        failures.append("INSUFFICIENT_BALANCE")
                                except (TypeError, ValueError, ArithmeticError):
                                    failures.append("MARKET_CONNECTIVITY_FAILED")
                        except CanaryBlocked as exc:
                            failures.append(str(exc))
                        except Exception:
                            failures.append("MARKET_CONNECTIVITY_FAILED")
                    elif market_id or token_id:
                        diagnostics["market"] = {
                            "status": "SKIPPED",
                            "reason": "MARKET_AND_TOKEN_REQUIRED",
                        }
            else:
                diagnostics["authentication"] = {
                    "status": "SKIPPED",
                    "reason": "CREDENTIALS_NOT_CONFIGURED",
                }
                diagnostics["account"] = {
                    "status": "SKIPPED",
                    "reason": "CREDENTIALS_NOT_CONFIGURED",
                }
                diagnostics["balance"] = {
                    "status": "SKIPPED",
                    "reason": "CREDENTIALS_NOT_CONFIGURED",
                }
                if market_id or token_id:
                    diagnostics["market"] = {
                        "status": "SKIPPED",
                        "reason": "CREDENTIALS_NOT_CONFIGURED",
                    }
                    diagnostics["book"] = {
                        "status": "SKIPPED",
                        "reason": "CREDENTIALS_NOT_CONFIGURED",
                    }
        return {
            "ready": not failures,
            "message": (
                "READY FOR CANARY CONNECTIVITY"
                if _connectivity_only and not failures
                else "NOT READY FOR CANARY CONNECTIVITY"
                if _connectivity_only
                else "READY FOR MICRO LIVE CANARY"
                if not failures
                else "NOT READY FOR MICRO LIVE CANARY"
            ),
            "failures": list(dict.fromkeys(failures)),
            "diagnostics": diagnostics,
            "live_execution": False,
            "connectivity_only": _connectivity_only,
        }

    def connectivity_check(self, **kwargs: Any) -> dict[str, Any]:
        """Run pre-arming read-only diagnostics without canary state gates."""
        values = dict(kwargs)
        values.pop("candidate_id", None)
        values["_connectivity_only"] = True
        return self.check(**values)

    def submit(
        self,
        *,
        signal_id: str,
        candidate_id: str,
        market_id: str,
        token_id: str,
        side: str,
        paper_expected_price: Decimal,
        venue: CanaryVenue,
        allow_test_venue: bool = False,
        allow_environment: bool | None = None,
    ) -> Mapping[str, Any]:
        now = ensure_utc(self.clock())
        request_settings_config_id, request_settings_generation = self._settings_binding()
        environment = (
            self.allow_environment
            if allow_environment is None
            else bool(allow_environment)
        )
        is_official_venue = type(venue) is PolymarketClobV2Venue
        reservation_control_state: str | None = None
        reservation_control_candidate: str | None = None
        reservation_control_expiry: str | None = None
        reservation_control_generation: int | None = None

        def block(reason: str) -> None:
            raise CanaryBlocked(reason)

        if _isolated_execution_profile():
            block("ISOLATED_EXECUTION_PROFILE")

        def enforce_controls(
            snapshot: Mapping[str, Any],
            *,
            additional_exposure: Decimal = Decimal("0"),
            reservation_event_id: str | None = None,
            state_only: bool = False,
        ) -> Mapping[str, Any]:
            limits = snapshot.get("limits", {})
            state = str(snapshot.get("micro_live_canary") or "DISABLED").upper()
            if state not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                block("CANARY_NOT_ARMED")
            snapshot_settings_id = snapshot.get("settings_config_id")
            snapshot_settings_generation = snapshot.get("settings_generation")
            if (
                request_settings_config_id is not None
                and str(snapshot_settings_id or "") != request_settings_config_id
            ):
                block("CANARY_SETTINGS_GENERATION_CHANGED")
            if request_settings_generation is not None:
                try:
                    if int(snapshot_settings_generation) != request_settings_generation:
                        block("CANARY_SETTINGS_GENERATION_CHANGED")
                except (TypeError, ValueError):
                    block("CANARY_SETTINGS_GENERATION_CHANGED")
            if state == AUTONOMOUS_MICRO_LIVE and limits != self.autonomous_limits():
                block("AUTONOMOUS_RISK_ENVELOPE_CORRUPT")
            if state_only:
                return limits
            existing = self.store.connection.execute(
                "SELECT event_id FROM canary_ledger WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            if existing is not None and existing["event_id"] != reservation_event_id:
                block("DUPLICATE_SIGNAL")
            if state == AUTONOMOUS_MICRO_LIVE:
                control_candidate = str(
                    snapshot.get("control_candidate") or ""
                ).strip()
                candidate_matches = (
                    bool(control_candidate)
                    and bool(str(candidate_id).strip())
                    and control_candidate == str(candidate_id).strip()
                )
            else:
                candidate_matches = candidate_id == snapshot.get("candidate")
            if not candidate_matches:
                block(
                    "AUTO_CANARY_CANDIDATE_NOT_SELECTED"
                    if state == AUTONOMOUS_MICRO_LIVE
                    else "CANDIDATE_MISMATCH"
                )
            if reservation_event_id is None:
                if snapshot["today_orders"] >= int(limits["max_orders_per_day"]):
                    block("DAILY_ORDER_LIMIT")
                if snapshot["open_positions"] >= int(limits["max_open_positions"]):
                    block("OPEN_POSITION_LIMIT")
                target = Decimal(limits["target_notional_usd"])
                exposure = Decimal(str(snapshot["total_exposure"])) + target
            else:
                if snapshot["today_orders"] > int(limits["max_orders_per_day"]):
                    block("DAILY_ORDER_LIMIT")
                if snapshot["open_positions"] > int(limits["max_open_positions"]):
                    block("OPEN_POSITION_LIMIT")
                exposure = Decimal(str(snapshot["total_exposure"]))
            if snapshot["today_realized_pnl"] <= -float(limits["max_daily_loss_usd"]):
                block("DAILY_LOSS_LIMIT")
            if exposure + additional_exposure > Decimal(limits["max_exposure_usd"]):
                block("EXPOSURE_LIMIT")
            return limits

        def lookup_candidate_signal() -> Mapping[str, Any]:
            row = self.store.connection.execute(
                "SELECT * FROM canary_signals WHERE signal_id=? AND candidate_id=?",
                (str(signal_id).strip(), str(candidate_id).strip()),
            ).fetchone()
            if row is None:
                block("CANARY_SIGNAL_NOT_FOUND")
            signal = self._signal_from_row(row)
            status = str(signal.get("status") or "").upper()
            if status != "READY":
                if status in {
                    "SUBMITTED",
                    "SUBMITTING",
                    "UNKNOWN",
                    "REJECTED",
                    "ACCEPTED",
                    "MATCHED",
                    "PARTIAL",
                    "PARTIALLY_FILLED",
                    "SETTLED",
                    "RESOLVED",
                }:
                    block("DUPLICATE_SIGNAL")
                block("CANARY_SIGNAL_NOT_READY")
            return signal

        def enforce_balance(
            available: Any,
            notional: Decimal,
            estimated_fees: Decimal,
        ) -> None:
            try:
                balance = Decimal(str(available))
                required = notional + estimated_fees
            except Exception:
                block("BALANCE_CHECK_FAILED")
            if not balance.is_finite() or not required.is_finite():
                block("BALANCE_CHECK_FAILED")
            if balance < required:
                block("INSUFFICIENT_BALANCE")
        def current_execution_market_cap_failure(
            signal: Mapping[str, Any],
            *,
            at: datetime,
        ) -> dict[str, Any] | None:
            """Return an explicit scope/cap failure, or ``None`` when safe."""
            candidate = str(signal.get("candidate_id") or "").strip()
            base_evidence: dict[str, Any] = {
                "candidate_id": candidate or None,
            }
            if not candidate:
                return {
                    "reason_code": "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
                    "evidence": base_evidence,
                }
            lifecycle = self.store.load_candidate_lifecycle(candidate)
            payload = self._merged_lifecycle_payload(lifecycle)
            if not isinstance(payload, Mapping):
                return {
                    "reason_code": "CANDIDATE_FROZEN_BINDING_INVALID",
                    "evidence": base_evidence,
                }
            scope_binding = _canary_scope_binding(payload)
            scope_resolution = _canary_current_scope_resolution(
                self.store,
                candidate,
                payload,
                now=at,
            )
            scope_evidence = {
                **base_evidence,
                "scope_hash": scope_resolution.get("scope_hash"),
                "scope_version": scope_resolution.get("scope_version"),
                "plan_hash": scope_resolution.get("plan_hash"),
                "scope_resolution_status": _canary_resolution_attr(
                    scope_resolution.get("resolution"), "status"
                ),
                "scope_resolution_reason": scope_resolution.get("reason_code"),
            }
            if not scope_resolution.get("bound"):
                return {
                    "reason_code": str(
                        scope_resolution.get("reason_code")
                        or "SCOPE_RESOLUTION_MISSING"
                    ),
                    "evidence": scope_evidence,
                }
            resolved_ids = tuple(
                str(item.get("market_id") or "").strip()
                for item in scope_resolution.get("matched_markets", ())
                if isinstance(item, Mapping)
                and str(item.get("market_id") or "").strip()
            )
            scope = scope_binding.get("market_scope", {})
            declared_values = (
                scope.get("market_ids", scope.get("exact_market_ids", ()))
                if isinstance(scope, Mapping)
                else ()
            )
            declared_ids = tuple(
                str(value).strip()
                for value in declared_values
                if str(value).strip()
            )
            if (
                len(resolved_ids) <= CANARY_EXECUTION_MARKET_CAP
                and len(declared_ids) <= CANARY_EXECUTION_MARKET_CAP
            ):
                return None
            evidence = _execution_market_cap_evidence(resolved_ids, declared_ids)
            evidence.update(scope_evidence)
            return {
                "reason_code": EXECUTION_FEASIBILITY_MARKET_CAP,
                "evidence": evidence,
            }

        def persist_submission_failure(
            reason: str,
            *,
            evidence: Mapping[str, Any] | None = None,
        ) -> None:
            """Persist a serialized rejection without opening a nested transaction."""
            row = self.store.connection.execute(
                "SELECT evidence_json FROM canary_signals WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            existing: Mapping[str, Any] = {}
            if row is not None:
                try:
                    decoded = json.loads(row["evidence_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    decoded = {}
                if isinstance(decoded, Mapping):
                    existing = decoded
            merged = dict(existing)
            if isinstance(evidence, Mapping):
                merged.update(dict(evidence))
            self.store.connection.execute(
                "UPDATE canary_signals SET status='NO_LONGER_VALID',"
                "reason=?,evidence_json=?,updated_at=? WHERE signal_id=? "
                "AND status IN ('READY','SUBMITTING')",
                (
                    reason,
                    json.dumps(merged, sort_keys=True, separators=(",", ":")),
                    ensure_utc(self.clock()).isoformat(),
                    signal_id,
                ),
            )


        def enforce_submission_fence(
            *,
            required_status: str,
        ) -> Mapping[str, Any]:
            """Require the persisted, current signal immediately before posting."""
            signal = self.get_signal(signal_id)
            if not isinstance(signal, Mapping):
                block("CANARY_SIGNAL_NOT_FOUND")
            if str(signal.get("status") or "").upper() != required_status.upper():
                if (
                    required_status.upper() == "READY"
                    and str(signal.get("status") or "").upper()
                    in {
                        "SUBMITTED",
                        "SUBMITTING",
                        "UNKNOWN",
                        "REJECTED",
                        "ACCEPTED",
                        "MATCHED",
                        "PARTIAL",
                        "PARTIALLY_FILLED",
                        "SETTLED",
                        "RESOLVED",
                    }
                ):
                    block("DUPLICATE_SIGNAL")
                block(
                    "CANARY_SIGNAL_NOT_READY"
                    if required_status.upper() == "READY"
                    else "CANARY_SIGNAL_STATE_CHANGED"
                )
            if any(
                str(signal.get(name) or "").strip() != str(expected).strip()
                for name, expected in (
                    ("candidate_id", candidate_id),
                    ("market_id", market_id),
                    ("token_id", token_id),
                    ("side", side),
                )
            ):
                block("CANARY_SIGNAL_BINDING_MISMATCH")
            try:
                signal_price = Decimal(str(signal.get("paper_expected_price")))
                expected_price = Decimal(str(paper_expected_price))
            except (TypeError, ValueError, ArithmeticError):
                signal_price = expected_price = Decimal("NaN")
            if (
                not signal_price.is_finite()
                or not expected_price.is_finite()
                or signal_price != expected_price
            ):
                block("CANARY_SIGNAL_BINDING_MISMATCH")
            signal_evidence = signal.get("evidence")
            if (
                not isinstance(signal_evidence, Mapping)
                or signal_evidence.get("current_execution_evidence")
                != CURRENT_ORDER_BOOK
            ):
                block("CURRENT_ORDER_BOOK_REQUIRED")
            fence_now = ensure_utc(self.clock())
            try:
                expires_at = parse_timestamp(signal.get("expires_at"))
            except (TypeError, ValueError, OverflowError):
                expires_at = None
            if expires_at is None or fence_now >= expires_at:
                was_in_transaction = self.store.connection.in_transaction
                self.store.connection.execute(
                    "UPDATE canary_signals SET status='REJECTED',reason=?,updated_at=? "
                    "WHERE signal_id=? AND status IN ('READY','SUBMITTING')",
                    (
                        "CANARY_SIGNAL_EXPIRED",
                        fence_now.isoformat(),
                        signal_id,
                    ),
                )
                if not was_in_transaction:
                    self.store.connection.commit()
                block("CANARY_SIGNAL_EXPIRED")
            if required_status.upper() == "READY":
                failure = current_execution_market_cap_failure(
                    signal,
                    at=fence_now,
                )
                if failure is not None:
                    reason = str(
                        failure.get("reason_code")
                        or "SCOPE_RESOLUTION_MISSING"
                    )
                    evidence = failure.get("evidence")
                    self._invalidate_signal(
                        signal_id,
                        reason,
                        evidence=evidence if isinstance(evidence, Mapping) else None,
                    )
                    block(reason)
            if required_status.upper() == "SUBMITTING":
                book_timestamp = parse_timestamp(
                    signal_evidence.get("current_order_book_timestamp")
                )
                source_timestamp = parse_timestamp(
                    signal_evidence.get("source_timestamp")
                    or signal.get("source_timestamp")
                )
                if (
                    book_timestamp is None
                    or source_timestamp is None
                    or book_timestamp > fence_now
                    or source_timestamp > fence_now
                    or (fence_now - book_timestamp).total_seconds()
                    > CANARY_SIGNAL_MAX_AGE_SECONDS
                    or (fence_now - source_timestamp).total_seconds()
                    > CANARY_SIGNAL_MAX_AGE_SECONDS
                ):
                    block("CANARY_SIGNAL_STALE")
                current_market = self._current_signal_market(
                    str(signal.get("market_id") or ""),
                    now=fence_now,
                )
                if current_market is None:
                    block("CANARY_SIGNAL_STALE")
                latest_row, latest_observation = current_market
                if str(latest_row.get("snapshot_id") or "") != str(
                    signal.get("source_snapshot_id") or ""
                ):
                    block("CANARY_SIGNAL_NO_LONGER_VALID")
                outcome = str(signal.get("outcome") or "").strip().lower()
                latest_book = self._current_order_book(
                    latest_observation,
                    outcome,
                    now=fence_now,
                    token_id=str(signal.get("token_id") or ""),
                )
                if latest_book is None:
                    block("CURRENT_ORDER_BOOK_REQUIRED")
                latest_token = latest_observation.get(f"{outcome}_token_id")
                token_ids = latest_observation.get("token_ids")
                if not latest_token and isinstance(token_ids, Mapping):
                    latest_token = token_ids.get(outcome)
                if str(latest_token or "") != str(signal.get("token_id") or ""):
                    block("CANARY_SIGNAL_NO_LONGER_VALID")
            if required_status.upper() == "READY":
                return signal
            lifecycle = self.store.load_candidate_lifecycle(
                str(signal.get("candidate_id") or "")
            )
            payload = self._merged_lifecycle_payload(lifecycle)
            if not isinstance(payload, Mapping):
                block("CANDIDATE_FROZEN_BINDING_INVALID")
            validation = self.validate_eligibility(
                str(signal.get("candidate_id") or ""),
                _record=lifecycle,
            )
            if not validation.get("eligible"):
                block(
                    str(
                        validation.get("reason_code")
                        or "CANDIDATE_RESEARCH_GATES_INCOMPLETE"
                    )
                )
            if any(
                str(payload.get(name) or "").strip()
                != str(signal.get(name) or "").strip()
                for name in (
                    "frozen_hash",
                    "strategy_hash",
                    "model_hash",
                    "config_hash",
                )
            ):
                block("CANDIDATE_LIFECYCLE_CHANGED")
            validation_binding = validation.get("binding")
            if (
                not isinstance(validation_binding, Mapping)
                or not validation_binding.get("bound")
            ):
                block("CANDIDATE_FROZEN_BINDING_INVALID")
            scope = _canary_current_scope_resolution(
                self.store,
                str(signal.get("candidate_id") or ""),
                payload,
                now=fence_now,
            )
            if required_status.upper() == "SUBMITTING" and (
                str(
                    signal_evidence.get("scope_hash")
                    or signal_evidence.get("market_scope_hash")
                    or ""
                ).strip()
                != str(scope.get("scope_hash") or "").strip()
                or str(
                    signal_evidence.get("scope_version")
                    or signal_evidence.get("market_scope_version")
                    or ""
                ).strip()
                != str(scope.get("scope_version") or "").strip()
            ):
                block("SCOPE_RESOLUTION_SCOPE_MISMATCH")
            if required_status.upper() == "SUBMITTING" and not scope.get("bound"):
                block(
                    str(scope.get("reason_code") or "SCOPE_RESOLUTION_MISSING")
                )
            matched = (
                scope.get("matched_markets", ())
                if isinstance(scope, Mapping)
                else ()
            )
            selected = next(
                (
                    item
                    for item in matched
                    if isinstance(item, Mapping)
                    and str(item.get("market_id") or "").strip()
                    == str(market_id).strip()
                ),
                None,
            )
            expected_token = (
                selected.get(
                    f"{str(signal.get('outcome') or '').strip().lower()}_token_id"
                )
                if isinstance(selected, Mapping)
                else None
            )
            if required_status.upper() == "SUBMITTING" and (
                not isinstance(selected, Mapping)
                or not scope.get("bound")
                or str(expected_token or "").strip() != str(token_id).strip()
            ):
                block("SCOPE_RESOLUTION_TOKEN_MISMATCH")
            return signal





        transport_state_lock = threading.Lock()
        submission_cancelled = threading.Event()
        network_send_started = False

        def submit_official_order(
            *,
            market_version: Any,
            neg_risk: Any,
            asset_id: str,
            side: str,
            price: Decimal,
            size: Decimal,
        ) -> Mapping[str, Any]:
            def before_post() -> None:
                enforce_submission_fence(required_status="SUBMITTING")
                final_submission_fence()
                fresh_id, fresh_generation, _ = self._settings_identity()
                if (
                    fresh_id != request_settings_config_id
                    or fresh_generation != request_settings_generation
                ):
                    raise CanaryBlocked("CANARY_SETTINGS_GENERATION_CHANGED")

            def mark_send_started() -> None:
                nonlocal network_send_started
                with transport_state_lock:
                    if submission_cancelled.is_set():
                        raise CanaryBlocked("CANARY_SUBMISSION_TIMEOUT")
                    network_send_started = True

            return self.submit_position_order(
                market_version=market_version,
                neg_risk=neg_risk,
                asset_id=asset_id,
                side=side,
                price=price,
                size=size,
                before_post=before_post,
                on_send_started=mark_send_started,
            )
        def execution_parameters(
            limits: Mapping[str, Any],
            context: Mapping[str, Any],
            best: Decimal,
            tick: Decimal,
        ) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
            try:
                target = Decimal(str(limits["target_notional_usd"]))
                expected_price = Decimal(str(paper_expected_price))
                slippage_bps = Decimal(str(limits["max_slippage_bps"]))
                minimum = Decimal(str(context["min_order_size"]))
                size_increment = Decimal(
                    str(
                        context.get(
                            "size_increment",
                            context.get("quantity_step", "0.01"),
                        )
                    )
                )
                documented_min_notional = Decimal(
                    str(
                        context.get(
                            "min_notional",
                            context.get("minimum_notional", "0"),
                        )
                    )
                )
            except Exception:
                block("INVALID_CANARY_PARAMETERS")
            allowed_ticks = {
                Decimal("0.1"),
                Decimal("0.01"),
                Decimal("0.005"),
                Decimal("0.0025"),
                Decimal("0.001"),
                Decimal("0.0001"),
            }
            if (
                not target.is_finite()
                or target <= 0
                or not expected_price.is_finite()
                or expected_price <= 0
                or not slippage_bps.is_finite()
                or slippage_bps < 0
                or not best.is_finite()
                or best <= 0
                or not tick.is_finite()
                or tick not in allowed_ticks
                or not minimum.is_finite()
                or minimum < 0
                or not size_increment.is_finite()
                or size_increment <= 0
                or not documented_min_notional.is_finite()
                or documented_min_notional < 0
            ):
                block("INVALID_CANARY_PARAMETERS")
            max_price = expected_price * (
                Decimal(1) + slippage_bps / Decimal(10000)
            )
            max_price = (
                max_price / tick
            ).to_integral_value(rounding=ROUND_DOWN) * tick
            if (
                not max_price.is_finite()
                or max_price < tick
                or max_price > Decimal(1) - tick
            ):
                block("INVALID_CANARY_PARAMETERS")
            if best > max_price:
                block("SLIPPAGE_LIMIT")
            minimum_quantity = (
                minimum / size_increment
            ).to_integral_value(rounding=ROUND_UP) * size_increment
            if documented_min_notional > 0:
                minimum_quantity = max(
                    minimum_quantity,
                    (
                        documented_min_notional / max_price / size_increment
                    ).to_integral_value(rounding=ROUND_UP)
                    * size_increment,
                )
            quantity = minimum_quantity
            if quantity <= 0:
                block("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
            notional = quantity * max_price
            if notional > target:
                block("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
            return target, max_price, quantity, notional, tick

        def prepare_order(
            limits: Mapping[str, Any],
        ) -> tuple[
            Mapping[str, Any],
            Mapping[str, Any],
            str,
            Decimal,
            Decimal,
            Decimal,
            Decimal,
            Decimal,
            dict[str, Any],
        ]:
            geo = dict(venue.geoblock())
            if geo.get("blocked") or geo.get("close_only"):
                block("GEOGRAPHICALLY_BLOCKED")
            context = dict(venue.market_context(market_id, token_id))
            context_asset_id = context.get("asset_id")
            if is_official_venue and not context_asset_id:
                block("MARKET_OUTCOME_ID_UNAVAILABLE")
            resolved_asset_id = str(context_asset_id or token_id)
            if not context.get("accepting_orders"):
                block("MARKET_NOT_ACCEPTING_ORDERS")
            asks = context.get("asks") or []
            if side.upper() != "BUY":
                block("NO_EXECUTABLE_BOOK")
            try:
                best = _best_ask_price(asks)
                tick = Decimal(str(context["tick_size"]))
            except Exception:
                block("INVALID_CANARY_PARAMETERS")
            target, max_price, quantity, notional, _ = execution_parameters(
                limits, context, best, tick
            )
            try:
                fee_rate = Decimal(
                    str(
                        context.get(
                            "fee_rate",
                            Decimal(str(context.get("fee_bps", 0)))
                            / Decimal("10000"),
                        )
                    )
                )
                fee_exponent = Decimal(str(context.get("fee_exponent", 1)))
                curve = max_price * (Decimal("1") - max_price)
                unit_fee = max(
                    fee_rate * (curve ** fee_exponent),
                    fee_rate * (Decimal("0.25") ** fee_exponent),
                )
                if quantity * (max_price + unit_fee) > target:
                    block("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
                estimated_fees = quantity * unit_fee
            except CanaryBlocked:
                raise
            except Exception:
                block("INVALID_CANARY_PARAMETERS")
            if (
                not fee_rate.is_finite()
                or fee_rate < 0
                or not fee_exponent.is_finite()
                or fee_exponent < 0
                or not estimated_fees.is_finite()
                or notional + estimated_fees > target
            ):
                block("INVALID_CANARY_PARAMETERS")
            evidence = {
                "bid": (context.get("bids") or [{}])[-1].get("price"),
                "ask": str(best),
                "depth": asks,
                "tick_size": str(tick),
                "min_order_size": str(context["min_order_size"]),
                "fee_rate": str(fee_rate),
                "fee_exponent": str(fee_exponent),
                "estimated_fees": str(estimated_fees),
                "resolved_asset_id": resolved_asset_id,
                "market_version": context.get("market_version"),
                "selected_token_id": context.get("token_id"),
                "selected_position_id": context.get("position_id"),
                "geoblock": {
                    "blocked": False,
                    "country": geo.get("country"),
                    "region": geo.get("region"),
                },
            }
            return (
                geo,
                context,
                resolved_asset_id,
                best,
                target,
                max_price,
                quantity,
                estimated_fees,
                evidence,
            )

        # All venue/network reads complete before the short writer
        # transactions below.  SQLite only fences persisted canary state.
        preflight_snapshot = self.authoritative_status()
        # Preserve persisted control-state precedence (including an expired
        # arm) before looking up the candidate-scoped signal.  The lookup
        # itself must precede the control candidate check so a signal for a
        # different candidate remains CANARY_SIGNAL_NOT_FOUND.
        enforce_controls(preflight_snapshot, state_only=True)
        stored_signal = lookup_candidate_signal()
        if preflight_snapshot.get("micro_live_canary") == AUTONOMOUS_MICRO_LIVE:
            stored_evidence = stored_signal.get("evidence", {})
            if (
                not isinstance(stored_evidence, Mapping)
                or stored_evidence.get("current_execution_evidence") != CURRENT_ORDER_BOOK
            ):
                block("CURRENT_ORDER_BOOK_REQUIRED")
        if str(self.store.polymarket_health(now=now).get("grade", "F")).upper() not in {"A", "B"}:
            block("COLLECTOR_DEGRADED")
        limits = enforce_controls(preflight_snapshot)
        # Direct callers are not allowed to manufacture a submission from
        # candidate arguments; a current persisted READY signal is mandatory.
        # Run this only after the persisted arm/kill/control and local cap gates
        # above so those established reasons retain precedence over scope freshness.
        enforce_submission_fence(required_status="READY")
        if not is_official_venue and not allow_test_venue:
            block("UNSUPPORTED_VENUE")
        if not self.credentials.configured(allow_environment=environment):
            block("CREDENTIALS_NOT_CONFIGURED")
        self.require_current_credential_binding()
        (
            geo,
            context,
            resolved_asset_id,
            best,
            target,
            max_price,
            quantity,
            estimated_fees,
            evidence,
        ) = prepare_order(limits)
        notional = quantity * max_price
        enforce_controls(
            preflight_snapshot,
            additional_exposure=estimated_fees,
        )
        # This authenticated balance read is intentionally outside every
        # SQLite writer transaction; the short reservation fence rechecks all
        # persisted control state immediately afterward.
        enforce_balance(venue.balance(), notional, estimated_fees)
        event_id = "canary-" + hashlib.sha256(signal_id.encode()).hexdigest()[:24]
        current_config_id, current_generation, current_config_hash = (
            self._settings_identity()
        )
        if (
            current_config_id != request_settings_config_id
            or current_generation != request_settings_generation
            or not current_config_hash
        ):
            block("CANARY_SETTINGS_GENERATION_CHANGED")
        try:
            reservation_control_generation = int(
                preflight_snapshot.get("control_generation") or 0
            )
        except (TypeError, ValueError):
            block("CANARY_CONTROL_CORRUPT")
        if reservation_control_generation <= 0:
            block("CANARY_CONTROL_CORRUPT")
        if not current_config_id or current_generation is None:
            block("CANARY_SETTINGS_GENERATION_CHANGED")
        try:
            self.store.reserve_canary_capacity(
                intent_id=event_id,
                reservation_id=event_id,
                event_id=event_id,
                side=side.upper(),
                requested_cost=notional,
                fee_reserve=estimated_fees,
                quantity=quantity,
                market_id=market_id,
                limits=self._active_settings_values(),
                config_id=current_config_id,
                config_generation=current_generation,
                config_hash=current_config_hash,
                control_generation=reservation_control_generation,
                detail={
                    "candidate_id": candidate_id,
                    "token_id": token_id,
                    "control_generation": reservation_control_generation,
                },
                timestamp=ensure_utc(self.clock()),
            )
        except Exception as exc:
            raise CanaryBlocked("CANARY_RESERVATION_FAILED") from exc
        risk_reservation_created = True
        def release_risk_reservation(status: str = "REJECTED") -> None:
            if not risk_reservation_created:
                return
            try:
                self.store.release_canary_capacity(
                    event_id,
                    status=status,
                    timestamp=ensure_utc(self.clock()),
                )
            except Exception:
                _LOGGER.exception(
                    "canary risk reservation release failed event=%s", event_id
                )

        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                locked_snapshot = self.authoritative_status()
                reservation_control_row = connection.execute(
                    "SELECT state,candidate_id,expires_at,control_generation,"
                    "settings_config_id,settings_generation,credential_fingerprint "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                if reservation_control_row is None:
                    block("CANARY_NOT_ARMED")
                self._require_control_credential_binding(reservation_control_row)
                try:
                    reservation_control_state = str(
                        reservation_control_row["state"] or ""
                    ).upper()
                    reservation_control_candidate = (
                        str(reservation_control_row["candidate_id"]).strip()
                        if reservation_control_row["candidate_id"] is not None
                        else None
                    )
                    reservation_control_expiry = (
                        str(reservation_control_row["expires_at"])
                        if reservation_control_row["expires_at"] is not None
                        else None
                    )
                    reservation_control_generation = int(
                        reservation_control_row["control_generation"] or 0
                    )
                    reservation_settings_config_id = (
                        str(reservation_control_row["settings_config_id"] or "").strip()
                        or None
                    )
                    reservation_settings_generation = (
                        int(reservation_control_row["settings_generation"])
                        if reservation_control_row["settings_generation"] is not None
                        else None
                    )
                except (TypeError, ValueError):
                    block("CANARY_CONTROL_CORRUPT")
                if reservation_control_generation <= 0:
                    block("CANARY_CONTROL_CORRUPT")
                if reservation_control_state != str(
                    locked_snapshot.get("micro_live_canary") or ""
                ).upper():
                    block("CANARY_CONTROL_CHANGED")
                if locked_snapshot["micro_live_canary"] == "KILLED":
                    block("CANARY_KILLED")
                if locked_snapshot["micro_live_canary"] not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                    block("CANARY_NOT_ARMED")
                locked_state = str(
                    locked_snapshot.get("micro_live_canary") or ""
                ).upper()
                if locked_state == AUTONOMOUS_MICRO_LIVE:
                    control_candidate = str(
                        locked_snapshot.get("control_candidate") or ""
                    ).strip()
                    candidate_matches = (
                        bool(control_candidate)
                        and bool(str(candidate_id).strip())
                        and control_candidate == str(candidate_id).strip()
                    )
                else:
                    candidate_matches = candidate_id == locked_snapshot.get("candidate")
                if not candidate_matches:
                    block(
                        "AUTO_CANARY_CANDIDATE_NOT_SELECTED"
                        if locked_state == AUTONOMOUS_MICRO_LIVE
                        else "CANDIDATE_MISMATCH"
                    )
                expiry = locked_snapshot.get("expiry")
                if locked_snapshot["micro_live_canary"] == "ARMED":
                    try:
                        expired = (
                            not expiry
                            or ensure_utc(datetime.fromisoformat(str(expiry)))
                            <= ensure_utc(self.clock())
                        )
                    except (TypeError, ValueError):
                        block("CANARY_CONTROL_CORRUPT")
                    if expired:
                        block("CANARY_NOT_ARMED")
                if str(
                    self.store.polymarket_health(
                        now=ensure_utc(self.clock())
                    ).get("grade", "F")
                ).upper() not in {"A", "B"}:
                    block("COLLECTOR_DEGRADED")
                locked_limits = enforce_controls(
                    locked_snapshot,
                    additional_exposure=estimated_fees,
                )
                if locked_limits != limits:
                    block("CANARY_CONTROL_CORRUPT")
                control_generation = int(
                    locked_snapshot.get("control_generation") or 0
                )
                if control_generation <= 0:
                    block("CANARY_CONTROL_CORRUPT")
                stored_signal = connection.execute(
                    "SELECT status FROM canary_signals WHERE signal_id=?",
                    (signal_id,),
                ).fetchone()
                if stored_signal is not None and str(stored_signal["status"]).upper() != "READY":
                    block("DUPLICATE_SIGNAL")
                prior_unresolved = connection.execute(
                    "SELECT 1 FROM canary_ledger WHERE candidate_id=? AND market_id=? "
                    "AND UPPER(status) IN ('SUBMITTING','UNKNOWN','RECOVERY_ATTACHED') "
                    "LIMIT 1",
                    (candidate_id, market_id),
                ).fetchone()
                if prior_unresolved is not None:
                    block("DUPLICATE_SIGNAL")
                event_time = ensure_utc(self.clock())
                evidence_record = dict(evidence)
                evidence_record["control_generation"] = control_generation
                evidence_record["control_state"] = reservation_control_state
                evidence_record["control_candidate"] = reservation_control_candidate
                evidence_record["control_expiry"] = reservation_control_expiry
                connection.execute(
                    "INSERT INTO canary_ledger("
                    "event_id,signal_id,timestamp,candidate_id,venue,market_id,"
                    "token_id,side,requested_notional,paper_expected_price,max_price,"
                    "submitted_quantity,status,evidence_json,control_generation) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id,
                        signal_id,
                        event_time.isoformat(),
                        candidate_id,
                        "polymarket",
                        market_id,
                        token_id,
                        side.upper(),
                        str(notional),
                        str(paper_expected_price),
                        str(max_price),
                        str(quantity),
                        "RESERVED",
                        json.dumps(evidence_record, sort_keys=True),
                        control_generation,
                    ),
                )
                if stored_signal is not None:
                    signal_updated = connection.execute(
                        "UPDATE canary_signals SET status='SUBMITTING',"
                        "reason=NULL,updated_at=? WHERE signal_id=? AND status='READY'",
                        (event_time.isoformat(), signal_id),
                    )
                    if signal_updated.rowcount != 1:
                        block("DUPLICATE_SIGNAL")
                connection.commit()
            except sqlite3.IntegrityError as exc:
                if connection.in_transaction:
                    connection.rollback()
                release_risk_reservation()
                raise CanaryBlocked("DUPLICATE_SIGNAL") from exc
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                release_risk_reservation()
                raise
        self.publish_readiness_snapshot(reason="CANARY_RESERVATION")

        # Transition RESERVED -> SUBMITTING under a second short fence.
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                control = connection.execute(
                    "SELECT state,candidate_id,expires_at,limits_json,"
                    "integrity_hash,control_generation,credential_fingerprint "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                if control is None:
                    block("CANARY_NOT_ARMED")
                self._require_control_credential_binding(control)
                control_state = str(control["state"]).upper()
                if control_state == "KILLED":
                    block("CANARY_KILLED")
                if control_state not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                    block("CANARY_NOT_ARMED")
                if control_state == AUTONOMOUS_MICRO_LIVE:
                    # Autonomous actionable candidates are fenced by the
                    # current control row.  The research winner in
                    # ``canary_selection`` is intentionally not rewritten
                    # when a READY fallback is bound.
                    control_candidate = str(
                        control["candidate_id"] or ""
                    ).strip()
                    if (
                        not control_candidate
                        or not str(candidate_id).strip()
                        or control_candidate != str(candidate_id).strip()
                    ):
                        block("AUTO_CANARY_CANDIDATE_NOT_SELECTED")
                elif str(control["candidate_id"]) != candidate_id:
                    block("CANDIDATE_MISMATCH")
                try:
                    control_expiry = str(control["expires_at"] or "")
                    if control_state == "ARMED" and (
                        not control_expiry
                        or ensure_utc(datetime.fromisoformat(control_expiry))
                        <= ensure_utc(self.clock())
                    ):
                        block("CANARY_NOT_ARMED")
                    if control_state == AUTONOMOUS_MICRO_LIVE and control_expiry:
                        block("CANARY_CONTROL_CORRUPT")
                    control_limits = json.loads(control["limits_json"] or "{}")
                    if not isinstance(control_limits, Mapping):
                        block("CANARY_CONTROL_CORRUPT")
                    control_generation = int(control["control_generation"] or 0)
                    if (
                        reservation_control_generation is None
                        or control_generation != reservation_control_generation
                    ):
                        block("CANARY_CONTROL_CHANGED")
                    if control_state != reservation_control_state:
                        block("CANARY_CONTROL_CHANGED")
                    if (
                        str(control["candidate_id"] or "").strip()
                        != (reservation_control_candidate or "")
                    ):
                        block(
                            "AUTO_CANARY_CANDIDATE_NOT_SELECTED"
                            if control_state == AUTONOMOUS_MICRO_LIVE
                            else "CANDIDATE_MISMATCH"
                        )
                    if control_expiry != (reservation_control_expiry or ""):
                        block("CANARY_CONTROL_CHANGED")
                except (TypeError, ValueError, json.JSONDecodeError):
                    block("CANARY_CONTROL_CORRUPT")
                expected_integrity = self._integrity(
                    "" if control_state == AUTONOMOUS_MICRO_LIVE else candidate_id,
                    AUTONOMOUS_CANARY_VENUE if control_state == AUTONOMOUS_MICRO_LIVE else "polymarket",
                    "" if control_state == AUTONOMOUS_MICRO_LIVE else control_expiry,
                    control_limits,
                )
                if expected_integrity != control["integrity_hash"]:
                    block("CANARY_CONTROL_CORRUPT")
                if control_generation <= 0:
                    block("CANARY_CONTROL_CORRUPT")
                if control_state == AUTONOMOUS_MICRO_LIVE and control_limits != self.autonomous_limits():
                    block("AUTONOMOUS_RISK_ENVELOPE_CORRUPT")
                if str(
                    self.store.polymarket_health(
                        now=ensure_utc(self.clock())
                    ).get("grade", "F")
                ).upper() not in {"A", "B"}:
                    block("COLLECTOR_DEGRADED")
                fenced_snapshot = self.authoritative_status()
                if fenced_snapshot["micro_live_canary"] == "KILLED":
                    block("CANARY_KILLED")
                if fenced_snapshot["micro_live_canary"] not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                    block("CANARY_NOT_ARMED")
                fenced_limits = enforce_controls(
                    fenced_snapshot,
                    additional_exposure=estimated_fees,
                    reservation_event_id=event_id,
                )
                if fenced_limits != control_limits:
                    block("CANARY_CONTROL_CORRUPT")
                reservation = connection.execute(
                    "SELECT status FROM canary_ledger WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if reservation is None or str(reservation["status"]).upper() != "RESERVED":
                    block("CANARY_RESERVATION_FAILED")
                updated = connection.execute(
                    "UPDATE canary_ledger SET status='SUBMITTING',"
                    "control_generation=? WHERE event_id=? AND status='RESERVED'",
                    (control_generation, event_id),
                )
                if updated.rowcount != 1:
                    block("CANARY_RESERVATION_FAILED")
                connection.commit()
            except BaseException as exc:
                if connection.in_transaction:
                    connection.rollback()
                reason = (
                    str(exc)
                    if isinstance(exc, CanaryBlocked)
                    else "CANARY_RESERVATION_FAILED"
                )
                failure_at = ensure_utc(self.clock())
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE canary_ledger SET status='REJECTED' "
                        "WHERE event_id=? AND status='RESERVED'",
                        (event_id,),
                    )
                    connection.execute(
                        "UPDATE canary_signals SET status='REJECTED',reason=?,updated_at=? "
                        "WHERE signal_id=? AND status='SUBMITTING'",
                        (reason, failure_at.isoformat(), signal_id),
                    )
                    connection.commit()
                except BaseException:
                    if connection.in_transaction:
                        connection.rollback()
                    raise CanaryBlocked("CANARY_RISK_ACCOUNTING_FAILED") from exc
                release_risk_reservation()
                raise
        self.publish_readiness_snapshot(reason="CANARY_SUBMITTING")
        try:
            self.store.record_canary_submission_attempt(
                attempt_id=event_id + "-attempt",
                intent_id=event_id,
                side=side.upper(),
                attempted_at=ensure_utc(self.clock()),
                status="ATTEMPTED",
                config_id=current_config_id,
                config_generation=current_generation,
                config_hash=current_config_hash,
                control_generation=reservation_control_generation,
                detail={"market_id": market_id, "candidate_id": candidate_id},
            )
        except Exception as exc:
            # The submission-attempt ledger is itself fenced by control
            # generation. Preserve the authoritative transition reason rather
            # than collapsing kill/disarm/TOCTOU changes into reservation
            # failure.
            try:
                current_control = connection.execute(
                    "SELECT state,control_generation FROM canary_control "
                    "WHERE singleton=1"
                ).fetchone()
                current_state = (
                    str(current_control["state"] or "").upper()
                    if current_control is not None
                    else ""
                )
                current_generation = (
                    int(current_control["control_generation"] or 0)
                    if current_control is not None
                    else 0
                )
            except (sqlite3.Error, TypeError, ValueError):
                current_state = ""
                current_generation = reservation_control_generation or 0
            if current_state == "KILLED":
                reason = "CANARY_KILLED"
            elif current_state not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                reason = "CANARY_NOT_ARMED"
            elif current_generation != (reservation_control_generation or 0):
                reason = "CANARY_CONTROL_CHANGED"
            else:
                reason = "CANARY_RESERVATION_FAILED"
            failure_at = ensure_utc(self.clock())
            try:
                with self.store._lock:
                    if connection.in_transaction:
                        connection.rollback()
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE canary_ledger SET status='REJECTED' "
                        "WHERE event_id=? AND status='SUBMITTING'",
                        (event_id,),
                    )
                    connection.execute(
                        "UPDATE canary_signals SET status='REJECTED',reason=?,updated_at=? "
                        "WHERE signal_id=? AND status='SUBMITTING'",
                        (reason, failure_at.isoformat(), signal_id),
                    )
                    connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise CanaryBlocked("CANARY_RISK_ACCOUNTING_FAILED") from exc
            release_risk_reservation()
            raise CanaryBlocked(reason) from exc

        submitted_at = ensure_utc(self.clock())

        def final_submission_fence() -> None:
            """Re-read every persisted authority immediately before posting."""
            with self.store._lock:
                if connection.in_transaction:
                    raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    control = connection.execute(
                        "SELECT state,candidate_id,expires_at,limits_json,"
                        "integrity_hash,control_generation,credential_fingerprint "
                        "FROM canary_control WHERE singleton=1"
                    ).fetchone()
                    if control is None:
                        block("CANARY_NOT_ARMED")
                    self._require_control_credential_binding(control)
                    control_state = str(control["state"] or "").upper()
                    if control_state == "KILLED":
                        block("CANARY_KILLED")
                    if control_state not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                        block("CANARY_NOT_ARMED")
                    try:
                        control_generation = int(
                            control["control_generation"] or 0
                        )
                        control_candidate = str(
                            control["candidate_id"] or ""
                        ).strip()
                        control_expiry = str(control["expires_at"] or "")
                        control_limits = json.loads(control["limits_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        block("CANARY_CONTROL_CORRUPT")
                    if (
                        reservation_control_generation is None
                        or control_generation != reservation_control_generation
                    ):
                        block("CANARY_CONTROL_CHANGED")
                    if control_state != reservation_control_state:
                        block("CANARY_CONTROL_CHANGED")
                    if (
                        control_candidate != (reservation_control_candidate or "")
                        or control_candidate != str(candidate_id).strip()
                    ):
                        block(
                            "AUTO_CANARY_CANDIDATE_NOT_SELECTED"
                            if control_state == AUTONOMOUS_MICRO_LIVE
                            else "CANDIDATE_MISMATCH"
                        )
                    if control_expiry != (reservation_control_expiry or ""):
                        block("CANARY_CONTROL_CHANGED")
                    try:
                        if control_state == "ARMED" and (
                            not control_expiry
                            or ensure_utc(datetime.fromisoformat(control_expiry))
                            <= ensure_utc(self.clock())
                        ):
                            block("CANARY_NOT_ARMED")
                        if control_state == AUTONOMOUS_MICRO_LIVE and control_expiry:
                            block("CANARY_CONTROL_CORRUPT")
                        if not isinstance(control_limits, Mapping):
                            block("CANARY_CONTROL_CORRUPT")
                    except (TypeError, ValueError):
                        block("CANARY_CONTROL_CORRUPT")
                    expected_integrity = self._integrity(
                        ""
                        if control_state == AUTONOMOUS_MICRO_LIVE
                        else str(candidate_id),
                        AUTONOMOUS_CANARY_VENUE
                        if control_state == AUTONOMOUS_MICRO_LIVE
                        else "polymarket",
                        ""
                        if control_state == AUTONOMOUS_MICRO_LIVE
                        else control_expiry,
                        control_limits,
                    )
                    if expected_integrity != control["integrity_hash"]:
                        block("CANARY_CONTROL_CORRUPT")
                    if control_state == AUTONOMOUS_MICRO_LIVE:
                        if control_limits != self.autonomous_limits():
                            block("AUTONOMOUS_RISK_ENVELOPE_CORRUPT")
                    elif control_limits != limits:
                        block("CANARY_CONTROL_CORRUPT")
                    reservation = connection.execute(
                        "SELECT status,control_generation FROM canary_ledger "
                        "WHERE event_id=?",
                        (event_id,),
                    ).fetchone()
                    if (
                        reservation is None
                        or str(reservation["status"]).upper() != "SUBMITTING"
                        or int(reservation["control_generation"] or 0)
                        != reservation_control_generation
                    ):
                        block("CANARY_RESERVATION_FAILED")
                    current_signal = self.get_signal(signal_id)
                    if not isinstance(current_signal, Mapping):
                        block("CANARY_SIGNAL_NOT_FOUND")
                    try:
                        # Keep every signal/lifecycle/scope/token check in the
                        # same writer transaction as the control fence.
                        enforce_submission_fence(required_status="SUBMITTING")
                    except CanaryBlocked as exc:
                        reason = str(exc)
                        persist_submission_failure(reason)
                        connection.commit()
                        block(reason)
                    failure = current_execution_market_cap_failure(
                        current_signal,
                        at=ensure_utc(self.clock()),
                    )
                    if failure is not None:
                        reason = str(
                            failure.get("reason_code")
                            or "SCOPE_RESOLUTION_MISSING"
                        )
                        evidence = failure.get("evidence")
                        persist_submission_failure(
                            reason,
                            evidence=evidence
                            if isinstance(evidence, Mapping)
                            else None,
                        )
                        connection.commit()
                        block(reason)
                    final_snapshot = self.authoritative_status()
                    try:
                        final_health = self.store.polymarket_health(
                            now=ensure_utc(self.clock())
                        )
                    except Exception:
                        final_health = {"grade": "F"}
                    if str(final_health.get("grade", "F")).upper() not in {"A", "B"}:
                        block("COLLECTOR_DEGRADED")
                    enforce_controls(
                        final_snapshot,
                        additional_exposure=estimated_fees,
                        reservation_event_id=event_id,
                    )
                    connection.commit()
                except BaseException:
                    if connection.in_transaction:
                        connection.rollback()
                    raise

        def external_submission() -> Mapping[str, Any]:
            nonlocal network_send_started
            fresh_id, fresh_generation, _ = self._settings_identity()
            if (
                fresh_id != request_settings_config_id
                or fresh_generation != request_settings_generation
            ):
                block("CANARY_SETTINGS_GENERATION_CHANGED")
            if is_official_venue:
                return submit_official_order(
                    market_version=context.get("market_version"),
                    neg_risk=context.get("neg_risk"),
                    asset_id=resolved_asset_id,
                    side=side.upper(),
                    price=max_price,
                    size=quantity,
                )
            # Test/dry-run venues use the same final durable fence as the
            # official sink. ``allow_test_venue`` changes transport identity,
            # never the control, signal, scope, risk, or expiry guarantees.
            enforce_submission_fence(required_status="SUBMITTING")
            final_submission_fence()
            fresh_id, fresh_generation, _ = self._settings_identity()
            if (
                fresh_id != request_settings_config_id
                or fresh_generation != request_settings_generation
            ):
                block("CANARY_SETTINGS_GENERATION_CHANGED")
            with transport_state_lock:
                if submission_cancelled.is_set():
                    block("CANARY_SUBMISSION_TIMEOUT")
                network_send_started = True
            return venue.submit_limit_order(
                token_id=resolved_asset_id,
                side=side.upper(),
                price=max_price,
                size=quantity,
            )
        def durable_accounting_failure(exc: BaseException) -> None:
            """Persist an unknown outcome and trip the canary kill latch."""
            reason = "CANARY_RISK_ACCOUNTING_FAILED"
            marker = {
                "accounting_status": "UNKNOWN",
                "accounting_error": type(exc).__name__,
                "reason": reason,
            }
            def merge_evidence(raw: Any) -> str:
                try:
                    payload = json.loads(raw or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    payload = {}
                if not isinstance(payload, Mapping):
                    payload = {}
                merged = dict(payload)
                merged["accounting"] = dict(marker)
                return json.dumps(merged, sort_keys=True)
            try:
                with self.store._lock:
                    if connection.in_transaction:
                        connection.rollback()
                    connection.execute("BEGIN IMMEDIATE")
                    ledger_row = connection.execute(
                        "SELECT evidence_json FROM canary_ledger WHERE event_id=?",
                        (event_id,),
                    ).fetchone()
                    event_row = connection.execute(
                        "SELECT evidence_json FROM canary_execution_events "
                        "WHERE canary_event_id=? ORDER BY execution_event_id DESC LIMIT 1",
                        (event_id,),
                    ).fetchone()
                    ledger_evidence = merge_evidence(
                        ledger_row["evidence_json"] if ledger_row is not None else None
                    )
                    event_evidence = merge_evidence(
                        event_row["evidence_json"] if event_row is not None else None
                    )
                    connection.execute(
                        "UPDATE canary_execution_events SET status='UNKNOWN',"
                        "evidence_json=? WHERE canary_event_id=?",
                        (event_evidence, event_id),
                    )
                    connection.execute(
                        "UPDATE canary_ledger SET status='UNKNOWN',"
                        "evidence_json=? WHERE event_id=?",
                        (ledger_evidence, event_id),
                    )
                    connection.execute(
                        "UPDATE canary_signals SET status='UNKNOWN',reason=?,"
                        "updated_at=? WHERE signal_id=? AND status IN "
                        "('ACCEPTED','MATCHED','PARTIALLY_FILLED','SETTLED','REJECTED','SUBMITTING')",
                        (reason, ensure_utc(self.clock()).isoformat(), signal_id),
                    )
                    connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                _LOGGER.exception(
                    "canary durable accounting marker failed event=%s",
                    event_id,
                )
            try:
                self.kill()
            except BaseException:
                _LOGGER.exception(
                    "canary accounting breaker failed event=%s",
                    event_id,
                )
            raise CanaryBlocked(reason) from exc

        def persist_outcome(
            *,
            outcome: str,
            response: Mapping[str, Any] | None = None,
            error: str | None = None,
        ) -> str:
            received_at = ensure_utc(self.clock())
            outcome = str(outcome or "").strip().upper()
            response = response or {}
            canonical_order_id = _canonical_exchange_order_id(
                response.get("order_id")
            )
            if outcome not in {"REJECTED", "UNKNOWN"} and canonical_order_id is None:
                raise CanaryBlocked("CANARY_EXTERNAL_IDENTITY_MISSING")
            def _decimal(value: Any, fallback: Any = None) -> Decimal | None:
                if value is None:
                    value = fallback
                try:
                    parsed = Decimal(str(value))
                except (TypeError, ValueError, ArithmeticError):
                    return None
                return parsed if parsed.is_finite() else None
            expected_price = _decimal(response.get("paper_expected_price"), paper_expected_price)
            actual_price = _decimal(response.get("actual_average_price"))
            actual_fees = _decimal(response.get("fees"))
            price_difference = (
                actual_price - expected_price
                if actual_price is not None and expected_price is not None
                else None
            )
            fee_difference = (
                actual_fees - estimated_fees
                if actual_fees is not None
                else None
            )
            slippage_difference = (
                ((actual_price - expected_price) / expected_price * Decimal("10000"))
                * (Decimal("1") if side.upper() == "BUY" else Decimal("-1"))
                if actual_price is not None and expected_price and expected_price > 0
                else None
            )
            latency_ms = max(
                0,
                int((received_at - submitted_at).total_seconds() * 1000),
            )
            with self.store._lock:
                if connection.in_transaction:
                    raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    control_row = connection.execute(
                        "SELECT state,control_generation FROM canary_control "
                        "WHERE singleton=1"
                    ).fetchone()
                    current_control_state = (
                        str(control_row["state"]).upper()
                        if control_row is not None
                        else "DISARMED"
                    )
                    current_generation = (
                        int(control_row["control_generation"] or 0)
                        if control_row is not None
                        else 0
                    )
                    trade_ids = response.get("trade_ids", [])
                    if not isinstance(trade_ids, (list, tuple)):
                        trade_ids = []
                    evidence_event: dict[str, Any] = {
                        "trade_ids": list(trade_ids),
                        "control_state": current_control_state,
                        "control_generation": current_generation,
                        "request_control_generation": control_generation,
                        "paper_expected_price": str(expected_price) if expected_price is not None else None,
                        "actual_average_price": str(actual_price) if actual_price is not None else None,
                        "estimated_fees": str(estimated_fees),
                        "actual_fees": str(actual_fees) if actual_fees is not None else None,
                        "price_difference": str(price_difference) if price_difference is not None else None,
                        "fee_difference": str(fee_difference) if fee_difference is not None else None,
                        "slippage_difference_bps": (
                            str(slippage_difference) if slippage_difference is not None else None
                        ),
                        "latency_ms": latency_ms,
                    }
                    if error:
                        evidence_event["error"] = error
                    connection.execute(
                        "INSERT INTO canary_execution_events("
                        "execution_event_id,canary_event_id,timestamp,exchange_order_id,"
                        "status,fill_quantity,actual_average_price,fees,latency_ms,"
                        "evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            event_id + "-submitted",
                            event_id,
                            received_at.isoformat(),
                            canonical_order_id,
                            outcome,
                            None,
                            str(response.get("actual_average_price"))
                            if response.get("actual_average_price") is not None
                            else None,
                            str(response.get("fees"))
                            if response.get("fees") is not None
                            else None,
                            latency_ms,
                            json.dumps(evidence_event, sort_keys=True),
                        ),
                    )
                    connection.execute(
                        "UPDATE canary_ledger SET status=?,exchange_order_id=?,"
                        "fill_quantity=?,actual_average_price=?,fees=?,latency_ms=?,"
                        "price_difference=?,fee_difference=?,slippage_difference=? WHERE "
                        "event_id=? AND status='SUBMITTING'",
                        (
                            outcome,
                            canonical_order_id,
                            None,
                            str(actual_price) if actual_price is not None else None,
                            str(actual_fees) if actual_fees is not None else None,
                            latency_ms,
                            str(price_difference) if price_difference is not None else None,
                            str(fee_difference) if fee_difference is not None else None,
                            str(slippage_difference) if slippage_difference is not None else None,
                            event_id,
                        ),
                    )
                    connection.execute(
                        "UPDATE canary_signals SET status=?,reason=?,updated_at=? "
                        "WHERE signal_id=? AND status='SUBMITTING'",
                        (outcome, error, received_at.isoformat(), signal_id),
                    )
                    connection.commit()
                except BaseException:
                    if connection.in_transaction:
                        connection.rollback()
                    raise
            if risk_reservation_created and outcome == "REJECTED":
                try:
                    self.store.release_canary_capacity(
                        event_id,
                        status="REJECTED",
                        timestamp=received_at,
                    )
                except Exception as exc:
                    durable_accounting_failure(exc)
            self.publish_readiness_snapshot(reason="CANARY_EXECUTION_OUTCOME")
            return current_control_state

        response: Mapping[str, Any] | None = None
        outcome: str | None = None
        outcome_error: str | None = None
        try:
            response = _call_with_timeout(
                external_submission,
                CANARY_SUBMISSION_TIMEOUT_SECONDS,
            )
            if not isinstance(response, Mapping):
                raise RuntimeError("external submission response is not a mapping")
            response = dict(response)
            ok_value = response.get("ok", _UNSET)
            status_value = response.get("status", _UNSET)
            code_value = response.get(
                "code",
                response.get("error_code", _UNSET),
            )
            malformed = (
                not isinstance(ok_value, bool)
                or (
                    status_value is not _UNSET
                    and status_value is not None
                    and not isinstance(status_value, str)
                )
                or (
                    code_value is not _UNSET
                    and code_value is not None
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
            canonical_order_id = _canonical_exchange_order_id(raw_order_id)
            response["order_id"] = canonical_order_id
            if malformed:
                outcome = "UNKNOWN"
                outcome_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
            elif ok_value is True:
                if status_text in _CANARY_REJECTION_STATUSES:
                    outcome = "REJECTED"
                elif canonical_order_id is None:
                    outcome = "UNKNOWN"
                    outcome_error = "CANARY_EXTERNAL_IDENTITY_MISSING"
                elif (
                    not status_text
                    or status_text not in _CANARY_ACCEPTED_STATUSES
                ):
                    outcome = "UNKNOWN"
                    outcome_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
                elif status_text in {"SETTLED", "RESOLVED"}:
                    outcome = status_text
                elif status_text in {"MATCHED", "FILLED"}:
                    outcome = "MATCHED"
                elif status_text in {"PARTIAL", "PARTIALLY_FILLED"}:
                    outcome = "PARTIALLY_FILLED"
                else:
                    outcome = "ACCEPTED"
            elif (
                status_text in _CANARY_REJECTION_STATUSES
                or _is_explicit_rejection_code(code_value)
            ):
                outcome = "REJECTED"
            else:
                outcome = "UNKNOWN"
                outcome_error = "CANARY_SUBMISSION_RESPONSE_INVALID"
            control_state = persist_outcome(
                outcome=outcome,
                response=response,
                error=outcome_error,
            )
        except CanaryBlocked as exc:
            code = str(exc)
            if code == "CANARY_RISK_ACCOUNTING_FAILED":
                raise
            pre_submission_rejection = (not network_send_started) or (
                code in {
                    "CANARY_ALLOWANCE_INSUFFICIENT",
                    "CANARY_ALLOWANCE_UNAVAILABLE",
                    "CANARY_SPENDER_UNAVAILABLE",
                    "INSUFFICIENT_BALANCE",
                    "CREDENTIALS_NOT_CONFIGURED",
                    "CLOB_API_CREDENTIALS_NOT_CONFIGURED",
                    "CLOB_API_CREDENTIALS_INVALID",
                    "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED",
                    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE",
                    "UNSUPPORTED_POLYMARKET_SDK",
                    "CANARY_CONTROL_CHANGED",
                    "CANARY_CONTROL_CORRUPT",
                    "CANARY_SETTINGS_GENERATION_CHANGED",
                    "AUTONOMOUS_RISK_ENVELOPE_CORRUPT",
                    "CANARY_KILLED",
                    "CANARY_RESERVATION_FAILED",
                    "COLLECTOR_DEGRADED",
                    "DAILY_LOSS_LIMIT",
                    "DAILY_ORDER_LIMIT",
                    "OPEN_POSITION_LIMIT",
                    "EXPOSURE_LIMIT",
                    "AUTONOMOUS_TARGET_LIMIT",
                    "EXECUTION_FEASIBILITY_MARKET_CAP",
                    "CANARY_NOT_ARMED",
                    "AUTO_CANARY_CANDIDATE_NOT_SELECTED",
                    "RESEARCH_ONLY",
                    "INVALID_POLICY",
                    "DEFERRED_MARKETS",
                    "CURRENT_ORDER_BOOK_REQUIRED",
                }
                or code.startswith("CANARY_SIGNAL_")
                or code.startswith("CANDIDATE_")
                or code.startswith("SCOPE_RESOLUTION_")
            )
            if pre_submission_rejection:
                control_state = persist_outcome(
                    outcome="REJECTED",
                    response={
                        "ok": False,
                        "status": "REJECTED",
                        "code": code,
                        "message": code,
                    },
                    error=code,
                )
                raise
            persist_outcome(outcome="UNKNOWN", error=code)
            raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN") from exc
        except TimeoutError as exc:
            with transport_state_lock:
                submission_cancelled.set()
                transport_started_before_timeout = network_send_started
            if not transport_started_before_timeout:
                control_state = persist_outcome(
                    outcome="REJECTED",
                    response={
                        "ok": False,
                        "status": "REJECTED",
                        "code": "CANARY_SUBMISSION_TIMEOUT",
                        "message": "CANARY_SUBMISSION_TIMEOUT",
                    },
                    error=str(exc),
                )
                raise CanaryBlocked("CANARY_SUBMISSION_TIMEOUT") from exc
            persist_outcome(outcome="UNKNOWN", error=str(exc))
            raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN") from exc
        except BaseException as exc:
            persist_outcome(outcome="UNKNOWN", error=type(exc).__name__)
            raise
        if outcome == "UNKNOWN":
            raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN")
        return {
            "event_id": event_id,
            "status": response.get("status") if response is not None else outcome,
            "execution_status": outcome,
            "canary_state": control_state,
            "requested_notional": str(notional),
            "production_live_execution": False,
        }
    def submit_position_order(
        self,
        *,
        market_version: Any,
        neg_risk: Any,
        asset_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        before_post: Callable[[], None],
        on_send_started: Callable[[], None],
    ) -> Mapping[str, Any]:
        """Submit one already-fenced owned-position order through the SDK.

        Position management performs ownership, settings, control, price, and
        reservation fences.  This method is the shared signer/allowance/post
        boundary for those exits; it never invokes SDK approval or credential
        bootstrap flows.
        """
        _deny_isolated_real_transport()
        try:
            values = self.credentials.load(
                allow_environment=self.allow_environment
            )
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED") from exc
        if (
            not isinstance(values, Mapping)
            or not all(values.get(name) for name in _MANDATORY_SECRET_NAMES)
        ):
            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
        normalized_values = {
            name: _credential_value(values, name)
            for name in _SECRET_NAMES
        }
        current_fingerprint = credential_fingerprint(normalized_values)
        control_row = self.store.connection.execute(
            "SELECT credential_fingerprint FROM canary_control WHERE singleton=1"
        ).fetchone()
        self._require_control_credential_binding(control_row, current_fingerprint)
        client: Any = None
        try:
            client = _build_safe_sdk_client(values)
            try:
                signed = client.create_limit_order(
                    asset_id=str(asset_id),
                    side=str(side).upper(),
                    price=str(price),
                    size=str(size),
                )
            except TypeError as exc:
                raise CanaryBlocked(
                    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE"
                ) from exc
            required = _base_units(
                _sdk_value(signed, "maker_amount", None),
                field="signed maker_amount",
            )
            asset_type, allowance_asset_id = _order_balance_allowance_target(
                side=side,
                asset_id=str(asset_id),
                market_version=market_version,
            )
            spender = _resolve_official_spender(
                client,
                asset_id=str(asset_id),
                market_version=market_version,
                neg_risk=neg_risk,
            )
            request: dict[str, Any] = {"asset_type": asset_type}
            if allowance_asset_id is not None:
                request["asset_id"] = allowance_asset_id
            try:
                balance_allowance = client.get_balance_allowance(**request)
                balance, allowances = _normalize_balance_allowance(
                    balance_allowance
                )
                allowance = _allowance_for_spender(allowances, spender)
            except CanaryBlocked:
                raise
            except (TypeError, ValueError) as exc:
                raise CanaryBlocked("CANARY_ALLOWANCE_UNAVAILABLE") from exc
            if balance < required:
                raise CanaryBlocked("INSUFFICIENT_BALANCE")
            if allowance < required:
                raise CanaryBlocked("CANARY_ALLOWANCE_INSUFFICIENT")
            before_post()
            on_send_started()
            # Recheck after transitioning the timeout state and immediately
            # before the irreversible transport call.
            before_post()
            try:
                response = client.post_order(signed)
            except CanaryBlocked:
                raise
            except Exception as exc:
                raise CanaryBlocked("ORDER_SUBMISSION_FAILED") from exc
            if isinstance(response, Mapping):
                return dict(response)
            try:
                normalized: dict[str, Any] = {}
                for name in (
                    "ok",
                    "order_id",
                    "orderId",
                    "id",
                    "status",
                    "state",
                    "code",
                    "message",
                    "trade_ids",
                    "tradeIds",
                ):
                    value = _sdk_value(response, name, _UNSET)
                    if value is not _UNSET:
                        normalized[name] = value
                return normalized
            except Exception as exc:
                raise CanaryBlocked("ORDER_SUBMISSION_FAILED") from exc
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("ORDER_SUBMISSION_FAILED") from exc
        finally:
            if client is not None:
                close = getattr(client, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass

    def submit_exit(
        self,
        position_id: str,
        venue: CanaryVenue,
        *,
        expected_generation: int,
        config_id: str,
        allow_test_venue: bool = False,
    ) -> Mapping[str, Any]:
        from . import canary_positions

        return canary_positions.submit_exit(
            self,
            position_id,
            venue,
            expected_generation=expected_generation,
            config_id=config_id,
            allow_test_venue=allow_test_venue,
        )

    def reconcile_pending(
        self,
        venue: CanaryVenue,
        *,
        allow_test_venue: bool = False,
    ) -> Mapping[str, Any]:
        from . import canary_positions

        return canary_positions.reconcile_pending(
            self,
            venue,
            allow_test_venue=allow_test_venue,
        )

    def manage_positions(
        self,
        venue: CanaryVenue,
        *,
        allow_test_venue: bool = False,
    ) -> Mapping[str, Any]:
        from . import canary_positions

        return canary_positions.manage_positions(
            self,
            venue,
            allow_test_venue=allow_test_venue,
        )
from .canary_positions import (
    RECOVERY_ACTION,
    RECOVERY_ATTACHED,
    RECOVERY_CONFIRMATION,
    UNKNOWN_ENTRY_STATUSES,
    RecoveryProfileError,
    decimal_value,
    mapping_value,
    normalize_recovery_profile,
    recovery_identifier,
    response_market,
    response_order_id,
    response_price,
    response_quantity,
    response_token,
    response_side,
    response_status,
    response_timestamp,
    timestamp_value,
    trade_order_id,
    trade_price,
    trade_quantity,
    trade_side,
    trade_timestamp,
    trade_token,
)
__all__=["AUTONOMOUS_MICRO_LIVE","AUTONOMOUS_CANARY_VENUE","AUTONOMOUS_CANARY_LIMITS","CANARY_EXECUTION_MARKET_CAP","CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS","CANARY_SIGNAL_TRANSIENT_RETENTION","EXECUTION_FEASIBILITY_MARKET_CAP","RECOVERY_ACTION","RECOVERY_ATTACHED","RECOVERY_CONFIRMATION","CanaryBlocked","CanaryLimits","CanaryService","CanaryVenue","CredentialStore","PolymarketClobV2Venue","PRODUCTION_LIVE_EXECUTION","credential_fingerprint"]
