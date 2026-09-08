"""Fail-closed micro-live Polymarket canary controls.

This module is deliberately separate from paper execution. It never enables the
platform's production-live flag. The official Polymarket venue is read-only;
only the gated canary service owns order-capable SDK construction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
import getpass
import hashlib
import importlib.metadata
import json
import logging
import math
import sqlite3
import queue
import threading
import time
from urllib.request import Request, urlopen
from typing import Any, Mapping, Protocol

from .domain import ensure_utc, parse_timestamp, utc_now
from .storage import AxiomStore, SQLiteBusyTimeout, sqlite_retry
from .data_quality import (
    CURRENT_ORDER_BOOK,
    CURRENT_ORDER_BOOK_REQUIRED,
    evaluate_prediction_data_quality,
    persisted_quality_fields,
)

_LOGGER = logging.getLogger(__name__)

SUPPORTED_POLYMARKET_SDK = "0.9"
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
CANARY_SUBMISSION_TIMEOUT_SECONDS = 15.0
CANARY_SIGNAL_TTL_SECONDS = 60.0
CANARY_SIGNAL_MAX_AGE_SECONDS = 60.0
CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS = 60.0
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
        if parsed_external_version is not None and parsed_external_version > 0:
            raw_version = projection_version
    if isinstance(raw_version, bool):
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
_SECRET_NAMES = _MANDATORY_SECRET_NAMES + _OPTIONAL_SECRET_NAMES
_ENV_NAMES = {
    "private_key": "POLYMARKET_PRIVATE_KEY",
    "wallet_address": "POLYMARKET_WALLET_ADDRESS",
    "relayer_api_key": "POLYMARKET_RELAYER_API_KEY",
    "relayer_api_key_address": "POLYMARKET_RELAYER_API_KEY_ADDRESS",
}
_SAFE_PROBE_TTL_SECONDS = 30.0
_SAFE_PROBE_LOCK = threading.Lock()
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
) -> Any:
    """Construct one local read client and return normalized diagnostics only."""
    if operation not in {"account", "balance", "allowance", "market_context"}:
        raise ValueError(f"unsupported read operation: {operation}")
    try:
        import polymarket
        from polymarket import SecureClient
    except ImportError as exc:
        raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED") from exc
    version = PolymarketClobV2Venue.installed_sdk_version()
    parts = str(version or "").split(".")
    if len(parts) < 2 or parts[0] != "0" or parts[1] != "9":
        raise CanaryBlocked("UNSUPPORTED_POLYMARKET_SDK")
    safe_create = getattr(SecureClient, "_create", None)
    if not callable(safe_create):
        raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE")
    # Official v0.9 ``SecureClient.create`` calls ``_ensure_wallet_ready``,
    # which can deploy a wallet or invoke relayer workflows.  The canary uses
    # private ``_create`` only after this compatibility guard to avoid that
    # side effect during read-only connectivity.
    try:
        # A signer-backed CLOB client does not need relayer/gasless credentials.
        client = safe_create(
            private_key=values["private_key"],
            wallet=values["wallet_address"],
            validate_credentials=True,
        )
    except CanaryBlocked:
        raise
    except TypeError as exc:
        raise CanaryBlocked(
            "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE"
        ) from exc
    except Exception as exc:
        raise CanaryBlocked("AUTHENTICATED_CONNECTIVITY_FAILED") from exc

    def close_client() -> None:
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                # Diagnostics must not be replaced by a transport cleanup
                # failure, and lightweight SDK test doubles need not expose
                # close().
                pass

    if operation == "account":
        try:
            return {
                "authenticated": bool(getattr(client, "wallet", None)),
                "wallet_type": str(getattr(client, "wallet_type", "") or ""),
            }
        finally:
            close_client()
    if operation == "balance":
        try:
            value = client.get_balance_allowance(asset_type="COLLATERAL")
            raw_balance = _sdk_value(value, "balance", 0)
            if isinstance(raw_balance, bool):
                raise ValueError("balance must not be bool")
            return Decimal(str(raw_balance)) / Decimal("1000000")
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
                "balance_base_units": str(balance),
                "spenders": sorted(allowances),
            }
        except CanaryBlocked:
            raise
        except Exception as exc:
            raise CanaryBlocked("BALANCE_RESPONSE_INVALID") from exc
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
        neg_risk = _sdk_value(
            state,
            "neg_risk",
            _sdk_value(market, "neg_risk"),
        )
        fee_rate = _sdk_value(fee_schedule, "rate")
        fee_bps = (
            Decimal(str(fee_rate)) * Decimal("10000")
            if fee_rate is not None
            else Decimal(str(_sdk_value(market, "fee_bps", 0) or 0))
        )
        allowance = {
            "status": "OK",
            "asset_type": "COLLATERAL",
            "spender": _resolve_official_spender(
                client,
                asset_id=asset_id,
                market_version=_sdk_value(market, "version", ""),
                neg_risk=neg_risk,
            ),
        }
        allowance_value = client.get_balance_allowance(asset_type="COLLATERAL")
        _, allowance_map = _normalize_balance_allowance(allowance_value)
        allowance["available_base_units"] = str(
            _allowance_for_spender(allowance_map, allowance["spender"])
        )
        allowance["available_usd"] = str(
            Decimal(allowance["available_base_units"]) / Decimal("1000000")
        )
        return {
            "market_version": str(_sdk_value(market, "version", "") or ""),
            "neg_risk": neg_risk,
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
            "tick_size": str(_sdk_value(book, "tick_size", "0")),
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
            "fee_bps": str(fee_bps),
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
    def submit_limit_order(self, *, token_id: str, side: str, price: Decimal, size: Decimal) -> Mapping[str, Any]: ...

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
        for name in ("max_open_positions", "max_orders_per_day", "max_slippage_bps"):
            raw_value = getattr(self, name)
            if isinstance(raw_value, bool):
                raise ValueError(f"{name} must be a positive integer")
            try:
                value = int(raw_value)
            except (TypeError, ValueError, ArithmeticError):
                raise ValueError(f"{name} must be a positive integer") from None
            if value <= 0:
                raise ValueError(f"{name} must be a positive integer")

class CredentialStore:
    """OS-keyring first; environment variables are an explicit fallback only."""
    service = "AXIOM-POLYMARKET-CANARY"

    def configure(self, *, reader=getpass.getpass) -> None:
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
        return bool(self.load(allow_environment=allow_environment))

    def safe_projection(
        self,
        *,
        allow_environment: bool = False,
        timeout_seconds: float = 0.25,
    ) -> dict[str, Any]:
        """Return cached credential metadata without exposing values or blocking reads."""
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
    ) -> Any:
        """Run one allowlisted authenticated read with a local SDK client."""
        if operation not in {"account", "balance", "allowance", "market_context"}:
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
        )

    def geoblock(self) -> Mapping[str, Any]:
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
        "strategy_id",
        "instrument",
        "timeframe",
        "source_type",
        "dataset_id",
        "dataset_version",
        "dataset_provenance",
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
                str(name).lower().startswith("forward_")
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
        result[name] = _canary_qualification_value(value, key=name)
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
    for key, value in body.items():
        name = str(key)
        lower = name.lower()
        if name == "forward_evidence":
            if isinstance(value, Mapping):
                ranking[name] = {
                    str(item): _canary_qualification_value(raw, key=str(item))
                    for item, raw in value.items()
                    if str(item) in _CANARY_FORWARD_RANKING_FIELDS
                }
            continue
        if (
            lower.startswith("validation_")
            or lower.startswith("historical_")
            or lower.startswith("robustness_")
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
    ) -> None:
        self.store = store
        self.credentials = credentials or CredentialStore()
        self.clock = clock
        self.allow_environment = bool(allow_environment)
        if initialize:
            self._initialize()

    def _initialize(self) -> None:
        with self.store._lock, self.store.connection:
            self.store.connection.executescript("""
            CREATE TABLE IF NOT EXISTS canary_control (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1), state TEXT NOT NULL,
              candidate_id TEXT, venue TEXT, armed_at TEXT, expires_at TEXT,
              limits_json TEXT NOT NULL, integrity_hash TEXT NOT NULL, updated_at TEXT NOT NULL);
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
              settlement TEXT, realized_pnl TEXT, evidence_json TEXT NOT NULL);
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
              next_signal_scan_end_rank INTEGER
            );
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

            self.store.connection.executescript("""
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_lifecycle_insert
            AFTER INSERT ON candidate_lifecycle BEGIN
              UPDATE canary_readiness_snapshot SET
                readiness_snapshot_status='STALE',
                readiness_snapshot_stale=1,
                readiness_snapshot_reason='LIFECYCLE_CHANGED'
              WHERE singleton=1;
            END;
            CREATE TRIGGER IF NOT EXISTS canary_readiness_stale_lifecycle_update
            AFTER UPDATE ON candidate_lifecycle BEGIN
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
        limits = {
            "target_notional_usd": str(DEFAULT_TARGET_NOTIONAL_USD),
            "max_exposure_usd": str(DEFAULT_MAX_EXPOSURE_USD),
            "max_daily_loss_usd": str(DEFAULT_DAILY_LOSS_USD),
            "max_open_positions": DEFAULT_MAX_OPEN_POSITIONS,
            "max_orders_per_day": DEFAULT_MAX_ORDERS_PER_DAY,
            "max_slippage_bps": DEFAULT_MAX_SLIPPAGE_BPS,
        }
        risk = dict(AUTONOMOUS_CANARY_LIMITS)
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
            "last_signal_id": None,
            "worker_status": "UNKNOWN",
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

        control = fetchone(
            "SELECT state,candidate_id,venue,expires_at,control_generation,"
            "updated_at,limits_json FROM canary_control WHERE singleton=1"
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
                        "next_decision",
                        "blocker",
                        "last_signal_id",
                        "worker_status",
                    )
                }
            )
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
                auto.update({name: state[name] for name in fields})
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
        """Return the cheap persisted readiness projection."""
        return self.readiness_snapshot()

    def status_report(self) -> dict[str, Any]:
        """Return a bounded, read-only operator report without schema work.

        Unlike ``authoritative_status`` this method never validates candidates,
        ranks data, loads datasets, or mutates the database.  It is suitable
        for cold-start CLI/dashboard reads and reports unknown values honestly
        when durable state is absent.
        """
        connection = self.store.connection
        lock = getattr(self.store, "_lock", None)

        def fetchone(query: str, parameters: tuple[Any, ...] = ()) -> Any:
            try:
                if lock is None:
                    return connection.execute(query, parameters).fetchone()
                with lock:
                    return connection.execute(query, parameters).fetchone()
            except sqlite3.Error:
                return None

        control_row = fetchone(
            "SELECT state,candidate_id,venue,expires_at,control_generation,"
            "updated_at FROM canary_control WHERE singleton=1"
        )
        if control_row is None:
            control = {
                "state": "UNKNOWN",
                "candidate": None,
                "venue": None,
                "expires_at": None,
                "generation": None,
                "updated_at": None,
            }
        else:
            try:
                generation = int(control_row["control_generation"])
            except (TypeError, ValueError):
                generation = None
            control = {
                "state": str(control_row["state"] or "UNKNOWN").upper(),
                "candidate": control_row["candidate_id"],
                "venue": control_row["venue"],
                "expires_at": control_row["expires_at"],
                "generation": generation,
                "updated_at": control_row["updated_at"],
            }

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

        now = ensure_utc(self.clock())
        start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        aggregates = fetchone(
            "SELECT COUNT(*) AS orders,"
            "COALESCE(SUM(CAST(realized_pnl AS REAL)),0) AS realized_pnl "
            "FROM canary_ledger WHERE timestamp>=?",
            (start,),
        )
        exposure = fetchone(
            "SELECT COALESCE(SUM(CAST(requested_notional AS REAL)),0) AS exposure,"
            "COUNT(DISTINCT market_id) AS open_positions FROM canary_ledger "
            "WHERE status IN ('RESERVED','SUBMITTING','UNKNOWN','OPEN','PARTIAL','SUBMITTED')"
        )
        event_count = fetchone(
            "SELECT COUNT(*) AS n FROM canary_execution_events WHERE timestamp>=?",
            (start,),
        )
        latest = fetchone(
            "SELECT status FROM canary_ledger ORDER BY timestamp DESC,event_id DESC LIMIT 1"
        )
        execution = {
            "today_orders": int(aggregates["orders"]) if aggregates is not None else 0,
            "today_realized_pnl": (
                float(aggregates["realized_pnl"]) if aggregates is not None else 0.0
            ),
            "total_exposure": float(exposure["exposure"]) if exposure is not None else 0.0,
            "open_positions": (
                int(exposure["open_positions"]) if exposure is not None else 0
            ),
            "execution_event_count": int(event_count["n"]) if event_count is not None else 0,
            "last_request_status": (
                str(latest["status"]).upper() if latest is not None else None
            ),
        }
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
    def _integrity(candidate: str, venue: str, expires: str, limits: Mapping[str, Any]) -> str:
        return hashlib.sha256(json.dumps({"candidate": candidate, "venue": venue, "expires": expires, "limits": limits}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    def _limits_record(self, limits: CanaryLimits) -> dict[str, Any]:
        return {"target_notional_usd": str(limits.target_notional_usd), "max_exposure_usd": str(limits.max_exposure_usd), "max_daily_loss_usd": str(limits.max_daily_loss_usd), "max_open_positions": limits.max_open_positions, "max_orders_per_day": limits.max_orders_per_day, "max_slippage_bps": limits.max_slippage_bps}
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
    @staticmethod
    def autonomous_limits() -> dict[str, Any]:
        """Return the immutable operator risk envelope as a fresh mapping."""
        return dict(AUTONOMOUS_CANARY_LIMITS)

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
                "selected_actionable_candidate",
            }:
                metadata[key] = str(value) if value is not None else None
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
        self._patch_autonomous_snapshot()
        if publish:
            self.publish_readiness_snapshot(reason="AUTONOMOUS_DECISION")
    def enable_autonomous_micro_live(self) -> Mapping[str, Any]:
        """Enable the single autonomous $1 canary envelope.

        The caller is the typed operator action that has already required the
        exact ``ENABLE AUTO CANARY`` confirmation.  No target, market, token,
        leverage, or risk-limit input is accepted here.
        """
        now = ensure_utc(self.clock())
        values = self.autonomous_limits()
        digest = self._integrity("", AUTONOMOUS_CANARY_VENUE, "", values)
        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            with connection:
                current = connection.execute(
                    "SELECT state,control_generation,limits_json,integrity_hash "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                if current is not None and str(current["state"]).upper() == "KILLED":
                    raise CanaryBlocked("CANARY_KILLED")
                if current is not None and str(current["state"]).upper() == AUTONOMOUS_MICRO_LIVE:
                    try:
                        persisted = json.loads(current["limits_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise CanaryBlocked("CANARY_CONTROL_CORRUPT") from None
                    if persisted != values or current["integrity_hash"] != digest:
                        raise CanaryBlocked("CANARY_CONTROL_CORRUPT")
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
                        "limits_json,integrity_hash,updated_at,control_generation) "
                        "VALUES(1,?,?,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                        "state=excluded.state,candidate_id=excluded.candidate_id,"
                        "venue=excluded.venue,armed_at=excluded.armed_at,expires_at=excluded.expires_at,"
                        "limits_json=excluded.limits_json,integrity_hash=excluded.integrity_hash,"
                        "updated_at=excluded.updated_at,control_generation=excluded.control_generation",
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
        return {
            "candidate_id": identifier,
            "lifecycle": lifecycle,
            "payload": payload,
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
        static_probability = model_document.get(
            "probability", model_document.get("yes_probability")
        )
        field = model_document.get("field")
        if static_probability is None and not isinstance(field, str):
            return False
        for observation in observations:
            if static_probability is not None:
                value = static_probability
            elif isinstance(field, str):
                value = observation.get(field)
            else:
                value = None
            try:
                probability = float(value)
            except (TypeError, ValueError):
                return False
            if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
                return False
            observation["model_probability"] = probability
        return True

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
            bool(observation.get("closed"))
            or observation.get("active") is False
            or settlement in {"resolved_yes", "resolved_no", "void", "closed"}
        ):
            return None
        return row, observation

    def generate_signal(self, candidate_id: str) -> Mapping[str, Any] | None:
        """Evaluate one frozen candidate against the latest stored live observation.

        This method only reads persisted research/market data and writes a
        deterministic signal record.  It never constructs a venue or makes a
        network request.
        """
        now = ensure_utc(self.clock())
        try:
            binding = self._candidate_signal_binding(candidate_id)
        except CanaryBlocked:
            return None
        try:
            health_grade = str(self.store.polymarket_health(now=now).get("grade", "F")).upper()
        except Exception:
            return None
        if health_grade not in {"A", "B"}:
            return None
        payload = binding["payload"]
        forward_test = binding.get("forward_test")
        market_values: Any = (
            forward_test.get("allowed_markets")
            if isinstance(forward_test, Mapping)
            else None
        )
        if not isinstance(market_values, (list, tuple)):
            plan = payload.get("experiment_plan", {})
            market_values = plan.get("target_markets") if isinstance(plan, Mapping) else ()
        market_ids = tuple(
            dict.fromkeys(
                str(item).strip() for item in (market_values or ()) if str(item).strip()
            )
        )
        if not market_ids:
            market_ids = tuple(
                self.store.tracked_polymarket_markets(
                    active_only=True, now=now, limit=1000
                )
            )
        for market_id in sorted(market_ids):
            rows = self._forward_snapshot_rows(market_id)
            if not rows:
                continue
            current_row, current_observation = rows[-1], self._signal_observation(rows[-1])
            if current_observation is None:
                continue
            source_timestamp = parse_timestamp(current_row.get("source_timestamp"))
            if (
                source_timestamp is None
                or source_timestamp > now
                or (now - source_timestamp).total_seconds() > CANARY_SIGNAL_MAX_AGE_SECONDS
                or bool(current_observation.get("closed"))
                or current_observation.get("active") is False
                or str(current_observation.get("settlement", "open")).strip().lower()
                in {"resolved_yes", "resolved_no", "void", "closed"}
            ):
                continue
            observations: list[dict[str, Any]] = []
            for row in rows:
                observation = self._signal_observation(row)
                stamp = parse_timestamp(row.get("source_timestamp"))
                if observation is not None and stamp is not None and stamp <= now:
                    observations.append(observation)
            if not observations or not self._apply_signal_model(
                observations, binding["model_document"]
            ):
                continue
            try:
                from .strategy import evaluate_signal_record

                evaluated = evaluate_signal_record(
                    binding["strategy"], {"snapshots": tuple(observations)}
                )
                score = float(evaluated.score)
            except (TypeError, ValueError, OverflowError):
                continue
            if not math.isfinite(score) or not evaluated.actionable:
                continue
            outcome = "yes" if score > 0 else "no"
            side = "BUY"
            token_ids = current_observation.get("token_ids")
            token_id = current_observation.get(f"{outcome}_token_id")
            if isinstance(token_ids, Mapping):
                token_id = token_id or token_ids.get(outcome)
            token_id = str(token_id or "").strip()
            if not token_id:
                continue
            current_book = self._current_order_book(
                current_observation,
                outcome,
                now=now,
                token_id=token_id,
            )
            if current_book is None:
                continue
            raw_price = current_observation.get(f"{outcome}_ask")
            if raw_price is None:
                try:
                    raw_price = _best_ask_price(current_book.get("asks"))
                except (TypeError, ValueError, ArithmeticError):
                    continue
            try:
                expected_price = Decimal(str(raw_price))
            except (TypeError, ValueError, ArithmeticError):
                continue
            if not expected_price.is_finite() or not 0 < expected_price <= 1:
                continue
            source_snapshot_id = str(current_row.get("snapshot_id") or "").strip()
            if not source_snapshot_id:
                continue
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
                # The snapshot id is the durable source binding, while this
                # digest also prevents a mutable/legacy snapshot projection
                # from reusing a signal whose executable book has changed.
                "current_order_book_hash": _canary_document_hash(current_book),
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
                # A submission outcome is a terminal fence for this exact
                # evidence.  Never refresh, rewrite, or otherwise touch such
                # rows; this preserves one-submit and UNKNOWN semantics.
                protected = next(
                    (
                        row
                        for row in matching
                        if str(row["status"] or "").upper()
                        in {"SUBMITTED", "SUBMITTING", "UNKNOWN", "REJECTED"}
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
                            # CAS the stale READY row out of the actionable
                            # state before allocating replacement evidence.
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
                                str(previous["signal_id"])
                                + "|"
                                + str(previous["expires_at"] or "")
                            )
                            signal_id = (
                                base_signal_id
                                + "-refresh-"
                                + hashlib.sha256(refresh_seed.encode()).hexdigest()[:16]
                            )
                        evidence = {
                            "score": score,
                            "model_probability": current_observation.get("model_probability"),
                            "market_price": str(expected_price),
                            "research_quality": current_observation.get("research_quality"),
                            "current_execution_evidence": CURRENT_ORDER_BOOK,
                            "current_order_book_timestamp": current_book.get("timestamp"),
                            "current_order_book_source": "FORWARD_COLLECTED",
                            "source_observed_at": (
                                current_row.get("observed_at").isoformat()
                                if isinstance(current_row.get("observed_at"), datetime)
                                else current_row.get("observed_at")
                            ),
                            "source_snapshot_id": source_snapshot_id,
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
                self.store.connection.commit()
            self.publish_readiness_snapshot(reason="SIGNAL_GENERATED")
            if existing_signal is not None:
                return existing_signal
            return self.get_signal(signal_id)
        return None

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
        self, signal_id: str, status: str, *, reason: str | None = None
    ) -> None:
        now = ensure_utc(self.clock()).isoformat()
        with self.store._lock:
            if self.store.connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            self.store.connection.execute(
                "UPDATE canary_signals SET status=?,reason=?,updated_at=? "
                "WHERE signal_id=? AND status='READY'",
                (str(status).upper(), reason, now, str(signal_id)),
            )
            self.store.connection.commit()
        self.publish_readiness_snapshot(reason="SIGNAL_STATUS_CHANGED")
    def _invalidate_signal(self, signal_id: str, reason: str) -> None:
        self._set_signal_status(signal_id, "NO_LONGER_VALID", reason=reason)

    def submit_signal(
        self,
        signal_id: str,
        *,
        venue: CanaryVenue,
        allow_test_venue: bool = False,
        allow_environment: bool | None = None,
    ) -> Mapping[str, Any]:
        """Submit exactly one persisted signal after immutable revalidation."""
        signal = self.get_signal(signal_id)
        if signal is None:
            raise CanaryBlocked("CANARY_SIGNAL_NOT_FOUND")
        current_status = str(signal.get("status", "")).upper()
        if current_status != "READY":
            if current_status in {"SUBMITTED", "SUBMITTING", "UNKNOWN", "REJECTED"}:
                raise CanaryBlocked("DUPLICATE_SIGNAL")
            raise CanaryBlocked("CANARY_SIGNAL_NOT_SUBMITTABLE")
        now = ensure_utc(self.clock())
        try:
            expires_at = parse_timestamp(signal.get("expires_at"))
        except (TypeError, ValueError):
            expires_at = None
        if expires_at is None or expires_at <= now:
            self._set_signal_status(signal_id, "EXPIRED", reason="SIGNAL_EXPIRED")
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
        control_snapshot = self.authoritative_status()
        if control_snapshot.get("micro_live_canary") == AUTONOMOUS_MICRO_LIVE:
            control_candidate = str(
                control_snapshot.get("control_candidate") or ""
            ).strip()
            signal_candidate = str(signal.get("candidate_id") or "").strip()
            if not control_candidate or not signal_candidate or control_candidate != signal_candidate:
                self._invalidate_signal(signal_id, "AUTO_CANARY_CANDIDATE_NOT_SELECTED")
                raise CanaryBlocked("AUTO_CANARY_CANDIDATE_NOT_SELECTED")
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
        transaction.  The write path only checks the lifecycle snapshot and
        immutable frozen hash, then stores the already-computed qualification
        evidence.  This keeps batch ranking from taking one writer lock per
        candidate or evaluating historical quality while that lock is held.
        """
        if not entries:
            return set()
        prepared: list[dict[str, Any]] = []
        for entry in entries:
            candidate_id = str(entry.get("candidate_id") or "").strip()
            initial = entry.get("record")
            validation = entry.get("validation")
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
                    if (
                        not isinstance(current, Mapping)
                        or current.get("stage") != initial.get("stage")
                        or current.get("payload") != initial.get("payload")
                        or current.get("updated_at") != initial.get("updated_at")
                        or self._lifecycle_frozen_hash(current) != item["frozen_hash"]
                    ):
                        continue
                    connection.execute(
                        "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                        "VALUES(?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET "
                        "eligible_at=excluded.eligible_at,frozen_hash=excluded.frozen_hash,"
                        "evidence_json=excluded.evidence_json",
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
        target_notional_usd: Decimal = DEFAULT_TARGET_NOTIONAL_USD,
        expires_hours: Decimal = Decimal("24"),
        limits: CanaryLimits | None = None,
        credentials_configured: bool | None = None,
    ) -> Mapping[str, Any]:
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
            if credentials_configured is False or not self.credentials.configured(
                allow_environment=self.allow_environment
            ):
                raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
            geo = venue.geoblock()
            if geo.get("blocked") or geo.get("close_only"):
                raise CanaryBlocked("GEOGRAPHICALLY_BLOCKED")
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
                    "limits_json,integrity_hash,updated_at,control_generation) "
                    "VALUES(1,'ARMED',?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "state=excluded.state,candidate_id=excluded.candidate_id,"
                    "venue=excluded.venue,armed_at=excluded.armed_at,"
                    "expires_at=excluded.expires_at,limits_json=excluded.limits_json,"
                    "integrity_hash=excluded.integrity_hash,"
                    "updated_at=excluded.updated_at,"
                    "control_generation=excluded.control_generation",
                    (
                        candidate_id,
                        "polymarket",
                        now.isoformat(),
                        expires.isoformat(),
                        json.dumps(values, sort_keys=True),
                        digest,
                        now.isoformat(),
                        current_generation + 1,
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
                    reason = "ELIGIBILITY_INVALID"
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
        risk_envelope = self.autonomous_limits()
        autonomous_state = None
        try:
            autonomous_state = _fetchone(
                "SELECT last_tick_at,last_tick_started_at,last_tick_completed_at,"
                "last_successful_tick,last_error_code,consecutive_failures,next_retry_at,"
                "candidates_evaluated,signals_generated,orders_attempted,"
                "candidates_ranked,candidates_signal_checked,candidates_no_signal,"
                "actionable_candidates_found,selected_actionable_candidate,"
                "selected_actionable_rank,selected_actionable_score,signal_scan_cursor,"
                "signal_scan_ranking_run_id,next_signal_scan_start_rank,next_signal_scan_end_rank,"
                "next_decision,blocker,last_signal_id,worker_status "
                "FROM canary_autonomous_state WHERE singleton=1"
            )
        except sqlite3.OperationalError:
            autonomous_state = None

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
            "candidates_ranked": (
                autonomous_state["candidates_ranked"]
                if autonomous_state is not None else None
            ),
            "candidates_signal_checked": (
                autonomous_state["candidates_signal_checked"]
                if autonomous_state is not None else None
            ),
            "candidates_no_signal": (
                autonomous_state["candidates_no_signal"]
                if autonomous_state is not None else None
            ),
            "actionable_candidates_found": (
                autonomous_state["actionable_candidates_found"]
                if autonomous_state is not None else None
            ),
            "selected_actionable_candidate": (
                autonomous_state["selected_actionable_candidate"]
                if autonomous_state is not None else None
            ),
            "selected_actionable_rank": (
                autonomous_state["selected_actionable_rank"]
                if autonomous_state is not None else None
            ),
            "selected_actionable_score": (
                autonomous_state["selected_actionable_score"]
                if autonomous_state is not None else None
            ),
            "signal_scan_cursor": (
                autonomous_state["signal_scan_cursor"]
                if autonomous_state is not None else 0
            ),
            "signal_scan_ranking_run_id": (
                autonomous_state["signal_scan_ranking_run_id"]
                if autonomous_state is not None else None
            ),
            "next_signal_scan_start_rank": (
                autonomous_state["next_signal_scan_start_rank"]
                if autonomous_state is not None else None
            ),
            "next_signal_scan_end_rank": (
                autonomous_state["next_signal_scan_end_rank"]
                if autonomous_state is not None else None
            ),
            **winner_quality,
            "next_decision": (
                autonomous_state["next_decision"]
                if autonomous_state is not None
                else "ENABLE AUTO CANARY"
            ),
            "blocker": (
                autonomous_state["blocker"]
                if autonomous_state is not None
                else "AUTONOMOUS_CANARY_DISABLED"
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
            "last_tick_started_at": (
                autonomous_state["last_tick_started_at"]
                if autonomous_state is not None else None
            ),
            "last_tick_completed_at": (
                autonomous_state["last_tick_completed_at"]
                if autonomous_state is not None else None
            ),
            "last_successful_tick": (
                autonomous_state["last_successful_tick"]
                if autonomous_state is not None else None
            ),
            "last_error_code": (
                autonomous_state["last_error_code"]
                if autonomous_state is not None else None
            ),
            "consecutive_failures": (
                autonomous_state["consecutive_failures"]
                if autonomous_state is not None else None
            ),
            "next_retry_at": (
                autonomous_state["next_retry_at"]
                if autonomous_state is not None else None
            ),
            "candidates_evaluated": (
                autonomous_state["candidates_evaluated"]
                if autonomous_state is not None else None
            ),
            "signals_generated": (
                autonomous_state["signals_generated"]
                if autonomous_state is not None else None
            ),
            "orders_attempted": (
                autonomous_state["orders_attempted"]
                if autonomous_state is not None else None
            ),
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
        if row is None:
            return {
                "production_live_trading": "DISABLED",
                "micro_live_canary": "DISABLED",
                "display_state": display_state,
                "control_state": "DISABLED",
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
                "last_request_status": None,
                "today_orders": 0,
                "today_realized_pnl": 0.0,
                "total_exposure": 0.0,
                "open_positions": 0,
                "daily_loss_budget_remaining": float(DEFAULT_DAILY_LOSS_USD),
                "limits": self._limits_record(CanaryLimits()),
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
        data=dict(row); state=str(data.get("state") or "DISABLED").upper()
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
        start=now.replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        active_states = "('RESERVED','SUBMITTING','UNKNOWN','OPEN','PARTIAL','SUBMITTED')"
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
        aggregates = _optional_fetchone(
            "SELECT COALESCE(SUM(CASE WHEN timestamp>=? THEN 1 ELSE 0 END),0) orders,"
            "COALESCE(SUM(CASE WHEN status IN " + active_states
            + " THEN CAST(requested_notional AS REAL) ELSE 0 END),0) exposure,"
            "COUNT(DISTINCT CASE WHEN status IN " + active_states
            + " THEN market_id END) positions,"
            "COALESCE(SUM(CASE WHEN timestamp>=? THEN CAST(realized_pnl AS REAL) "
            "ELSE 0 END),0) pnl FROM canary_ledger WHERE timestamp>=? OR status IN "
            + active_states,
            (start, start, start),
        )
        if aggregates is None:
            aggregates = {
                "orders": 0,
                "exposure": 0,
                "positions": 0,
                "pnl": 0,
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
        auto_enabled = state == AUTONOMOUS_MICRO_LIVE
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
                if autonomous_state is not None and (auto_enabled or state == "KILLED")
                else "KILL_LATCHED" if state == "KILLED" else "ENABLE AUTO CANARY"
            ),
            "blocker": (
                autonomous_state["blocker"]
                if autonomous_state is not None and (auto_enabled or state == "KILLED")
                else "CANARY_KILLED" if state == "KILLED" else "AUTONOMOUS_CANARY_DISABLED"
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
        return {
            "production_live_trading": "DISABLED",
            "micro_live_canary": state,
            "display_state": display_state,
            "control_state": state,
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
            **scan_projection,
            "ranking_run_id": ranking_run_id,
            "ranking_timestamp": ranking_timestamp,
            "venue": data.get("venue"),
            "expiry": data.get("expires_at"),
            "control_generation": control_generation,
            "last_request_status": last_request_status,
            "today_orders": int(aggregates["orders"]),
            "today_realized_pnl": float(aggregates["pnl"]),
            "total_exposure": float(aggregates["exposure"]),
            "open_positions": int(aggregates["positions"]),
            "limits": limits,
            "risk_envelope": risk_envelope,
            "risk_limits": risk_envelope,
            "eligibility_raw_count": eligibility_raw_count,
            "eligible_count": eligible_count,
            "rankable_raw_count": rankable_raw_count,
            "rankable_count": rankable_count,
            "real_execution_events": execution_event_count,
            "execution_event_count": execution_event_count,
            **winner_quality,
            "daily_loss_budget_remaining": max(
                0,
                float(limits.get("max_daily_loss_usd", 2)) + float(aggregates["pnl"]),
            ),
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
        credentials_configured = self.credentials.configured(
            allow_environment=environment
        )
        if not credentials_configured:
            failures.append("CREDENTIALS_NOT_CONFIGURED")
        if venue is None and (
            credentials_configured
            or (
                not _connectivity_only
                and status["micro_live_canary"] in {"ARMED", AUTONOMOUS_MICRO_LIVE}
            )
        ):
            failures.append("VENUE_REQUIRED")
        diagnostics: dict[str, Any] = {
            "sdk_version": (
                venue.installed_sdk_version()
                if venue is not None and callable(getattr(venue, "installed_sdk_version", None))
                else None
            ),
            "credentials_configured": credentials_configured,
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
                                for key in ("authenticated", "wallet_type")
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
                            str(status.get("limits", {}).get("target_notional_usd", 1))
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
                                        str(status.get("limits", {}).get("target_notional_usd", 1))
                                    )
                                    available_allowance = Decimal(
                                        _base_units(
                                            allowance.get("available_base_units"),
                                            field="allowance",
                                        )
                                    )
                                    if available_allowance < target * Decimal("1000000"):
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
                            if not context.get("accepting_orders"):
                                failures.append("MARKET_NOT_ACCEPTING_ORDERS")
                            asks = context.get("asks")
                            best_ask = _best_ask_price(asks)
                            minimum = Decimal(str(context.get("min_order_size", 0)))
                            target = Decimal(
                                str(status.get("limits", {}).get("target_notional_usd", 1))
                            )
                            if minimum * best_ask > target:
                                failures.append("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
                            if diagnostics["balance"].get("status") == "OK":
                                fee_bps = Decimal(
                                    str(context.get("fee_bps", 0))
                                )
                                if not fee_bps.is_finite() or fee_bps < 0:
                                    failures.append("MARKET_CONNECTIVITY_FAILED")
                                elif available < target + (
                                    target * fee_bps / Decimal(10000)
                                ):
                                    failures.append("INSUFFICIENT_BALANCE")
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
        environment = (
            self.allow_environment
            if allow_environment is None
            else bool(allow_environment)
        )
        is_official_venue = type(venue) is PolymarketClobV2Venue

        def block(reason: str) -> None:
            raise CanaryBlocked(reason)

        def enforce_controls(
            snapshot: Mapping[str, Any],
            *,
            additional_exposure: Decimal = Decimal("0"),
            reservation_event_id: str | None = None,
        ) -> Mapping[str, Any]:
            limits = snapshot.get("limits", {})
            state = str(snapshot.get("micro_live_canary") or "DISABLED").upper()
            if state not in {"ARMED", AUTONOMOUS_MICRO_LIVE}:
                block("CANARY_NOT_ARMED")
            if state == AUTONOMOUS_MICRO_LIVE and limits != self.autonomous_limits():
                block("AUTONOMOUS_RISK_ENVELOPE_CORRUPT")
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
                if state == AUTONOMOUS_MICRO_LIVE and target > DEFAULT_TARGET_NOTIONAL_USD:
                    block("AUTONOMOUS_TARGET_LIMIT")
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


        def submit_official_order(
            *,
            market_version: Any,
            neg_risk: Any,
            asset_id: str,
            side: str,
            price: Decimal,
            size: Decimal,
        ) -> Mapping[str, Any]:
            """Sign, check allowance, and post only after reservation."""
            try:
                values = self.credentials.load(allow_environment=environment)
            except CanaryBlocked:
                raise
            except Exception as exc:
                raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED") from exc
            if (
                not isinstance(values, Mapping)
                or not all(values.get(name) for name in _MANDATORY_SECRET_NAMES)
            ):
                raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
            try:
                import polymarket
                from polymarket import SecureClient
            except ImportError as exc:
                raise CanaryBlocked(
                    "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED"
                ) from exc
            version = PolymarketClobV2Venue.installed_sdk_version()
            parts = str(version or "").split(".")
            if len(parts) < 2 or parts[0] != "0" or parts[1] != "9":
                raise CanaryBlocked("UNSUPPORTED_POLYMARKET_SDK")
            safe_create = getattr(SecureClient, "_create", None)
            if not callable(safe_create):
                raise CanaryBlocked(
                    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE"
                )
            # Official v0.9 ``SecureClient.create`` calls ``_ensure_wallet_ready``,
            # which can deploy a wallet or invoke relayer workflows.  The
            # canary uses private ``_create`` only after this compatibility
            # guard to avoid that side effect.

            # In official v0.9, ``place_limit_order`` delegates to allowance
            # recovery: an allowance rejection can reach ``approve_erc20`` or
            # ``approve_erc1155_for_all`` and the ``/balance-allowance/update``
            # endpoint.  The canary therefore splits local signing from
            # posting, checks the exact maker amount first, and never invokes
            # that recovery path.
            client: Any = None
            try:
                client = safe_create(
                    private_key=values["private_key"],
                    wallet=values["wallet_address"],
                    validate_credentials=True,
                )
                signed = client.create_limit_order(
                    asset_id=asset_id,
                    side=side.upper(),
                    price=str(price),
                    size=str(size),
                )
                try:
                    required = _base_units(
                        _sdk_value(signed, "maker_amount", None),
                        field="signed maker_amount",
                    )
                except ValueError as exc:
                    raise CanaryBlocked("CANARY_ALLOWANCE_UNAVAILABLE") from exc

                asset_type, allowance_asset_id = _order_balance_allowance_target(
                    side=side,
                    asset_id=asset_id,
                    market_version=market_version,
                )
                spender = _resolve_official_spender(
                    client,
                    asset_id=asset_id,
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
                response = client.post_order(signed)
                accepted = bool(_sdk_value(response, "ok", False))
                if accepted:
                    return {
                        "ok": True,
                        "order_id": _sdk_value(response, "order_id"),
                        "status": _sdk_value(response, "status"),
                        "trade_ids": list(
                            _sdk_value(response, "trade_ids", ()) or ()
                        ),
                    }
                return {
                    "ok": False,
                    "status": "REJECTED",
                    "error_code": _sdk_value(response, "code", "unknown"),
                    "error_message": _sdk_value(
                        response, "message", "order rejected"
                    ),
                }
            except CanaryBlocked:
                raise
            except TypeError as exc:
                raise CanaryBlocked(
                    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE"
                ) from exc
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
            except Exception:
                block("INVALID_CANARY_PARAMETERS")
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
                or tick <= 0
                or not minimum.is_finite()
                or minimum < 0
            ):
                block("INVALID_CANARY_PARAMETERS")
            max_price = expected_price * (
                Decimal(1) + slippage_bps / Decimal(10000)
            )
            max_price = (
                max_price / tick
            ).to_integral_value(rounding=ROUND_DOWN) * tick
            if not max_price.is_finite() or max_price <= 0:
                block("INVALID_CANARY_PARAMETERS")
            if best > max_price:
                block("SLIPPAGE_LIMIT")
            # Size against the worst permitted execution price, never just
            # the current ask.  This keeps the target a hard notional cap.
            quantity = (target / max_price).quantize(
                Decimal("0.01"), rounding=ROUND_DOWN
            )
            if quantity < minimum:
                block("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
            notional = quantity * max_price
            if notional > target:
                block("CANARY_TARGET_EXCEEDED")
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
                fee_bps = Decimal(str(context.get("fee_bps", 0)))
                estimated_fees = notional * fee_bps / Decimal(10000)
            except Exception:
                block("INVALID_CANARY_PARAMETERS")
            if (
                not fee_bps.is_finite()
                or fee_bps < 0
                or not estimated_fees.is_finite()
            ):
                block("INVALID_CANARY_PARAMETERS")
            evidence = {
                "bid": (context.get("bids") or [{}])[-1].get("price"),
                "ask": str(best),
                "depth": asks,
                "tick_size": str(tick),
                "min_order_size": str(context["min_order_size"]),
                "estimated_fees": str(estimated_fees),
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
        if preflight_snapshot.get("micro_live_canary") == AUTONOMOUS_MICRO_LIVE:
            stored_signal = self.get_signal(signal_id)
            stored_evidence = (
                stored_signal.get("evidence", {})
                if isinstance(stored_signal, Mapping)
                else {}
            )
            if (
                not isinstance(stored_evidence, Mapping)
                or stored_evidence.get("current_execution_evidence") != CURRENT_ORDER_BOOK
            ):
                block("CURRENT_ORDER_BOOK_REQUIRED")
        if str(self.store.polymarket_health(now=now).get("grade", "F")).upper() not in {"A", "B"}:
            block("COLLECTOR_DEGRADED")
        limits = enforce_controls(preflight_snapshot)
        if not is_official_venue and not allow_test_venue:
            block("UNSUPPORTED_VENUE")
        if not self.credentials.configured(allow_environment=environment):
            block("CREDENTIALS_NOT_CONFIGURED")
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

        connection = self.store.connection
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                locked_snapshot = self.authoritative_status()
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
                event_time = ensure_utc(self.clock())
                evidence_record = dict(evidence)
                evidence_record["control_generation"] = control_generation
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
                raise CanaryBlocked("DUPLICATE_SIGNAL") from exc
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
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
                    "integrity_hash,control_generation "
                    "FROM canary_control WHERE singleton=1"
                ).fetchone()
                if control is None:
                    block("CANARY_NOT_ARMED")
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
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        self.publish_readiness_snapshot(reason="CANARY_SUBMITTING")

        submitted_at = ensure_utc(self.clock())

        def external_submission() -> Mapping[str, Any]:
            if is_official_venue:
                return submit_official_order(
                    market_version=context.get("market_version"),
                    neg_risk=context.get("neg_risk"),
                    asset_id=resolved_asset_id,
                    side=side.upper(),
                    price=max_price,
                    size=quantity,
                )
            return venue.submit_limit_order(
                token_id=resolved_asset_id,
                side=side.upper(),
                price=max_price,
                size=quantity,
            )

        def persist_outcome(
            *,
            outcome: str,
            response: Mapping[str, Any] | None = None,
            error: str | None = None,
        ) -> str:
            received_at = ensure_utc(self.clock())
            response = response or {}
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
                            response.get("order_id"),
                            outcome,
                            str(response.get("fill_quantity"))
                            if response.get("fill_quantity") is not None
                            else None,
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
                            response.get("order_id"),
                            str(response.get("fill_quantity"))
                            if response.get("fill_quantity") is not None
                            else None,
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
            self.publish_readiness_snapshot(reason="CANARY_EXECUTION_OUTCOME")
            return current_control_state

        response: Mapping[str, Any] | None = None
        try:
            response = _call_with_timeout(
                external_submission,
                CANARY_SUBMISSION_TIMEOUT_SECONDS,
            )
            if not isinstance(response, Mapping):
                raise RuntimeError("external submission response is not a mapping")
            raw_status = str(
                response.get("status")
                or ("SUBMITTED" if response.get("ok") else "REJECTED")
            ).upper()
            outcome = (
                "SUBMITTED"
                if response.get("ok")
                and raw_status
                not in {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED", "EXPIRED"}
                else "REJECTED"
            )
            control_state = persist_outcome(outcome=outcome, response=response)
        except CanaryBlocked as exc:
            code = str(exc)
            pre_submission_rejection = code in {
                "CANARY_ALLOWANCE_INSUFFICIENT",
                "CANARY_ALLOWANCE_UNAVAILABLE",
                "CANARY_SPENDER_UNAVAILABLE",
                "INSUFFICIENT_BALANCE",
                "CREDENTIALS_NOT_CONFIGURED",
                "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED",
                "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE",
                "UNSUPPORTED_POLYMARKET_SDK",
            }
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
            persist_outcome(outcome="UNKNOWN", error=str(exc))
            raise CanaryBlocked("CANARY_SUBMISSION_UNKNOWN") from exc
        except BaseException as exc:
            persist_outcome(outcome="UNKNOWN", error=type(exc).__name__)
            raise
        return {
            "event_id": event_id,
            "status": response.get("status") if response is not None else outcome,
            "execution_status": outcome,
            "canary_state": control_state,
            "requested_notional": str(notional),
            "production_live_execution": False,
        }

__all__=["AUTONOMOUS_MICRO_LIVE","AUTONOMOUS_CANARY_VENUE","AUTONOMOUS_CANARY_LIMITS","CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS","CanaryBlocked","CanaryLimits","CanaryService","CanaryVenue","CredentialStore","PolymarketClobV2Venue","PRODUCTION_LIVE_EXECUTION"]
