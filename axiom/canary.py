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
import os
import sqlite3
from typing import Any, Mapping, Protocol
from urllib.request import Request, urlopen

from .domain import ensure_utc, utc_now
from .storage import AxiomStore

SUPPORTED_POLYMARKET_SDK = "0.9"
_OFFICIAL_GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
PRODUCTION_LIVE_EXECUTION = False
DEFAULT_TARGET_NOTIONAL_USD = Decimal("1.00")
DEFAULT_MAX_EXPOSURE_USD = Decimal("5.00")
DEFAULT_DAILY_LOSS_USD = Decimal("2.00")
DEFAULT_MAX_OPEN_POSITIONS = 3
DEFAULT_MAX_ORDERS_PER_DAY = 5
DEFAULT_MAX_SLIPPAGE_BPS = 100
_CANARY_ELIGIBLE_STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
_MANDATORY_SECRET_NAMES = ("private_key", "wallet_address")
_OPTIONAL_SECRET_NAMES = ("relayer_api_key", "relayer_api_key_address")
_SECRET_NAMES = _MANDATORY_SECRET_NAMES + _OPTIONAL_SECRET_NAMES
_ENV_NAMES = {
    "private_key": "POLYMARKET_PRIVATE_KEY",
    "wallet_address": "POLYMARKET_WALLET_ADDRESS",
    "relayer_api_key": "POLYMARKET_RELAYER_API_KEY",
    "relayer_api_key_address": "POLYMARKET_RELAYER_API_KEY_ADDRESS",
}


 

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
    if operation not in {"account", "balance", "market_context"}:
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
        if operation not in {"account", "balance", "market_context"}:
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


def _canary_eligibility_is_bound(
    store: AxiomStore,
    candidate_id: str,
    eligibility: Mapping[str, Any] | None,
) -> bool:
    """Verify eligibility and its frozen hash still bind to the lifecycle."""
    if eligibility is None:
        return False
    try:
        eligibility = dict(eligibility)
    except (TypeError, ValueError):
        return False
    frozen_hash = eligibility.get("frozen_hash")
    if not isinstance(frozen_hash, str) or not frozen_hash:
        return False
    try:
        evidence = json.loads(str(eligibility.get("evidence_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(evidence, Mapping) or evidence.get("frozen_hash") != frozen_hash:
        return False
    try:
        record = store.load_candidate_lifecycle(candidate_id)
    except Exception:
        return False
    payload = _canary_merged_lifecycle_payload(record)
    return (
        _canary_lifecycle_frozen_hash(store, record) == frozen_hash
        and payload is not None
        and dict(evidence) == payload
    )


class CanaryService:
    def __init__(
        self,
        store: AxiomStore,
        *,
        credentials: CredentialStore | None = None,
        clock=utc_now,
        allow_environment: bool = False,
    ) -> None:
        self.store = store
        self.credentials = credentials or CredentialStore()
        self.clock = clock
        self.allow_environment = bool(allow_environment)
        self._initialize()



    def _initialize(self) -> None:
        with self.store.connection:
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
            CREATE INDEX IF NOT EXISTS idx_canary_ledger_time ON canary_ledger(timestamp, event_id);
            """)

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

    def _eligibility_is_bound(self, candidate_id: str, eligibility: Mapping[str, Any] | None) -> bool:
        return _canary_eligibility_is_bound(self.store, candidate_id, eligibility)

    def mark_eligible(self, candidate_id: str) -> None:
        record = self.store.load_candidate_lifecycle(candidate_id)
        if not record or record.get("stage") not in _CANARY_ELIGIBLE_STAGES:
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        payload = self._merged_lifecycle_payload(record)
        if payload is None:
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")

        required_true = (
            "schema_validated",
            "historical_backtest_passed",
            "validation_passed",
            "robustness_passed",
            "data_quality_passed",
        )
        if (
            any(payload.get(name) is not True for name in required_true)
            or payload.get("holdout_used") is not False
            or payload.get("frozen") is not True
            or bool(payload.get("critical_error"))
        ):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")

        frozen_hash = self._lifecycle_frozen_hash(record)
        if frozen_hash is None:
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")

        # Keep the persisted values verbatim: eligibility is a binding, not a
        # re-derived summary that can silently lose additive forward evidence.
        evidence = dict(payload)
        evidence["frozen_hash"] = frozen_hash
        try:
            evidence_json = json.dumps(evidence, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE") from None
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                "VALUES(?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET "
                "eligible_at=excluded.eligible_at,frozen_hash=excluded.frozen_hash,"
                "evidence_json=excluded.evidence_json",
                (
                    candidate_id,
                    ensure_utc(self.clock()).isoformat(),
                    frozen_hash,
                    evidence_json,
                ),
            )

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
                "SELECT state FROM canary_control WHERE singleton=1"
            ).fetchone()
            if (
                existing_control is not None
                and str(existing_control["state"]).upper() == "KILLED"
            ):
                raise CanaryBlocked("CANARY_KILLED")
            limits = limits or CanaryLimits(
                target_notional_usd=Decimal(target_notional_usd)
            )
            eligible = connection.execute(
                "SELECT candidate_id,frozen_hash,evidence_json "
                "FROM canary_eligibility WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if not self._eligibility_is_bound(candidate_id, eligible):
                raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
            health = self.store.polymarket_health(now=now)
            if str(health.get("grade", "F")).upper() not in {"A", "B"}:
                raise CanaryBlocked("COLLECTOR_DEGRADED")
            # A caller-provided assertion must never substitute for checking
            # the credentials actually held by this service.
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
                    "SELECT state FROM canary_control WHERE singleton=1"
                ).fetchone()
                if (
                    current is not None
                    and str(current["state"]).upper() == "KILLED"
                ):
                    raise CanaryBlocked("CANARY_KILLED")
                connection.execute(
                    "INSERT INTO canary_control "
                    "VALUES(1,'ARMED',?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "state=excluded.state,candidate_id=excluded.candidate_id,"
                    "venue=excluded.venue,armed_at=excluded.armed_at,"
                    "expires_at=excluded.expires_at,limits_json=excluded.limits_json,"
                    "integrity_hash=excluded.integrity_hash,"
                    "updated_at=excluded.updated_at",
                    (
                        candidate_id,
                        "polymarket",
                        now.isoformat(),
                        expires.isoformat(),
                        json.dumps(values, sort_keys=True),
                        digest,
                        now.isoformat(),
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.status()


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
                    "SELECT state FROM canary_control WHERE singleton=1"
                ).fetchone()
                if current is not None and str(current["state"]).upper() == "KILLED":
                    # KILLED is a terminal latch; disarm must not make it
                    # possible to arm again.
                    return
                connection.execute(
                    "INSERT INTO canary_control(singleton,state,limits_json,integrity_hash,updated_at) "
                    "VALUES(1,?,'{}','',?) ON CONFLICT(singleton) DO UPDATE SET "
                    "state=excluded.state,updated_at=excluded.updated_at",
                    (state, now),
                )

    def status(self) -> dict[str, Any]:
        row=self.store.connection.execute("SELECT * FROM canary_control WHERE singleton=1").fetchone()
        now=ensure_utc(self.clock())
        if row is None:
            return {"production_live_trading":"DISABLED","micro_live_canary":"DISARMED","candidate":None,"venue":None,"expiry":None,"today_orders":0,"today_realized_pnl":0.0,"total_exposure":0.0,"open_positions":0,"daily_loss_budget_remaining":float(DEFAULT_DAILY_LOSS_USD),"limits":self._limits_record(CanaryLimits()),"live_execution":False,"trades":[]}
        data=dict(row); state=data["state"]
        limits: dict[str, Any] = {}
        try:
            parsed_limits = json.loads(data.get("limits_json") or "{}")
            if not isinstance(parsed_limits, Mapping):
                raise ValueError("canary limits must be a mapping")
            limits = dict(parsed_limits)
        except (TypeError, ValueError, json.JSONDecodeError):
            state = "KILLED"
        if state=="ARMED":
            expected=self._integrity(str(data.get("candidate_id") or ""),str(data.get("venue") or ""),str(data.get("expires_at") or ""),limits)
            eligible=self.store.connection.execute("SELECT candidate_id,frozen_hash,evidence_json FROM canary_eligibility WHERE candidate_id=?",(data.get("candidate_id"),)).fetchone()
            if not self._eligibility_is_bound(str(data.get("candidate_id") or ""), eligible) or expected!=data.get("integrity_hash"):
                state="KILLED"
            elif not data.get("expires_at"):
                state="DISARMED"
            else:
                try:
                    expired = ensure_utc(datetime.fromisoformat(data["expires_at"]))<=now
                except (TypeError, ValueError):
                    state = "KILLED"
                else:
                    if expired:
                        state="DISARMED"
        start=now.replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        trades=[dict(x) for x in self.store.connection.execute("SELECT l.timestamp,l.candidate_id,l.market_id,l.side,l.requested_notional,l.paper_expected_price,e.actual_average_price,CASE WHEN e.actual_average_price IS NOT NULL THEN CAST(e.actual_average_price AS REAL)-CAST(l.paper_expected_price AS REAL) END price_difference,COALESCE(e.status,l.status) status,l.realized_pnl FROM canary_ledger l LEFT JOIN canary_execution_events e ON e.execution_event_id=(SELECT e2.execution_event_id FROM canary_execution_events e2 WHERE e2.canary_event_id=l.event_id ORDER BY e2.timestamp DESC,e2.execution_event_id DESC LIMIT 1) ORDER BY l.timestamp DESC LIMIT 100")]
        aggregates=self.store.connection.execute("SELECT COALESCE(SUM(CASE WHEN timestamp>=? THEN 1 ELSE 0 END),0) orders, COALESCE(SUM(CASE WHEN status IN ('RESERVED','UNKNOWN','OPEN','PARTIAL','SUBMITTED') THEN CAST(requested_notional AS REAL) ELSE 0 END),0) exposure, COUNT(DISTINCT CASE WHEN status IN ('RESERVED','UNKNOWN','OPEN','PARTIAL','SUBMITTED') THEN market_id END) positions, COALESCE(SUM(CASE WHEN timestamp>=? THEN CAST(realized_pnl AS REAL) ELSE 0 END),0) pnl FROM canary_ledger WHERE timestamp>=? OR status IN ('RESERVED','UNKNOWN','OPEN','PARTIAL','SUBMITTED')",(start,start,start)).fetchone()
        return {"production_live_trading":"DISABLED","micro_live_canary":state,"candidate":data.get("candidate_id"),"venue":data.get("venue"),"expiry":data.get("expires_at"),"today_orders":int(aggregates["orders"]),"today_realized_pnl":float(aggregates["pnl"]),"total_exposure":float(aggregates["exposure"]),"open_positions":int(aggregates["positions"]),"daily_loss_budget_remaining":max(0,float(limits.get("max_daily_loss_usd",2))+float(aggregates["pnl"])),"limits":limits,"trades":trades,"live_execution":False}

    def check(
        self,
        *,
        candidate_id: str | None,
        venue: CanaryVenue | None,
        market_id: str | None = None,
        token_id: str | None = None,
        allow_environment: bool | None = None,
    ) -> dict[str, Any]:
        """Run read-only canary readiness checks and return bounded diagnostics."""
        failures: list[str] = []
        environment = (
            self.allow_environment
            if allow_environment is None
            else bool(allow_environment)
        )
        status = self.status()
        if status["micro_live_canary"] != "ARMED":
            failures.append("CANARY_NOT_ARMED")
        if candidate_id and candidate_id != status.get("candidate"):
            failures.append("CANDIDATE_NOT_ARMED")
        credentials_configured = self.credentials.configured(
            allow_environment=environment
        )
        if not credentials_configured:
            failures.append("CREDENTIALS_NOT_CONFIGURED")
        if venue is None and (
            credentials_configured or status["micro_live_canary"] == "ARMED"
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
            "market": {"status": "SKIPPED"},
            "book": {"status": "SKIPPED"},
        }
        try:
            health = self.store.polymarket_health(now=ensure_utc(self.clock()))
            diagnostics["collector"] = dict(health)
            if str(health.get("grade", "F")).upper() not in {"A", "B"}:
                failures.append("COLLECTOR_DEGRADED")
        except Exception:
            diagnostics["collector"] = {"grade": "F"}
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
                    if market_id and token_id:
                        try:
                            context = dict(venue.market_context(market_id, token_id))
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
            "message": "READY FOR MICRO LIVE CANARY"
            if not failures
            else "NOT READY FOR MICRO LIVE CANARY",
            "failures": list(dict.fromkeys(failures)),
            "diagnostics": diagnostics,
            "live_execution": False,
        }

    def connectivity_check(self, **kwargs: Any) -> dict[str, Any]:
        """Strict alias for the no-order connectivity command."""
        return self.check(**kwargs)

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
        ) -> Mapping[str, Any]:
            limits = snapshot.get("limits", {})
            if snapshot["micro_live_canary"] != "ARMED":
                block("CANARY_NOT_ARMED")
            if self.store.connection.execute(
                "SELECT 1 FROM canary_ledger WHERE signal_id=?",
                (signal_id,),
            ).fetchone():
                block("DUPLICATE_SIGNAL")
            if candidate_id != snapshot.get("candidate"):
                block("CANDIDATE_MISMATCH")
            if snapshot["today_orders"] >= int(limits["max_orders_per_day"]):
                block("DAILY_ORDER_LIMIT")
            if snapshot["open_positions"] >= int(limits["max_open_positions"]):
                block("OPEN_POSITION_LIMIT")
            if snapshot["today_realized_pnl"] <= -float(limits["max_daily_loss_usd"]):
                block("DAILY_LOSS_LIMIT")
            target = Decimal(limits["target_notional_usd"])
            if (
                Decimal(str(snapshot["total_exposure"]))
                + target
                + additional_exposure
                > Decimal(limits["max_exposure_usd"])
            ):
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

        # Preflight reads provide fast rejection, but every value is repeated
        # after BEGIN IMMEDIATE before reservation or order placement.
        preflight_snapshot = self.status()
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
        enforce_balance(venue.balance(), notional, estimated_fees)
        event_id = "canary-" + hashlib.sha256(signal_id.encode()).hexdigest()[:24]

        connection = self.store.connection
        # AxiomStore serializes same-store callers with its re-entrant lock;
        # BEGIN IMMEDIATE additionally serializes writers on shared stores.
        with self.store._lock:
            if connection.in_transaction:
                raise CanaryBlocked("CANARY_TRANSACTION_ACTIVE")

            # Commit the reservation before invoking anything that can reach
            # the exchange.  A process crash in the gap must leave this row
            # durable so a retry is rejected by signal_id uniqueness.
            connection.execute("BEGIN IMMEDIATE")
            try:
                locked_snapshot = self.status()
                if str(self.store.polymarket_health(now=ensure_utc(self.clock())).get("grade", "F")).upper() not in {"A", "B"}:
                    block("COLLECTOR_DEGRADED")
                locked_limits = enforce_controls(locked_snapshot)
                (
                    _locked_geo,
                    locked_context,
                    resolved_asset_id,
                    best,
                    target,
                    max_price,
                    quantity,
                    estimated_fees,
                    evidence,
                ) = prepare_order(locked_limits)
                notional = quantity * max_price
                enforce_controls(
                    locked_snapshot,
                    additional_exposure=estimated_fees,
                )
                # Re-read account funds while holding the reservation lock;
                # the preflight value may have become stale.
                enforce_balance(venue.balance(), notional, estimated_fees)
                event_time = ensure_utc(self.clock())
                connection.execute(
                    "INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,submitted_quantity,status,evidence_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                        json.dumps(evidence, sort_keys=True),
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                if connection.in_transaction:
                    connection.rollback()
                raise CanaryBlocked("DUPLICATE_SIGNAL") from exc
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

            def persist_unknown() -> None:
                """Durably mark a submitted-but-unobserved order as unknown."""
                unknown_at = ensure_utc(self.clock())
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "UPDATE canary_ledger SET status='UNKNOWN' WHERE event_id=?",
                        (event_id,),
                    )
                    connection.execute(
                        "INSERT INTO canary_execution_events(execution_event_id,canary_event_id,timestamp,exchange_order_id,status,fill_quantity,actual_average_price,fees,latency_ms,evidence_json) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(execution_event_id) DO UPDATE SET timestamp=excluded.timestamp,status=excluded.status,evidence_json=excluded.evidence_json",
                        (
                            event_id + "-submitted",
                            event_id,
                            unknown_at.isoformat(),
                            None,
                            "UNKNOWN",
                            None,
                            None,
                            None,
                            None,
                            json.dumps(
                                {"error": "VENUE_SUBMIT_FAILED"},
                                sort_keys=True,
                            ),
                        ),
                    )
                    connection.commit()
                except BaseException:
                    if connection.in_transaction:
                        connection.rollback()
                    raise

            # Fence control state and hold the writer lock over the external
            # call.  A concurrent kill either commits before this transaction
            # (and is observed below) or waits until the sink result is durable.
            connection.execute("BEGIN IMMEDIATE")
            try:
                fenced_snapshot = self.status()
                if fenced_snapshot["micro_live_canary"] != "ARMED":
                    block("CANARY_NOT_ARMED")
                if candidate_id != fenced_snapshot.get("candidate"):
                    block("CANDIDATE_MISMATCH")
                if str(self.store.polymarket_health(now=ensure_utc(self.clock())).get("grade", "F")).upper() not in {"A", "B"}:
                    block("COLLECTOR_DEGRADED")
                reservation = connection.execute(
                    "SELECT status FROM canary_ledger WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if reservation is None or str(reservation["status"]).upper() != "RESERVED":
                    block("CANARY_RESERVATION_FAILED")

                submitted_at = ensure_utc(self.clock())
                try:
                    if is_official_venue:
                        response = submit_official_order(
                            market_version=locked_context.get("market_version"),
                            neg_risk=locked_context.get("neg_risk"),
                            asset_id=resolved_asset_id,
                            side=side.upper(),
                            price=max_price,
                            size=quantity,
                        )
                    else:
                        response = venue.submit_limit_order(
                            token_id=resolved_asset_id,
                            side=side.upper(),
                            price=max_price,
                            size=quantity,
                        )
                except BaseException:
                    # Roll back only the fence transaction.  The reservation
                    # was committed separately and therefore survives a crash.
                    if connection.in_transaction:
                        connection.rollback()
                    try:
                        persist_unknown()
                    except BaseException:
                        # Never mask the sink failure; RESERVED still blocks a
                        # duplicate retry if this best-effort update fails.
                        pass
                    raise

                received_at = ensure_utc(self.clock())
                execution_id = event_id + "-submitted"
                raw_status = str(
                    response.get("status")
                    or ("SUBMITTED" if response.get("ok") else "REJECTED")
                ).upper()
                execution_status = (
                    "OPEN"
                    if response.get("ok")
                    and raw_status
                    not in {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED", "EXPIRED"}
                    else "REJECTED"
                )
                connection.execute(
                    "INSERT INTO canary_execution_events(execution_event_id,canary_event_id,timestamp,exchange_order_id,status,fill_quantity,actual_average_price,fees,latency_ms,evidence_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        execution_id,
                        event_id,
                        received_at.isoformat(),
                        response.get("order_id"),
                        execution_status,
                        str(response.get("fill_quantity"))
                        if response.get("fill_quantity") is not None
                        else None,
                        str(response.get("actual_average_price"))
                        if response.get("actual_average_price") is not None
                        else None,
                        str(response.get("fees"))
                        if response.get("fees") is not None
                        else None,
                        max(
                            0,
                            int((received_at - submitted_at).total_seconds() * 1000),
                        ),
                        json.dumps(
                            {"trade_ids": response.get("trade_ids", [])},
                            sort_keys=True,
                        ),
                    ),
                )
                connection.execute(
                    "UPDATE canary_ledger SET status=? WHERE event_id=?",
                    (execution_status, event_id),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return {
            "event_id": event_id,
            "status": response.get("status"),
            "requested_notional": str(notional),
            "production_live_execution": False,
        }

__all__=["CanaryBlocked","CanaryLimits","CanaryService","CanaryVenue","CredentialStore","PolymarketClobV2Venue","PRODUCTION_LIVE_EXECUTION"]
