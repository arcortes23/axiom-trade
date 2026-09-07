"""Offline-safe authenticated Binance Spot TESTNET gate and execution probe.

The gate is intentionally independent from the strategy/execution ledgers.  It
owns only its ``binance_testnet_*`` tables, keeps all credentials in memory,
and exposes plain redacted dictionaries to consumers.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import inspect
import json
import sqlite3
import threading
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE, BinanceRiskEnvelope, SymbolRules, size_limit_order
from .binance_spot import (
    BINANCE_SPOT_TESTNET,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
    BinanceSpotCredentials,
    BinanceSpotEnvironment,
    BinanceSpotRESTClient,
    BinanceSpotResult,
    BinanceSpotStatus,
    canonical_sha256,
    credential_fingerprint,
)

UTC = timezone.utc
try:
    PHT = ZoneInfo("Asia/Manila")
except Exception:  # pragma: no cover - supported Python ships zoneinfo data
    PHT = timezone(timedelta(hours=8))

PROBE_LABEL = "TESTNET EXECUTION PROBE"
SOURCE = "binance_testnet_gate"
FEE_RATE = Decimal("0.001")
MAX_BALANCE_ROWS = 64
MAX_JSON_TEXT = 16_384
MAX_ID_TEXT = 64
UNKNOWN_RESULT_CODES = frozenset({-1000, -1006, -1007})
AUTH_REASONS = {
    -1002: "UNAUTHORIZED",
    -1021: "TIME_SKEW",
    -1022: "SIGNATURE_REJECTED",
    -2014: "INVALID_API_KEY",
    -2015: "INVALID_API_KEY_OR_PERMISSION",
}
TERMINAL = frozenset({"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "DUST"})
_RAW_SQLITE_LOCK = threading.RLock()



def _decimal(value: Any, default: Decimal | None = None) -> Decimal:
    if value is None or value == "":
        if default is not None:
            return default
        raise ValueError("decimal value is required")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("decimal value is invalid") from exc
    if not result.is_finite():
        raise ValueError("decimal value is not finite")
    return result


def _dstr(value: Any) -> str:
    return format(_decimal(value), "f")


def _utc(value: Any = None) -> datetime:
    if isinstance(value, Mapping):
        value = value.get("utc", value.get("timestamp", value.get("time")))
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
    if value is None:
        return datetime.now(UTC)
    try:
        number = float(value)
        if abs(number) > 100_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
            return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)
        except ValueError:
            return datetime.now(UTC)


def _jsonable(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if any(token in lowered for token in ("secret", "signature", "password", "passphrase", "apikey", "api_key", "authorization")):
        return "<redacted>"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v, key=str(k)) for k, v in list(value.items())[:128]}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in list(value)[:128]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)[:MAX_JSON_TEXT]


def _json(value: Any) -> str:
    try:
        encoded = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        encoded = json.dumps({"redacted": True}, separators=(",", ":"))
    return encoded[:MAX_JSON_TEXT]


def _safe_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:MAX_ID_TEXT]


def _symbol(value: Any) -> str:
    text = str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()
    if not text:
        raise ValueError("symbol is required")
    return text


def _now_ms(clock: Callable[[], Any]) -> int:
    value = clock()
    return int(_utc(value).timestamp() * 1000)


def _timestamp_projection(value: Any) -> dict[str, str]:
    stamp = _utc(value)
    return {"utc": stamp.isoformat(), "pht": stamp.astimezone(PHT).isoformat()}


def _payload(result: Any) -> Any:
    if isinstance(result, BinanceSpotResult):
        return result.payload
    if isinstance(result, Mapping):
        # Duck-typed venues commonly return a metadata mapping around the
        # actual Binance payload.  Decode that wrapper before inspecting
        # serverTime, symbols, order status, or trade rows.
        if "payload" in result and any(
            key in result
            for key in (
                "status",
                "error_code",
                "http_status",
                "retry_after",
                "endpoint",
                "validation_only",
                "label",
            )
        ):
            return result.get("payload")
        return result
    return getattr(result, "payload", result)


def _status(result: Any) -> str:
    value = result.get("status") if isinstance(result, Mapping) else getattr(result, "status", None)
    value = getattr(value, "value", value)
    text = str(value or "").upper()
    if text in {"OK", "SUCCESS", "PASS", "ACKNOWLEDGED"}:
        status = "OK"
    elif text in {"UNKNOWN", "TIMEOUT", "DISCONNECTED"}:
        status = "UNKNOWN"
    elif text in {"RATE_LIMIT", "RATELIMIT", "429"}:
        status = "RATE_LIMIT"
    elif text in {"REJECTED", "ERROR", "FAILED", "FAILURE", "AUTH", "BLOCKED"}:
        status = "REJECTED"
    else:
        status = ""
    body = _payload(result)
    code = _error_code(result)
    if code in UNKNOWN_RESULT_CODES:
        return "UNKNOWN"
    if code is not None and code < 0:
        return "REJECTED"
    http_status = result.get("http_status") if isinstance(result, Mapping) else getattr(result, "http_status", None)
    try:
        http_status = int(http_status) if http_status is not None else None
    except (TypeError, ValueError):
        http_status = None
    if http_status in {418, 429}:
        return "RATE_LIMIT"
    if http_status is not None and http_status >= 500:
        return "UNKNOWN"
    if http_status is not None and http_status >= 400:
        return "REJECTED"
    if status:
        return status
    return "OK" if isinstance(body, Mapping) else "UNKNOWN"


def _error_code(result: Any) -> int | None:
    value = result.get("error_code") if isinstance(result, Mapping) else getattr(result, "error_code", None)
    if value is None:
        body = _payload(result)
        value = body.get("code") if isinstance(body, Mapping) else None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
def _row_value(row: Any, key: str, default: Any = None) -> Any:
    """Read a named field from mappings and sqlite rows alike."""
    if isinstance(row, Mapping):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default




def _invoke(method: Callable[..., Any], kwargs: Mapping[str, Any]) -> Any:
    """Call one venue method with only the keyword arguments it supports.

    Signature inspection is deliberately performed once, before invocation.
    A local signature mismatch is raised as ``TypeError`` rather than retried
    with a different call shape and misreported as a remote UNKNOWN result.
    """
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(**dict(kwargs))
    parameters = signature.parameters
    accepts_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())
    if accepts_kwargs:
        return method(**dict(kwargs))
    accepted = {
        key: value
        for key, value in kwargs.items()
        if key in parameters
        and parameters[key].kind
        in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    }
    required_missing = [
        p.name
        for p in parameters.values()
        if p.default is inspect.Parameter.empty
        and p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        and p.name not in accepted
    ]
    if required_missing:
        raise TypeError(f"venue method missing supported keyword parameters: {', '.join(required_missing)}")
    positional_only = [
        p.name
        for p in parameters.values()
        if p.kind is inspect.Parameter.POSITIONAL_ONLY and p.name in kwargs
    ]
    if positional_only:
        raise TypeError(f"venue method requires positional-only parameters: {', '.join(positional_only)}")
    return method(**accepted)



class BinanceTestnetGateService:
    """Strict TESTNET connectivity, validation, and explicit probe lifecycle."""

    def __init__(
        self,
        store: Any,
        *,
        profile: BinanceRuntimeProfile,
        venue: Any = None,
        credential_store: BinanceCredentialStore | Any = None,
        credentials: BinanceSpotCredentials | Mapping[str, str] | Sequence[str] | None = None,
        clock: Callable[[], Any] | None = None,
    ) -> None:
        if not isinstance(profile, BinanceRuntimeProfile):
            raise TypeError("profile must be BinanceRuntimeProfile")
        if profile.environment is not BinanceSpotEnvironment.BINANCE_SPOT_TESTNET:
            raise ValueError("Binance testnet gate requires the strict TESTNET profile")
        self.store = store
        self.connection = getattr(store, "connection", store)
        if not isinstance(self.connection, sqlite3.Connection):
            raise TypeError("store must provide a sqlite3 connection")
        self.profile = profile
        self.credential_store = credential_store
        self.credentials = self._normalize_credentials(credentials)
        self.clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        self._closed = False
        self._offset_ms: int | None = None
        self._venue = venue
        self._initialize_schema()
        if self._venue is None and self.credentials is not None:
            self._venue = BinanceSpotRESTClient(profile, self.credentials, clock=self.clock, recv_window=5000)

    @staticmethod
    def _normalize_credentials(value: Any) -> BinanceSpotCredentials | None:
        if value is None:
            return None
        if isinstance(value, BinanceSpotCredentials):
            raw_key, raw_secret = value.api_key, value.api_secret
        elif isinstance(value, Mapping):
            raw_key, raw_secret = value.get("api_key"), value.get("api_secret")
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and len(value) == 2:
            raw_key, raw_secret = value
        else:
            raise TypeError("credentials must be explicit key/secret values")
        key = "" if raw_key is None else str(raw_key).strip()
        secret = "" if raw_secret is None else str(raw_secret).strip()
        if not key or not secret:
            return None
        return BinanceSpotCredentials(key, secret)


    @property
    def venue(self) -> Any:
        return self._venue

    def _initialize_schema(self) -> None:
        with _RAW_SQLITE_LOCK, self._lock, self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_testnet_gate_connectivity (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL, source TEXT NOT NULL, probe_kind TEXT NOT NULL,
                    status TEXT NOT NULL, reason TEXT NOT NULL, credential_status_json TEXT NOT NULL,
                    server_time_ms INTEGER, offset_ms INTEGER, account_json TEXT NOT NULL,
                    checked_at_utc TEXT NOT NULL, checked_at_pht TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_gate_connectivity_time
                    ON binance_testnet_gate_connectivity(record_id);
                CREATE TABLE IF NOT EXISTS binance_testnet_gate_validation (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL, source TEXT NOT NULL, probe_kind TEXT NOT NULL,
                    status TEXT NOT NULL, reason TEXT NOT NULL, symbol TEXT, price TEXT,
                    quantity TEXT, notional TEXT, fee_reserve TEXT, result_json TEXT NOT NULL,
                    checked_at_utc TEXT NOT NULL, checked_at_pht TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_gate_validation_time
                    ON binance_testnet_gate_validation(record_id);
                CREATE TABLE IF NOT EXISTS binance_testnet_probe_intents (
                    intent_id TEXT PRIMARY KEY, environment TEXT NOT NULL, source TEXT NOT NULL,
                    probe_kind TEXT NOT NULL, profile_hash TEXT NOT NULL, envelope_hash TEXT NOT NULL,
                    credential_hash TEXT NOT NULL, client_order_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
                    time_in_force TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL,
                    notional TEXT NOT NULL, fee_reserve TEXT NOT NULL, state TEXT NOT NULL,
                    exchange_order_id TEXT, filled_quantity TEXT NOT NULL DEFAULT '0',
                    fee_paid TEXT NOT NULL DEFAULT '0', realized_pnl TEXT NOT NULL DEFAULT '0',
                    reason TEXT NOT NULL DEFAULT '', raw_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_probe_intents_state
                    ON binance_testnet_probe_intents(state, updated_at_utc);
                CREATE TABLE IF NOT EXISTS binance_testnet_probe_reservations (
                    reservation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE,
                    environment TEXT NOT NULL, source TEXT NOT NULL, probe_kind TEXT NOT NULL,
                    symbol TEXT NOT NULL, side TEXT NOT NULL, amount TEXT NOT NULL,
                    fee_reserve TEXT NOT NULL, reserved_quantity TEXT NOT NULL,
                    status TEXT NOT NULL, created_at_utc TEXT NOT NULL, released_at_utc TEXT
                );
                CREATE TABLE IF NOT EXISTS binance_testnet_probe_fills (
                    trade_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, environment TEXT NOT NULL,
                    source TEXT NOT NULL, probe_kind TEXT NOT NULL, symbol TEXT NOT NULL,
                    side TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL,
                    quote_quantity TEXT NOT NULL, commission TEXT NOT NULL,
                    commission_asset TEXT, trade_time_utc TEXT NOT NULL, payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_probe_fills_intent
                    ON binance_testnet_probe_fills(intent_id, trade_time_utc, trade_id);
                CREATE TABLE IF NOT EXISTS binance_testnet_probe_events (
                    event_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, environment TEXT NOT NULL,
                    source TEXT NOT NULL, probe_kind TEXT NOT NULL, from_state TEXT,
                    to_state TEXT NOT NULL, reason TEXT NOT NULL, payload_json TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL, observed_at_pht TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_probe_events_intent
                    ON binance_testnet_probe_events(intent_id, observed_at_utc, event_id);
                """
            )

    @contextmanager
    def _write(self):
        if hasattr(self.store, "transaction"):
            with self.store.transaction(immediate=True):
                yield self.connection
        else:
            with _RAW_SQLITE_LOCK, self._lock, self.connection:
                yield self.connection

    @contextmanager
    def _immediate_write(self):
        """Acquire the SQLite writer boundary used by admission decisions."""
        if hasattr(self.store, "transaction"):
            with self.store.transaction(immediate=True):
                yield self.connection
            return
        with _RAW_SQLITE_LOCK, self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
            except BaseException:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()


    def _credential_value(self) -> BinanceSpotCredentials | None:
        if self.credentials is not None:
            if self._venue is None:
                self._venue = BinanceSpotRESTClient(self.profile, self.credentials, clock=self.clock, recv_window=5000)
            return self.credentials
        venue_credentials = getattr(self._venue, "credentials", None) if self._venue is not None else None
        if venue_credentials is not None:
            try:
                self.credentials = self._normalize_credentials(venue_credentials)
                if self.credentials is not None:
                    return self.credentials
            except (TypeError, ValueError):
                pass
        if self.credential_store is None:
            return None
        try:
            loader = getattr(self.credential_store, "load", None)
            loaded = loader() if callable(loader) else None
            self.credentials = self._normalize_credentials(loaded)
            if self.credentials is not None and self._venue is None:
                self._venue = BinanceSpotRESTClient(self.profile, self.credentials, clock=self.clock, recv_window=5000)
            return self.credentials
        except Exception:
            return None

    def _credential_projection(self) -> dict[str, Any]:
        # Hydrate through the same source precedence used by connectivity and
        # binding checks.  This also turns malformed/blank store values into
        # the unconfigured projection instead of trusting a stale flag.
        credential = self._credential_value()
        if credential is not None:
            configured = True
        else:
            configured = False
            if self.credential_store is not None:
                try:
                    projection = getattr(self.credential_store, "safe_projection", None)
                    raw = projection() if callable(projection) else None
                    if isinstance(raw, Mapping):
                        if "api_key" in raw or "api_secret" in raw:
                            configured = bool(
                                str(raw.get("api_key") or "").strip()
                                and str(raw.get("api_secret") or "").strip()
                            )
                        elif "api_key_configured" in raw or "api_secret_configured" in raw:
                            configured = bool(
                                raw.get("api_key_configured")
                                and raw.get("api_secret_configured")
                            )
                        else:
                            configured = bool(raw.get("configured"))
                except Exception:
                    configured = False
        return {
            "configured": configured,
            "api_key_configured": configured,
            "api_secret_configured": configured,
            "secret_values_exposed": False,
        }

    def _base(self, status: str, *, reason: str = "", checked: Any = None) -> dict[str, Any]:
        timestamp = _timestamp_projection(checked or self.clock())
        return {
            "status": status,
            "environment": BINANCE_SPOT_TESTNET,
            "source": SOURCE,
            "probe_kind": PROBE_LABEL,
            "reason": str(reason or "")[:256],
            "checked_at": timestamp,
            "timestamp": timestamp,
        }

    def _persist_connectivity(self, result: Mapping[str, Any]) -> None:
        checked = result.get("checked_at") if isinstance(result.get("checked_at"), Mapping) else _timestamp_projection(self.clock())
        with self._write() as conn:
            conn.execute(
                "INSERT INTO binance_testnet_gate_connectivity(environment,source,probe_kind,status,reason,credential_status_json,server_time_ms,offset_ms,account_json,checked_at_utc,checked_at_pht) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, str(result.get("status", "BLOCKED")), str(result.get("reason", ""))[:256],
                    _json(result.get("credentials", self._credential_projection())), result.get("server_time_ms"), result.get("offset_ms"),
                    _json(result.get("account", {})), str(checked.get("utc")), str(checked.get("pht")),
                ),
            )

    def _persist_validation(self, result: Mapping[str, Any]) -> None:
        checked = result.get("checked_at") if isinstance(result.get("checked_at"), Mapping) else _timestamp_projection(self.clock())
        with self._write() as conn:
            conn.execute(
                "INSERT INTO binance_testnet_gate_validation(environment,source,probe_kind,status,reason,symbol,price,quantity,notional,fee_reserve,result_json,checked_at_utc,checked_at_pht) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, str(result.get("status", "BLOCKED")), str(result.get("reason", ""))[:256],
                    result.get("symbol"), result.get("price"), result.get("quantity"), result.get("notional"), result.get("fee_reserve"),
                    _json(result), str(checked.get("utc")), str(checked.get("pht")),
                ),
            )

    def connectivity_status(self) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM binance_testnet_gate_connectivity ORDER BY record_id DESC LIMIT 1").fetchone()
        if row is None:
            return {**self._base("BLOCKED", reason="NOT_CHECKED"), "credentials": self._credential_projection(), "account": {}}
        result = dict(row)
        try:
            account = json.loads(result.pop("account_json") or "{}")
            credentials = json.loads(result.pop("credential_status_json") or "{}")
        except (TypeError, ValueError):
            account, credentials = {}, self._credential_projection()
        return {
            **self._base(result.get("status", "BLOCKED"), reason=result.get("reason", ""), checked={"utc": result.get("checked_at_utc"), "pht": result.get("checked_at_pht")}),
            "credentials": credentials,
            "account": account,
            "server_time_ms": result.get("server_time_ms"),
            "offset_ms": result.get("offset_ms"),
            "reset_detected": bool(account.get("reset_detected")) if isinstance(account, Mapping) else False,
        }

    def check_connectivity(self) -> dict[str, Any]:
        checked = self.clock()
        creds = self._credential_value()
        projection = self._credential_projection()
        if creds is None:
            result = {**self._base("BLOCKED", reason="CREDENTIALS_NOT_CONFIGURED", checked=checked), "credentials": projection, "account": {}}
            self._persist_connectivity(result)
            return result
        if self._venue is None:
            result = {**self._base("BLOCKED", reason="VENUE_NOT_CONFIGURED", checked=checked), "credentials": projection, "account": {}}
            self._persist_connectivity(result)
            return result
        # A supplied real client must still be pinned to TESTNET; duck-typed fake
        # venues are allowed for deterministic consumer tests.
        venue_env = getattr(self._venue, "environment", None)
        venue_origin = getattr(self._venue, "origin", None)
        if venue_env is not None and str(getattr(venue_env, "value", venue_env)) not in {BINANCE_SPOT_TESTNET, "TESTNET"}:
            result = {**self._base("BLOCKED", reason="VENUE_ENVIRONMENT_MISMATCH", checked=checked), "credentials": projection, "account": {}}
            self._persist_connectivity(result)
            return result
        if venue_origin is not None and venue_origin != "https://testnet.binance.vision":
            result = {**self._base("BLOCKED", reason="VENUE_ORIGIN_MISMATCH", checked=checked), "credentials": projection, "account": {}}
            self._persist_connectivity(result)
            return result
        try:
            time_method = next(
                (
                    getattr(self._venue, name, None)
                    for name in ("time", "server_time", "get_server_time", "public_time", "get_time")
                    if callable(getattr(self._venue, name, None))
                ),
                None,
            )
            account_method = next(
                (
                    getattr(self._venue, name, None)
                    for name in ("account", "get_account", "account_status", "get_account_status")
                    if callable(getattr(self._venue, name, None))
                ),
                None,
            )
            if time_method is None or account_method is None:
                raise RuntimeError("venue lacks server time/account methods")
            time_result = _invoke(time_method, {})
            time_status = _status(time_result)
            if time_status != "OK":
                code = _error_code(time_result)
                reason = AUTH_REASONS.get(code, "SERVER_TIME_UNAVAILABLE")
                status = "UNKNOWN" if time_status == "UNKNOWN" else "BLOCKED"
                result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": {}, "error_code": code}
                self._persist_connectivity(result)
                return result
            time_payload = _payload(time_result)
            server_value = time_payload.get("serverTime", time_payload.get("server_time")) if isinstance(time_payload, Mapping) else None
            if server_value is None:
                raise ValueError("malformed server time response")
            server_ms = int(server_value)
            local_ms = _now_ms(self.clock)
            self._offset_ms = server_ms - local_ms
            adjusted_ms = local_ms + self._offset_ms
            account_result = _invoke(account_method, {"timestamp": adjusted_ms, "recvWindow": 5000})
            account_status = _status(account_result)
            if account_status != "OK":
                code = _error_code(account_result)
                reason = AUTH_REASONS.get(code, "ACCOUNT_AUTHENTICATION_FAILED")
                status = "UNKNOWN" if account_status == "UNKNOWN" else "BLOCKED"
                result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": {}, "server_time_ms": server_ms, "offset_ms": self._offset_ms, "error_code": code}
                self._persist_connectivity(result)
                return result
            body = _payload(account_result)
            valid, reason, account = self._account_projection(body)
            previous = self.connection.execute("SELECT account_json FROM binance_testnet_gate_connectivity ORDER BY record_id DESC LIMIT 1").fetchone()
            reset = False
            if previous is not None:
                try:
                    old = json.loads(previous[0] or "{}")
                    reset = bool(old.get("epoch") is not None and account.get("epoch") is not None and old.get("epoch") != account.get("epoch"))
                except (TypeError, ValueError):
                    reset = False
            account["reset_detected"] = reset
            status = "PASS" if valid else "BLOCKED"
            result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": account, "server_time_ms": server_ms, "offset_ms": self._offset_ms}
            self._persist_connectivity(result)
            return result
        except TypeError:
            # A local duck-typed signature mismatch is not a remote UNKNOWN.
            raise
        except Exception as exc:
            result = {**self._base("UNKNOWN", reason=f"CONNECTIVITY_EXCEPTION:{type(exc).__name__}", checked=checked), "credentials": projection, "account": {}}
            self._persist_connectivity(result)
            return result
    def _account_projection(self, body: Any) -> tuple[bool, str, dict[str, Any]]:
        if not isinstance(body, Mapping):
            return False, "ACCOUNT_MALFORMED", {}
        account_type = str(body.get("accountType", body.get("account_type", ""))).upper()
        can_trade = body.get("canTrade")
        permissions = body.get("permissions", body.get("accountPermissions", body.get("account_permissions", ()))) or ()
        permission_values = {str(item).upper() for item in permissions} if isinstance(permissions, (list, tuple, set)) else {str(permissions).upper()}
        safe_permissions = sorted(permission_values & {"SPOT", "TRADE", "USER_DATA"})
        if account_type != "SPOT":
            return False, "ACCOUNT_NOT_SPOT", {"account_type": account_type, "can_trade": bool(can_trade), "permissions": safe_permissions}
        if can_trade is not True:
            return False, "ACCOUNT_CANNOT_TRADE", {"account_type": account_type, "can_trade": bool(can_trade), "permissions": safe_permissions}
        if permissions and not ({"SPOT", "TRADE"} & permission_values):
            return False, "SPOT_PERMISSION_REQUIRED", {"account_type": account_type, "can_trade": True, "permissions": safe_permissions}
        balances: list[dict[str, str]] = []
        raw_balances = body.get("balances", ())
        if not isinstance(raw_balances, Sequence) or isinstance(raw_balances, (str, bytes, bytearray)):
            return False, "BALANCES_MALFORMED", {"account_type": account_type, "can_trade": True, "permissions": safe_permissions}
        for row in list(raw_balances)[:MAX_BALANCE_ROWS]:
            if not isinstance(row, Mapping):
                continue
            asset = str(row.get("asset", "")).upper()[:24]
            try:
                free = _decimal(row.get("free", 0))
                locked = _decimal(row.get("locked", 0))
            except ValueError:
                continue
            if free < 0 or locked < 0 or free + locked <= 0:
                continue
            balances.append({"asset": asset, "free": _dstr(free), "locked": _dstr(locked), "total": _dstr(free + locked)})
        account = {
            "account_type": account_type,
            "can_trade": True,
            "permissions": safe_permissions,
            "balances": balances,
            "epoch": body.get("epoch", body.get("accountEpoch")),
        }
        return True, "", account

    def validation_status(self) -> dict[str, Any]:
        row = self.connection.execute("SELECT result_json FROM binance_testnet_gate_validation ORDER BY record_id DESC LIMIT 1").fetchone()
        if row is None:
            return {**self._base("BLOCKED", reason="NOT_CHECKED"), "symbol": None, "price": None, "quantity": None}
        try:
            result = json.loads(row[0])
            return result if isinstance(result, dict) else {**self._base("BLOCKED", reason="MALFORMED_PERSISTED_RESULT")}
        except (TypeError, ValueError):
            return {**self._base("BLOCKED", reason="MALFORMED_PERSISTED_RESULT")}

    def _exchange_symbols(self, body: Any) -> list[Mapping[str, Any]]:
        if isinstance(body, Mapping) and isinstance(body.get("symbols"), Sequence):
            return [item for item in body["symbols"] if isinstance(item, Mapping)]
        if isinstance(body, Mapping) and body.get("symbol"):
            return [body]
        if isinstance(body, Sequence) and not isinstance(body, (str, bytes, bytearray)):
            return [item for item in body if isinstance(item, Mapping)]
        return []
    def _market(self, symbol: str) -> tuple[Mapping[str, Any], Decimal, Decimal | None, Decimal | None]:
        ticker_result = self._public(("ticker_book", "book_ticker", "ticker", "ticker_price", "book"), symbol=symbol)
        if _status(ticker_result) != "OK":
            code = _error_code(ticker_result)
            raise RuntimeError(f"MARKET_DATA_UNAVAILABLE:{code}" if code is not None else "MARKET_DATA_UNAVAILABLE")
        ticker = _payload(ticker_result)
        if isinstance(ticker, Sequence) and not isinstance(ticker, (str, bytes, bytearray)):
            ticker = ticker[0] if ticker else {}
        ticker = ticker if isinstance(ticker, Mapping) else {}
        ask = ticker.get("askPrice", ticker.get("ask_price", ticker.get("price")))
        bid = ticker.get("bidPrice", ticker.get("bid_price"))
        reference = ticker.get("weightedAvgPrice", ticker.get("weighted_avg_price", ticker.get("lastPrice", ticker.get("last_price"))))
        try:
            ask_value = _decimal(ask)
        except ValueError:
            ask_value = Decimal("0")
        try:
            bid_value = _decimal(bid) if bid is not None else None
        except ValueError:
            bid_value = None
        try:
            ref_value = _decimal(reference) if reference is not None else None
        except ValueError:
            ref_value = None
        if ask_value <= 0 or bid_value is None or bid_value <= 0:
            depth_result = self._public(("depth", "order_book"), symbol=symbol, limit=5)
            if _status(depth_result) != "OK":
                code = _error_code(depth_result)
                raise RuntimeError(f"MARKET_DEPTH_UNAVAILABLE:{code}" if code is not None else "MARKET_DEPTH_UNAVAILABLE")
            depth = _payload(depth_result)
            asks = depth.get("asks", ()) if isinstance(depth, Mapping) else ()
            if ask_value <= 0 and asks:
                first = asks[0]
                ask_value = _decimal(first[0] if isinstance(first, (list, tuple)) else first.get("price"))
            bids = depth.get("bids", ()) if isinstance(depth, Mapping) else ()
            if (bid_value is None or bid_value <= 0) and bids:
                first = bids[0]
                bid_value = _decimal(first[0] if isinstance(first, (list, tuple)) else first.get("price"))
        if ask_value <= 0:
            raise ValueError("ASK_PRICE_UNAVAILABLE")
        return ticker, ask_value, bid_value, ref_value or bid_value

    def _public(self, names: Sequence[str], **kwargs: Any) -> Any:
        for name in names:
            method = getattr(self._venue, name, None)
            if callable(method):
                return _invoke(method, kwargs)
        raise RuntimeError("venue lacks public market method")

    @staticmethod
    def _next_step(value: Decimal, step: Decimal | None) -> Decimal:
        if step is None or step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_CEILING) * step

    def _size_buy(
        self,
        info: Mapping[str, Any],
        price: Decimal,
        reference: Decimal | None,
        bid: Decimal | None,
    ) -> tuple[dict[str, Any] | None, SymbolRules, tuple[str, ...], dict[str, Any]]:
        rules = SymbolRules.from_exchange_info(info)
        planned_exit: dict[str, Any] = {
            "status": "BLOCKED",
            "bid_price": _dstr(bid) if bid is not None and bid > 0 else None,
            "fee_rate": _dstr(FEE_RATE),
        }
        if rules.status and rules.status != "TRADING":
            planned_exit["reason"] = "SYMBOL_NOT_TRADING"
            return None, rules, ("SYMBOL_NOT_TRADING",), planned_exit
        if rules.quote_asset != "USDT" or not rules.spot_trading_allowed:
            planned_exit["reason"] = "SYMBOL_NOT_SPOT_USDT"
            return None, rules, ("SYMBOL_NOT_SPOT_USDT",), planned_exit
        if rules.order_types and "LIMIT" not in rules.order_types:
            planned_exit["reason"] = "LIMIT_NOT_SUPPORTED"
            return None, rules, ("LIMIT_NOT_SUPPORTED",), planned_exit
        if rules.time_in_force and not ({"IOC", "FOK"} & set(rules.time_in_force)):
            planned_exit["reason"] = "IOC_FOK_NOT_SUPPORTED"
            return None, rules, ("IOC_FOK_NOT_SUPPORTED",), planned_exit
        tif = "IOC" if not rules.time_in_force or "IOC" in rules.time_in_force else "FOK"
        try:
            entry_price = rules.round_price(price, "BUY")
        except ValueError as exc:
            planned_exit["reason"] = str(exc)
            return None, rules, (str(exc),), planned_exit
        if bid is None or bid <= 0:
            planned_exit["reason"] = "BID_PRICE_UNAVAILABLE"
            return None, rules, ("PLANNED_EXIT_BID_UNAVAILABLE",), planned_exit
        try:
            exit_price = rules.round_price(bid, "SELL")
        except ValueError as exc:
            planned_exit["reason"] = str(exc)
            return None, rules, ("PLANNED_EXIT_PRICE_INVALID",), planned_exit
        if exit_price <= 0:
            planned_exit["reason"] = "BID_PRICE_UNAVAILABLE"
            return None, rules, ("PLANNED_EXIT_BID_UNAVAILABLE",), planned_exit

        # Solve the minimum on the exchange's quantity grid before calling
        # size_limit_order.  The fee is conservatively assumed to be charged
        # in the base asset, then the remaining inventory is rounded down to
        # the SELL step.  The final candidate is still checked by both sides'
        # real filter implementation below.
        retained_factor = Decimal("1") - FEE_RATE
        start = rules.min_qty or rules.step_size or Decimal("0.00000001")
        if rules.min_notional and rules.min_notional > 0:
            start = max(start, self._next_step(rules.min_notional / entry_price, rules.step_size))
        minimum_exit = rules.min_qty or Decimal("0")
        if rules.min_notional and rules.min_notional > 0:
            minimum_exit = max(minimum_exit, rules.min_notional / exit_price)
        minimum_exit = self._next_step(minimum_exit, rules.step_size)
        buy_for_exit = self._next_step(minimum_exit / retained_factor, rules.step_size)
        cap_qty = rules.round_quantity(
            DEFAULT_BINANCE_RISK_ENVELOPE.entry_notional
            / (entry_price * (Decimal("1") + FEE_RATE)),
            "BUY",
        )
        increment = rules.step_size if rules.step_size and rules.step_size > 0 else Decimal("0.00000001")
        candidate = self._next_step(max(start, buy_for_exit), rules.step_size)
        planned_exit.update(
            {
                "price": _dstr(exit_price),
                "required_quantity": _dstr(minimum_exit),
                "entry_cap_quantity": _dstr(cap_qty),
                "entry_price": _dstr(entry_price),
            }
        )
        last_reasons: tuple[str, ...] = ()
        exit_candidate_valid = False
        while candidate > 0 and candidate <= cap_qty:
            sized = size_limit_order(
                rules,
                "BUY",
                entry_price,
                candidate,
                envelope=DEFAULT_BINANCE_RISK_ENVELOPE,
                fee_rate=FEE_RATE,
            )
            expected_base_fee = sized.quantity * FEE_RATE
            expected_base = max(Decimal("0"), sized.quantity - expected_base_fee)
            rounded_base = rules.round_quantity(expected_base, "SELL", available=expected_base)
            planned = size_limit_order(
                rules,
                "SELL",
                exit_price,
                rounded_base,
                envelope=DEFAULT_BINANCE_RISK_ENVELOPE,
                fee_rate=FEE_RATE,
                available_inventory=expected_base,
            )
            planned_exit = {
                "status": "BLOCKED",
                "bid_price": _dstr(bid),
                "price": _dstr(planned.price),
                "required_quantity": _dstr(minimum_exit),
                "entry_cap_quantity": _dstr(cap_qty),
                "entry_price": _dstr(entry_price),
                "candidate_buy_quantity": _dstr(candidate),
                "buy_quantity": _dstr(sized.quantity),
                "expected_base_fee": _dstr(expected_base_fee),
                "expected_base_quantity": _dstr(expected_base),
                "step_rounded_base_quantity": _dstr(rounded_base),
                "quantity": _dstr(planned.quantity),
                "notional": _dstr(planned.notional),
                "fee_reserve": _dstr(planned.fee_reserve),
                "fee_rate": _dstr(FEE_RATE),
                "reasons": list(planned.reasons),
            }
            exit_candidate_valid = planned.valid and planned.quantity > 0
            last_reasons = tuple(dict.fromkeys((*sized.reasons, *planned.reasons)))
            percent_reasons: list[str] = []
            if reference and reference > 0:
                if rules.percent_multiplier_up and sized.price > reference * rules.percent_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_ABOVE_MAX")
                if rules.percent_multiplier_down and sized.price < reference * rules.percent_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BELOW_MIN")
                if rules.bid_multiplier_up and sized.price > reference * rules.bid_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_ABOVE_MAX")
                if rules.bid_multiplier_down and sized.price < reference * rules.bid_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_BELOW_MIN")
            elif any(x and x > 0 for x in (rules.percent_multiplier_up, rules.percent_multiplier_down, rules.bid_multiplier_up, rules.bid_multiplier_down)):
                percent_reasons.append("FILTER_REFERENCE_UNAVAILABLE")
            if percent_reasons:
                planned_exit["reasons"] = percent_reasons
                return None, rules, tuple(percent_reasons), planned_exit
            if sized.valid and sized.quantity > 0 and planned.valid and planned.quantity > 0:
                planned_exit.update(
                    {
                        "status": "PASS",
                        "quantity": _dstr(planned.quantity),
                        "notional": _dstr(planned.notional),
                        "fee_reserve": _dstr(planned.fee_reserve),
                    }
                )
                return (
                    {
                        "price": sized.price,
                        "quantity": sized.quantity,
                        "notional": sized.notional,
                        "fee_reserve": sized.fee_reserve,
                        "time_in_force": tif,
                        "rules": rules,
                        "planned_exit": planned_exit,
                    },
                    rules,
                    (),
                    planned_exit,
                )
            candidate += increment
        if exit_candidate_valid or buy_for_exit <= cap_qty:
            planned_exit["reason"] = "NO_COMPLIANT_BUY_WITHIN_ENTRY_CAP"
            return None, rules, ("NO_COMPLIANT_BUY_WITHIN_ENTRY_CAP",), planned_exit
        planned_exit["reason"] = "NO_VIABLE_PLANNED_EXIT_WITHIN_ENTRY_CAP"
        planned_exit["reasons"] = list(last_reasons)
        return None, rules, ("NO_VIABLE_PLANNED_EXIT_WITHIN_ENTRY_CAP",), planned_exit

    def validate_order(self, symbol: str | None = None) -> dict[str, Any]:
        connectivity = self.check_connectivity()
        if connectivity.get("status") != "PASS":
            result = {**self._base("BLOCKED", reason="CONNECTIVITY_" + str(connectivity.get("reason") or "NOT_PASS")), "symbol": None, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
            self._persist_validation(result)
            return result
        try:
            requested = _symbol(symbol) if symbol else None
            exchange_result = self._public(("exchange_info", "get_exchange_info"), **({"symbol": requested} if requested else {}))
            if _status(exchange_result) != "OK":
                code = _error_code(exchange_result)
                result = {
                    **self._base("UNKNOWN" if _status(exchange_result) == "UNKNOWN" else "REJECTED",
                                 reason=AUTH_REASONS.get(code, f"EXCHANGE_INFO_REJECTED:{code}" if code is not None else "EXCHANGE_INFO_REJECTED")),
                    "symbol": requested, "price": None, "quantity": None, "notional": None,
                    "fee_reserve": None, "error_code": code,
                }
                self._persist_validation(result)
                return result
            infos = self._exchange_symbols(_payload(exchange_result))
            candidates = [item for item in infos if str(item.get("status", "")).upper() == "TRADING" and str(item.get("quoteAsset", item.get("quote_asset", ""))).upper() == "USDT" and item.get("isSpotTradingAllowed", True) is not False]
            candidates.sort(key=lambda item: str(item.get("symbol", "")).upper())
            info = next((item for item in candidates if not requested or str(item.get("symbol", "")).upper() == requested), None)
            if info is None:
                result = {**self._base("REJECTED", reason="NO_CURRENT_TRADING_SPOT_USDT_SYMBOL"), "symbol": requested, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
                self._persist_validation(result)
                return result
            selected = _symbol(info.get("symbol"))
            _, ask, bid, reference = self._market(selected)
            sized, rules, reasons, planned_exit = self._size_buy(info, ask, reference, bid)
            if sized is None:
                reason = ";".join(reasons) or str(planned_exit.get("reason") or "NO_COMPLIANT_BUY")
                status = (
                    "BLOCKED"
                    if reason.startswith("PLANNED_EXIT_")
                    or reason.startswith("NO_VIABLE_PLANNED_EXIT")
                    else "REJECTED"
                )
                result = {
                    **self._base(status, reason=reason),
                    "symbol": selected,
                    "price": _dstr(ask),
                    "quantity": None,
                    "notional": None,
                    "fee_reserve": None,
                    "rules": _jsonable(rules.raw_filters),
                    "planned_exit": planned_exit,
                    "planned_exit_viable": False,
                }
                self._persist_validation(result)
                return result
            planned_exit = sized["planned_exit"]
            test_method = next((getattr(self._venue, name, None) for name in ("test_order", "order_test", "place_test_order") if callable(getattr(self._venue, name, None))), None)
            if test_method is None:
                raise RuntimeError("venue lacks test_order")
            test_result = _invoke(test_method, {"symbol": selected, "side": "BUY", "quantity": _dstr(sized["quantity"]), "price": _dstr(sized["price"]), "time_in_force": sized["time_in_force"]})
            test_status = _status(test_result)
            if test_status != "OK":
                code = _error_code(test_result)
                result = {
                    **self._base("UNKNOWN" if test_status == "UNKNOWN" else "REJECTED", reason=AUTH_REASONS.get(code, f"VALIDATION_REJECTED:{code}" if code is not None else "VALIDATION_REJECTED")),
                    "symbol": selected,
                    "price": _dstr(sized["price"]),
                    "quantity": _dstr(sized["quantity"]),
                    "notional": _dstr(sized["notional"]),
                    "fee_reserve": _dstr(sized["fee_reserve"]),
                    "planned_exit": planned_exit,
                    "planned_exit_viable": True,
                    "error_code": code,
                }
                self._persist_validation(result)
                return result
            result = {
                **self._base("PASS", checked=self.clock()),
                "symbol": selected,
                "price": _dstr(sized["price"]),
                "quantity": _dstr(sized["quantity"]),
                "notional": _dstr(sized["notional"]),
                "fee_reserve": _dstr(sized["fee_reserve"]),
                "time_in_force": sized["time_in_force"],
                "rules": _jsonable(rules.raw_filters),
                "planned_exit": planned_exit,
                "planned_exit_viable": True,
                "validation_only": True,
            }
            self._persist_validation(result)
            return result
        except TypeError:
            raise
        except Exception as exc:
            result = {**self._base("UNKNOWN", reason=f"VALIDATION_EXCEPTION:{type(exc).__name__}"), "symbol": symbol, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
            self._persist_validation(result)
            return result

    def _latest_intent(self, side: str | None = None) -> sqlite3.Row | None:
        query = "SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=?"
        args: list[Any] = [PROBE_LABEL]
        if side:
            query += " AND side=?"
            args.append(side)
        query += " ORDER BY created_at_utc DESC, intent_id DESC LIMIT 1"
        return self.connection.execute(query, args).fetchone()

    def _event(self, intent_id: str, from_state: str | None, to_state: str, reason: str, payload: Any = None) -> None:
        stamp = _timestamp_projection(self.clock())
        self.connection.execute(
            "INSERT INTO binance_testnet_probe_events(event_id,intent_id,environment,source,probe_kind,from_state,to_state,reason,payload_json,observed_at_utc,observed_at_pht) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, intent_id, BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, from_state, to_state, str(reason)[:256], _json(payload or {}), stamp["utc"], stamp["pht"]),
        )

    def _transition(self, intent_id: str, state: str, reason: str, *, exchange_order_id: Any = None, raw: Any = None) -> None:
        with self._write() as conn:
            row = conn.execute("SELECT state FROM binance_testnet_probe_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                return
            old = str(row[0])
            fields = ["state=?", "reason=?", "raw_json=?", "updated_at_utc=?"]
            args: list[Any] = [state, str(reason)[:256], _json(raw or {}), _utc(self.clock()).isoformat()]
            if exchange_order_id is not None:
                fields.append("exchange_order_id=?")
                args.append(_safe_id(exchange_order_id))
            args.append(intent_id)
            conn.execute(f"UPDATE binance_testnet_probe_intents SET {','.join(fields)} WHERE intent_id=?", args)
            self._event(intent_id, old, state, reason, raw)

    def _intent_projection(self, row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
        value = dict(row)
        result = {key: value.get(key) for key in ("intent_id", "symbol", "side", "order_type", "time_in_force", "state", "reason", "created_at_utc", "updated_at_utc")}
        for key in ("price", "quantity", "notional", "fee_reserve", "filled_quantity", "fee_paid", "realized_pnl"):
            result[key] = value.get(key)
        result["client_order_id"] = _safe_id(value.get("client_order_id"))
        result["exchange_order_id"] = _safe_id(value.get("exchange_order_id"))
        return result

    def _probe_projection(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? ORDER BY created_at_utc, intent_id", (PROBE_LABEL,)).fetchall()
        if not rows:
            return {**self._base("BLOCKED", reason="NOT_STARTED"), "intent": None, "orders": [], "risk_reservation": None, "owned_quantity": "0", "realized_pnl": "0"}
        buy = next((row for row in rows if str(row["side"]).upper() == "BUY"), rows[0])
        exit_row = next((row for row in rows if str(row["side"]).upper() == "SELL"), None)
        reservations = self.connection.execute("SELECT * FROM binance_testnet_probe_reservations WHERE status='HELD' ORDER BY created_at_utc").fetchall()
        owned = self._owned_quantity(str(buy["symbol"]))
        status = str(exit_row["state"] if exit_row is not None and exit_row["state"] in {"DUST", "EXIT_BELOW_MINIMUM"} else buy["state"])
        return {**self._base(status, reason=str(exit_row["reason"] if status in {"DUST", "EXIT_BELOW_MINIMUM"} and exit_row else buy["reason"])), "intent": self._intent_projection(buy), "exit": self._intent_projection(exit_row) if exit_row is not None else None, "orders": [self._intent_projection(row) for row in rows], "risk_reservation": [dict(row) for row in reservations], "owned_quantity": _dstr(owned), "realized_pnl": _dstr(self._realized_pnl(str(buy["symbol"])))}

    def _binding_matches(self, row: Any) -> bool:
        return (
            str(_row_value(row, "profile_hash", "")) == canonical_sha256(self.profile.projection())
            and str(_row_value(row, "envelope_hash", "")) == DEFAULT_BINANCE_RISK_ENVELOPE.canonical_hash
            and str(_row_value(row, "credential_hash", "")) == credential_fingerprint(self.credentials)
        )

    def probe_status(self) -> dict[str, Any]:
        return self._probe_projection()

    def _new_client_id(self, symbol: str, side: str) -> str:
        return ("AXIOM-TESTNET-PROBE-" + canonical_sha256({"symbol": symbol, "side": side, "label": PROBE_LABEL})[:20])[:36]

    def _insert_intent(
        self,
        *,
        symbol: str,
        side: str,
        price: Decimal,
        quantity: Decimal,
        notional: Decimal,
        fee_reserve: Decimal,
        tif: str,
        client_id: str,
        state: str = "INTENT",
        conn: sqlite3.Connection | None = None,
    ) -> str:
        intent_id = uuid.uuid4().hex
        stamp = _utc(self.clock()).isoformat()

        def insert(connection: sqlite3.Connection) -> None:
            connection.execute(
                "INSERT INTO binance_testnet_probe_intents(intent_id,environment,source,probe_kind,profile_hash,envelope_hash,credential_hash,client_order_id,symbol,side,order_type,time_in_force,price,quantity,notional,fee_reserve,state,reason,raw_json,created_at_utc,updated_at_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    intent_id, BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL,
                    canonical_sha256(self.profile.projection()),
                    DEFAULT_BINANCE_RISK_ENVELOPE.canonical_hash,
                    credential_fingerprint(self.credentials),
                    client_id, symbol, side, "LIMIT", tif, _dstr(price),
                    _dstr(quantity), _dstr(notional), _dstr(fee_reserve), state,
                    "", "{}", stamp, stamp,
                ),
            )
            self._event(intent_id, None, state, "created", {"client_order_id": client_id})
            connection.execute(
                "INSERT INTO binance_testnet_probe_reservations(reservation_id,intent_id,environment,source,probe_kind,symbol,side,amount,fee_reserve,reserved_quantity,status,created_at_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, intent_id, BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, symbol, side, _dstr(notional), _dstr(fee_reserve), _dstr(quantity) if side == "SELL" else "0", "HELD", stamp),
            )
            self._event(intent_id, state, "RESERVED", "risk reserved", {"amount": _dstr(notional), "fee_reserve": _dstr(fee_reserve)})
            connection.execute("UPDATE binance_testnet_probe_intents SET state=?,updated_at_utc=? WHERE intent_id=?", ("RESERVED", stamp, intent_id))

        if conn is not None:
            insert(conn)
        else:
            with self._write() as connection:
                insert(connection)
        return intent_id

    def _release_reservation(self, intent_id: str) -> None:
        with self._write() as conn:
            conn.execute("UPDATE binance_testnet_probe_reservations SET status='RELEASED',released_at_utc=? WHERE intent_id=? AND status='HELD'", (_utc(self.clock()).isoformat(), intent_id))

    def _submit_venue(self, *, symbol: str, side: str, quantity: str, price: str, tif: str, client_id: str) -> Any:
        method = next((getattr(self._venue, name, None) for name in ("place_limit_order", "place_order", "submit_order", "order") if callable(getattr(self._venue, name, None))), None)
        if method is None:
            raise RuntimeError("venue lacks LIMIT order method")
        kwargs: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "time_in_force": tif,
        }
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        client_keys = ("newClientOrderId", "new_client_order_id", "client_order_id", "clientOrderId")
        client_key = next((key for key in client_keys if key in parameters), None)
        if client_key is None and any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
            # Binance's REST API names this parameter newClientOrderId.
            client_key = "newClientOrderId"
        if client_key is None:
            raise TypeError("venue LIMIT method must support a client order id")
        kwargs[client_key] = client_id
        return _invoke(method, kwargs)
    def execute_probe(self, symbol: str | None = None) -> dict[str, Any]:
        # A restarted service may have no explicit credentials of its own.
        # Resolve venue/store credentials before reading durable bindings.
        self._credential_value()
        existing = self._latest_intent("BUY")
        if existing is not None:
            if not self._binding_matches(existing):
                return self._probe_projection() | {"status": "BLOCKED", "reason": "BINDING_CHANGED"}
            return self._probe_projection()
        connectivity = self.check_connectivity()
        if connectivity.get("status") != "PASS":
            return self._probe_projection() | {"status": "BLOCKED", "reason": "CONNECTIVITY_" + str(connectivity.get("reason") or "NOT_PASS")}
        validation = self.validate_order(symbol)
        if validation.get("status") != "PASS":
            return self._probe_projection() | {"status": "BLOCKED", "reason": "VALIDATION_" + str(validation.get("reason") or "NOT_PASS")}
        selected = _symbol(validation["symbol"])
        price = _decimal(validation["price"])
        quantity = _decimal(validation["quantity"])
        notional = _decimal(validation["notional"])
        fee = _decimal(validation["fee_reserve"])
        tif = str(validation.get("time_in_force", "IOC"))
        client_id = self._new_client_id(selected, "BUY")
        blocked_reason: str | None = None
        intent_id: str | None = None
        # Admission checks and both durable rows are one immediate transaction.
        # The second existing-intent read closes the race between validation
        # (which performs venue reads) and this writer boundary.
        with self._immediate_write() as conn:
            existing = conn.execute(
                "SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? AND side='BUY' ORDER BY created_at_utc DESC, intent_id DESC LIMIT 1",
                (PROBE_LABEL,),
            ).fetchone()
            if existing is not None:
                if not self._binding_matches(existing):
                    blocked_reason = "BINDING_CHANGED"
                else:
                    return self._probe_projection()
            else:
                held_rows = conn.execute(
                    "SELECT amount,fee_reserve FROM binance_testnet_probe_reservations WHERE status='HELD'"
                ).fetchall()
                held_amount = sum(
                    (
                        _decimal(_row_value(row, "amount", "0"), Decimal("0"))
                        + _decimal(_row_value(row, "fee_reserve", "0"), Decimal("0"))
                    )
                    for row in held_rows
                )
                if held_amount + notional + fee > DEFAULT_BINANCE_RISK_ENVELOPE.max_reserved_exposure:
                    blocked_reason = "RESERVED_EXPOSURE"
                else:
                    today = _utc(self.clock()).date().isoformat()
                    submissions = conn.execute(
                        "SELECT COUNT(*) FROM binance_testnet_probe_intents "
                        "WHERE side='BUY' AND created_at_utc >= ?",
                        (today + "T00:00:00+00:00",),
                    ).fetchone()
                    if int(submissions[0] if submissions else 0) >= DEFAULT_BINANCE_RISK_ENVELOPE.max_submissions_per_day:
                        blocked_reason = "SUBMISSIONS_PER_DAY"
                    else:
                        intent_id = self._insert_intent(
                            symbol=selected,
                            side="BUY",
                            price=price,
                            quantity=quantity,
                            notional=notional,
                            fee_reserve=fee,
                            tif=tif,
                            client_id=client_id,
                            conn=conn,
                        )
        if blocked_reason is not None:
            return self._probe_projection() | {"status": "BLOCKED", "reason": blocked_reason}
        if intent_id is None:
            return self._probe_projection()
        self._transition(intent_id, "SUBMITTING", "before venue call")
        try:
            response = self._submit_venue(symbol=selected, side="BUY", quantity=_dstr(quantity), price=_dstr(price), tif=tif, client_id=client_id)
        except TypeError:
            raise
        except Exception as exc:
            self._transition(intent_id, "UNKNOWN", f"VENUE_EXCEPTION:{type(exc).__name__}")
            return self._probe_projection()
        status = _status(response)
        body = _payload(response)
        if status in {"UNKNOWN", "RATE_LIMIT"}:
            self._transition(intent_id, "UNKNOWN", "AMBIGUOUS_VENUE_RESULT", raw=response)
        elif status == "REJECTED":
            self._transition(intent_id, "REJECTED", "VENUE_REJECTED", raw=response)
            self._release_reservation(intent_id)
        elif isinstance(body, Mapping) and body.get("orderId") is not None:
            # An acknowledgement never establishes a fill; reconciliation does.
            self._transition(intent_id, "ACKNOWLEDGED", "VENUE_ACKNOWLEDGED", exchange_order_id=body.get("orderId"), raw=response)
        else:
            self._transition(intent_id, "UNKNOWN", "MALFORMED_VENUE_ACK", raw=response)
        return self._probe_projection()

    def _order_query(self, row: Mapping[str, Any]) -> Any:
        method = next((getattr(self._venue, name, None) for name in ("query_order", "get_order", "query") if callable(getattr(self._venue, name, None))), None)
        if method is None:
            raise RuntimeError("venue lacks order query")
        return _invoke(method, {"symbol": _row_value(row, "symbol"), "order_id": _row_value(row, "exchange_order_id"), "orig_client_order_id": _row_value(row, "client_order_id")})

    def _trades_query(self, row: Mapping[str, Any]) -> Any:
        method = next((getattr(self._venue, name, None) for name in ("my_trades", "trades", "get_my_trades") if callable(getattr(self._venue, name, None))), None)
        if method is None:
            raise RuntimeError("venue lacks myTrades query")
        return _invoke(method, {"symbol": _row_value(row, "symbol"), "order_id": _row_value(row, "exchange_order_id")})

    def _fills_for(self, intent_id: str) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM binance_testnet_probe_fills WHERE intent_id=? ORDER BY trade_time_utc,trade_id", (intent_id,)).fetchall()

    def _persist_trades(self, row: Mapping[str, Any], result: Any) -> bool:
        body = _payload(result)
        trades = body.get("trades", ()) if isinstance(body, Mapping) else body
        if not isinstance(trades, Sequence) or isinstance(trades, (str, bytes, bytearray)):
            return False
        evidence = False
        with self._write() as conn:
            for trade in list(trades)[:128]:
                if not isinstance(trade, Mapping):
                    continue
                try:
                    qty = _decimal(trade.get("qty", trade.get("quantity", 0)))
                    price = _decimal(trade.get("price", _row_value(row, "price")))
                    quote = _decimal(trade.get("quoteQty", trade.get("quote_quantity", qty * price)))
                    commission = _decimal(trade.get("commission", trade.get("fee", 0)), Decimal("0"))
                except ValueError:
                    continue
                if qty <= 0 or price <= 0:
                    continue
                evidence = True
                intent_id = _row_value(row, "intent_id")
                trade_id = _safe_id(trade.get("tradeId", trade.get("id"))) or canonical_sha256({"intent": intent_id, "trade": dict(trade)})[:32]
                conn.execute(
                    "INSERT OR IGNORE INTO binance_testnet_probe_fills(trade_id,intent_id,environment,source,probe_kind,symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time_utc,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (trade_id, intent_id, BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, _row_value(row, "symbol"), _row_value(row, "side"), _dstr(qty), _dstr(price), _dstr(quote), _dstr(commission), str(trade.get("commissionAsset", trade.get("commission_asset", ""))).upper()[:24] or None, _utc(trade.get("time", trade.get("timestamp", self.clock()))).isoformat(), _json(trade)),
                )
        return evidence

    def _owned_quantity(self, symbol: str) -> Decimal:
        rows = self.connection.execute("SELECT side,quantity,commission,commission_asset FROM binance_testnet_probe_fills WHERE symbol=? ORDER BY trade_time_utc,trade_id", (symbol,)).fetchall()
        base = symbol[:-4] if symbol.endswith("USDT") else ""
        total = Decimal("0")
        for row in rows:
            qty = _decimal(row["quantity"], Decimal("0"))
            commission = _decimal(row["commission"], Decimal("0"))
            asset = str(row["commission_asset"] or "").upper()
            if str(row["side"]).upper() == "BUY":
                total += qty - (commission if asset == base else Decimal("0"))
            else:
                total -= qty
        return max(Decimal("0"), total)

    def _realized_pnl(self, symbol: str) -> Decimal:
        rows = self.connection.execute("SELECT side,quantity,price,quote_quantity,commission,commission_asset,trade_time_utc,trade_id FROM binance_testnet_probe_fills WHERE symbol=? ORDER BY trade_time_utc,trade_id", (symbol,)).fetchall()
        base = symbol[:-4] if symbol.endswith("USDT") else ""
        lots: list[list[Decimal]] = []
        realized = Decimal("0")
        for row in rows:
            qty = _decimal(row["quantity"], Decimal("0"))
            quote = _decimal(row["quote_quantity"], Decimal("0"))
            fee = _decimal(row["commission"], Decimal("0"))
            asset = str(row["commission_asset"] or "").upper()
            if str(row["side"]).upper() == "BUY":
                net = max(Decimal("0"), qty - fee if asset == base else qty)
                lots.append([net, quote + (fee if asset == "USDT" else Decimal("0"))])
                continue
            remaining = qty
            proceeds = quote - (fee if asset == "USDT" else Decimal("0"))
            while remaining > 0 and lots:
                lot_qty, lot_cost = lots[0]
                matched = min(remaining, lot_qty)
                realized += proceeds * (matched / qty if qty else Decimal("0")) - lot_cost * (matched / lot_qty if lot_qty else Decimal("0"))
                lot_qty -= matched
                lot_cost -= lot_cost * (matched / (lot_qty + matched) if lot_qty + matched else Decimal("0"))
                remaining -= matched
                if lot_qty <= 0:
                    lots.pop(0)
                else:
                    lots[0] = [lot_qty, lot_cost]
        return realized

    def _apply_authoritative(self, row: Mapping[str, Any], order_result: Any) -> None:
        body = _payload(order_result)
        if not isinstance(body, Mapping):
            self._transition(str(row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
            return
        state = str(body.get("status", "")).upper()
        known = {"NEW": "ACKNOWLEDGED", "ACKNOWLEDGED": "ACKNOWLEDGED", "PARTIALLY_FILLED": "PARTIALLY_FILLED", "FILLED": "FILLED", "CANCELED": "CANCELED", "CANCELLED": "CANCELED", "EXPIRED": "EXPIRED"}
        target = known.get(state)
        if target is None:
            self._transition(str(row["intent_id"]), "UNKNOWN", "UNKNOWN_AUTHORITATIVE_STATE", raw=order_result)
            return
        try:
            cumulative = _decimal(body.get("executedQty", body.get("executed_quantity", 0)))
        except ValueError:
            cumulative = Decimal("0")
        current = str(row["state"])
        if current in TERMINAL and current not in {"DUST", "EXIT_BELOW_MINIMUM"}:
            target = current
        elif current == "PARTIALLY_FILLED" and target in {"ACKNOWLEDGED"}:
            target = current
        with self._write() as conn:
            conn.execute("UPDATE binance_testnet_probe_intents SET state=?,exchange_order_id=COALESCE(?,exchange_order_id),filled_quantity=?,fee_paid=?,updated_at_utc=?,raw_json=? WHERE intent_id=?", (target, _safe_id(body.get("orderId")), _dstr(cumulative), _dstr(sum((_decimal(item["commission"], Decimal("0")) for item in self._fills_for(str(row["intent_id"]))), Decimal("0"))), _utc(self.clock()).isoformat(), _json(order_result), row["intent_id"]))
            self._event(str(row["intent_id"]), current, target, "authoritative order", order_result)
            if target in TERMINAL or target == "FILLED":
                conn.execute("UPDATE binance_testnet_probe_reservations SET status='RELEASED',released_at_utc=? WHERE intent_id=? AND status='HELD'", (_utc(self.clock()).isoformat(), row["intent_id"]))
            elif str(row["side"]).upper() == "BUY":
                remaining = max(Decimal("0"), _decimal(row["quantity"]) - cumulative)
                amount = remaining * _decimal(row["price"])
                conn.execute("UPDATE binance_testnet_probe_reservations SET amount=?,fee_reserve=?,status='HELD' WHERE intent_id=? AND status='HELD'", (_dstr(amount), _dstr(amount * FEE_RATE), row["intent_id"]))
            else:
                remaining = max(Decimal("0"), _decimal(row["quantity"]) - cumulative)
                conn.execute("UPDATE binance_testnet_probe_reservations SET reserved_quantity=? WHERE intent_id=? AND status='HELD'", (_dstr(remaining), row["intent_id"]))

    def _make_exit(self, buy: Mapping[str, Any]) -> None:
        symbol = str(buy["symbol"])
        owned = self._owned_quantity(symbol)
        if owned <= 0:
            return
        if self._latest_intent("SELL") is not None:
            return
        try:
            exchange_result = self._public(("exchange_info", "get_exchange_info"), symbol=symbol)
            infos = self._exchange_symbols(_payload(exchange_result))
            info = next((item for item in infos if str(item.get("symbol", "")).upper() == symbol), None)
            if info is None:
                raise ValueError("SYMBOL_METADATA_UNAVAILABLE")
            _, _, bid, _ = self._market(symbol)
            if bid is None or bid <= 0:
                raise ValueError("BID_PRICE_UNAVAILABLE")
            ask = bid
            rules = SymbolRules.from_exchange_info(info)
            price = rules.round_price(ask, "SELL")
            sized = size_limit_order(rules, "SELL", price, owned, envelope=DEFAULT_BINANCE_RISK_ENVELOPE, fee_rate=FEE_RATE, available_inventory=owned)
            if not sized.valid or sized.quantity <= 0:
                reason = ";".join(sized.reasons) or "EXIT_BELOW_MINIMUM"
                self._insert_intent(symbol=symbol, side="SELL", price=price, quantity=Decimal("0"), notional=Decimal("0"), fee_reserve=Decimal("0"), tif="IOC", client_id=self._new_client_id(symbol, "SELL"), state="DUST")
                self._transition(str(self._latest_intent("SELL")["intent_id"]), "DUST", reason)
                self._release_reservation(str(self._latest_intent("SELL")["intent_id"]))
                return
            intent_id = self._insert_intent(symbol=symbol, side="SELL", price=sized.price, quantity=sized.quantity, notional=sized.notional, fee_reserve=Decimal("0"), tif="IOC" if not rules.time_in_force or "IOC" in rules.time_in_force else "FOK", client_id=self._new_client_id(symbol, "SELL"))
            self._transition(intent_id, "SUBMITTING", "before venue call")
            try:
                response = self._submit_venue(symbol=symbol, side="SELL", quantity=_dstr(sized.quantity), price=_dstr(sized.price), tif="IOC" if not rules.time_in_force or "IOC" in rules.time_in_force else "FOK", client_id=self._new_client_id(symbol, "SELL"))
            except TypeError:
                raise
            except Exception as exc:
                self._transition(intent_id, "UNKNOWN", f"VENUE_EXCEPTION:{type(exc).__name__}")
                return
            body = _payload(response)
            if _status(response) in {"UNKNOWN", "RATE_LIMIT"}:
                self._transition(intent_id, "UNKNOWN", "AMBIGUOUS_VENUE_RESULT", raw=response)
            elif _status(response) == "REJECTED":
                self._transition(intent_id, "REJECTED", "VENUE_REJECTED", raw=response)
                self._release_reservation(intent_id)
            elif isinstance(body, Mapping) and body.get("orderId") is not None:
                self._transition(intent_id, "ACKNOWLEDGED", "VENUE_ACKNOWLEDGED", exchange_order_id=body.get("orderId"), raw=response)
            else:
                self._transition(intent_id, "UNKNOWN", "MALFORMED_VENUE_ACK", raw=response)
        except TypeError:
            raise
        except Exception as exc:
            # Do not fabricate an exit or close the buy when market metadata is
            # unavailable; the owned amount remains visible for reconciliation.
            return

    def reconcile_probe(self) -> dict[str, Any]:
        # Rehydrate credentials from the venue or credential store before any
        # persisted binding is trusted after a restart.
        self._credential_value()
        rows = self.connection.execute("SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? ORDER BY created_at_utc, intent_id", (PROBE_LABEL,)).fetchall()
        if not rows:
            return self._probe_projection()
        if any(not self._binding_matches(row) for row in rows):
            return self._probe_projection() | {"status": "BLOCKED", "reason": "BINDING_CHANGED"}
        connectivity = self.check_connectivity()
        if connectivity.get("status") != "PASS":
            return self._probe_projection() | {"status": "UNKNOWN", "reason": "CONNECTIVITY_" + str(connectivity.get("reason") or "NOT_PASS")}
        reset = bool(connectivity.get("reset_detected"))
        for row in rows:
            current = str(row["state"])
            if current in {"DUST", "REJECTED", "CANCELED", "EXPIRED", "FILLED"}:
                continue
            try:
                order_result = self._order_query(dict(row))
                if _status(order_result) != "OK":
                    code = _error_code(order_result)
                    self._transition(str(row["intent_id"]), "UNKNOWN", "TESTNET_RESET_HISTORY_MISSING" if reset and code == -2013 else "AUTHORITATIVE_ORDER_UNAVAILABLE", raw=order_result)
                    continue
                authoritative = _payload(order_result)
                trade_row = dict(row)
                if isinstance(authoritative, Mapping) and authoritative.get("orderId") is not None:
                    trade_row["exchange_order_id"] = authoritative.get("orderId")
                trades_result = self._trades_query(trade_row)
                if _status(trades_result) != "OK":
                    self._transition(str(row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_TRADES_UNAVAILABLE", raw=trades_result)
                    continue
                evidence = self._persist_trades(row, trades_result)
                authoritative_state = str(authoritative.get("status", "")).upper() if isinstance(authoritative, Mapping) else ""
                if authoritative_state in {"FILLED", "PARTIALLY_FILLED"} and not evidence:
                    self._transition(str(row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_FILL_EVIDENCE_MISSING", raw=order_result)
                    continue
                self._apply_authoritative(row, order_result)
            except TypeError:
                raise
            except Exception as exc:
                self._transition(str(row["intent_id"]), "UNKNOWN", f"RECONCILE_EXCEPTION:{type(exc).__name__}")
        buy = self._latest_intent("BUY")
        if buy is not None and str(buy["state"]) in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"}:
            self._make_exit(dict(buy))
        # Re-read after a possible exit submission, then reconcile newly-created
        # exits in the same call without issuing a duplicate mutation.
        exit_row = self._latest_intent("SELL")
        if exit_row is not None and str(exit_row["state"]) not in {"DUST", "REJECTED", "CANCELED", "EXPIRED", "FILLED"}:
            try:
                order_result = self._order_query(dict(exit_row))
                if _status(order_result) == "OK":
                    authoritative = _payload(order_result)
                    trade_row = dict(exit_row)
                    if isinstance(authoritative, Mapping) and authoritative.get("orderId") is not None:
                        trade_row["exchange_order_id"] = authoritative.get("orderId")
                    trades_result = self._trades_query(trade_row)
                    if _status(trades_result) != "OK":
                        self._transition(str(exit_row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_TRADES_UNAVAILABLE", raw=trades_result)
                    else:
                        evidence = self._persist_trades(exit_row, trades_result)
                        authoritative_state = str(authoritative.get("status", "")).upper() if isinstance(authoritative, Mapping) else ""
                        if authoritative_state in {"FILLED", "PARTIALLY_FILLED"} and not evidence:
                            self._transition(str(exit_row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_FILL_EVIDENCE_MISSING", raw=order_result)
                        else:
                            self._apply_authoritative(exit_row, order_result)
                else:
                    self._transition(str(exit_row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_ORDER_UNAVAILABLE", raw=order_result)
            except TypeError:
                raise
            except Exception as exc:
                self._transition(str(exit_row["intent_id"]), "UNKNOWN", f"RECONCILE_EXCEPTION:{type(exc).__name__}")
        projection = self._probe_projection()
        if projection.get("exit") and projection["exit"].get("state") == "FILLED":
            projection["realized_pnl"] = _dstr(self._realized_pnl(str(projection["intent"]["symbol"])))
        return projection

    def dashboard_projection(self) -> dict[str, Any]:
        return {
            "environment": BINANCE_SPOT_TESTNET,
            "source": SOURCE,
            "probe_kind": PROBE_LABEL,
            "isolation": {
                "schema_namespace": "binance_testnet_*",
                "strategy_ledgers_touched": False,
                "autonomous_activation": False,
            },
            "profile": _jsonable(self.profile.projection()),
            "credentials": self._credential_projection(),
            "connectivity": self.connectivity_status(),
            "validation": self.validation_status(),
            "probe": self.probe_status(),
            "risk_envelope": _jsonable(DEFAULT_BINANCE_RISK_ENVELOPE.as_dict()),
            "permissions": {"can_trade": True, "withdrawal": False, "transfer": False},
        }

    def close(self) -> None:
        self._closed = True
BinanceTestnetGate = BinanceTestnetGateService
BinanceSpotTestnetGateService = BinanceTestnetGateService
TESTNET_EXECUTION_PROBE = PROBE_LABEL

__all__ = [
    "BinanceTestnetGateService",
    "BinanceTestnetGate",
    "BinanceSpotTestnetGateService",
    "PROBE_LABEL",
    "TESTNET_EXECUTION_PROBE",
    "SOURCE",
]

