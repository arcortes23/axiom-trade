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
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE, BinanceRiskEnvelope, SymbolRules, size_limit_order
from .binance_spot import (
    BINANCE_SPOT_PROHIBITED_PERMISSIONS,
    BINANCE_SPOT_TESTNET,
    BinanceCredentialRef,
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
TERMINAL = frozenset({"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED", "DUST"})
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

def _exact_order_id(value: Any) -> str | None:
    """Accept only a positive canonical decimal exchange order ID."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if value <= 0:
            return None
        text = str(value)
    elif isinstance(value, str):
        if not value or not value.isascii() or not value.isdigit() or value[0] == "0":
            return None
        text = value
    else:
        return None
    if len(text) > MAX_ID_TEXT:
        return None
    return text
def _exact_trade_id(value: Any) -> str | None:
    """Accept only a positive canonical decimal Binance trade identity."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if value <= 0:
            return None
        text = str(value)
    elif isinstance(value, str):
        if not value or not value.isascii() or not value.isdigit() or value[0] == "0":
            return None
        text = value
    else:
        return None
    return text if len(text) <= MAX_ID_TEXT else None


def _authoritative_order_identity_matches(row: Mapping[str, Any], body: Mapping[str, Any]) -> bool:
    """Require all persisted intent identity fields in an order response."""
    client = body.get("clientOrderId")
    expected_client = _row_value(row, "client_order_id")
    if not isinstance(client, str) or client != expected_client:
        return False
    symbol = body.get("symbol")
    expected_symbol = _row_value(row, "symbol")
    if not isinstance(symbol, str) or symbol != expected_symbol:
        return False
    side = body.get("side")
    return isinstance(side, str) and side == str(_row_value(row, "side"))


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

def _sqlite_table_columns(connection: sqlite3.Connection, table: str) -> set[str] | None:
    """Return a peer table's columns, or ``None`` when absent."""
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if exists is None:
            return None
        return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()




class _AutoDeadlineExpired(RuntimeError):
    """Internal control flow for a strict auto-cycle deadline."""


AUTO_DEADLINE_EXPIRED = "AUTO_DEADLINE_EXPIRED"
_DEADLINE_REQUEST_CEILING_SECONDS = 1.0


def _deadline_expired(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


def _deadline_checkpoint(deadline_monotonic: float | None) -> None:
    """Raise the internal control-flow sentinel once the deadline is observed."""
    if _deadline_expired(deadline_monotonic):
        raise _AutoDeadlineExpired


def _invoke(
    method: Callable[..., Any],
    kwargs: Mapping[str, Any],
    *,
    deadline_monotonic: float | None = None,
) -> Any:
    """Call one venue method with only supported API fields plus transport deadline."""
    if deadline_monotonic is not None:
        _deadline_checkpoint(deadline_monotonic)
        invoke_kwargs = dict(kwargs)
        invoke_kwargs["deadline_monotonic"] = deadline_monotonic
        return _invoke_supported(method, invoke_kwargs)
    return _invoke_supported(method, kwargs)


def _invoke_supported(method: Callable[..., Any], kwargs: Mapping[str, Any]) -> Any:
    """Call a venue method after filtering unsupported keyword arguments."""
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        # Dynamic callables with no inspectable signature cannot safely receive
        # the optional deadline keyword.  Timeout capping still applies to the
        # exact REST client because its methods are inspectable.
        if "deadline_monotonic" in kwargs:
            return method(**{key: value for key, value in kwargs.items() if key != "deadline_monotonic"})
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
        if credential_store is not None:
            expected_ref = BinanceCredentialRef("binance-testnet", BINANCE_SPOT_TESTNET)
            try:
                actual_ref = credential_store.ref
            except AttributeError as exc:
                raise ValueError("TESTNET credential store must expose the exact credential ref") from exc
            if type(actual_ref) is not BinanceCredentialRef or actual_ref != expected_ref:
                raise ValueError("TESTNET credential store identity mismatch")
        self.profile = profile
        self.credential_store = credential_store
        self.credentials = self._normalize_credentials(credentials)
        self.clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        self._operation_lock = threading.RLock()
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


    def _credential_value(self, deadline_monotonic: float | None = None) -> BinanceSpotCredentials | None:
        _deadline_checkpoint(deadline_monotonic)
        if self.credentials is not None:
            if self._venue is None:
                _deadline_checkpoint(deadline_monotonic)
                self._venue = BinanceSpotRESTClient(self.profile, self.credentials, clock=self.clock, recv_window=5000)
            _deadline_checkpoint(deadline_monotonic)
            return self.credentials
        _deadline_checkpoint(deadline_monotonic)
        venue_credentials = getattr(self._venue, "credentials", None) if self._venue is not None else None
        if venue_credentials is not None:
            try:
                _deadline_checkpoint(deadline_monotonic)
                self.credentials = self._normalize_credentials(venue_credentials)
                _deadline_checkpoint(deadline_monotonic)
                if self.credentials is not None:
                    return self.credentials
            except (TypeError, ValueError):
                pass
        _deadline_checkpoint(deadline_monotonic)
        if self.credential_store is None:
            return None
        try:
            _deadline_checkpoint(deadline_monotonic)
            loader = getattr(self.credential_store, "load", None)
            loaded = loader() if callable(loader) else None
            _deadline_checkpoint(deadline_monotonic)
            self.credentials = self._normalize_credentials(loaded)
            _deadline_checkpoint(deadline_monotonic)
            if self.credentials is not None and self._venue is None:
                self._venue = BinanceSpotRESTClient(self.profile, self.credentials, clock=self.clock, recv_window=5000)
            _deadline_checkpoint(deadline_monotonic)
            return self.credentials
        except _AutoDeadlineExpired:
            raise
        except Exception:
            return None

    def _credential_projection(self, deadline_monotonic: float | None = None) -> dict[str, Any]:
        # Hydrate through the same source precedence used by connectivity and
        # binding checks.  This also turns malformed/blank store values into
        # the unconfigured projection instead of trusting a stale flag.
        _deadline_checkpoint(deadline_monotonic)
        credential = self._credential_value(deadline_monotonic)
        _deadline_checkpoint(deadline_monotonic)
        if credential is not None:
            configured = True
        else:
            configured = False
            if self.credential_store is not None:
                try:
                    _deadline_checkpoint(deadline_monotonic)
                    projection = getattr(self.credential_store, "safe_projection", None)
                    raw = projection() if callable(projection) else None
                    _deadline_checkpoint(deadline_monotonic)
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
                except _AutoDeadlineExpired:
                    raise
                except Exception:
                    configured = False
        _deadline_checkpoint(deadline_monotonic)
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
        credentials = result.get("credentials")
        if credentials is None:
            credentials = self._credential_projection()
        with self._write() as conn:
            conn.execute(
                "INSERT INTO binance_testnet_gate_connectivity(environment,source,probe_kind,status,reason,credential_status_json,server_time_ms,offset_ms,account_json,checked_at_utc,checked_at_pht) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    BINANCE_SPOT_TESTNET, SOURCE, PROBE_LABEL, str(result.get("status", "BLOCKED")), str(result.get("reason", ""))[:256],
                    _json(credentials), result.get("server_time_ms"), result.get("offset_ms"),
                    _json(result.get("account", {})), str(checked.get("utc")), str(checked.get("pht")),
                ),
            )
    def _persist_connectivity_guarded(
        self,
        result: Mapping[str, Any],
        deadline_monotonic: float | None,
    ) -> dict[str, Any]:
        """Persist a gate verdict without returning it after expiry."""
        if deadline_monotonic is None:
            self._persist_connectivity(result)
            return dict(result)
        try:
            _deadline_checkpoint(deadline_monotonic)
            self._persist_connectivity(result)
            _deadline_checkpoint(deadline_monotonic)
        except _AutoDeadlineExpired:
            credentials = result.get("credentials")
            if not isinstance(credentials, Mapping):
                credentials = {
                    "configured": False,
                    "api_key_configured": False,
                    "api_secret_configured": False,
                    "secret_values_exposed": False,
                }
            return self._deadline_connectivity_result(
                result.get("checked_at"),
                credentials,
                server_time_ms=result.get("server_time_ms"),
                offset_ms=result.get("offset_ms"),
            )
        return dict(result)

    def _persist_validation_guarded(
        self,
        result: Mapping[str, Any],
        deadline_monotonic: float | None,
    ) -> dict[str, Any]:
        """Persist a validation verdict without returning it after expiry."""
        if deadline_monotonic is None:
            self._persist_validation(result)
            return dict(result)
        try:
            _deadline_checkpoint(deadline_monotonic)
            self._persist_validation(result)
            _deadline_checkpoint(deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_validation_result(
                result.get("symbol"),
                checked=result.get("checked_at"),
                price=result.get("price"),
                quantity=result.get("quantity"),
                notional=result.get("notional"),
                fee_reserve=result.get("fee_reserve"),
            )
        return dict(result)


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

    def check_connectivity(self, deadline_monotonic: float | None = None) -> dict[str, Any]:
        checked = self.clock()
        projection: Mapping[str, Any] = {
            "configured": self.credentials is not None,
            "api_key_configured": self.credentials is not None,
            "api_secret_configured": self.credentials is not None,
            "secret_values_exposed": False,
        }
        try:
            _deadline_checkpoint(deadline_monotonic)
            creds = self._credential_value(deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
            projection = self._credential_projection(deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_connectivity_result(checked, projection)
        if _deadline_expired(deadline_monotonic):
            return self._deadline_connectivity_result(checked, projection)
        if creds is None:
            result = {**self._base("BLOCKED", reason="CREDENTIALS_NOT_CONFIGURED", checked=checked), "credentials": projection, "account": {}}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
        if self._venue is None:
            result = {**self._base("BLOCKED", reason="VENUE_NOT_CONFIGURED", checked=checked), "credentials": projection, "account": {}}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
        # A supplied real client must still be pinned to TESTNET; duck-typed fake
        # venues are allowed for deterministic consumer tests.
        venue_env = getattr(self._venue, "environment", None)
        venue_origin = getattr(self._venue, "origin", None)
        if venue_env is not None and str(getattr(venue_env, "value", venue_env)) not in {BINANCE_SPOT_TESTNET, "TESTNET"}:
            result = {**self._base("BLOCKED", reason="VENUE_ENVIRONMENT_MISMATCH", checked=checked), "credentials": projection, "account": {}}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
        if venue_origin is not None and venue_origin != "https://testnet.binance.vision":
            result = {**self._base("BLOCKED", reason="VENUE_ORIGIN_MISMATCH", checked=checked), "credentials": projection, "account": {}}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
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
            time_result = _invoke(time_method, {}, deadline_monotonic=deadline_monotonic)
            if _deadline_expired(deadline_monotonic):
                return self._deadline_connectivity_result(checked, projection)
            time_status = _status(time_result)
            if time_status != "OK":
                code = _error_code(time_result)
                reason = AUTH_REASONS.get(code, "SERVER_TIME_UNAVAILABLE")
                status = "UNKNOWN" if time_status == "UNKNOWN" else "BLOCKED"
                result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": {}, "error_code": code}
                return self._persist_connectivity_guarded(result, deadline_monotonic)
            time_payload = _payload(time_result)
            server_value = time_payload.get("serverTime", time_payload.get("server_time")) if isinstance(time_payload, Mapping) else None
            if server_value is None:
                raise ValueError("malformed server time response")
            server_ms = int(server_value)
            local_ms = _now_ms(self.clock)
            self._offset_ms = server_ms - local_ms
            account_kwargs: dict[str, Any] = {}
            if isinstance(self._venue, BinanceSpotRESTClient):
                self._venue.set_server_time_offset_ms(self._offset_ms)
            else:
                # Duck-typed fixtures still expose Binance's signed fields;
                # the concrete REST client owns these fields itself.
                account_kwargs = {
                    "timestamp": local_ms + self._offset_ms,
                    "recvWindow": 5000,
                }
            account_result = _invoke(
                account_method,
                account_kwargs,
                deadline_monotonic=deadline_monotonic,
            )
            if _deadline_expired(deadline_monotonic):
                return self._deadline_connectivity_result(checked, projection, server_time_ms=server_ms, offset_ms=self._offset_ms)
            account_status = _status(account_result)
            if account_status != "OK":
                code = _error_code(account_result)
                reason = AUTH_REASONS.get(code, "ACCOUNT_AUTHENTICATION_FAILED")
                status = "UNKNOWN" if account_status == "UNKNOWN" else "BLOCKED"
                result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": {}, "server_time_ms": server_ms, "offset_ms": self._offset_ms, "error_code": code}
                return self._persist_connectivity_guarded(result, deadline_monotonic)
            body = _payload(account_result)
            valid, reason, account = self._account_projection(body)
            _deadline_checkpoint(deadline_monotonic)
            status = "PASS" if valid else "BLOCKED"
            result = {**self._base(status, reason=reason, checked=checked), "credentials": projection, "account": account, "server_time_ms": server_ms, "offset_ms": self._offset_ms}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_connectivity_result(checked, projection)
        except TypeError:
            # A local duck-typed signature mismatch is not a remote UNKNOWN.
            raise
        except Exception as exc:
            if _deadline_expired(deadline_monotonic):
                return self._deadline_connectivity_result(checked, projection)
            result = {**self._base("UNKNOWN", reason=f"CONNECTIVITY_EXCEPTION:{type(exc).__name__}", checked=checked), "credentials": projection, "account": {}}
            return self._persist_connectivity_guarded(result, deadline_monotonic)
    def _deadline_connectivity_result(
        self,
        checked: Any,
        projection: Mapping[str, Any],
        *,
        server_time_ms: int | None = None,
        offset_ms: int | None = None,
    ) -> dict[str, Any]:
        result = {
            **self._base("BLOCKED", reason=AUTO_DEADLINE_EXPIRED, checked=checked),
            "credentials": dict(projection),
            "account": {},
        }
        if server_time_ms is not None:
            result["server_time_ms"] = server_time_ms
        if offset_ms is not None:
            result["offset_ms"] = offset_ms
        self._persist_connectivity(result)
        return result

    def _deadline_validation_result(
        self,
        symbol: Any = None,
        *,
        checked: Any = None,
        price: Any = None,
        quantity: Any = None,
        notional: Any = None,
        fee_reserve: Any = None,
    ) -> dict[str, Any]:
        result = {
            **self._base("BLOCKED", reason=AUTO_DEADLINE_EXPIRED, checked=checked),
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "notional": notional,
            "fee_reserve": fee_reserve,
        }
        self._persist_validation(result)
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
        if not permissions or not ({"SPOT", "TRADE"} & permission_values):
            return False, "SPOT_PERMISSION_REQUIRED", {"account_type": account_type, "can_trade": True, "permissions": safe_permissions}
        if permission_values & BINANCE_SPOT_PROHIBITED_PERMISSIONS:
            return False, "PROHIBITED_PERMISSION", {"account_type": account_type, "can_trade": True, "permissions": safe_permissions}
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
        return []

    def _market(
        self,
        symbol: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> tuple[Mapping[str, Any], Decimal, Decimal | None, Decimal | None]:
        """Read book prices and the official 24-hour reference independently.

        Binance's documented ``bookTicker`` payload contains only bid/ask.  The
        reference used by PERCENT_PRICE filters is supplied by ``ticker/24hr``
        (weighted average, falling back to last price), so never infer it from
        a book-only response when the authoritative endpoint is available.
        """
        ticker_result = self._public(
            ("ticker_book", "book_ticker", "ticker_book_ticker", "book"),
            deadline_monotonic=deadline_monotonic,
            symbol=symbol,
        )
        if _deadline_expired(deadline_monotonic):
            raise _AutoDeadlineExpired
        if _status(ticker_result) != "OK":
            code = _error_code(ticker_result)
            raise RuntimeError(f"MARKET_DATA_UNAVAILABLE:{code}" if code is not None else "MARKET_DATA_UNAVAILABLE")
        ticker = _payload(ticker_result)
        if isinstance(ticker, Sequence) and not isinstance(ticker, (str, bytes, bytearray)):
            ticker = ticker[0] if ticker else {}
        ticker = ticker if isinstance(ticker, Mapping) else {}
        ask = ticker.get("askPrice", ticker.get("ask_price", ticker.get("price")))
        bid = ticker.get("bidPrice", ticker.get("bid_price"))
        try:
            ask_value = _decimal(ask)
        except ValueError:
            ask_value = Decimal("0")
        try:
            bid_value = _decimal(bid) if bid is not None else None
        except ValueError:
            bid_value = None

        # The concrete REST client exposes ticker_24hr/ticker_stats aliases.
        # Keep a fixture fallback to an explicitly supplied reference so older
        # duck-typed venues remain usable when no percent filter is present.
        reference: Any = ticker.get("weightedAvgPrice", ticker.get("weighted_avg_price"))
        try:
            reference_value = _decimal(reference)
        except ValueError:
            reference_value = Decimal("0")
        if reference_value <= 0:
            reference = ticker.get("lastPrice", ticker.get("last_price"))
        reference_method = next(
            (
                getattr(self._venue, name, None)
                for name in (
                    "ticker_24hr",
                    "ticker_24hr_stats",
                    "ticker_stats",
                    "ticker",
                )
                if callable(getattr(self._venue, name, None))
            ),
            None,
        )
        if reference_method is not None:
            reference_result = _invoke(
                reference_method,
                {"symbol": symbol},
                deadline_monotonic=deadline_monotonic,
            )
            if _deadline_expired(deadline_monotonic):
                raise _AutoDeadlineExpired
            if _status(reference_result) != "OK":
                code = _error_code(reference_result)
                raise RuntimeError(
                    f"MARKET_REFERENCE_UNAVAILABLE:{code}"
                    if code is not None
                    else "MARKET_REFERENCE_UNAVAILABLE"
                )
            reference_payload = _payload(reference_result)
            if isinstance(reference_payload, Sequence) and not isinstance(
                reference_payload, (str, bytes, bytearray)
            ):
                reference_payload = reference_payload[0] if reference_payload else {}
            if isinstance(reference_payload, Mapping):
                reference = reference_payload.get(
                    "weightedAvgPrice",
                    reference_payload.get("weighted_avg_price"),
                )
                try:
                    reference_value = _decimal(reference)
                except ValueError:
                    reference_value = Decimal("0")
                if reference_value <= 0:
                    reference = reference_payload.get(
                        "lastPrice",
                        reference_payload.get("last_price"),
                    )
        try:
            ref_value = _decimal(reference) if reference is not None else None
        except ValueError:
            ref_value = None

        if ask_value <= 0 or bid_value is None or bid_value <= 0:
            depth_result = self._public(
                ("depth", "order_book"),
                deadline_monotonic=deadline_monotonic,
                symbol=symbol,
                limit=5,
            )
            if _deadline_expired(deadline_monotonic):
                raise _AutoDeadlineExpired
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

    def _public(
        self,
        names: Sequence[str],
        *,
        deadline_monotonic: float | None = None,
        **kwargs: Any,
    ) -> Any:
        for name in names:
            method = getattr(self._venue, name, None)
            if callable(method):
                return _invoke(method, kwargs, deadline_monotonic=deadline_monotonic)
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
        *,
        deadline_monotonic: float | None = None,
    ) -> tuple[dict[str, Any] | None, SymbolRules, tuple[str, ...], dict[str, Any]]:
        _deadline_checkpoint(deadline_monotonic)
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
        reference_available = reference is not None and reference > 0
        # A candidate loop can be empty when the minimum viable exit already
        # exceeds the all-in entry cap.  Keep the terminal diagnostics defined
        # so this bounded rejection never falls through an unbound local.
        exit_candidate_valid = False
        last_reasons: tuple[str, ...] = ()
        candidate = self._next_step(max(start, buy_for_exit), rules.step_size)
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
            # size_limit_order cannot see the market reference and therefore
            # reports FILTER_REFERENCE_UNAVAILABLE for percent filters.  Once
            # _market has obtained the documented 24-hour reference, that one
            # local reason is resolved below alongside the exact filter checks.
            sized_reasons = tuple(
                reason
                for reason in sized.reasons
                if reason != "FILTER_REFERENCE_UNAVAILABLE" or not reference_available
            )
            planned_reasons = tuple(
                reason
                for reason in planned.reasons
                if reason != "FILTER_REFERENCE_UNAVAILABLE" or not reference_available
            )
            sized_valid = not sized_reasons
            planned_valid = not planned_reasons
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
                "reasons": list(planned_reasons),
            }
            exit_candidate_valid = planned_valid and planned.quantity > 0
            last_reasons = tuple(dict.fromkeys((*sized_reasons, *planned_reasons)))
            percent_reasons: list[str] = []
            if reference_available:
                if rules.percent_multiplier_up and sized.price > reference * rules.percent_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_ABOVE_MAX")
                if rules.percent_multiplier_down and sized.price < reference * rules.percent_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BELOW_MIN")
                if rules.bid_multiplier_up and sized.price > reference * rules.bid_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_ABOVE_MAX")
                if rules.bid_multiplier_down and sized.price < reference * rules.bid_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_BELOW_MIN")
                if rules.percent_multiplier_up and planned.price > reference * rules.percent_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_ABOVE_MAX")
                if rules.percent_multiplier_down and planned.price < reference * rules.percent_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BELOW_MIN")
                if rules.ask_multiplier_up and planned.price > reference * rules.ask_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_ABOVE_MAX")
                if rules.ask_multiplier_down and planned.price < reference * rules.ask_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_BELOW_MIN")
                deviation_bps = abs(sized.price - reference) * Decimal("10000") / reference
                planned_exit["reference_price"] = _dstr(reference)
                planned_exit["deviation_bps"] = _dstr(deviation_bps)
                if deviation_bps > DEFAULT_BINANCE_RISK_ENVELOPE.max_execution_deviation_bps:
                    percent_reasons.append("EXECUTION_DEVIATION_ABOVE_MAX")
            elif any(
                x and x > 0
                for x in (
                    rules.percent_multiplier_up,
                    rules.percent_multiplier_down,
                    rules.bid_multiplier_up,
                    rules.bid_multiplier_down,
                    rules.ask_multiplier_up,
                    rules.ask_multiplier_down,
                )
            ):
                percent_reasons.append("FILTER_REFERENCE_UNAVAILABLE")
            if percent_reasons:
                planned_exit["reasons"] = percent_reasons
                return None, rules, tuple(percent_reasons), planned_exit
            _deadline_checkpoint(deadline_monotonic)
            if sized_valid and sized.quantity > 0 and planned_valid and planned.quantity > 0:
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
            _deadline_checkpoint(deadline_monotonic)
        if exit_candidate_valid or buy_for_exit <= cap_qty:
            planned_exit["reason"] = "NO_COMPLIANT_BUY_WITHIN_ENTRY_CAP"
            return None, rules, ("NO_COMPLIANT_BUY_WITHIN_ENTRY_CAP",), planned_exit
        planned_exit["reason"] = "NO_VIABLE_PLANNED_EXIT_WITHIN_ENTRY_CAP"
        planned_exit["reasons"] = list(last_reasons)
        return None, rules, ("NO_VIABLE_PLANNED_EXIT_WITHIN_ENTRY_CAP",), planned_exit

    def validate_order(self, symbol: str | None = None, deadline_monotonic: float | None = None) -> dict[str, Any]:
        if _deadline_expired(deadline_monotonic):
            return self._deadline_validation_result(symbol, checked=self.clock())
        connectivity = self.check_connectivity(deadline_monotonic=deadline_monotonic)
        if _deadline_expired(deadline_monotonic):
            return self._deadline_validation_result(symbol, checked=self.clock())
        if connectivity.get("status") != "PASS":
            if connectivity.get("reason") == AUTO_DEADLINE_EXPIRED:
                return self._deadline_validation_result(symbol, checked=self.clock())
            result = {**self._base("BLOCKED", reason="CONNECTIVITY_" + str(connectivity.get("reason") or "NOT_PASS")), "symbol": None, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
            return self._persist_validation_guarded(result, deadline_monotonic)
        if _deadline_expired(deadline_monotonic):
            return self._deadline_validation_result(symbol, checked=self.clock())
        selected: Any = symbol
        try:
            requested = _symbol(symbol) if symbol else None
            selected = requested
            exchange_result = self._public(
                ("exchange_info", "get_exchange_info"),
                deadline_monotonic=deadline_monotonic,
                **({"symbol": requested} if requested else {}),
            )
            if _deadline_expired(deadline_monotonic):
                return self._deadline_validation_result(requested, checked=self.clock())
            if _status(exchange_result) != "OK":
                code = _error_code(exchange_result)
                result = {
                    **self._base("UNKNOWN" if _status(exchange_result) == "UNKNOWN" else "REJECTED",
                                 reason=AUTH_REASONS.get(code, f"EXCHANGE_INFO_REJECTED:{code}" if code is not None else "EXCHANGE_INFO_REJECTED")),
                    "symbol": requested, "price": None, "quantity": None, "notional": None,
                    "fee_reserve": None, "error_code": code,
                }
                return self._persist_validation_guarded(result, deadline_monotonic)
            infos = self._exchange_symbols(_payload(exchange_result))
            candidates = [item for item in infos if str(item.get("status", "")).upper() == "TRADING" and str(item.get("quoteAsset", item.get("quote_asset", ""))).upper() == "USDT" and item.get("isSpotTradingAllowed", True) is not False]
            candidates.sort(key=lambda item: str(item.get("symbol", "")).upper())
            info = next((item for item in candidates if not requested or str(item.get("symbol", "")).upper() == requested), None)
            if info is None:
                result = {**self._base("REJECTED", reason="NO_CURRENT_TRADING_SPOT_USDT_SYMBOL"), "symbol": requested, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
                return self._persist_validation_guarded(result, deadline_monotonic)
            selected = _symbol(info.get("symbol"))
            _, ask, bid, reference = self._market(selected, deadline_monotonic=deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
            sized, rules, reasons, planned_exit = self._size_buy(
                info,
                ask,
                reference,
                bid,
                deadline_monotonic=deadline_monotonic,
            )
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
                    "market_bid": _dstr(bid) if bid is not None else None,
                    "market_reference": _dstr(reference) if reference is not None else None,
                    "quantity": None,
                    "notional": None,
                    "fee_reserve": None,
                    "rules": _jsonable(rules.raw_filters),
                    "planned_exit": planned_exit,
                    "planned_exit_viable": False,
                }
                return self._persist_validation_guarded(result, deadline_monotonic)
            planned_exit = sized["planned_exit"]
            test_method = next((getattr(self._venue, name, None) for name in ("test_order", "order_test", "place_test_order") if callable(getattr(self._venue, name, None))), None)
            if test_method is None:
                raise RuntimeError("venue lacks test_order")
            _deadline_checkpoint(deadline_monotonic)
            test_result = _invoke(
                test_method,
                {"symbol": selected, "side": "BUY", "quantity": _dstr(sized["quantity"]), "price": _dstr(sized["price"]), "time_in_force": sized["time_in_force"]},
                deadline_monotonic=deadline_monotonic,
            )
            if _deadline_expired(deadline_monotonic):
                return self._deadline_validation_result(
                    selected,
                    checked=self.clock(),
                    price=_dstr(sized["price"]),
                    quantity=_dstr(sized["quantity"]),
                    notional=_dstr(sized["notional"]),
                    fee_reserve=_dstr(sized["fee_reserve"]),
                )
            test_status = _status(test_result)
            if test_status != "OK":
                code = _error_code(test_result)
                result = {
                    **self._base("UNKNOWN" if test_status == "UNKNOWN" else "REJECTED", reason=AUTH_REASONS.get(code, f"VALIDATION_REJECTED:{code}" if code is not None else "VALIDATION_REJECTED")),
                    "symbol": selected,
                    "price": _dstr(sized["price"]),
                    "market_bid": _dstr(bid) if bid is not None else None,
                    "market_reference": _dstr(reference) if reference is not None else None,
                    "quantity": _dstr(sized["quantity"]),
                    "notional": _dstr(sized["notional"]),
                    "fee_reserve": _dstr(sized["fee_reserve"]),
                    "planned_exit": planned_exit,
                    "planned_exit_viable": True,
                    "error_code": code,
                }
                return self._persist_validation_guarded(result, deadline_monotonic)
            result = {
                **self._base("PASS", checked=self.clock()),
                "symbol": selected,
                "price": _dstr(sized["price"]),
                "market_bid": _dstr(bid) if bid is not None else None,
                "market_reference": _dstr(reference) if reference is not None else None,
                "quantity": _dstr(sized["quantity"]),
                "notional": _dstr(sized["notional"]),
                "fee_reserve": _dstr(sized["fee_reserve"]),
                "time_in_force": sized["time_in_force"],
                "rules": _jsonable(rules.raw_filters),
                "planned_exit": planned_exit,
                "planned_exit_viable": True,
                "validation_only": True,
            }
            return self._persist_validation_guarded(result, deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_validation_result(selected, checked=self.clock())
        except TypeError:
            raise
        except Exception as exc:
            if _deadline_expired(deadline_monotonic):
                return self._deadline_validation_result(selected, checked=self.clock())
            result = {**self._base("UNKNOWN", reason=f"VALIDATION_EXCEPTION:{type(exc).__name__}"), "symbol": symbol, "price": None, "quantity": None, "notional": None, "fee_reserve": None}
            return self._persist_validation_guarded(result, deadline_monotonic)

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
                args.append(_exact_order_id(exchange_order_id))
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

    def _reset_reason(self) -> str | None:
        row = self.connection.execute(
            "SELECT reason FROM binance_testnet_probe_intents "
            "WHERE probe_kind=? AND reason=? ORDER BY updated_at_utc DESC LIMIT 1",
            (PROBE_LABEL, "TESTNET_RESET_HISTORY_MISSING"),
        ).fetchone()
        return str(row["reason"]) if row is not None else None

    def _submission_count(self, *, connection: sqlite3.Connection | None = None) -> int:
        conn = connection or self.connection
        today = _utc(self.clock()).date().isoformat()
        row = conn.execute(
            "SELECT COUNT(DISTINCT intents.intent_id) "
            "FROM binance_testnet_probe_intents AS intents "
            "LEFT JOIN binance_testnet_probe_events AS events "
            "ON events.intent_id=intents.intent_id AND events.to_state='SUBMITTING' "
            "WHERE intents.probe_kind=? AND intents.created_at_utc >= ? "
            "AND (events.intent_id IS NOT NULL OR intents.state='RESERVED')",
            (PROBE_LABEL, today + "T00:00:00+00:00"),
        ).fetchone()
        return int(row[0] if row else 0)

    def _quote_balance(self) -> Decimal | None:
        row = self.connection.execute(
            "SELECT account_json FROM binance_testnet_gate_connectivity "
            "ORDER BY record_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        try:
            account = json.loads(row[0] or "{}")
        except (TypeError, ValueError):
            return None
        balances = account.get("balances", ()) if isinstance(account, Mapping) else ()
        if not isinstance(balances, Sequence) or isinstance(balances, (str, bytes, bytearray)):
            return None
        for balance in balances:
            if not isinstance(balance, Mapping) or str(balance.get("asset", "")).upper() != "USDT":
                continue
            try:
                # Only quote balance is eligible for admission.  Foreign
                # base-asset inventory in the account is never owned by AXIOM.
                return max(Decimal("0"), _decimal(balance.get("free", "0")))
            except ValueError:
                return None
        return Decimal("0")

    def _ledger_snapshot(
        self,
        *,
        mark_prices: Mapping[str, Decimal] | None = None,
        now: Any = None,
    ) -> dict[str, Any]:
        lots: dict[str, list[list[Decimal]]] = {}
        realized: dict[str, Decimal] = {}
        fees: dict[str, Decimal] = {}
        unknown_fee_assets: set[str] = set()
        realized_today = Decimal("0")
        day = _utc(now or self.clock()).date()
        rows = self.connection.execute(
            "SELECT symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time_utc "
            "FROM binance_testnet_probe_fills ORDER BY trade_time_utc,trade_id"
        ).fetchall()
        for row in rows:
            symbol = str(row["symbol"]).upper()
            base = symbol[:-4] if symbol.endswith("USDT") else ""
            side = str(row["side"]).upper()
            try:
                qty = _decimal(row["quantity"])
                price = _decimal(row["price"])
                quote = _decimal(row["quote_quantity"])
                commission = _decimal(row["commission"])
            except ValueError:
                continue
            if qty <= 0 or price <= 0 or quote < 0 or commission < 0:
                continue
            asset = str(row["commission_asset"] or "").upper()
            fee_quote = (
                commission
                if asset == "USDT"
                else commission * price
                if asset == base
                else Decimal("0")
            )
            if commission > 0 and asset not in {"USDT", base}:
                unknown_fee_assets.add(asset or "<MISSING>")
            fees[symbol] = fees.get(symbol, Decimal("0")) + fee_quote
            symbol_lots = lots.setdefault(symbol, [])
            symbol_realized = realized.get(symbol, Decimal("0"))
            if side == "BUY":
                net_quantity = max(
                    Decimal("0"),
                    qty - (commission if asset == base else Decimal("0")),
                )
                if net_quantity > 0:
                    symbol_lots.append(
                        [
                            net_quantity,
                            quote + (commission if asset == "USDT" else Decimal("0")),
                        ]
                    )
                realized[symbol] = symbol_realized
                continue
            if side != "SELL":
                continue
            remaining = qty
            proceeds = quote - (commission if asset == "USDT" else Decimal("0"))
            trade_realized = Decimal("0")
            while remaining > 0 and symbol_lots:
                lot_quantity, lot_cost = symbol_lots[0]
                matched = min(remaining, lot_quantity)
                delta = (
                    proceeds * (matched / qty)
                    - lot_cost * (matched / lot_quantity)
                    if qty > 0 and lot_quantity > 0
                    else Decimal("0")
                )
                symbol_realized += delta
                trade_realized += delta
                lot_quantity -= matched
                lot_cost -= lot_cost * (matched / (lot_quantity + matched))
                remaining -= matched
                if lot_quantity <= 0:
                    symbol_lots.pop(0)
                else:
                    symbol_lots[0] = [lot_quantity, lot_cost]
            realized[symbol] = symbol_realized
            if _utc(row["trade_time_utc"]).date() == day:
                realized_today += trade_realized
        positions: dict[str, dict[str, Any]] = {}
        for symbol, symbol_lots in lots.items():
            quantity = sum((lot[0] for lot in symbol_lots), Decimal("0"))
            cost_basis = sum((lot[1] for lot in symbol_lots), Decimal("0"))
            if quantity <= 0:
                continue
            mark = (mark_prices or {}).get(symbol)
            unrealized = (
                mark * quantity - cost_basis
                if mark is not None and mark > 0
                else Decimal("0")
            )
            positions[symbol] = {
                "quantity": quantity,
                "cost_basis": cost_basis,
                "realized_pnl": realized.get(symbol, Decimal("0")),
                "fees_quote": fees.get(symbol, Decimal("0")),
                "mark_price": mark,
                "unrealized_pnl": unrealized,

            }
        return {
            "positions": positions,
            "realized_pnl": sum(realized.values(), Decimal("0")),
            "realized_pnl_today": realized_today,
            "fees_quote": sum(fees.values(), Decimal("0")),
            "fee_valuation_available": not unknown_fee_assets,
            "unknown_fee_assets": sorted(unknown_fee_assets),
            "unrealized_pnl": sum(
                (value["unrealized_pnl"] for value in positions.values()),
                Decimal("0"),
            ),
        }
    def _autonomous_peer_snapshot(self, now: Any = None) -> dict[str, Any]:
        """Read the autonomous execution ledger from the shared account DB."""
        empty = {
            "present": False,
            "malformed": False,
            "reason": None,
            "positions": {},
            "inventory": {},
            "held_quote": Decimal("0"),
            "held_buy_exposure": Decimal("0"),
            "submissions": 0,
            "realized": Decimal("0"),
            "realized_today": Decimal("0"),
            "fees_quote": Decimal("0"),
            "fees_today": Decimal("0"),
            "equity_loss": Decimal("0"),
            "unknown_fee": False,
            "unknown_fee_assets": [],
            "block_probe": None,
        }
        names = (
            "binance_execution_order_intents",
            "binance_execution_risk_reservations",
            "binance_execution_fills",
            "binance_execution_positions",
            "binance_execution_control",
            "binance_execution_reconciliation",
        )
        columns = {name: _sqlite_table_columns(self.connection, name) for name in names}
        if all(value is None for value in columns.values()):
            return empty
        required = {
            "binance_execution_order_intents": {
                "intent_id", "intent", "side", "symbol", "quantity",
                "notional", "fee_reserve", "state", "submitted_at", "updated_at",
                "client_order_id", "exchange_order_id",
            },
            "binance_execution_risk_reservations": {
                "intent_id", "symbol", "side", "amount", "reserved_quantity",
                "fee_reserve", "status",
            },
            "binance_execution_fills": {
                "trade_id", "intent_id", "client_order_id", "exchange_order_id",
                "symbol", "side", "quantity", "price", "quote_quantity",
                "commission", "commission_asset", "trade_time", "payload_json",
            },
            "binance_execution_positions": {
                "symbol", "quantity", "cost_basis", "realized_pnl",
                "unrealized_pnl", "fees_quote", "valuation_status",
            },
        }
        optional_required = {
            "binance_execution_control": {"singleton", "state", "pause_reason"},
            "binance_execution_reconciliation": {"key", "status", "details_json"},
        }
        if any(
            columns[name] is not None and not required_columns.issubset(columns[name] or set())
            for name, required_columns in optional_required.items()
        ):
            empty.update(
                present=True,
                malformed=True,
                reason="AUTONOMOUS_STATE_MALFORMED",
                block_probe="AUTONOMOUS_STATE_MALFORMED",
            )
            return empty
        if any(columns[name] is None or not required[name].issubset(columns[name] or set()) for name in required):
            empty.update(
                present=True,
                malformed=True,
                reason="AUTONOMOUS_STATE_MALFORMED",
                block_probe="AUTONOMOUS_STATE_MALFORMED",
            )
            return empty
        state = dict(empty)
        state["present"] = True
        try:
            intents = self.connection.execute(
                "SELECT intent_id,intent,side,symbol,quantity,notional,fee_reserve,state,submitted_at,updated_at,client_order_id,exchange_order_id "
                "FROM binance_execution_order_intents"
            ).fetchall()
            reservations = self.connection.execute(
                "SELECT intent_id,symbol,side,amount,reserved_quantity,fee_reserve,status "
                "FROM binance_execution_risk_reservations"
            ).fetchall()
            fills = self.connection.execute(
                "SELECT trade_id,intent_id,client_order_id,exchange_order_id,symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time,payload_json "
                "FROM binance_execution_fills ORDER BY trade_time,trade_id"
            ).fetchall()
            positions = self.connection.execute(
                "SELECT symbol,quantity,cost_basis,realized_pnl,unrealized_pnl,fees_quote,valuation_status "
                "FROM binance_execution_positions"
            ).fetchall()
        except sqlite3.Error:
            state.update(malformed=True, reason="AUTONOMOUS_STATE_MALFORMED", block_probe="AUTONOMOUS_STATE_MALFORMED")
            return state
        try:
            by_intent = {str(row["intent_id"]): row for row in intents}
            instant = _utc(now or self.clock())
            day = instant.date()
            for intent in intents:
                intent_kind = str(intent["intent"]).upper()
                intent_side = str(intent["side"]).upper()
                if intent_kind not in {"ENTRY", "EXIT"} or (
                    intent_kind == "ENTRY" and intent_side != "BUY"
                ) or (intent_kind == "EXIT" and intent_side != "SELL"):
                    raise ValueError("invalid autonomous intent")
            peer_lots: dict[str, list[list[Decimal]]] = {}
            for row in fills:
                intent = by_intent.get(str(row["intent_id"]))
                if intent is None:
                    raise ValueError("orphan fill")
                if (
                    str(row["symbol"]).upper() != str(intent["symbol"]).upper()
                    or str(row["side"]).upper() != str(intent["side"]).upper()
                    or str(row["client_order_id"] or "") != str(intent["client_order_id"] or "")
                    or (
                        row["exchange_order_id"] not in (None, "")
                        and intent["exchange_order_id"] not in (None, "")
                        and str(row["exchange_order_id"]) != str(intent["exchange_order_id"])
                    )
                ):
                    raise ValueError("fill identity mismatch")
                symbol = _symbol(row["symbol"])
                quantity = _decimal(row["quantity"])
                price = _decimal(row["price"])
                quote = _decimal(row["quote_quantity"])
                commission = _decimal(row["commission"])
                if quantity <= 0 or price <= 0 or quote < 0 or commission < 0:
                    raise ValueError("invalid fill")
                base = symbol[:-4] if symbol.endswith("USDT") else ""
                asset = str(row["commission_asset"] or "").upper()
                fee_quote = commission if asset == "USDT" else commission * price if base and asset == base else Decimal("0")
                if commission > 0 and asset not in {"USDT", base}:
                    state["unknown_fee_assets"].append(asset or "<MISSING>")
                state["fees_quote"] += fee_quote
                if _utc(row["trade_time"]).date() == day:
                    state["fees_today"] += fee_quote
                book = peer_lots.setdefault(symbol, [])
                if str(intent["intent"]).upper() == "ENTRY":
                    net_qty = max(Decimal("0"), quantity - (commission if asset == base else Decimal("0")))
                    if net_qty > 0:
                        book.append([net_qty, quote + (commission if asset == "USDT" else Decimal("0"))])
                elif str(intent["intent"]).upper() == "EXIT":
                    remaining = quantity
                    proceeds = quote - (commission if asset == "USDT" else Decimal("0"))
                    trade_pnl = Decimal("0")
                    while remaining > 0 and book:
                        lot_qty, lot_cost = book[0]
                        matched = min(remaining, lot_qty)
                        trade_pnl += proceeds * (matched / quantity) - lot_cost * (matched / lot_qty)
                        lot_qty -= matched
                        lot_cost -= lot_cost * (matched / (lot_qty + matched))
                        remaining -= matched
                        if lot_qty <= 0:
                            book.pop(0)
                        else:
                            book[0] = [lot_qty, lot_cost]
                    if _utc(row["trade_time"]).date() == day:
                        state["realized_today"] += trade_pnl
            state["unknown_fee_assets"] = sorted(set(state["unknown_fee_assets"]))
            state["unknown_fee"] = bool(state["unknown_fee_assets"])
            for row in positions:
                symbol = _symbol(row["symbol"])
                quantity = _decimal(row["quantity"])
                cost = _decimal(row["cost_basis"])
                realized = _decimal(row["realized_pnl"])
                unrealized_value = _row_value(row, "unrealized_pnl")
                unrealized = Decimal("0") if unrealized_value in (None, "") else _decimal(unrealized_value)
                fees_quote = _decimal(row["fees_quote"])
                if quantity < 0 or cost < 0 or fees_quote < 0:
                    raise ValueError("invalid position")
                if quantity > 0 and symbol != "USDT":
                    state["positions"][symbol] = {
                        "quantity": quantity,
                        "cost_basis": cost,
                        "realized_pnl": realized,
                        "unrealized_pnl": unrealized,
                        "fees_quote": fees_quote,
                    }
                    state["inventory"][symbol] = quantity
                state["realized"] += realized
                state["equity_loss"] += max(Decimal("0"), -realized) + max(Decimal("0"), -unrealized)
                if str(row["valuation_status"]).upper() == "UNKNOWN":
                    state["reason"] = "AUTONOMOUS_UNKNOWN_FEE"
                    state["unknown_fee"] = True
            for row in reservations:
                intent_id = str(row["intent_id"])
                intent = by_intent.get(intent_id)
                if intent is None:
                    raise ValueError("orphan reservation")
                if (
                    str(row["symbol"]).upper() != str(intent["symbol"]).upper()
                    or str(row["side"]).upper() != str(intent["side"]).upper()
                ):
                    raise ValueError("reservation identity mismatch")
                amount = _decimal(row["amount"])
                fee = _decimal(row["fee_reserve"])
                reserved_quantity = _decimal(row["reserved_quantity"])
                if amount < 0 or fee < 0 or reserved_quantity < 0:
                    raise ValueError("invalid reservation")
                if str(row["status"]).upper() == "HELD":
                    if str(row["side"]).upper() == "BUY":
                        state["held_quote"] += amount + fee
                        state["held_buy_exposure"] += amount
            for row in intents:
                if row["submitted_at"] not in (None, "") and _utc(row["submitted_at"]).date() == day:
                    state["submissions"] += 1
            blocker = None
            control_columns = columns["binance_execution_control"]
            if control_columns is not None and {"state", "pause_reason"}.issubset(control_columns):
                control = self.connection.execute(
                    "SELECT state,pause_reason FROM binance_execution_control WHERE singleton=1"
                ).fetchone()
                if control is not None and (
                    str(control["state"]).upper() == "PAUSED"
                    or any(token in str(control["pause_reason"] or "").upper() for token in ("RESET", "UNKNOWN"))
                ):
                    blocker = "AUTONOMOUS_RESET" if "RESET" in str(control["pause_reason"] or "").upper() else "AUTONOMOUS_PAUSED"
            recon_columns = columns["binance_execution_reconciliation"]
            if blocker is None and recon_columns is not None and {"status", "details_json"}.issubset(recon_columns):
                for row in self.connection.execute("SELECT status,details_json FROM binance_execution_reconciliation").fetchall():
                    details = _row_value(row, "details_json", "{}")
                    try:
                        details = json.loads(details or "{}")
                    except (TypeError, ValueError):
                        raise ValueError("invalid reconciliation details")
                    if str(row["status"]).upper() in {"RESET", "FAILURE", "RUNNING"} or (
                        isinstance(details, Mapping) and details.get("reset_detected")
                    ):
                        blocker = "AUTONOMOUS_RESET" if str(row["status"]).upper() == "RESET" or (
                            isinstance(details, Mapping) and details.get("reset_detected")
                        ) else "AUTONOMOUS_UNRESOLVED"
                        break
            states = {str(row["state"]).upper() for row in intents}
            if blocker is None and state["unknown_fee"]:
                blocker = "AUTONOMOUS_UNKNOWN_FEE"
            elif blocker is None and states & {"UNKNOWN", "RESERVED", "SUBMITTING", "ACKNOWLEDGED", "PARTIALLY_FILLED"}:
                blocker = "AUTONOMOUS_UNRESOLVED"
            elif blocker is None and any(str(row["state"]).upper() == "DUST" for row in intents):
                blocker = "AUTONOMOUS_DUST"
            elif blocker is None and any(state["positions"].values()):
                blocker = "AUTONOMOUS_OPEN_POSITION"
            elif blocker is None and any(str(row["status"]).upper() == "HELD" for row in reservations):
                blocker = "AUTONOMOUS_HELD"
            state["block_probe"] = blocker
            state["reason"] = blocker
            return state
        except (KeyError, TypeError, ValueError, InvalidOperation, sqlite3.Error):
            state.update(malformed=True, reason="AUTONOMOUS_STATE_MALFORMED", block_probe="AUTONOMOUS_STATE_MALFORMED")
            return state

    def _probe_risk_snapshot(
        self,
        *,
        candidate_symbol: str | None = None,
        candidate_side: str = "BUY",
        candidate_quantity: Decimal = Decimal("0"),
        candidate_notional: Decimal = Decimal("0"),
        candidate_fee: Decimal = Decimal("0"),
        candidate_deviation_bps: Decimal | None = None,
        mark_prices: Mapping[str, Decimal] | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        conn = connection or self.connection
        held_rows = conn.execute(
            "SELECT amount,fee_reserve,side FROM binance_testnet_probe_reservations "
            "WHERE status='HELD' AND probe_kind=? AND UPPER(side)='BUY'",
            (PROBE_LABEL,),
        ).fetchall()
        held_quote = sum(
            (
                _decimal(_row_value(row, "amount", "0"), Decimal("0"))
                + _decimal(_row_value(row, "fee_reserve", "0"), Decimal("0"))
                for row in held_rows
            ),
            Decimal("0"),
        )
        ledger = self._ledger_snapshot(mark_prices=mark_prices, now=self.clock())
        peer = self._autonomous_peer_snapshot(self.clock())
        positions: dict[str, dict[str, Any]] = {
            symbol: dict(value) for symbol, value in ledger["positions"].items()
        }
        for symbol, value in peer["positions"].items():
            if symbol in positions:
                for key in ("quantity", "cost_basis", "realized_pnl", "unrealized_pnl", "fees_quote"):
                    positions[symbol][key] = positions[symbol].get(key, Decimal("0")) + value.get(key, Decimal("0"))
            else:
                positions[symbol] = dict(value)
        active_positions = len(positions)
        candidate_symbol = _symbol(candidate_symbol) if candidate_symbol else None
        candidate_total = max(Decimal("0"), candidate_notional + candidate_fee)
        candidate_is_new_position = bool(
            str(candidate_side).upper() == "BUY"
            and candidate_symbol
            and candidate_quantity > 0
            and _decimal(positions.get(candidate_symbol, {}).get("quantity", "0"), Decimal("0")) <= 0
        )
        peer_held_quote = _decimal(peer["held_quote"])
        aggregate_exposure = sum(
            (value["cost_basis"] for value in positions.values()),
            Decimal("0"),
        ) + held_quote + peer_held_quote
        realized = ledger["realized_pnl"] + _decimal(peer["realized"])
        realized_today = ledger["realized_pnl_today"] + _decimal(peer["realized_today"])
        fees_quote = ledger["fees_quote"] + _decimal(peer["fees_quote"])
        unrealized = sum(
            (value.get("unrealized_pnl", Decimal("0")) for value in positions.values()),
            Decimal("0"),
        )
        equity = realized + unrealized
        equity_loss = max(Decimal("0"), -realized_today) + max(Decimal("0"), -unrealized)
        submissions = self._submission_count(connection=conn) + int(peer["submissions"])
        quote_balance = self._quote_balance()
        reasons: list[str] = []
        unknown_fee_assets = sorted(set(ledger["unknown_fee_assets"]) | set(peer["unknown_fee_assets"]))
        fee_valuation_available = bool(ledger["fee_valuation_available"]) and not peer["unknown_fee"]
        if str(candidate_side).upper() == "BUY" and not fee_valuation_available:
            reasons.append("FEE_VALUATION_UNAVAILABLE")
        if peer["block_probe"]:
            reasons.append(str(peer["block_probe"]))
        if (
            str(candidate_side).upper() == "BUY"
            and candidate_symbol
            and _decimal(positions.get(candidate_symbol, {}).get("quantity", "0"), Decimal("0")) > 0
        ):
            reasons.append("OWNED_POSITION")
        if held_quote + peer_held_quote + candidate_total > DEFAULT_BINANCE_RISK_ENVELOPE.max_reserved_exposure:
            reasons.append("RESERVED_EXPOSURE")
        if aggregate_exposure + candidate_total > DEFAULT_BINANCE_RISK_ENVELOPE.max_aggregate_exposure:
            reasons.append("AGGREGATE_EXPOSURE")
        if active_positions + int(candidate_is_new_position) > DEFAULT_BINANCE_RISK_ENVELOPE.max_positions:
            reasons.append("MAX_POSITIONS")
        if submissions >= DEFAULT_BINANCE_RISK_ENVELOPE.max_submissions_per_day:
            reasons.append("SUBMISSIONS_PER_DAY")
        if realized_today <= -DEFAULT_BINANCE_RISK_ENVELOPE.realized_loss_entry_stop:
            reasons.append("REALIZED_LOSS")
        if equity_loss >= DEFAULT_BINANCE_RISK_ENVELOPE.equity_loss_entry_stop:
            reasons.append("EQUITY_LOSS")
        if candidate_total > 0 and quote_balance is None:
            reasons.append("QUOTE_BALANCE_UNAVAILABLE")
        elif candidate_total > 0 and quote_balance is not None and candidate_total > quote_balance:
            reasons.append("QUOTE_BALANCE")
        if (
            candidate_deviation_bps is not None
            and candidate_deviation_bps > DEFAULT_BINANCE_RISK_ENVELOPE.max_execution_deviation_bps
        ):
            reasons.append("EXECUTION_DEVIATION_ABOVE_MAX")
        return {
            "limits": _jsonable(DEFAULT_BINANCE_RISK_ENVELOPE.as_dict()),
            "held_exposure": _dstr(held_quote + peer_held_quote),
            "aggregate_exposure": _dstr(aggregate_exposure),
            "open_positions": active_positions,
            "positions": {
                symbol: {
                    key: (
                        _dstr(value[key])
                        if isinstance(value.get(key), Decimal)
                        else value[key]
                    )
                    for key in (
                        "quantity",
                        "cost_basis",
                        "realized_pnl",
                        "fees_quote",
                        "mark_price",
                        "unrealized_pnl",
                    )
                }
                for symbol, value in positions.items()
            },
            "submissions_today": submissions,
            "realized_pnl": _dstr(realized),
            "realized_pnl_today": _dstr(realized_today),
            "fees_quote": _dstr(fees_quote),
            "fee_valuation_available": fee_valuation_available,
            "unknown_fee_assets": unknown_fee_assets,
            "realized_loss": _dstr(max(Decimal("0"), -realized_today)),
            "equity_loss": _dstr(equity_loss),
            "equity_pnl": _dstr(equity),
            "quote_balance": _dstr(quote_balance) if quote_balance is not None else None,
            "reasons": list(dict.fromkeys(reasons)),
            "admissible": not reasons,
        }
    def _probe_projection(self) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? "
            "ORDER BY created_at_utc, intent_id",
            (PROBE_LABEL,),
        ).fetchall()
        risk = self._probe_risk_snapshot()
        if not rows:
            return {
                **self._base("BLOCKED", reason="NOT_STARTED"),
                "intent": None,
                "orders": [],
                "risk_reservation": None,
                "owned_quantity": "0",
                "realized_pnl": "0",
                "risk": risk,
                "reset_detected": False,
                "reset_reason": None,
                "control_path_paused": False,
                "execution_paused": False,
            }
        buy = next((row for row in rows if str(row["side"]).upper() == "BUY"), rows[0])
        exit_row = next((row for row in rows if str(row["side"]).upper() == "SELL"), None)
        reservations = self.connection.execute(
            "SELECT * FROM binance_testnet_probe_reservations "
            "WHERE status='HELD' AND probe_kind=? ORDER BY created_at_utc",
            (PROBE_LABEL,),
        ).fetchall()
        owned = self._owned_quantity(str(buy["symbol"]))
        reset_reason = self._reset_reason()
        if reset_reason is not None:
            status = "UNKNOWN"
            reason = reset_reason
            risk["reasons"] = list(dict.fromkeys((*risk["reasons"], reset_reason)))
            risk["admissible"] = False
        else:
            status = str(
                exit_row["state"]
                if exit_row is not None
                and exit_row["state"] in {"DUST", "EXIT_BELOW_MINIMUM"}
                else buy["state"]
            )
            reason = str(
                exit_row["reason"]
                if status in {"DUST", "EXIT_BELOW_MINIMUM"} and exit_row
                else buy["reason"]
            )
        return {
            **self._base(status, reason=reason),
            "intent": self._intent_projection(buy),
            "exit": self._intent_projection(exit_row) if exit_row is not None else None,
            "orders": [self._intent_projection(row) for row in rows],
            "risk_reservation": [dict(row) for row in reservations],
            "owned_quantity": _dstr(owned),
            "realized_pnl": _dstr(self._realized_pnl(str(buy["symbol"]))),
            "risk": risk,
            "reset_detected": reset_reason is not None,
            "reset_reason": reset_reason,
            "control_path_paused": reset_reason is not None,
            "execution_paused": reset_reason is not None,
        }

    def _binding_matches(self, row: Any) -> bool:
        return (
            str(_row_value(row, "profile_hash", "")) == canonical_sha256(self.profile.projection())
            and str(_row_value(row, "envelope_hash", "")) == DEFAULT_BINANCE_RISK_ENVELOPE.canonical_hash
            and str(_row_value(row, "credential_hash", "")) == credential_fingerprint(self.credentials)
        )

    def probe_status(self) -> dict[str, Any]:
        return self._probe_projection()
    def _deadline_probe_result(self, symbol: Any = None) -> dict[str, Any]:
        """Return a durable blocked verdict without creating an order intent."""
        projection = self._probe_projection()
        if projection.get("reset_detected"):
            return projection
        projection = projection | {"status": "BLOCKED", "reason": AUTO_DEADLINE_EXPIRED}
        self._persist_validation(
            {
                **self._base("BLOCKED", reason=AUTO_DEADLINE_EXPIRED),
                "symbol": symbol,
                "price": None,
                "quantity": None,
                "notional": None,
                "fee_reserve": None,
            }
        )
        return projection

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

    def _submit_venue(
        self,
        *,
        symbol: str,
        side: str,
        quantity: str,
        price: str,
        tif: str,
        client_id: str,
        deadline_monotonic: float | None = None,
    ) -> Any:
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
            client_key = "newClientOrderId"
        if client_key is None:
            raise TypeError("venue LIMIT method must support a client order id")
        kwargs[client_key] = client_id
        _deadline_checkpoint(deadline_monotonic)
        return _invoke(method, kwargs, deadline_monotonic=deadline_monotonic)
    def execute_probe(self, symbol: str | None = None, deadline_monotonic: float | None = None) -> dict[str, Any]:
        # A restarted service may have no explicit credentials of its own.
        # Resolve venue/store credentials before reading durable bindings.
        try:
            _deadline_checkpoint(deadline_monotonic)
            self._credential_value(deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_probe_result(symbol)
        autonomous_peer = self._autonomous_peer_snapshot()
        if autonomous_peer.get("block_probe"):
            return self._probe_projection() | {
                "status": "BLOCKED",
                "reason": autonomous_peer["block_probe"],
            }
        existing = self._latest_intent("BUY")
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(symbol)
        if existing is not None:
            if not self._binding_matches(existing):
                return self._probe_projection() | {"status": "BLOCKED", "reason": "BINDING_CHANGED"}
            return self._probe_projection()
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(symbol)
        connectivity = self.check_connectivity(deadline_monotonic=deadline_monotonic)
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(symbol)
        if connectivity.get("status") != "PASS":
            return self._probe_projection() | {"status": "BLOCKED", "reason": "CONNECTIVITY_" + str(connectivity.get("reason") or "NOT_PASS")}
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(symbol)
        validation = self.validate_order(symbol, deadline_monotonic=deadline_monotonic)
        if validation.get("status") != "PASS":
            reason = str(validation.get("reason") or "NOT_PASS")
            return self._deadline_probe_result(symbol) if reason == AUTO_DEADLINE_EXPIRED else self._probe_projection() | {"status": "BLOCKED", "reason": "VALIDATION_" + reason}
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(symbol)
        selected = _symbol(validation["symbol"])
        price = _decimal(validation["price"])
        quantity = _decimal(validation["quantity"])
        notional = _decimal(validation["notional"])
        fee = _decimal(validation["fee_reserve"])
        tif = str(validation.get("time_in_force", "IOC"))
        client_id = self._new_client_id(selected, "BUY")
        blocked_reason: str | None = None
        intent_id: str | None = None
        try:
            reference = _decimal(validation.get("market_reference"))
        except (TypeError, ValueError):
            reference = None
        try:
            market_bid = _decimal(validation.get("market_bid"))
        except (TypeError, ValueError):
            market_bid = None
        deviation_bps = (
            abs(price - reference) * Decimal("10000") / reference
            if reference is not None and reference > 0
            else None
        )
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result(selected)
        with self._immediate_write() as conn:
            existing = conn.execute(
                "SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? AND side='BUY' "
                "ORDER BY created_at_utc DESC, intent_id DESC LIMIT 1",
                (PROBE_LABEL,),
            ).fetchone()
            if existing is not None:
                if not self._binding_matches(existing):
                    blocked_reason = "BINDING_CHANGED"
                else:
                    return self._probe_projection()
            else:
                risk = self._probe_risk_snapshot(
                    candidate_symbol=selected,
                    candidate_quantity=quantity,
                    candidate_notional=notional,
                    candidate_fee=fee,
                    candidate_deviation_bps=deviation_bps,
                    mark_prices={selected: market_bid} if market_bid is not None else None,
                    connection=conn,
                )
                if not risk["admissible"]:
                    blocked_reason = str(risk["reasons"][0])
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
        if _deadline_expired(deadline_monotonic):
            if intent_id is not None:
                self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED)
                self._release_reservation(intent_id)
            return self._deadline_probe_result(selected)
        if blocked_reason is not None:
            projection = self._probe_projection() | {"status": "BLOCKED", "reason": blocked_reason}
            self._persist_validation(
                {
                    **self._base("BLOCKED", reason=blocked_reason),
                    "symbol": selected,
                    "price": _dstr(price),
                    "quantity": _dstr(quantity),
                    "notional": _dstr(notional),
                    "fee_reserve": _dstr(fee),
                    "risk": projection.get("risk", {}),
                }
            )
            return projection
        if intent_id is None:
            return self._probe_projection()
        if _deadline_expired(deadline_monotonic):
            self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED)
            self._release_reservation(intent_id)
            return self._deadline_probe_result(selected)
        self._transition(intent_id, "SUBMITTING", "before venue call")
        try:
            _deadline_checkpoint(deadline_monotonic)
            response = self._submit_venue(
                symbol=selected,
                side="BUY",
                quantity=_dstr(quantity),
                price=_dstr(price),
                tif=tif,
                client_id=client_id,
                deadline_monotonic=deadline_monotonic,
            )
        except TypeError:
            raise
        except _AutoDeadlineExpired:
            self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED)
            return self._deadline_probe_result(selected)
        except Exception as exc:
            self._transition(intent_id, "UNKNOWN", f"VENUE_EXCEPTION:{type(exc).__name__}")
            return self._probe_projection()
        if _deadline_expired(deadline_monotonic):
            self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED, raw=response)
            return self._deadline_probe_result(selected)
        status = _status(response)
        body = _payload(response)
        if status in {"UNKNOWN", "RATE_LIMIT"}:
            self._transition(intent_id, "UNKNOWN", "AMBIGUOUS_VENUE_RESULT", raw=response)
        elif status == "REJECTED":
            self._transition(intent_id, "REJECTED", "VENUE_REJECTED", raw=response)
            self._release_reservation(intent_id)
        elif (
            isinstance(body, Mapping)
            and _exact_order_id(body.get("orderId")) is not None
            and _authoritative_order_identity_matches(
                {"client_order_id": client_id, "symbol": selected, "side": "BUY"},
                body,
            )
        ):
            # An acknowledgement never establishes a fill; reconciliation does.
            self._transition(intent_id, "ACKNOWLEDGED", "VENUE_ACKNOWLEDGED", exchange_order_id=_exact_order_id(body.get("orderId")), raw=response)
        else:
            self._transition(intent_id, "UNKNOWN", "MALFORMED_VENUE_ACK", raw=response)
        if _deadline_expired(deadline_monotonic):
            self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED, raw=response)
            return self._deadline_probe_result(selected)
        return self._probe_projection()

    def _order_query(self, row: Mapping[str, Any], *, deadline_monotonic: float | None = None) -> Any:
        method = next((getattr(self._venue, name, None) for name in ("query_order", "get_order", "query") if callable(getattr(self._venue, name, None))), None)
        if method is None:
            raise RuntimeError("venue lacks order query")
        # Reconcile by the durable exact client ID; a persisted ACK order ID is
        # only a consistency check against the authoritative response.
        _deadline_checkpoint(deadline_monotonic)
        return _invoke(
            method,
            {"symbol": _row_value(row, "symbol"), "order_id": None, "orig_client_order_id": _row_value(row, "client_order_id")},
            deadline_monotonic=deadline_monotonic,
        )

    def _trades_query(self, row: Mapping[str, Any], *, deadline_monotonic: float | None = None) -> Any:
        method = next((getattr(self._venue, name, None) for name in ("my_trades", "trades", "get_my_trades") if callable(getattr(self._venue, name, None))), None)
        if method is None:
            raise RuntimeError("venue lacks myTrades query")
        _deadline_checkpoint(deadline_monotonic)
        return _invoke(
            method,
            {"symbol": _row_value(row, "symbol"), "order_id": _row_value(row, "exchange_order_id")},
            deadline_monotonic=deadline_monotonic,
        )

    def _fills_for(self, intent_id: str) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM binance_testnet_probe_fills WHERE intent_id=? ORDER BY trade_time_utc,trade_id", (intent_id,)).fetchall()

    def _durable_fill_quantity(self, intent_id: str) -> Decimal:
        return sum(
            (_decimal(fill["quantity"], Decimal("0")) for fill in self._fills_for(intent_id)),
            Decimal("0"),
        )

    def _durable_fee_paid(self, intent_id: str) -> Decimal:
        return sum(
            (_decimal(fill["commission"], Decimal("0")) for fill in self._fills_for(intent_id)),
            Decimal("0"),
        )

    @staticmethod
    def _fill_evidence_sufficient(state: str, cumulative: Decimal, durable: Decimal) -> bool:
        # A zero-execution canceled/expired order has no inventory or fee
        # consequence and can be finalized without a myTrades row.  Every
        # order which claims execution must otherwise have durable trade
        # evidence covering the authoritative cumulative quantity.
        if state in {"CANCELED", "EXPIRED"} and cumulative <= 0:
            return True
        if state in {"FILLED", "PARTIALLY_FILLED"} and durable <= 0:
            return False
        return durable >= cumulative

    def _mark_fill_evidence_pending(
        self,
        row: Mapping[str, Any],
        order_result: Any,
        cumulative: Decimal,
        reason: str,
    ) -> None:
        intent_id = str(row["intent_id"])
        fee_paid = self._durable_fee_paid(intent_id)
        with self._write() as conn:
            current_row = conn.execute(
                "SELECT state,filled_quantity FROM binance_testnet_probe_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if current_row is None:
                return
            current = str(current_row["state"])
            prior = _decimal(current_row["filled_quantity"], Decimal("0"))
            # Authoritative quantity without acceptable trade evidence is not
            # durable accounting; preserve the previously evidenced total.
            conn.execute(
                "UPDATE binance_testnet_probe_intents "
                "SET state=?,filled_quantity=?,fee_paid=?,updated_at_utc=?,raw_json=? "
                "WHERE intent_id=?",
                (
                    "UNKNOWN",
                    _dstr(prior),
                    _dstr(fee_paid),
                    _utc(self.clock()).isoformat(),
                    _json(order_result),
                    intent_id,
                ),
            )
            self._event(intent_id, current, "UNKNOWN", reason, order_result)

    def _persist_trades(
        self,
        row: Mapping[str, Any],
        result: Any,
        *,
        expected_order_id: Any = None,
        cumulative: Decimal | None = None,
    ) -> bool:
        body = _payload(result)
        trades = body.get("trades", ()) if isinstance(body, Mapping) else body
        expected_order_id = _exact_order_id(expected_order_id)
        if (
            expected_order_id is None
            or not isinstance(trades, Sequence)
            or isinstance(trades, (str, bytes, bytearray))
        ):
            return False
        expected_symbol = _row_value(row, "symbol")
        if not isinstance(expected_symbol, str) or not expected_symbol:
            return False
        intent_id = str(_row_value(row, "intent_id"))
        candidates: list[tuple[str, Mapping[str, Any], Decimal, Decimal, Decimal, Decimal]] = []
        seen: set[str] = set()
        for trade in list(trades)[:128]:
            if not isinstance(trade, Mapping):
                continue
            trade_order_id = _exact_order_id(trade.get("orderId", trade.get("order_id")))
            if trade_order_id != expected_order_id:
                continue
            trade_symbol = trade.get("symbol")
            if not isinstance(trade_symbol, str) or trade_symbol != expected_symbol:
                continue
            trade_id = _exact_trade_id(trade.get("tradeId", trade.get("trade_id", trade.get("id"))))
            if trade_id is None or trade_id in seen:
                continue
            seen.add(trade_id)
            raw_quote = trade.get("quoteQty", trade.get("quote_quantity"))
            raw_commission = trade.get("commission", trade.get("fee"))
            if raw_quote is None or raw_commission is None:
                continue
            try:
                qty = _decimal(trade.get("qty", trade.get("quantity")))
                price = _decimal(trade.get("price"))
                quote = _decimal(raw_quote)
                commission = _decimal(raw_commission)
            except (TypeError, ValueError):
                continue
            if qty <= 0 or price <= 0 or quote < 0 or commission < 0:
                continue
            candidates.append((trade_id, trade, qty, price, quote, commission))
        if not candidates:
            return False
        prior = self._durable_fill_quantity(intent_id)
        if cumulative is not None and (cumulative < 0 or prior > cumulative):
            return False
        with self._write() as conn:
            existing_ids: set[str] = set()
            for trade_id, trade, qty, price, quote, commission in candidates:
                existing = conn.execute(
                    "SELECT intent_id,quantity,price,quote_quantity,commission FROM binance_testnet_probe_fills WHERE trade_id=?",
                    (trade_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["intent_id"]) != intent_id
                        or _decimal(existing["quantity"]) != qty
                        or _decimal(existing["price"]) != price
                        or _decimal(existing["quote_quantity"]) != quote
                        or _decimal(existing["commission"]) != commission
                    ):
                        return False
                    existing_ids.add(trade_id)
            new_total = prior + sum(
                (item[2] for item in candidates if item[0] not in existing_ids),
                Decimal("0"),
            )
            if cumulative is not None and new_total != cumulative:
                return False
            for trade_id, trade, qty, price, quote, commission in candidates:
                conn.execute(
                    "INSERT OR IGNORE INTO binance_testnet_probe_fills(trade_id,intent_id,environment,source,probe_kind,symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time_utc,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        trade_id,
                        intent_id,
                        BINANCE_SPOT_TESTNET,
                        SOURCE,
                        PROBE_LABEL,
                        expected_symbol,
                        _row_value(row, "side"),
                        _dstr(qty),
                        _dstr(price),
                        _dstr(quote),
                        _dstr(commission),
                        str(trade.get("commissionAsset", trade.get("commission_asset", ""))).upper()[:24] or None,
                        _utc(trade.get("time", trade.get("timestamp", self.clock()))).isoformat(),
                        _json(trade),
                    ),
                )
        return True

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
    def _validated_authoritative_cumulative(
        self,
        row: Mapping[str, Any],
        body: Mapping[str, Any],
    ) -> tuple[Decimal | None, str]:
        """Validate exchange cumulative execution before importing trades."""
        if "executedQty" not in body and "executed_quantity" not in body:
            return None, "MALFORMED_AUTHORITATIVE_QUANTITY"
        try:
            cumulative = _decimal(body.get("executedQty", body.get("executed_quantity")))
            requested = _decimal(_row_value(row, "quantity"))
        except (TypeError, ValueError):
            return None, "MALFORMED_AUTHORITATIVE_QUANTITY"
        if cumulative < 0 or cumulative > requested:
            return None, "MALFORMED_AUTHORITATIVE_QUANTITY"
        if self._durable_fill_quantity(str(_row_value(row, "intent_id"))) > cumulative:
            return None, "AUTHORITATIVE_FILL_QUANTITY_INCOMPATIBLE"
        return cumulative, ""

    def _apply_authoritative(self, row: Mapping[str, Any], order_result: Any) -> None:
        body = _payload(order_result)
        if not isinstance(body, Mapping):
            self._transition(str(row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
            return
        expected_order_id = _exact_order_id(body.get("orderId"))
        stored_order_id = _exact_order_id(_row_value(row, "exchange_order_id"))
        if (
            expected_order_id is None
            or (stored_order_id is not None and expected_order_id != stored_order_id)
            or not _authoritative_order_identity_matches(row, body)
        ):
            self._transition(str(row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
            return
        state = str(body.get("status", "")).upper()
        known = {
            "NEW": "ACKNOWLEDGED",
            "ACKNOWLEDGED": "ACKNOWLEDGED",
            "PARTIALLY_FILLED": "PARTIALLY_FILLED",
            "PENDING_CANCEL": "PENDING_CANCEL",
            "FILLED": "FILLED",
            "CANCELED": "CANCELED",
            "CANCELLED": "CANCELED",
            "EXPIRED": "EXPIRED",
            "EXPIRED_IN_MATCH": "EXPIRED",
            "REJECTED": "REJECTED",
        }
        target = known.get(state)
        if target is None:
            self._transition(str(row["intent_id"]), "UNKNOWN", "UNKNOWN_AUTHORITATIVE_STATE", raw=order_result)
            return
        cumulative, cumulative_reason = self._validated_authoritative_cumulative(row, body)
        if cumulative is None:
            self._transition(str(row["intent_id"]), "UNKNOWN", cumulative_reason, raw=order_result)
            return
        durable = self._durable_fill_quantity(str(row["intent_id"]))
        if not self._fill_evidence_sufficient(target, cumulative, durable):
            self._mark_fill_evidence_pending(
                row,
                order_result,
                cumulative,
                "AUTHORITATIVE_FILL_EVIDENCE_INCOMPLETE",
            )
            return
        current = str(row["state"])
        if current in TERMINAL and current not in {"DUST", "EXIT_BELOW_MINIMUM"}:
            target = current
        elif current == "PARTIALLY_FILLED" and target in {"ACKNOWLEDGED"}:
            target = current
        fee_paid = self._durable_fee_paid(str(row["intent_id"]))
        remaining = max(Decimal("0"), _decimal(row["quantity"]) - cumulative)
        with self._write() as conn:
            conn.execute("UPDATE binance_testnet_probe_intents SET state=?,exchange_order_id=COALESCE(?,exchange_order_id),filled_quantity=?,fee_paid=?,updated_at_utc=?,raw_json=? WHERE intent_id=?", (target, expected_order_id, _dstr(cumulative), _dstr(fee_paid), _utc(self.clock()).isoformat(), _json(order_result), row["intent_id"]))
            self._event(str(row["intent_id"]), current, target, "authoritative order", order_result)
            if target in TERMINAL or target == "FILLED":
                stamp = _utc(self.clock()).isoformat()
                if str(row["side"]).upper() == "BUY":
                    amount = remaining * _decimal(row["price"])
                    conn.execute(
                        "UPDATE binance_testnet_probe_reservations "
                        "SET amount=?,fee_reserve=?,reserved_quantity='0',status='RELEASED',released_at_utc=? "
                        "WHERE intent_id=? AND status='HELD'",
                        (_dstr(amount), _dstr(amount * FEE_RATE), stamp, row["intent_id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE binance_testnet_probe_reservations "
                        "SET reserved_quantity=?,status='RELEASED',released_at_utc=? "
                        "WHERE intent_id=? AND status='HELD'",
                        (_dstr(remaining), stamp, row["intent_id"]),
                    )
            elif str(row["side"]).upper() == "BUY":
                amount = remaining * _decimal(row["price"])
                conn.execute("UPDATE binance_testnet_probe_reservations SET amount=?,fee_reserve=?,status='HELD' WHERE intent_id=? AND status='HELD'", (_dstr(amount), _dstr(amount * FEE_RATE), row["intent_id"]))
            else:
                conn.execute("UPDATE binance_testnet_probe_reservations SET reserved_quantity=? WHERE intent_id=? AND status='HELD'", (_dstr(remaining), row["intent_id"]))

    def _make_exit(self, buy: Mapping[str, Any], *, deadline_monotonic: float | None = None) -> None:
        _deadline_checkpoint(deadline_monotonic)
        symbol = str(buy["symbol"])
        owned = self._owned_quantity(symbol)
        if owned <= 0:
            return
        latest_sell = self._latest_intent("SELL")
        if latest_sell is not None and str(latest_sell["state"]).upper() not in TERMINAL:
            return
        try:
            _deadline_checkpoint(deadline_monotonic)
            exchange_result = self._public(
                ("exchange_info", "get_exchange_info"),
                deadline_monotonic=deadline_monotonic,
                symbol=symbol,
            )
            infos = self._exchange_symbols(_payload(exchange_result))
            info = next((item for item in infos if str(item.get("symbol", "")).upper() == symbol), None)
            if info is None:
                raise ValueError("SYMBOL_METADATA_UNAVAILABLE")
            _deadline_checkpoint(deadline_monotonic)
            _, _, bid, reference = self._market(symbol, deadline_monotonic=deadline_monotonic)
            if bid is None or bid <= 0:
                raise ValueError("BID_PRICE_UNAVAILABLE")
            ask = bid

            rules = SymbolRules.from_exchange_info(info)
            price = rules.round_price(ask, "SELL")
            if reference is not None and reference > 0:
                deviation_bps = abs(price - reference) * Decimal("10000") / reference
                percent_reasons: list[str] = []
                if rules.percent_multiplier_up and price > reference * rules.percent_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_ABOVE_MAX")
                if rules.percent_multiplier_down and price < reference * rules.percent_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BELOW_MIN")
                if rules.ask_multiplier_up and price > reference * rules.ask_multiplier_up:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_ABOVE_MAX")
                if rules.ask_multiplier_down and price < reference * rules.ask_multiplier_down:
                    percent_reasons.append("PERCENT_PRICE_BY_SIDE_BELOW_MIN")
                if deviation_bps > DEFAULT_BINANCE_RISK_ENVELOPE.max_execution_deviation_bps:
                    percent_reasons.append("EXECUTION_DEVIATION_ABOVE_MAX")
                if percent_reasons:
                    return
            elif any(
                value and value > 0
                for value in (
                    rules.percent_multiplier_up,
                    rules.percent_multiplier_down,
                    rules.ask_multiplier_up,
                    rules.ask_multiplier_down,
                )
            ):
                return
            else:
                deviation_bps = None
            sized = size_limit_order(rules, "SELL", price, owned, envelope=DEFAULT_BINANCE_RISK_ENVELOPE, fee_rate=FEE_RATE, available_inventory=owned)
            if not self._reconcile_owned_buy_authority(dict(buy), deadline_monotonic=deadline_monotonic):
                return
            refreshed_buy = self._latest_intent("BUY")
            if refreshed_buy is None or str(refreshed_buy["state"]).upper() not in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"}:
                return
            owned = self._owned_quantity(str(refreshed_buy["symbol"]))
            if owned <= 0:
                return
            sized = size_limit_order(rules, "SELL", price, owned, envelope=DEFAULT_BINANCE_RISK_ENVELOPE, fee_rate=FEE_RATE, available_inventory=owned)
            if not sized.valid or sized.quantity <= 0:
                reason = ";".join(sized.reasons) or "EXIT_BELOW_MINIMUM"
                self._insert_intent(symbol=symbol, side="SELL", price=price, quantity=Decimal("0"), notional=Decimal("0"), fee_reserve=Decimal("0"), tif="IOC", client_id=self._new_client_id(symbol, "SELL"), state="DUST")
                self._transition(str(self._latest_intent("SELL")["intent_id"]), "DUST", reason)
                self._release_reservation(str(self._latest_intent("SELL")["intent_id"]))
                return
            if self._submission_count() >= DEFAULT_BINANCE_RISK_ENVELOPE.max_submissions_per_day:
                return
            exit_client_id = f"AXIOM-TESTNET-EXIT-{uuid.uuid4().hex[:16]}"
            intent_id = self._insert_intent(symbol=symbol, side="SELL", price=sized.price, quantity=sized.quantity, notional=sized.notional, fee_reserve=Decimal("0"), tif="IOC" if not rules.time_in_force or "IOC" in rules.time_in_force else "FOK", client_id=exit_client_id)
            try:
                _deadline_checkpoint(deadline_monotonic)
            except _AutoDeadlineExpired:
                self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED)
                self._release_reservation(intent_id)
                raise
            self._transition(intent_id, "SUBMITTING", "before venue call")
            try:
                _deadline_checkpoint(deadline_monotonic)
                response = self._submit_venue(
                    symbol=symbol,
                    side="SELL",
                    quantity=_dstr(sized.quantity),
                    price=_dstr(sized.price),
                    tif="IOC" if not rules.time_in_force or "IOC" in rules.time_in_force else "FOK",
                    client_id=exit_client_id,
                    deadline_monotonic=deadline_monotonic,
                )
            except TypeError:
                raise
            except _AutoDeadlineExpired:
                self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED)
                raise
            except Exception as exc:
                self._transition(intent_id, "UNKNOWN", f"VENUE_EXCEPTION:{type(exc).__name__}")
                return
            if _deadline_expired(deadline_monotonic):
                self._transition(intent_id, "UNKNOWN", AUTO_DEADLINE_EXPIRED, raw=response)
                raise _AutoDeadlineExpired
            body = _payload(response)
            if _status(response) in {"UNKNOWN", "RATE_LIMIT"}:
                self._transition(intent_id, "UNKNOWN", "AMBIGUOUS_VENUE_RESULT", raw=response)
            elif _status(response) == "REJECTED":
                self._transition(intent_id, "REJECTED", "VENUE_REJECTED", raw=response)
                self._release_reservation(intent_id)
            elif (
                isinstance(body, Mapping)
                and _exact_order_id(body.get("orderId")) is not None
                and _authoritative_order_identity_matches(
                    {"client_order_id": exit_client_id, "symbol": symbol, "side": "SELL"},
                    body,
                )
            ):
                self._transition(intent_id, "ACKNOWLEDGED", "VENUE_ACKNOWLEDGED", exchange_order_id=_exact_order_id(body.get("orderId")), raw=response)
            else:
                self._transition(intent_id, "UNKNOWN", "MALFORMED_VENUE_ACK", raw=response)
        except TypeError:
            raise
        except _AutoDeadlineExpired:
            raise
        except Exception as exc:
            # Do not fabricate an exit or close the buy when market metadata is
            # unavailable; the owned amount remains visible for reconciliation.
            return
    def _rehold_buy_reservation(self, buy: Mapping[str, Any]) -> None:
        intent_id = str(_row_value(buy, "intent_id"))
        symbol = str(_row_value(buy, "symbol"))
        with self._write() as conn:
            row = conn.execute(
                "SELECT symbol,side,amount,fee_reserve FROM binance_testnet_probe_reservations WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if (
                row is None
                or str(row["side"]).upper() != "BUY"
                or str(row["symbol"]).upper() != symbol.upper()
            ):
                return
            amount = max(_decimal(row["amount"], Decimal("0")), _decimal(_row_value(buy, "notional"), Decimal("0")))
            fee_reserve = max(_decimal(row["fee_reserve"], Decimal("0")), _decimal(_row_value(buy, "fee_reserve"), Decimal("0")))
            conn.execute(
                "UPDATE binance_testnet_probe_reservations "
                "SET amount=?,fee_reserve=?,reserved_quantity='0',status='HELD',released_at_utc=NULL "
                "WHERE intent_id=? AND UPPER(side)='BUY' AND UPPER(symbol)=?",
                (_dstr(amount), _dstr(fee_reserve), intent_id, symbol.upper()),
            )

    def _reconcile_owned_buy_authority(
        self,
        buy: Mapping[str, Any],
        *,
        deadline_monotonic: float | None = None,
    ) -> bool:
        """Require fresh exact order/trade evidence before an owned SELL."""
        intent_id = str(_row_value(buy, "intent_id"))

        def unknown(reason: str, raw: Any) -> bool:
            self._rehold_buy_reservation(buy)
            self._transition(intent_id, "UNKNOWN", reason, raw=raw)
            return False

        try:
            _deadline_checkpoint(deadline_monotonic)
            order_result = self._order_query(buy, deadline_monotonic=deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
            if _status(order_result) != "OK":
                return unknown("TESTNET_RESET_HISTORY_MISSING", order_result)
            authoritative = _payload(order_result)
            if not isinstance(authoritative, Mapping):
                return unknown("TESTNET_RESET_HISTORY_MISSING", order_result)
            expected_order_id = _exact_order_id(authoritative.get("orderId"))
            stored_order_id = _exact_order_id(_row_value(buy, "exchange_order_id"))
            if (
                expected_order_id is None
                or (stored_order_id is not None and expected_order_id != stored_order_id)
                or not _authoritative_order_identity_matches(buy, authoritative)
            ):
                return unknown("TESTNET_RESET_HISTORY_MISSING", order_result)
            trade_row = dict(buy)
            trade_row["exchange_order_id"] = expected_order_id
            _deadline_checkpoint(deadline_monotonic)
            trades_result = self._trades_query(trade_row, deadline_monotonic=deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
            if _status(trades_result) != "OK":
                return unknown("TESTNET_RESET_HISTORY_MISSING", trades_result)
            cumulative, cumulative_reason = self._validated_authoritative_cumulative(buy, authoritative)
            if cumulative is None:
                return unknown("TESTNET_RESET_HISTORY_MISSING", {"reason": cumulative_reason, "order": order_result})
            if not self._persist_trades(
                trade_row,
                trades_result,
                expected_order_id=expected_order_id,
                cumulative=cumulative,
            ):
                return unknown("TESTNET_RESET_HISTORY_MISSING", trades_result)
            _deadline_checkpoint(deadline_monotonic)
            self._apply_authoritative(trade_row, order_result)
            refreshed = self._latest_intent("BUY")
            return refreshed is not None and str(refreshed["state"]).upper() in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"} and self._owned_quantity(str(refreshed["symbol"])) > 0
        except TypeError:
            raise
        except _AutoDeadlineExpired:
            raise
        except Exception as exc:
            return unknown("TESTNET_RESET_HISTORY_MISSING", {"reason": f"RECONCILE_EXCEPTION:{type(exc).__name__}"})
    def _reconcile_probe_unlocked(self, deadline_monotonic: float | None = None) -> dict[str, Any]:

        # Rehydrate credentials from the venue or credential store before any
        # persisted binding is trusted after a restart.
        try:
            _deadline_checkpoint(deadline_monotonic)
            self._credential_value(deadline_monotonic)
            _deadline_checkpoint(deadline_monotonic)
        except _AutoDeadlineExpired:
            return self._deadline_probe_result()
        rows = self.connection.execute("SELECT * FROM binance_testnet_probe_intents WHERE probe_kind=? ORDER BY created_at_utc, intent_id", (PROBE_LABEL,)).fetchall()
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        # A reset verdict is durable and terminal for the strict control path.
        # Do not query or mutate the venue again, and never blind-resubmit.
        if self._reset_reason() is not None:
            return self._probe_projection()
        if not rows:
            return self._probe_projection()
        if any(not self._binding_matches(row) for row in rows):
            return self._probe_projection() | {"status": "BLOCKED", "reason": "BINDING_CHANGED"}
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        connectivity = self.check_connectivity(deadline_monotonic=deadline_monotonic)
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        if connectivity.get("status") != "PASS":
            projection = self._probe_projection()
            connectivity_reason = str(connectivity.get("reason") or "NOT_PASS")
            return projection | {
                "status": "BLOCKED",
                "reason": "CONNECTIVITY_" + connectivity_reason,
            }
        for row in rows:
            current = str(row["state"])
            if current in {"DUST", "REJECTED", "FILLED"}:
                continue
            try:
                _deadline_checkpoint(deadline_monotonic)
                order_result = self._order_query(dict(row), deadline_monotonic=deadline_monotonic)
                _deadline_checkpoint(deadline_monotonic)
                if _status(order_result) != "OK":
                    code = _error_code(order_result)
                    if (
                        code == -2013
                        and current in {"SUBMITTING", "ACKNOWLEDGED", "UNKNOWN"}
                    ):
                        self._transition(
                            str(row["intent_id"]),
                            "UNKNOWN",
                            "TESTNET_RESET_HISTORY_MISSING",
                            raw=order_result,
                        )
                        break
                    self._transition(
                        str(row["intent_id"]),
                        "UNKNOWN",
                        "AUTHORITATIVE_ORDER_UNAVAILABLE",
                        raw=order_result,
                    )
                    continue
                authoritative = _payload(order_result)
                if not isinstance(authoritative, Mapping):
                    self._transition(str(row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
                    continue
                expected_order_id = _exact_order_id(authoritative.get("orderId"))
                stored_order_id = _exact_order_id(_row_value(row, "exchange_order_id"))
                if (
                    expected_order_id is None
                    or (stored_order_id is not None and expected_order_id != stored_order_id)
                    or not _authoritative_order_identity_matches(row, authoritative)
                ):
                    self._transition(str(row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
                    continue
                trade_row = dict(row)
                trade_row["exchange_order_id"] = expected_order_id
                _deadline_checkpoint(deadline_monotonic)
                trades_result = self._trades_query(trade_row, deadline_monotonic=deadline_monotonic)
                _deadline_checkpoint(deadline_monotonic)
                if _status(trades_result) != "OK":
                    self._transition(str(row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_TRADES_UNAVAILABLE", raw=trades_result)
                    continue
                _deadline_checkpoint(deadline_monotonic)
                cumulative, cumulative_reason = self._validated_authoritative_cumulative(trade_row, authoritative)
                if cumulative is None:
                    self._transition(str(row["intent_id"]), "UNKNOWN", cumulative_reason, raw=order_result)
                    continue
                self._persist_trades(
                    trade_row,
                    trades_result,
                    expected_order_id=expected_order_id,
                    cumulative=cumulative,
                )
                _deadline_checkpoint(deadline_monotonic)
                self._apply_authoritative(trade_row, order_result)
            except TypeError:
                raise
            except _AutoDeadlineExpired:
                return self._deadline_probe_result()
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        buy = self._latest_intent("BUY")
        if buy is not None and str(buy["state"]).upper() in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"}:
            try:
                self._make_exit(dict(buy), deadline_monotonic=deadline_monotonic)
            except _AutoDeadlineExpired:
                return self._deadline_probe_result()
        # Re-read after a possible exit submission, then reconcile newly-created
        # exits in the same call without issuing a duplicate mutation.
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        exit_row = self._latest_intent("SELL")
        if exit_row is not None and str(exit_row["state"]) not in {"DUST", "REJECTED", "CANCELED", "EXPIRED", "FILLED"}:
            try:
                _deadline_checkpoint(deadline_monotonic)
                order_result = self._order_query(dict(exit_row), deadline_monotonic=deadline_monotonic)
                _deadline_checkpoint(deadline_monotonic)
                if _status(order_result) == "OK":
                    authoritative = _payload(order_result)
                    if not isinstance(authoritative, Mapping):
                        self._transition(str(exit_row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
                    else:
                        expected_order_id = _exact_order_id(authoritative.get("orderId"))
                        stored_order_id = _exact_order_id(_row_value(exit_row, "exchange_order_id"))
                        if (
                            expected_order_id is None
                            or (stored_order_id is not None and expected_order_id != stored_order_id)
                            or not _authoritative_order_identity_matches(exit_row, authoritative)
                        ):
                            self._transition(str(exit_row["intent_id"]), "UNKNOWN", "MALFORMED_AUTHORITATIVE_ORDER", raw=order_result)
                        else:
                            trade_row = dict(exit_row)
                            trade_row["exchange_order_id"] = expected_order_id
                            _deadline_checkpoint(deadline_monotonic)
                            trades_result = self._trades_query(trade_row, deadline_monotonic=deadline_monotonic)
                            _deadline_checkpoint(deadline_monotonic)
                            if _status(trades_result) != "OK":
                                self._transition(str(exit_row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_TRADES_UNAVAILABLE", raw=trades_result)
                            else:
                                cumulative, cumulative_reason = self._validated_authoritative_cumulative(exit_row, authoritative)
                                if cumulative is None:
                                    self._transition(str(exit_row["intent_id"]), "UNKNOWN", cumulative_reason, raw=order_result)
                                else:
                                    self._persist_trades(
                                        trade_row,
                                        trades_result,
                                        expected_order_id=expected_order_id,
                                        cumulative=cumulative,
                                    )
                                    _deadline_checkpoint(deadline_monotonic)
                                    self._apply_authoritative(trade_row, order_result)
                else:
                    self._transition(str(exit_row["intent_id"]), "UNKNOWN", "AUTHORITATIVE_ORDER_UNAVAILABLE", raw=order_result)
            except TypeError:
                raise
            except _AutoDeadlineExpired:
                return self._deadline_probe_result()
            except Exception as exc:
                self._transition(str(exit_row["intent_id"]), "UNKNOWN", f"RECONCILE_EXCEPTION:{type(exc).__name__}")
        if _deadline_expired(deadline_monotonic):
            return self._deadline_probe_result()
        projection = self._probe_projection()
        if projection.get("exit") and projection["exit"].get("state") == "FILLED":
            projection["realized_pnl"] = _dstr(self._realized_pnl(str(projection["intent"]["symbol"])))
        return projection

    def reconcile_probe(self, deadline_monotonic: float | None = None) -> dict[str, Any]:
        with self._operation_lock:
            return self._reconcile_probe_unlocked(deadline_monotonic=deadline_monotonic)

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

