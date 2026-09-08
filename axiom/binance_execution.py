"""Durable, isolated Binance Spot execution orchestration.

This module deliberately owns a separate ``binance_execution_*`` SQLite schema.
It never uses the Polymarket control rows and it never holds a SQLite transaction
while calling a venue.  Values crossing the execution boundary are represented
as Decimal strings in SQLite; credentials are only used to derive a one-way
fingerprint and are never persisted.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import inspect
import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Iterable, Mapping, Sequence

from .binance_research import CryptoExecutionBinding
from .binance_risk import (
    DEFAULT_BINANCE_RISK_ENVELOPE,
    BinanceRiskEnvelope,
    PendingOrder,
    RiskSnapshot,
    SymbolRules,
    assess_entry,
    assess_exit,
    size_limit_order,
)
from .binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceCredentialRef,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
    BinanceSpotEnvironment,
    BinanceSpotResult,
    BinanceSpotRESTClient,
    canonical_sha256,
    credential_fingerprint,
    validate_spot_venue_identity,
)
from .storage import AxiomStore

UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
_TESTNET_CREDENTIAL_REF = BinanceCredentialRef(
    instance="binance-testnet",
    environment=BINANCE_SPOT_TESTNET,
)
QUOTE_ASSET = "USDT"
SCHEMA_VERSION = "binance-execution-v1"
ENABLE_CONFIRMATION = "ENABLE BINANCE AUTO CANARY"
ENABLE_TESTNET_CONFIRMATION = "ENABLE BINANCE TESTNET AUTO CANARY"

INTENT = "INTENT"
RESERVED = "RESERVED"
SUBMITTING = "SUBMITTING"
ACKNOWLEDGED = "ACKNOWLEDGED"
REJECTED = "REJECTED"
UNKNOWN = "UNKNOWN"
PARTIALLY_FILLED = "PARTIALLY_FILLED"
FILLED = "FILLED"
CANCELED = "CANCELED"
EXPIRED = "EXPIRED"
TERMINAL = frozenset({REJECTED, FILLED, CANCELED, EXPIRED})
_RISK_RESERVATION_COLUMNS = (
    "reservation_id",
    "intent_id",
    "symbol",
    "side",
    "amount",
    "reserved_quantity",
    "fee_reserve",
    "status",
    "created_at",
    "released_at",
)
_MISSING_RESERVATION = object()


DISABLED = "DISABLED"
ARMED = "ARMED"
PAUSED = "PAUSED"
DISARMED = "DISARMED"
KILLED = "KILLED"


def _utc(value: Any = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        number = float(value)
        if abs(number) > 100_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, UTC)
        except (OverflowError, OSError, ValueError):
            return datetime.now(UTC)
    text = str(value).strip()
    if not text:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC)
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def _iso(value: Any = None) -> str:
    return _utc(value).isoformat()


def _dec(value: Any, default: Any = ZERO) -> Decimal:
    if value is None or value == "":
        value = default
    if isinstance(value, bool):
        raise ValueError("boolean is not a decimal")
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal: {value!r}") from exc
    if not out.is_finite():
        raise ValueError("decimal must be finite")
    return out


def _dstr(value: Any) -> str:
    return format(_dec(value), "f")


def _sqlite_table_columns(connection: sqlite3.Connection, table: str) -> set[str] | None:
    """Return a peer table's columns, or ``None`` when it is absent/malformed."""
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if exists is None:
            return None
        rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
        columns = {str(row[1]) for row in rows}
        return columns or set()
    except sqlite3.Error:
        return set()


def _json(value: Any) -> str:
    secret_words = ("secret", "password", "token", "credential", "api_key", "apikey", "authorization")
    def convert(item: Any, key: str | None = None) -> Any:
        if key and any(word in key.lower() for word in secret_words):
            return "<redacted>"
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, datetime):
            return _iso(item)
        if isinstance(item, Mapping):
            return {str(k): convert(v, str(k)) for k, v in item.items()}
        if isinstance(item, (tuple, list)):
            return [convert(v) for v in item]
        if isinstance(item, set):
            return sorted(convert(v) for v in item)
        if hasattr(item, "value") and not isinstance(item, (str, bytes)):
            return convert(item.value, key)
        return item
    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _load(value: Any, default: Any = None) -> Any:
    """Decode a durable JSON column without double-encoding mappings."""
    if value is None:
        return default
    if isinstance(value, (Mapping, list, tuple, int, float, bool)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _canonical(value: Any, default: Any = None) -> Any:
    """Return the exact JSON-safe value used for durable provenance."""
    if value is None:
        return default
    try:
        return _load(_json(value), default)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default

def _env(value: Any) -> str:
    if isinstance(value, BinanceSpotEnvironment):
        return value.value
    text = str(value or PAPER).strip()
    aliases = {"TESTNET": BINANCE_SPOT_TESTNET, "LIVE": BINANCE_SPOT_LIVE, "PAPER": PAPER}
    return aliases.get(text.upper(), text)


def _symbol(value: Any) -> str:
    text = str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()
    if not text:
        raise ValueError("symbol is required")
    return text

def _status(result: Any) -> str:
    if isinstance(result, BinanceSpotResult):
        return str(result.status).upper()
    if isinstance(result, Mapping):
        value = result.get("status")
        if value is not None:
            text = str(value).upper()
            if text in {"OK", "ACCEPTED", "SUCCESS"}:
                return "OK"
            if text in {"REJECTED", "ERROR", "FAILED", "FAILURE", "AUTH", "AUTH_ERROR", "INVALID_TIMESTAMP"}:
                return REJECTED
            if text in {"UNKNOWN", "TIMEOUT", "DISCONNECTED"}:
                return UNKNOWN
            if text in {"RATE_LIMIT", "RATELIMIT", "TOO_MANY_REQUESTS"}:
                return "RATE_LIMIT"
            if text in {
                "NEW",
                "ACKNOWLEDGED",
                "PARTIALLY_FILLED",
                "FILLED",
                "CANCELED",
                "CANCELLED",
                "EXPIRED",
            }:
                return "OK"
            return "MALFORMED"
        if "code" in result and result.get("code") not in (None, 0, "0"):
            try:
                code = int(result.get("code"))
            except (TypeError, ValueError):
                code = 0
            return UNKNOWN if code in {-1000, -1006, -1007} else REJECTED
        return "OK"
    value = getattr(result, "status", None)
    if value is not None:
        return str(getattr(value, "value", value)).upper()
    return "UNKNOWN"


_EXCHANGE_ORDER_STATES = frozenset(
    {"NEW", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CANCELLED", "EXPIRED"}
)


def _authoritative_order_state(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("status")
    if value is None:
        return None
    state = str(value).upper()
    return state if state in _EXCHANGE_ORDER_STATES else None
    return "UNKNOWN"


def _payload(result: Any) -> Any:
    if isinstance(result, BinanceSpotResult):
        return result.payload
    if isinstance(result, Mapping):
        payload = result.get("payload")
        return payload if payload is not None else result
    return getattr(result, "payload", result)
def _canonical_order_id(value: Any) -> str | None:
    """Return Binance's canonical positive decimal order identity."""
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    if (
        not text.isascii()
        or not text.isdigit()
        or int(text) <= 0
        or (len(text) > 1 and text.startswith("0"))
    ):
        return None
    return text


def _account_free_quote(payload: Mapping[str, Any]) -> Decimal | None:
    """Extract the official account ``balances[].free`` USDT value."""
    balances = payload.get("balances")
    if isinstance(balances, Sequence) and not isinstance(balances, (str, bytes, Mapping)):
        for row in balances:
            if isinstance(row, Mapping) and str(row.get("asset", "")).upper() == QUOTE_ASSET:
                return _dec(row.get("free"), ZERO)
    return None


def _rows(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("fills", "trades", "orders", "data", "items"):
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return [row for row in value if isinstance(row, Mapping)]
        return [payload]
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        return [row for row in payload if isinstance(row, Mapping)]
    return []
def _exchange_trade_id(fill: Mapping[str, Any]) -> str | None:
    """Return the venue's exact trade identity when one is present."""
    for key in ("tradeId", "trade_id", "id"):
        value = fill.get(key)
        if value is not None and value != "":
            return str(value)
    return None


def _scoped_trade_id(symbol: Any, trade_id: Any) -> str:
    """Keep exchange trade identities unique within their Binance symbol."""

    normalized_symbol = _symbol(symbol)
    raw = str(trade_id)
    prefix = normalized_symbol + ":"
    return raw if raw.startswith(prefix) else prefix + raw

def _explicit_fill_rows(payload: Any) -> list[Mapping[str, Any]]:
    """Extract actual trade rows, never an aggregate order response.

    Binance order responses carry cumulative quantities on the order object,
    while fills/trades are nested rows with exchange trade identities.  The
    former is evidence for state transitions, not a ledger fill.
    """
    if isinstance(payload, Mapping):
        for key in ("fills", "trades"):
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return [
                    row for row in value
                    if isinstance(row, Mapping)
                    and _exchange_trade_id(row) is not None
                    and any(name in row for name in ("qty", "quantity", "executedQty"))
                ]
        if (
            _exchange_trade_id(payload) is not None
            and any(name in payload for name in ("qty", "quantity", "executedQty"))
        ):
            return [payload]
        return []
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        return [
            row for row in payload
            if isinstance(row, Mapping)
            and _exchange_trade_id(row) is not None
            and any(name in row for name in ("qty", "quantity", "executedQty"))
        ]
    return []


def _cumulative_quantity(payload: Any) -> Decimal:
    if not isinstance(payload, Mapping):
        return ZERO
    for key in (
        "executedQty",
        "executed_qty",
        "filledQty",
        "filled_qty",
        "filled_quantity",
        "cumulativeFilledQty",
        "cumulative_filled_qty",
    ):
        if payload.get(key) is not None:
            return _dec(payload[key])
    return ZERO


def _call(method: Any, kwargs: Mapping[str, Any]) -> Any:
    """Call one duck-typed venue method without retrying a possibly sent order."""
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        params = signature.parameters
        if not any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
            kwargs = {key: value for key, value in kwargs.items() if key in params}
    return method(**dict(kwargs))


@dataclass(frozen=True)
class BinancePosition:
    symbol: str
    quantity: Decimal
    cost_basis: Decimal
    average_cost: Decimal
    mark_price: Decimal | None
    realized_pnl: Decimal
    unrealized_pnl: Decimal | None
    fees_quote: Decimal
    valuation_status: str
    candidate_id: str | None = None
    binding_hash: str | None = None
    binding: Any = None
    provenance: Any = None
    strategy_ref: Any = None
    origin_epoch: str | None = None
    exit_policy: Any = None

    @property
    def net_pnl(self) -> Decimal | None:
        if self.unrealized_pnl is None:
            return None
        return self.realized_pnl + self.unrealized_pnl

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "quantity": str(self.quantity),
            "cost_basis": str(self.cost_basis),
            "average_cost": str(self.average_cost),
            "mark_price": None if self.mark_price is None else str(self.mark_price),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": None if self.unrealized_pnl is None else str(self.unrealized_pnl),
            "net_pnl": None if self.net_pnl is None else str(self.net_pnl),
            "fees_quote": str(self.fees_quote),
            "valuation_status": self.valuation_status,
            "candidate_id": self.candidate_id,
            "binding_hash": self.binding_hash,
        }

class BinanceExecutionService:
    """Durable Spot execution coordinator."""
    def __init__(
        self,
        store: AxiomStore | str | sqlite3.Connection = ":memory:",
        *,
        venue: Any | None = None,
        adapter: Any | None = None,
        profile: BinanceRuntimeProfile | None = None,
        environment: BinanceSpotEnvironment | str | None = None,
        binding: CryptoExecutionBinding | Mapping[str, Any] | None = None,
        risk_envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE,
        envelope: BinanceRiskEnvelope | None = None,
        credentials: Any | None = None,
        credential_store: BinanceCredentialStore | None = None,
        credential_ref: BinanceCredentialRef | None = None,
        qualification: Any | None = None,
        owner_id: str | None = None,
        lease_seconds: int | float = 30,
        clock: Callable[[], Any] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        account: Mapping[str, Any] | None = None,
        db_path: str | None = None,
        runtime_profile: BinanceRuntimeProfile | None = None,
        fault_hook: Callable[[str], Any] | None = None,
        entry_binding_authorizer: Callable[[Mapping[str, Any]], Any] | None = None,
        entry_policy_hash: str | None = None,
    ) -> None:
        if entry_binding_authorizer is not None and not callable(entry_binding_authorizer):
            raise TypeError("entry_binding_authorizer must be callable")
        if entry_policy_hash is not None:
            if not isinstance(entry_policy_hash, str):
                raise TypeError("entry_policy_hash must be a string")
            if (
                not entry_policy_hash
                or entry_policy_hash != entry_policy_hash.strip()
                or len(entry_policy_hash) > 256
                or any(ord(character) < 0x20 or ord(character) == 0x7F for character in entry_policy_hash)
            ):
                raise ValueError("entry_policy_hash must be a safe, non-empty stable scalar")
        if (entry_binding_authorizer is None) != (entry_policy_hash is None):
            raise ValueError("entry_binding_authorizer and entry_policy_hash must be provided together")
        if db_path is not None:
            store = db_path
        if profile is None:
            profile = runtime_profile
        if profile is not None and not isinstance(profile, BinanceRuntimeProfile):
            raise TypeError("profile must be BinanceRuntimeProfile")
        selected_venue = venue if venue is not None else adapter
        selected_environment = _env(
            environment if environment is not None else (profile.environment if profile else PAPER)
        )
        if profile is not None and selected_environment != profile.environment.value:
            raise ValueError("profile/environment mismatch")
        if selected_environment == BINANCE_SPOT_LIVE:
            raise ValueError("development execution refuses Binance LIVE")
        if selected_environment == BINANCE_SPOT_TESTNET:
            if type(credential_ref) is not BinanceCredentialRef or credential_ref != _TESTNET_CREDENTIAL_REF:
                raise ValueError("TESTNET execution requires the exact credential ref")
            if envelope is not None and envelope is not DEFAULT_BINANCE_RISK_ENVELOPE:
                raise ValueError("TESTNET execution requires DEFAULT_BINANCE_RISK_ENVELOPE")
            if risk_envelope is not DEFAULT_BINANCE_RISK_ENVELOPE:
                raise ValueError("TESTNET execution requires DEFAULT_BINANCE_RISK_ENVELOPE")
            if (
                entry_policy_hash != "BINANCE_TESTNET_CURRENT_QUALIFICATION_V1"
                or entry_binding_authorizer is None
                or getattr(entry_binding_authorizer, "_axiom_testnet_runtime_authorizer", False) is not True
                or qualification is None
                or getattr(entry_binding_authorizer, "_axiom_testnet_qualification", None) is not qualification
            ):
                raise ValueError("TESTNET execution requires the runtime qualification authorizer")
            if credential_store is not None:
                actual_ref = getattr(credential_store, "ref", None)
                if type(actual_ref) is not BinanceCredentialRef or actual_ref != _TESTNET_CREDENTIAL_REF:
                    raise ValueError("TESTNET execution requires the exact credential ref")
        authorization_credentials = credentials
        if authorization_credentials is None and credential_store is not None:
            try:
                authorization_credentials = credential_store.load(credential_ref)
            except Exception:
                authorization_credentials = None
        validate_spot_venue_identity(
            selected_venue,
            selected_environment,
            credential_hash=(
                credential_fingerprint(authorization_credentials)
                if selected_environment == BINANCE_SPOT_TESTNET
                else None
            ),
        )

        self._owns_store = False
        try:
            if isinstance(store, AxiomStore):
                self.store = store
                self._conn = store.connection
            elif isinstance(store, sqlite3.Connection):
                self._owns_store = True
                self.store = AxiomStore(connection=store)
                self._conn = store
            else:
                self.store = AxiomStore(str(store))
                self._owns_store = True
                self._conn = self.store.connection
            self._conn.row_factory = sqlite3.Row
            self._lock = threading.RLock()
            self.venue = selected_venue
            self.profile = profile
            self.environment = selected_environment
            self.binding = binding
            self.entry_binding_authorizer = entry_binding_authorizer
            self.entry_policy_hash = entry_policy_hash
            self.qualification = qualification
            self.risk_envelope = envelope or risk_envelope
            if not isinstance(self.risk_envelope, BinanceRiskEnvelope):
                raise TypeError("risk_envelope must be BinanceRiskEnvelope")
            if self.environment == BINANCE_SPOT_TESTNET and self.risk_envelope is not DEFAULT_BINANCE_RISK_ENVELOPE:
                raise ValueError("TESTNET execution requires DEFAULT_BINANCE_RISK_ENVELOPE")
            self.credential_store = credential_store
            self.credentials = credentials
            self.credential_ref = credential_ref
            self.owner_id = f"{str(owner_id or 'worker')}-{uuid.uuid4().hex}"
            self.lease_seconds = max(1, int(lease_seconds))
            self.clock = clock or (lambda: datetime.now(UTC))
            self.fault_hook = fault_hook
            self.monotonic_clock = monotonic_clock or time.monotonic
            self.account_state: dict[str, Any] = dict(account or {})
            self.namespace = "BINANCE_SPOT_EXECUTION"
            self.schema_version = SCHEMA_VERSION
            self._init_schema()
            self._check_persisted_boundary()
        except BaseException:
            owned_store = getattr(self, "store", None)
            closer = getattr(owned_store, "close", None)
            if self._owns_store and callable(closer):
                try:
                    closer()
                except BaseException:
                    pass
            elif self._owns_store and isinstance(store, sqlite3.Connection):
                try:
                    store.close()
                except BaseException:
                    pass
            raise
    # ---- schema and safe identity -------------------------------------------------
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_execution_schema_lock (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), namespace TEXT NOT NULL,
                    schema_version TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_control (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), state TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0, environment TEXT NOT NULL,
                    binding_hash TEXT NOT NULL, envelope_hash TEXT NOT NULL,
                    credential_hash TEXT NOT NULL, entry_policy_hash TEXT NOT NULL DEFAULT '',
                    authorized INTEGER NOT NULL DEFAULT 0,
                    kill_requested INTEGER NOT NULL DEFAULT 0, pause_reason TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_connectivity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), status TEXT NOT NULL,
                    checked_at TEXT, heartbeat_at TEXT, error TEXT, account_epoch TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_actions (
                    action_id TEXT PRIMARY KEY, action TEXT NOT NULL, actor TEXT NOT NULL,
                    confirmation TEXT, generation INTEGER NOT NULL, created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_action_results (
                    result_id TEXT PRIMARY KEY, action_id TEXT NOT NULL, status TEXT NOT NULL,
                    result_json TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_signals (
                    signal_id TEXT PRIMARY KEY, opportunity_id TEXT, candidate_id TEXT NOT NULL,
                    binding_hash TEXT NOT NULL, symbol TEXT NOT NULL, environment TEXT NOT NULL,
                    decision_interval TEXT, decision_at TEXT NOT NULL, intent TEXT NOT NULL,
                    side TEXT NOT NULL, reason TEXT, exit_policy_json TEXT,
                    binding_json TEXT, provenance_json TEXT, strategy_ref_json TEXT,
                    signal_json TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_order_intents (
                    intent_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL UNIQUE,
                    opportunity_id TEXT, candidate_id TEXT NOT NULL, binding_hash TEXT NOT NULL,
                    symbol TEXT NOT NULL, environment TEXT NOT NULL, intent TEXT NOT NULL,
                    side TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL,
                    notional TEXT NOT NULL, fee_reserve TEXT NOT NULL,
                    client_order_id TEXT NOT NULL UNIQUE, exchange_order_id TEXT,
                    state TEXT NOT NULL, generation INTEGER NOT NULL, owner_id TEXT,
                    lease_expires_at TEXT, reason TEXT, exit_policy_json TEXT,
                    binding_json TEXT, provenance_json TEXT, strategy_ref_json TEXT,
                    submitted_at TEXT, acknowledged_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
                    raw_json TEXT
                );
                CREATE TABLE IF NOT EXISTS binance_execution_order_transitions (
                    transition_id INTEGER PRIMARY KEY AUTOINCREMENT, intent_id TEXT NOT NULL,
                    from_state TEXT, to_state TEXT NOT NULL, generation INTEGER NOT NULL,
                    observed_at TEXT NOT NULL, reason TEXT, payload_json TEXT NOT NULL,
                    UNIQUE(intent_id, to_state, generation, observed_at)
                );
                CREATE TABLE IF NOT EXISTS binance_execution_fills (
                    trade_id TEXT PRIMARY KEY, intent_id TEXT, client_order_id TEXT,
                    exchange_order_id TEXT, symbol TEXT NOT NULL, side TEXT NOT NULL,
                    quantity TEXT NOT NULL, price TEXT NOT NULL, quote_quantity TEXT NOT NULL,
                    commission TEXT NOT NULL, commission_asset TEXT, trade_time TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_risk_reservations (
                    reservation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL, side TEXT NOT NULL, amount TEXT NOT NULL,
                    reserved_quantity TEXT NOT NULL DEFAULT '0', fee_reserve TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, released_at TEXT
                );
                CREATE TABLE IF NOT EXISTS binance_execution_positions (
                    symbol TEXT PRIMARY KEY, quantity TEXT NOT NULL, cost_basis TEXT NOT NULL,
                    average_cost TEXT NOT NULL, mark_price TEXT, realized_pnl TEXT NOT NULL,
                    unrealized_pnl TEXT, fees_quote TEXT NOT NULL, valuation_status TEXT NOT NULL,
                    candidate_id TEXT, binding_hash TEXT, origin_epoch TEXT, exit_policy_json TEXT,
                    binding_json TEXT, provenance_json TEXT, strategy_ref_json TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_reconciliation (
                    key TEXT PRIMARY KEY, status TEXT NOT NULL, attempted_at TEXT,
                    completed_at TEXT, heartbeat_at TEXT, error TEXT, details_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_execution_account (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), account_json TEXT NOT NULL,
                    epoch TEXT, observed_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_exec_intents_state ON binance_execution_order_intents(state, updated_at);
                CREATE INDEX IF NOT EXISTS idx_binance_exec_fills_order ON binance_execution_fills(client_order_id, trade_time);
                """
            )
            json_columns = {
                "binance_execution_control": {"entry_policy_hash": "TEXT NOT NULL DEFAULT ''"},
                "binance_execution_signals": {
                    "binding_json": "TEXT", "provenance_json": "TEXT", "strategy_ref_json": "TEXT",
                },
                "binance_execution_order_intents": {
                    "binding_json": "TEXT", "provenance_json": "TEXT", "strategy_ref_json": "TEXT",
                },
                "binance_execution_positions": {
                    "origin_epoch": "TEXT", "binding_json": "TEXT", "provenance_json": "TEXT", "strategy_ref_json": "TEXT",
                },
            }
            for table, definitions in json_columns.items():
                present = {
                    str(row[1])
                    for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                for column, definition in definitions.items():
                    if column not in present:
                        self._conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                        )
            columns = {
                str(row[1])
                for row in self._conn.execute(
                    "PRAGMA table_info(binance_execution_risk_reservations)"
                ).fetchall()
            }
            added_reserved_quantity = "reserved_quantity" not in columns
            if added_reserved_quantity:
                self._conn.execute(
                    "ALTER TABLE binance_execution_risk_reservations "
                    "ADD COLUMN reserved_quantity TEXT NOT NULL DEFAULT '0'"
                )
            self._migrate_fill_ids()
            if added_reserved_quantity:
                self._migrate_reserved_quantities()
            now = _iso(self.clock())
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_schema_lock(singleton,namespace,schema_version,created_at) VALUES(1,?,?,?)",
                (self.namespace, self.schema_version, now),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_control(singleton,state,generation,environment,binding_hash,envelope_hash,credential_hash,entry_policy_hash,authorized,kill_requested,updated_at) VALUES(1,?,?,?,?,?,?,?,?,?,?)",
                (
                    DISABLED, 0, self.environment, self._binding_hash(),
                    self.risk_envelope.canonical_hash, self._credential_hash(),
                    self.entry_policy_hash or "", 0, 0, now,
                ),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_connectivity(singleton,status,updated_at) VALUES(1,'UNKNOWN',?)",
                (now,),
            )
            self._migrate_signal_evidence()
            self._conn.commit()

    def _migrate_signal_evidence(self) -> None:
        """Backfill only exact provenance fields present in legacy signal JSON."""
        rows = self._conn.execute(
            "SELECT signal_id,signal_json,binding_json,provenance_json,strategy_ref_json "
            "FROM binance_execution_signals"
        ).fetchall()
        for row in rows:
            payload = _load(row["signal_json"], None)
            if not isinstance(payload, Mapping):
                continue
            updates: dict[str, str] = {}
            for key in ("binding", "provenance", "strategy_ref"):
                column = key + "_json"
                if row[column] is None and key in payload and payload[key] is not None:
                    updates[column] = _json(_canonical(payload[key]))
            if updates:
                self._conn.execute(
                    "UPDATE binance_execution_signals SET "
                    + ",".join(f"{column}=?" for column in updates)
                    + " WHERE signal_id=?",
                    (*updates.values(), row["signal_id"]),
                )
        for column in ("binding_json", "provenance_json", "strategy_ref_json"):
            self._conn.execute(
                f"UPDATE binance_execution_order_intents SET {column}=COALESCE({column},(SELECT s.{column} FROM binance_execution_signals s WHERE s.signal_id=binance_execution_order_intents.signal_id)) "
                f"WHERE {column} IS NULL"
            )

    def _migrate_fill_ids(self) -> None:
        """Scope legacy raw exchange trade IDs by symbol exactly once."""
        rows = self._conn.execute(
            "SELECT rowid,trade_id,symbol FROM binance_execution_fills ORDER BY rowid"
        ).fetchall()
        pending: list[tuple[str, str]] = []
        for row in rows:
            old_id = str(row["trade_id"])
            target = _scoped_trade_id(row["symbol"], old_id)
            if target == old_id:
                continue
            temporary = "__binance_fill_migration__" + uuid.uuid4().hex
            self._conn.execute(
                "UPDATE binance_execution_fills SET trade_id=? WHERE rowid=?",
                (temporary, row["rowid"]),
            )
            pending.append((temporary, target))
        claimed: set[str] = set()
        for temporary, target in pending:
            duplicate = (
                target in claimed
                or self._conn.execute(
                    "SELECT 1 FROM binance_execution_fills WHERE trade_id=?",
                    (target,),
                ).fetchone()
                is not None
            )
            if duplicate:
                self._conn.execute(
                    "DELETE FROM binance_execution_fills WHERE trade_id=?", (temporary,)
                )
                continue
            self._conn.execute(
                "UPDATE binance_execution_fills SET trade_id=? WHERE trade_id=?",
                (target, temporary),
            )
            claimed.add(target)

    def _migrate_reserved_quantities(self) -> None:
        """Backfill legacy held SELL reservations from durable fills once."""
        reservations = self._conn.execute(
            """
            SELECT r.reservation_id, r.intent_id, r.symbol,
                   i.quantity, i.client_order_id, i.exchange_order_id
            FROM binance_execution_risk_reservations AS r
            JOIN binance_execution_order_intents AS i ON i.intent_id=r.intent_id
            WHERE UPPER(r.side)='SELL' AND r.status='HELD'
            ORDER BY r.reservation_id
            """
        ).fetchall()
        fills = self._conn.execute(
            """
            SELECT intent_id, client_order_id, exchange_order_id, symbol, quantity
            FROM binance_execution_fills
            ORDER BY rowid
            """
        ).fetchall()
        for reservation in reservations:
            symbol = _symbol(reservation["symbol"])
            intent_id = str(reservation["intent_id"])
            client_order_id = reservation["client_order_id"]
            exchange_order_id = reservation["exchange_order_id"]
            filled = ZERO
            for fill in fills:
                if _symbol(fill["symbol"]) != symbol:
                    continue
                associated = str(fill["intent_id"]) == intent_id if fill["intent_id"] is not None else False
                if not associated and client_order_id not in (None, ""):
                    associated = fill["client_order_id"] == client_order_id
                if not associated and exchange_order_id not in (None, ""):
                    associated = fill["exchange_order_id"] == exchange_order_id
                if associated:
                    filled += max(ZERO, _dec(fill["quantity"]))
            remainder = max(ZERO, _dec(reservation["quantity"]) - filled)
            self._conn.execute(
                """
                UPDATE binance_execution_risk_reservations
                SET reserved_quantity=?
                WHERE reservation_id=? AND UPPER(side)='SELL'
                  AND status='HELD' AND reserved_quantity='0'
                """,
                (_dstr(remainder), reservation["reservation_id"]),
            )


    def _binding_hash(self) -> str:
        value = self.binding
        if isinstance(value, CryptoExecutionBinding):
            return value.binding_hash
        if isinstance(value, Mapping):
            return str(value.get("binding_hash") or canonical_sha256(dict(value)))
        if value is None:
            return canonical_sha256({"binding": None})
        return canonical_sha256(str(value))

    @staticmethod
    def _binding_hash_for(value: Any) -> str:
        if isinstance(value, CryptoExecutionBinding):
            return value.binding_hash
        if isinstance(value, Mapping):
            explicit = value.get("binding_hash")
            if explicit not in (None, ""):
                return str(explicit)
            return canonical_sha256(dict(value))
        return canonical_sha256(value)

    @staticmethod
    def _mapping_equal(left: Any, right: Any) -> bool:
        return _json(left) == _json(right)

    @staticmethod
    def _deadline_expired(deadline_monotonic: float | None) -> bool:
        return deadline_monotonic is not None and time.monotonic() >= float(deadline_monotonic)

    @property
    def _dynamic_mode(self) -> bool:
        return self.entry_binding_authorizer is not None and self.entry_policy_hash is not None

    def _dynamic_entry_rejection(self, data: Mapping[str, Any]) -> str | None:
        """Validate and authorize the complete successor binding at one boundary."""
        if not self._dynamic_mode:
            return None
        authorizer = self.entry_binding_authorizer
        binding = data.get("binding")
        provenance = data.get("provenance")
        if not isinstance(binding, Mapping):
            return "ENTRY_BINDING_REQUIRED"
        if not isinstance(provenance, Mapping):
            return "ENTRY_PROVENANCE_REQUIRED"
        strategy_ref = data.get("strategy_ref")
        if not isinstance(strategy_ref, Mapping) or not strategy_ref:
            return "ENTRY_STRATEGY_REF_REQUIRED"
        if str(binding.get("candidate_id", "")) != str(data["candidate_id"]):
            return "ENTRY_BINDING_CANDIDATE_MISMATCH"
        try:
            if _symbol(binding.get("symbol")) != str(data["symbol"]):
                return "ENTRY_BINDING_SYMBOL_MISMATCH"
        except ValueError:
            return "ENTRY_BINDING_SYMBOL_MISMATCH"
        binding_environment = binding.get("environment")
        if binding_environment not in (None, "") and _env(binding_environment) != self.environment:
            return "ENTRY_BINDING_ENVIRONMENT_MISMATCH"
        expected_hash = self._binding_hash_for(binding)
        if str(data.get("binding_hash") or "") != expected_hash:
            return "ENTRY_BINDING_HASH_MISMATCH"
        supplied_policy = data.get("entry_policy_hash", data.get("policy_hash"))
        if supplied_policy not in (None, "") and str(supplied_policy) != self.entry_policy_hash:
            return "ENTRY_POLICY_HASH_MISMATCH"
        provenance_strategy = provenance.get("strategy_ref")
        if provenance_strategy is not None and not self._mapping_equal(provenance_strategy, strategy_ref):
            return "ENTRY_PROVENANCE_STRATEGY_REF_MISMATCH"
        if str(provenance.get("candidate_id", data["candidate_id"])) != str(data["candidate_id"]):
            return "ENTRY_PROVENANCE_CANDIDATE_MISMATCH"
        provenance_symbol = provenance.get("symbol", provenance.get("source_symbol"))
        if provenance_symbol not in (None, "") and _symbol(provenance_symbol) != str(data["symbol"]):
            return "ENTRY_PROVENANCE_SYMBOL_MISMATCH"
        source_candidate = provenance.get("source_candidate_id")
        if source_candidate not in (None, "") and str(source_candidate) != str(data["candidate_id"]):
            return "ENTRY_PROVENANCE_SOURCE_CANDIDATE_MISMATCH"
        try:
            decision = authorizer(data)
        except BaseException as exc:
            return "ENTRY_BINDING_AUTHORIZER_" + type(exc).__name__.upper()
        if not isinstance(decision, tuple) or len(decision) != 2 or not isinstance(decision[0], bool) or not isinstance(decision[1], str):
            return "ENTRY_BINDING_AUTHORIZER_INVALID_RESULT"
        approved, reason = decision
        if not approved:
            return reason or "ENTRY_BINDING_NOT_AUTHORIZED"
        return None

    def _exit_origin_rejection(self, data: Mapping[str, Any]) -> str | None:
        if data.get("intent") != "EXIT":
            return None
        if not self._dynamic_mode:
            return None
        position = self._conn.execute(
            "SELECT candidate_id,binding_hash,binding_json,provenance_json,strategy_ref_json "
            "FROM binance_execution_positions WHERE symbol=? AND CAST(quantity AS REAL)>0",
            (data["symbol"],),
        ).fetchone()
        if position is None:
            return "EXIT_ORIGIN_UNRESOLVED" if self._dynamic_mode else None
        expected_candidate = position["candidate_id"]
        expected_hash = position["binding_hash"]
        expected_values = {
            "binding": _load(position["binding_json"], None),
            "provenance": _load(position["provenance_json"], None),
            "strategy_ref": _load(position["strategy_ref_json"], None),
        }
        if (
            (self._dynamic_mode and (
                expected_candidate in (None, "")
                or expected_hash in (None, "")
                or any(value is None for value in expected_values.values())
            ))
            or (
                not self._dynamic_mode
                and expected_candidate in (None, "")
                and expected_hash in (None, "")
                and all(value is None for value in expected_values.values())
            )
        ):
            return "EXIT_ORIGIN_UNRESOLVED"
        if expected_candidate not in (None, "") and str(data["candidate_id"]) != str(expected_candidate):
            return "EXIT_ORIGIN_CANDIDATE_MISMATCH"
        if expected_hash not in (None, "") and str(data.get("binding_hash") or "") != str(expected_hash):
            return "EXIT_ORIGIN_BINDING_MISMATCH"
        for key, expected in expected_values.items():
            if expected is not None and not self._mapping_equal(data.get(key), expected):
                return "EXIT_ORIGIN_" + key.upper() + "_MISMATCH"
        return None

    def _entry_origin_rejection(self, data: Mapping[str, Any]) -> str | None:
        if data.get("intent") != "ENTRY":
            return None
        if not self._dynamic_mode:
            return None
        position = self._conn.execute(
            "SELECT candidate_id,binding_hash,binding_json,provenance_json,strategy_ref_json "
            "FROM binance_execution_positions WHERE symbol=? AND CAST(quantity AS REAL)>0",
            (data["symbol"],),
        ).fetchone()
        if position is None:
            return None
        if position["candidate_id"] not in (None, "") and str(data["candidate_id"]) != str(position["candidate_id"]):
            return "ENTRY_ORIGIN_MIXED"
        if position["binding_hash"] not in (None, "") and str(data.get("binding_hash") or "") != str(position["binding_hash"]):
            return "ENTRY_ORIGIN_MIXED"
        for key in ("binding", "provenance", "strategy_ref"):
            expected = _load(position[key + "_json"], None)
            if expected is not None and not self._mapping_equal(data.get(key), expected):
                return "ENTRY_ORIGIN_MIXED"
        return None

    def _credential_value(self) -> Any:
        value = self.credentials
        if value is None and self.credential_store is not None:
            try:
                value = self.credential_store.load(self.credential_ref)
            except Exception:
                value = None
        if value is None:
            return None
        if isinstance(value, Mapping):
            return {"api_key": value.get("api_key"), "api_secret": value.get("api_secret")}
        return {"api_key": getattr(value, "api_key", None), "api_secret": getattr(value, "api_secret", None)}

    def _credential_hash(self) -> str:
        value = self._credential_value()
        if not value or not value.get("api_key") or not value.get("api_secret"):
            return canonical_sha256({"configured": False})
        return canonical_sha256({"api_key": str(value["api_key"]), "api_secret": str(value["api_secret"])})

    def _check_persisted_boundary(self) -> None:
        row = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
        if row is None:
            return
        # A tampered LIVE row cannot turn a development profile into a live one.
        if self.profile is not None and str(row["environment"]) == BINANCE_SPOT_LIVE:
            raise ValueError("persisted Binance LIVE environment is refused by development profile")
        current = (
            self.environment, self._binding_hash(), self.risk_envelope.canonical_hash,
            self._credential_hash(), self.entry_policy_hash or "",
        )
        persisted = (
            str(row["environment"]), str(row["binding_hash"]), str(row["envelope_hash"]),
            str(row["credential_hash"]), str(row["entry_policy_hash"] or ""),
        )
        if current != persisted:
            self._conn.execute(
                "UPDATE binance_execution_control SET authorized=0,state=?,generation=generation+1,binding_hash=?,envelope_hash=?,credential_hash=?,entry_policy_hash=?,environment=?,updated_at=? WHERE singleton=1",
                (DISABLED, current[1], current[2], current[3], current[4], current[0], _iso(self.clock())),
            )
            self._conn.commit()

    def _fault(self, point: str) -> None:
        """Invoke an opt-in deterministic crash hook used by boundary tests."""
        if self.fault_hook is not None:
            self.fault_hook(str(point))

    # ---- control ------------------------------------------------------------------
    def control(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
        if row is None:
            return {"state": DISABLED, "authorized": False}
        current = (
            self.environment, self._binding_hash(), self.risk_envelope.canonical_hash,
            self._credential_hash(), self.entry_policy_hash or "",
        )
        authorized = bool(row["authorized"]) and tuple(
            str(row[key] or "") for key in
            ("environment", "binding_hash", "envelope_hash", "credential_hash", "entry_policy_hash")
        ) == current
        return {
            **dict(row), "authorized": authorized,
            "generation": int(row["generation"]), "kill_requested": bool(row["kill_requested"]),
        }

    status = control

    def _required_confirmation(self) -> str:
        """Return the environment-specific autonomous activation phrase."""
        if self.environment == BINANCE_SPOT_TESTNET:
            return ENABLE_TESTNET_CONFIRMATION
        return ENABLE_CONFIRMATION
    def _deadline_checkpoint(self, deadline_monotonic: float | None) -> None:
        if deadline_monotonic is None:
            return
        try:
            expired = self.monotonic_clock() >= float(deadline_monotonic)
        except Exception:
            expired = True
        if expired:
            raise TimeoutError("AUTO_DEADLINE_EXPIRED")


    def enable_auto_canary(
        self,
        confirmation: str = "",
        *,
        actor: str = "operator",
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        self._deadline_checkpoint(deadline_monotonic)
        required = self._required_confirmation()
        if confirmation != required:
            raise PermissionError(f"exact confirmation required: {required}")
        if self.environment == BINANCE_SPOT_LIVE or (self.profile is not None and self.profile.environment.value == BINANCE_SPOT_LIVE):
            raise PermissionError("development profile refuses Binance LIVE")
        if self.environment == BINANCE_SPOT_TESTNET:
            probe_peer = self._probe_peer_snapshot(self.clock())
            if probe_peer.get("block_auto"):
                raise PermissionError(f"TESTNET_PROBE_{probe_peer['block_auto']}")
        now = _iso(self.clock())
        with self._lock:
            self._deadline_checkpoint(deadline_monotonic)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._deadline_checkpoint(deadline_monotonic)
                row = self._conn.execute("SELECT generation FROM binance_execution_control WHERE singleton=1").fetchone()
                generation = int(row[0] if row else 0) + 1
                self._deadline_checkpoint(deadline_monotonic)
                self._conn.execute(
                    "UPDATE binance_execution_control SET state=?,generation=?,environment=?,binding_hash=?,envelope_hash=?,credential_hash=?,entry_policy_hash=?,authorized=1,kill_requested=0,pause_reason=NULL,updated_at=? WHERE singleton=1",
                    (
                        ARMED, generation, self.environment, self._binding_hash(),
                        self.risk_envelope.canonical_hash, self._credential_hash(),
                        self.entry_policy_hash or "", now,
                    ),
                )
                self._conn.execute("INSERT INTO binance_execution_actions(action_id,action,actor,confirmation,generation,created_at,payload_json) VALUES(?,?,?,?,?,?,?)", (uuid.uuid4().hex, "ENABLE", actor, confirmation, generation, now, _json({"environment": self.environment})))
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self.control()

    enable = enable_auto_canary
    authorize = enable_auto_canary

    def pause(self, reason: str = "operator pause", *, actor: str = "operator") -> dict[str, Any]:
        return self._set_control(PAUSED, reason, actor, "PAUSE")

    def disarm(self, reason: str = "operator disarm", *, actor: str = "operator") -> dict[str, Any]:
        return self._set_control(DISARMED, reason, actor, "DISARM")

    def kill(self, reason: str = "operator kill", *, actor: str = "operator") -> dict[str, Any]:
        return self._set_control(KILLED, reason, actor, "KILL", kill=True)

    def reset(self, confirmation: str = "", *, actor: str = "operator") -> dict[str, Any]:
        required = self._required_confirmation()
        accepted = {required}
        if self.environment == PAPER:
            accepted.add("RESET BINANCE AUTO CANARY")
        if confirmation not in accepted:
            raise PermissionError(f"exact confirmation required: {required}")
        return self.enable_auto_canary(required, actor=actor)

    def _set_control(self, state: str, reason: str, actor: str, action: str, *, kill: bool = False) -> dict[str, Any]:
        now = _iso(self.clock())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT generation,authorized FROM binance_execution_control WHERE singleton=1").fetchone()

                generation = int(row[0] if row else 0) + 1
                authorized = 0 if kill else int(row[1] if row else 0)
                self._conn.execute(
                    "UPDATE binance_execution_control SET state=?,generation=?,authorized=?,kill_requested=?,pause_reason=?,updated_at=? WHERE singleton=1",
                    (state, generation, authorized, 1 if kill else 0, reason, now),
                )
                self._conn.execute(
                    "INSERT INTO binance_execution_actions(action_id,action,actor,confirmation,generation,created_at,payload_json) VALUES(?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, action, actor, None, generation, now, _json({"reason": reason})),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self.control()
    def _probe_peer_snapshot(self, now: Any = None) -> dict[str, Any]:
        """Read the probe ledger when it shares this service's SQLite account.

        The probe ledger is deliberately parsed here instead of importing the
        gate service: standalone execution services must remain usable when the
        peer schema is absent, while a present but malformed schema must fail
        closed rather than silently reducing account-wide risk.
        """
        empty = {
            "present": False,
            "malformed": False,
            "reason": None,
            "positions": {},
            "inventory": {},
            "held_quote": ZERO,
            "held_buy_exposure": ZERO,
            "pending": [],
            "submissions": 0,
            "realized": ZERO,
            "realized_today": ZERO,
            "fees_today": ZERO,
            "equity_loss": ZERO,
            "unknown_fee": False,
            "unknown_fee_assets": [],
        }
        names = (
            "binance_testnet_probe_intents",
            "binance_testnet_probe_reservations",
            "binance_testnet_probe_fills",
            "binance_testnet_probe_events",
        )
        columns = {name: _sqlite_table_columns(self._conn, name) for name in names}
        if all(value is None for value in columns.values()):
            return empty
        required = {
            "binance_testnet_probe_intents": {
                "intent_id", "probe_kind", "symbol", "side", "quantity",
                "notional", "fee_reserve", "state", "reason", "created_at_utc",
            },
            "binance_testnet_probe_reservations": {
                "intent_id", "probe_kind", "symbol", "side", "amount",
                "fee_reserve", "reserved_quantity", "status",
            },
            "binance_testnet_probe_fills": {
                "intent_id", "symbol", "side", "quantity", "price",
                "quote_quantity", "commission", "commission_asset",
                "trade_time_utc",
            },
            "binance_testnet_probe_events": {
                "intent_id", "to_state", "observed_at_utc",
            },
        }
        if any(columns[name] is None or not required[name].issubset(columns[name] or set()) for name in names):
            empty.update(present=True, malformed=True, reason="PROBE_STATE_MALFORMED", block_auto="PROBE_STATE_MALFORMED")
            return empty
        try:
            intents = self._conn.execute(
                "SELECT intent_id,probe_kind,symbol,side,quantity,notional,fee_reserve,state,reason,created_at_utc "
                "FROM binance_testnet_probe_intents"
            ).fetchall()
            reservations = self._conn.execute(
                "SELECT intent_id,probe_kind,symbol,side,amount,fee_reserve,reserved_quantity,status "
                "FROM binance_testnet_probe_reservations"
            ).fetchall()
            fills = self._conn.execute(
                "SELECT intent_id,symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time_utc "
                "FROM binance_testnet_probe_fills ORDER BY trade_time_utc,trade_id"
            ).fetchall()
            events = self._conn.execute(
                "SELECT intent_id,to_state,observed_at_utc FROM binance_testnet_probe_events"
            ).fetchall()
        except sqlite3.Error:
            empty.update(present=True, malformed=True, reason="PROBE_STATE_MALFORMED", block_auto="PROBE_STATE_MALFORMED")
            return empty
        state = dict(empty)
        state["present"] = True
        by_intent = {str(row["intent_id"]): row for row in intents}
        try:
            for row in intents:
                if str(row["probe_kind"]) != "TESTNET EXECUTION PROBE":
                    raise ValueError("probe kind")
                if str(row["side"]).upper() not in {"BUY", "SELL"}:
                    raise ValueError("probe side")
                _symbol(row["symbol"])
                _dec(row["quantity"])
                _dec(row["notional"])
                _dec(row["fee_reserve"])
                if str(row["state"]).upper() not in {
                    "INTENT", "RESERVED", "SUBMITTING", "ACKNOWLEDGED",
                    "UNKNOWN", "PARTIALLY_FILLED", "FILLED", "CANCELED",
                    "EXPIRED", "DUST", "EXIT_BELOW_MINIMUM",
                }:
                    raise ValueError("probe state")
            lots: dict[str, list[list[Decimal]]] = {}
            realized: dict[str, Decimal] = {}
            fees: dict[str, Decimal] = {}
            unknown_assets: set[str] = set()
            instant = _utc(now or self.clock())
            day = instant.date()
            for row in fills:
                intent_id = str(row["intent_id"])
                intent = by_intent.get(intent_id)
                if intent is None:
                    raise ValueError("orphan fill")
                symbol = _symbol(row["symbol"])
                side = str(row["side"]).upper()
                if (
                    symbol != _symbol(intent["symbol"])
                    or side != str(intent["side"]).upper()
                ):
                    raise ValueError("fill identity mismatch")
                qty = _dec(row["quantity"])
                price = _dec(row["price"])
                quote = _dec(row["quote_quantity"])
                commission = _dec(row["commission"])
                if qty <= ZERO or price <= ZERO or quote < ZERO or commission < ZERO or side not in {"BUY", "SELL"}:
                    raise ValueError("invalid fill")
                base = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else ""
                asset = str(row["commission_asset"] or "").upper()
                if asset == QUOTE_ASSET:
                    fee_quote = commission
                elif base and asset == base:
                    fee_quote = commission * price
                else:
                    fee_quote = ZERO
                    if commission > ZERO:
                        unknown_assets.add(asset or "<MISSING>")
                fees[symbol] = fees.get(symbol, ZERO) + fee_quote
                symbol_lots = lots.setdefault(symbol, [])
                prior_realized = realized.get(symbol, ZERO)
                symbol_realized = prior_realized
                if side == "BUY":
                    net_quantity = max(ZERO, qty - (commission if asset == base else ZERO))
                    if net_quantity > ZERO:
                        symbol_lots.append([net_quantity, quote + (commission if asset == QUOTE_ASSET else ZERO)])
                else:
                    remaining = qty
                    proceeds = quote - (commission if asset == QUOTE_ASSET else ZERO)
                    while remaining > ZERO and symbol_lots:
                        lot_quantity, lot_cost = symbol_lots[0]
                        matched = min(remaining, lot_quantity)
                        symbol_realized += proceeds * (matched / qty) - lot_cost * (matched / lot_quantity)
                        lot_quantity -= matched
                        lot_cost -= lot_cost * (matched / (lot_quantity + matched))
                        remaining -= matched
                        if lot_quantity <= ZERO:
                            symbol_lots.pop(0)
                        else:
                            symbol_lots[0] = [lot_quantity, lot_cost]
                realized[symbol] = symbol_realized
                if side == "SELL" and _utc(row["trade_time_utc"]).date() == day:
                    state["realized_today"] += symbol_realized - prior_realized
                if _utc(row["trade_time_utc"]).date() == day:
                    state["fees_today"] += fee_quote
            positions: dict[str, dict[str, Decimal]] = {}
            for symbol, symbol_lots in lots.items():
                quantity = sum((lot[0] for lot in symbol_lots), ZERO)
                if quantity <= ZERO:
                    continue
                positions[symbol] = {
                    "quantity": quantity,
                    "cost_basis": sum((lot[1] for lot in symbol_lots), ZERO),
                    "realized_pnl": realized.get(symbol, ZERO),
                    "unrealized_pnl": ZERO,
                    "fees_quote": fees.get(symbol, ZERO),
                }
            state["positions"] = positions
            state["realized"] = sum(realized.values(), ZERO)
            state["equity_loss"] = max(ZERO, -state["realized_today"])
            state["unknown_fee"] = bool(unknown_assets)
            state["unknown_fee_assets"] = sorted(unknown_assets)
            held_rows = []
            for row in reservations:
                intent = by_intent.get(str(row["intent_id"]))
                if (
                    intent is None
                    or str(row["probe_kind"]) != "TESTNET EXECUTION PROBE"
                    or str(row["symbol"]).upper() != str(intent["symbol"]).upper()
                    or str(row["side"]).upper() != str(intent["side"]).upper()
                ):
                    raise ValueError("reservation identity")
                if str(row["side"]).upper() not in {"BUY", "SELL"}:
                    raise ValueError("reservation side")
                for key in ("amount", "fee_reserve", "reserved_quantity"):
                    if _dec(row[key]) < ZERO:
                        raise ValueError("negative reservation")
                reservation_status = str(row["status"]).upper()
                if reservation_status not in {"HELD", "RELEASED"}:
                    raise ValueError("reservation status")
                if reservation_status == "HELD":
                    amount = _dec(row["amount"])
                    fee = _dec(row["fee_reserve"])
                    state["held_quote"] += amount + fee if str(row["side"]).upper() == "BUY" else ZERO
                    if str(row["side"]).upper() == "BUY":
                        state["held_buy_exposure"] += amount
                    held_rows.append({
                        "symbol": str(row["symbol"]).upper(),
                        "side": str(row["side"]).upper(),
                        "notional": amount,
                        "fee_reserve": fee,
                        "status": "UNKNOWN",
                    })
            state["pending"] = held_rows
            submitted = set()
            for row in events:
                if str(row["intent_id"]) not in by_intent:
                    raise ValueError("orphan event")
                if str(row["to_state"]).upper() == "SUBMITTING" and _utc(row["observed_at_utc"]).date() == day:
                    submitted.add(str(row["intent_id"]))
            for row in intents:
                if str(row["state"]).upper() == "RESERVED" and _utc(row["created_at_utc"]).date() == day:
                    submitted.add(str(row["intent_id"]))
            state["submissions"] = len(submitted)
            blocker = None
            reasons = [str(row["reason"]).upper() for row in intents]
            if any("TESTNET_RESET" in reason or "RESET_HISTORY" in reason for reason in reasons):
                blocker = "PROBE_RESET"
            elif state["unknown_fee"]:
                blocker = "PROBE_UNKNOWN_FEE"
            elif any(str(row["state"]).upper() == "UNKNOWN" for row in intents):
                blocker = "PROBE_UNKNOWN"
            elif state["pending"]:
                blocker = "PROBE_HELD"
            elif any(str(row["state"]).upper() == "DUST" for row in intents):
                blocker = "PROBE_DUST"
            elif state["positions"]:
                blocker = "PROBE_OPEN_INVENTORY"
            state["block_auto"] = blocker
            state["reason"] = blocker
            return state
        except (KeyError, TypeError, ValueError, InvalidOperation, sqlite3.Error):
            state.update(malformed=True, reason="PROBE_STATE_MALFORMED", block_auto="PROBE_STATE_MALFORMED")
            return state
    def _probe_entry_block_reason(self, now: Any = None) -> str | None:
        if self.environment != BINANCE_SPOT_TESTNET:
            return None
        blocker = self._probe_peer_snapshot(now or self.clock()).get("block_auto")
        return f"TESTNET_PROBE_{blocker}" if blocker else None


    def _strict_snapshot(self, account: Mapping[str, Any], now: Any = None) -> RiskSnapshot:
        """Build strict risk exclusively from the account quote and AXIOM ledgers."""
        instant = _utc(now or self.clock())
        day_prefix = instant.date().isoformat() + "%"
        positions = self._conn.execute(
            "SELECT symbol,quantity,cost_basis,realized_pnl,unrealized_pnl,fees_quote,valuation_status "
            "FROM binance_execution_positions ORDER BY symbol"
        ).fetchall()
        inventory = {
            str(row["symbol"]).upper(): _dec(row["quantity"])
            for row in positions
            if _dec(row["quantity"]) > ZERO
        }
        reservations = self._conn.execute(
            """
            SELECT i.intent_id,i.symbol,r.side,i.quantity,i.price,r.fee_reserve,
                   i.exchange_order_id,r.amount,r.reserved_quantity,r.status,i.state
            FROM binance_execution_risk_reservations r
            JOIN binance_execution_order_intents i ON i.intent_id=r.intent_id
            WHERE r.status='HELD'
            ORDER BY r.created_at,r.reservation_id
            """
        ).fetchall()
        pending: list[dict[str, Any]] = []
        held_exposure = ZERO
        # Local HELD BUY reservations are emitted as pending UNKNOWN orders
        # below, where RiskSnapshot.pending_reservation accounts for amount
        # plus fee exactly once.  Peer reservations have no local pending
        # order, so the probe's held_quote belongs in reserved_exposure.
        for reservation in reservations:
            fills = self._conn.execute(
                "SELECT quantity FROM binance_execution_fills WHERE intent_id=?",
                (reservation["intent_id"],),
            ).fetchall()
            filled = sum((_dec(fill["quantity"]) for fill in fills), ZERO)
            side = str(reservation["side"]).upper()
            remainder = max(ZERO, _dec(reservation["quantity"]) - filled) if side == "SELL" else ZERO
            if side == "BUY":
                held_exposure += _dec(reservation["amount"])
            elif side == "SELL":
                if remainder > ZERO:
                    symbol = str(reservation["symbol"]).upper()
                    available = max(ZERO, _dec(inventory.get(symbol, ZERO)) - remainder)
                    if available > ZERO:
                        inventory[symbol] = available
                    else:
                        inventory.pop(symbol, None)
            pending.append(
                {
                    "intent_id": reservation["intent_id"],
                    "symbol": reservation["symbol"],
                    "side": side,
                    "quantity": reservation["quantity"],
                    "price": reservation["price"],
                    "fee_reserve": reservation["fee_reserve"],
                    "exchange_order_id": reservation["exchange_order_id"],
                    "amount": reservation["amount"],
                    "reserved_quantity": _dstr(remainder),
                    "reservation_status": "HELD",
                    "status": "UNKNOWN" if side == "BUY" else "OPEN",
                    "state": "OPEN",
                    "filled_quantity": _dstr(filled),
                }
            )
        peer = self._probe_peer_snapshot(instant)
        for symbol, quantity in peer["inventory"].items():
            inventory[symbol] = inventory.get(symbol, ZERO) + _dec(quantity)
        local_position_symbols = {
            str(row["symbol"]).upper()
            for row in positions
            if _dec(row["quantity"]) > ZERO and str(row["symbol"]).upper() != QUOTE_ASSET
        }
        peer_position_symbols = set(peer["positions"])
        aggregate = sum(
            (_dec(row["cost_basis"]) for row in positions if _dec(row["quantity"]) > ZERO),
            ZERO,
        ) + held_exposure
        aggregate += sum(
            (_dec(value["cost_basis"]) for value in peer["positions"].values()),
            ZERO,
        ) + _dec(peer["held_buy_exposure"])
        # ``pending_reservation`` carries local BUY amount+fee; adding it here
        # would double count local buying power.  SELL reservations reserve
        # base inventory only, never quote buying power.
        reserved = _dec(peer["held_quote"])
        submission_row = self._conn.execute(
            "SELECT "
            "COALESCE(SUM(CASE WHEN intent='ENTRY' THEN 1 ELSE 0 END),0) AS entry_count,"
            "COALESCE(SUM(CASE WHEN intent='EXIT' THEN 1 ELSE 0 END),0) AS exit_count "
            "FROM binance_execution_order_intents "
            "WHERE submitted_at IS NOT NULL AND submitted_at LIKE ?",
            (day_prefix,),
        ).fetchone()
        entry_count = int(submission_row["entry_count"] or 0) + int(peer["submissions"])
        exit_count = int(submission_row["exit_count"] or 0)
        fee_rows = self._conn.execute(
            "SELECT * FROM binance_execution_fills WHERE trade_time LIKE ?",
            (day_prefix,),
        ).fetchall()
        all_fill_rows = self._conn.execute(
            "SELECT * FROM binance_execution_fills ORDER BY trade_time,trade_id"
        ).fetchall()
        local_realized_today = ZERO
        lots: dict[str, list[list[Decimal]]] = {}
        day_date = instant.date()
        for fill in all_fill_rows:
            symbol = str(fill["symbol"]).upper()
            side = str(fill["side"]).upper()
            qty = _dec(fill["quantity"])
            price = _dec(fill["price"])
            quote = _dec(fill["quote_quantity"])
            commission = _dec(fill["commission"])
            asset = str(fill["commission_asset"] or "").upper()
            fee_quote, _ = self._fee_quote(fill, commission, asset, price, _load(fill["payload_json"], {}) or {})
            base = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else ""
            book = lots.setdefault(symbol, [])
            if side == "BUY":
                net_qty = max(ZERO, qty - (commission if asset == base else ZERO))
                if net_qty > ZERO:
                    book.append([net_qty, quote + (commission if asset == QUOTE_ASSET else ZERO)])
            elif side == "SELL":
                remaining = qty
                proceeds = quote - (commission if asset == QUOTE_ASSET else ZERO)
                trade_pnl = ZERO
                while remaining > ZERO and book:
                    lot_qty, lot_cost = book[0]
                    matched = min(remaining, lot_qty)
                    trade_pnl += proceeds * (matched / qty) - lot_cost * (matched / lot_qty)
                    lot_qty -= matched
                    lot_cost -= lot_cost * (matched / (lot_qty + matched))
                    remaining -= matched
                    if lot_qty <= ZERO:
                        book.pop(0)
                    else:
                        book[0] = [lot_qty, lot_cost]
                if _utc(fill["trade_time"]).date() == day_date:
                    local_realized_today += trade_pnl
        fees_today = _dec(peer["fees_today"])
        unknown_fee = bool(peer["unknown_fee"])
        for fill in fee_rows:
            payload = _load(fill["payload_json"], {}) or {}
            fee_quote, fee_unknown = self._fee_quote(
                fill,
                _dec(fill["commission"]),
                fill["commission_asset"],
                _dec(fill["price"]),
                payload if isinstance(payload, Mapping) else {},
            )
            fees_today += fee_quote
            unknown_fee = unknown_fee or fee_unknown
        realized_total = sum((_dec(row["realized_pnl"]) for row in positions), ZERO) + _dec(peer["realized"])
        realized_today = local_realized_today + _dec(peer["realized_today"])
        unrealized_values = [
            _dec(row["unrealized_pnl"])
            for row in positions
            if row["unrealized_pnl"] is not None
        ]
        unrealized = sum(unrealized_values, ZERO)
        realized_loss = max(ZERO, -realized_today)
        equity_loss = realized_loss + max(ZERO, -unrealized)
        account_body = {
            "quote_available": _account_free_quote(account) or ZERO,
            "aggregate_exposure": aggregate,
            "reserved_exposure": reserved,
            "owned_inventory": {symbol: _dstr(quantity) for symbol, quantity in inventory.items() if quantity > ZERO},
            "pending_orders": pending,
            "realized_pnl_today": realized_today,
            "positions": len(local_position_symbols | peer_position_symbols),
            "entry_submissions_today": entry_count,
            "exit_submissions_today": exit_count,
            "realized_loss_today": realized_loss,
            "equity_loss_today": equity_loss,
            "unrealized_pnl": unrealized,
            "fees_today": fees_today,
            "open_orders": len(pending),
            "account_order_count": len(pending),
            "exchange_order_count": len(pending),
            "account_paused": (
                unknown_fee
                or bool(peer["malformed"])
                or peer["reason"] in {"PROBE_RESET", "PROBE_UNKNOWN"}
                or any(str(row["valuation_status"]).upper() == "UNKNOWN" for row in positions)
            ),
        }
        if self.control().get("state") == PAUSED:
            account_body["account_paused"] = True
        return RiskSnapshot.from_account(account_body, now=instant)



    # ---- account/risk -------------------------------------------------------------
    def update_account(self, account: Mapping[str, Any], *, epoch: Any = None, observed_at: Any = None) -> dict[str, Any]:
        # Account payloads are exchange data, but redact accidental credentials
        # before they enter the durable execution namespace.
        body = dict(_load(_json(dict(account)), {}) or {})
        if self.environment == BINANCE_SPOT_TESTNET:
            quote = _account_free_quote(body)
            body["quote_available"] = _dstr(quote) if quote is not None else "0"
        if epoch is None:
            epoch = body.get("epoch", body.get("accountEpoch", body.get("account_epoch")))
        now = _iso(observed_at or self.clock())
        with self._lock:
            self._conn.execute(
                "INSERT INTO binance_execution_account(singleton,account_json,epoch,observed_at) VALUES(1,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET account_json=excluded.account_json,epoch=excluded.epoch,observed_at=excluded.observed_at",
                (_json(body), None if epoch is None else str(epoch), now),
            )
            self._conn.commit()
        self.account_state = body
        return body

    set_account = update_account

    def _snapshot(self, now: Any = None) -> RiskSnapshot:
        account = dict(self.account_state)
        row = self._conn.execute(
            "SELECT account_json FROM binance_execution_account WHERE singleton=1"
        ).fetchone()
        if row is not None:
            account.update(_load(row[0], {}) or {})

        if self.environment == BINANCE_SPOT_TESTNET:
            return self._strict_snapshot(account, now)
        # The durable ledger is authoritative for inventory and reservations.
        position_rows = self._conn.execute(
            "SELECT symbol,quantity,valuation_status FROM binance_execution_positions ORDER BY symbol"
        ).fetchall()
        positions = [dict(position) for position in position_rows]
        reservation_rows = self._conn.execute(
            """
            SELECT i.intent_id AS intent_id, i.symbol AS symbol, r.side AS side,
                   i.quantity AS quantity, i.price AS price, r.fee_reserve AS fee_reserve,
                   i.exchange_order_id AS exchange_order_id, r.amount AS amount,
                   r.reserved_quantity AS reserved_quantity, r.status AS reservation_status,
                   i.state AS intent_state
            FROM binance_execution_risk_reservations AS r
            JOIN binance_execution_order_intents AS i ON i.intent_id = r.intent_id
            WHERE r.status='HELD'
            ORDER BY r.created_at, r.reservation_id
            """
        ).fetchall()
        pending = []
        pending_buy_exposure = ZERO
        inventory = dict(account.get("owned_inventory", account.get("inventory", {})) or {})
        for position in positions:
            if _dec(position["quantity"]) > ZERO:
                inventory[str(position["symbol"])] = str(position["quantity"])
        for reservation in reservation_rows:
            pending_row = dict(reservation)
            filled_rows = self._conn.execute(
                "SELECT quantity FROM binance_execution_fills WHERE intent_id=?",
                (reservation["intent_id"],),
            ).fetchall()
            filled_quantity = sum((_dec(item[0]) for item in filled_rows), ZERO)
            pending_row["filled_quantity"] = _dstr(filled_quantity)
            pending_row["state"] = "OPEN"
            side = str(reservation["side"]).upper()
            if side == "SELL":
                original = _dec(reservation["quantity"])
                remainder = max(ZERO, original - filled_quantity)
                pending_row["reserved_quantity"] = _dstr(remainder)
                symbol = str(reservation["symbol"])
                available = _dec(inventory.get(symbol, ZERO))
                inventory[symbol] = _dstr(max(ZERO, available - remainder))
            else:
                pending_row["reserved_quantity"] = "0"
                pending_buy_exposure += _dec(reservation["amount"], ZERO)
            pending.append(pending_row)

        account["aggregate_exposure"] = _dstr(
            _dec(account.get("aggregate_exposure", account.get("exposure", ZERO)))
            + pending_buy_exposure
        )
        account.setdefault("reserved_exposure", "0")
        if positions:
            account["positions"] = len(
                [
                    position
                    for position in positions
                    if str(position["symbol"]).upper() != QUOTE_ASSET
                    and _dec(position["quantity"]) > ZERO
                ]
            )
        else:
            account.setdefault(
                "positions",
                len(
                    [
                        value
                        for key, value in inventory.items()
                        if str(key).upper() != QUOTE_ASSET and _dec(value) > ZERO
                    ]
                ),
            )
        account["owned_inventory"] = inventory
        account["pending_orders"] = pending

        # Durable orders must participate in exchange/order-limit checks even
        # when the latest account payload predates this service's reservation.
        pending_count = len(pending)
        account["open_orders"] = max(
            int(account.get("open_orders", account.get("open_order_count", 0)) or 0),
            pending_count,
        )
        account["account_order_count"] = max(
            int(account.get("account_order_count", account.get("open_orders", 0)) or 0),
            pending_count,
        )
        account["exchange_order_count"] = max(
            int(account.get("exchange_order_count", account.get("open_orders", 0)) or 0),
            pending_count,
        )
        if any(str(position["valuation_status"]).upper() == "UNKNOWN" for position in positions):
            account["account_paused"] = True
        return RiskSnapshot.from_account(account, now=_utc(now or self.clock()))

    # ---- signals and order lifecycle ----------------------------------------------
    def _normalize_signal(self, signal: Mapping[str, Any]) -> dict[str, Any]:
        required = ("signal_id", "candidate_id", "symbol", "environment", "decision_interval", "decision_at", "intent", "side")
        missing = [key for key in required if signal.get(key) in (None, "")]
        if missing:
            raise ValueError("signal missing: " + ",".join(missing))
        output = dict(signal)
        output["signal_id"] = str(signal["signal_id"])
        output["candidate_id"] = str(signal["candidate_id"])
        output["symbol"] = _symbol(signal["symbol"])
        output["environment"] = _env(signal["environment"])
        output["intent"] = str(signal["intent"]).upper()
        output["side"] = str(signal["side"]).upper()
        if output["intent"] not in {"ENTRY", "EXIT"} or output["side"] not in {"BUY", "SELL"}:
            raise ValueError("signal intent/side invalid")
        if output["intent"] == "ENTRY" and output["side"] != "BUY":
            raise ValueError("ENTRY must be BUY")
        if output["intent"] == "EXIT" and output["side"] != "SELL":
            raise ValueError("EXIT must be SELL")
        if output["environment"] != self.environment:
            raise ValueError("signal environment mismatch")
        output["binding"] = _canonical(signal.get("binding", signal.get("successor_binding")), None)
        output["provenance"] = _canonical(signal.get("provenance"), None)
        provenance_strategy = output["provenance"].get("strategy_ref") if isinstance(output["provenance"], Mapping) else None
        output["strategy_ref"] = _canonical(signal.get("strategy_ref", provenance_strategy), None)
        output["binding_hash"] = str(signal.get("binding_hash") or (self._binding_hash_for(output["binding"]) if isinstance(output["binding"], Mapping) else self._binding_hash()))
        output["opportunity_id"] = None if signal.get("opportunity_id") is None else str(signal["opportunity_id"])
        if self._dynamic_mode:
            output["entry_policy_hash"] = self.entry_policy_hash
        return output

    def _client_order_id(self, signal: Mapping[str, Any]) -> str:
        raw = "AXIOM-" + canonical_sha256({"signal_id": str(signal["signal_id"]), "symbol": signal["symbol"], "binding_hash": signal.get("binding_hash")})[:24]
        return raw[:36]

    def _intent_response(self, row: sqlite3.Row | None, *, risk_reservation: Any = _MISSING_RESERVATION) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        if risk_reservation is _MISSING_RESERVATION:
            risk_reservation = self._conn.execute(
                "SELECT reservation_id,intent_id,symbol,side,amount,reserved_quantity,"
                "fee_reserve,status,created_at,released_at "
                "FROM binance_execution_risk_reservations WHERE intent_id=?",
                (result["intent_id"],),
            ).fetchone()
        result["risk_reservation"] = dict(risk_reservation) if risk_reservation is not None else None
        for key in ("exit_policy_json", "binding_json", "provenance_json", "strategy_ref_json", "raw_json"):
            if key in result:
                result[key[:-5]] = _load(result[key], None)
        return result
    @staticmethod
    def _market_value(market: Any, *names: str, default: Any = None) -> Any:
        if isinstance(market, Mapping):
            for name in names:
                if name in market:
                    return market[name]
            return default
        for name in names:
            value = getattr(market, name, None)
            if value is not None:
                return value
        return default

    def _strict_market_rejection(
        self,
        data: Mapping[str, Any],
        price: Decimal,
        market: Any,
        *,
        now: Any = None,
    ) -> str | None:
        if self.environment != BINANCE_SPOT_TESTNET or data.get("intent") != "ENTRY":
            return None
        if market is None:
            return "MARKET_EVIDENCE_REQUIRED"
        try:
            symbol = _symbol(self._market_value(market, "symbol", "pair"))
        except ValueError:
            return "MARKET_SYMBOL_MISMATCH"
        if symbol != data["symbol"]:
            return "MARKET_SYMBOL_MISMATCH"
        fill_evidence = self._market_value(market, "fill_evidence", default={})
        if not isinstance(fill_evidence, Mapping):
            return "MARKET_EVIDENCE_STALE"
        buy = fill_evidence.get("buy")
        if not isinstance(buy, Mapping):
            buy = {}
        reference = buy.get("price")
        if reference is None:
            ticker = self._market_value(market, "ticker", default=None)
            reference = self._market_value(ticker, "ask", "last") if ticker is not None else None
        try:
            reference_value = _dec(reference)
        except (TypeError, ValueError):
            return "MARKET_REFERENCE_UNAVAILABLE"
        if reference_value <= ZERO:
            return "MARKET_REFERENCE_UNAVAILABLE"
        ticker_fresh = self._market_value(market, "ticker_fresh", "fresh_ticker", default=None)
        book_fresh = self._market_value(market, "book_fresh", "fresh_book", default=None)
        fill_fresh = buy.get("fresh", fill_evidence.get("fresh"))
        if ticker_fresh is None:
            ticker_fresh = self._market_value(market, "fresh", "market_fresh", default=False)
        if book_fresh is None:
            book_fresh = self._market_value(market, "fresh", "market_fresh", default=False)
        if not bool(ticker_fresh) or not bool(book_fresh) or fill_fresh is False:
            return "MARKET_EVIDENCE_STALE"
        observed_at = self._market_value(market, "observed_at", "timestamp", default=None)
        if observed_at is not None:
            age = (_utc(now or self.clock()) - _utc(observed_at)).total_seconds()
            if age < 0 or age > 120:
                return "MARKET_EVIDENCE_STALE"
        deviation = (abs(price - reference_value) / reference_value) * Decimal("10000")
        if deviation > self.risk_envelope.max_execution_deviation_bps:
            return "EXECUTION_DEVIATION_EXCEEDED"
        return None


    def submit_signal(
        self,
        signal: Mapping[str, Any],
        *,
        price: Any,
        quantity: Any,
        rules: SymbolRules | Mapping[str, Any] | None = None,
        fee_rate: Any = ZERO,
        time_in_force: str = "IOC",
        now: Any = None,
        opportunity_id: str | None = None,
        risk_snapshot: RiskSnapshot | Mapping[str, Any] | None = None,
        market: Any = None,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        data = self._normalize_signal({**dict(signal), **({"opportunity_id": opportunity_id} if opportunity_id is not None else {})})
        if self._deadline_expired(deadline_monotonic):
            return self._reject_signal(data, "AUTO_DEADLINE_EXPIRED", now=now)
        if not self._dynamic_mode and data["binding_hash"] != self._binding_hash():
            return self._reject_signal(data, "BINDING_CHANGED", now=now)
        if data["intent"] == "ENTRY":
            origin_reason = self._entry_origin_rejection(data)
            if origin_reason is not None:
                return self._reject_signal(data, origin_reason, now=now)
        if data["intent"] == "EXIT":
            origin_reason = self._exit_origin_rejection(data)
            if origin_reason is not None:
                return self._reject_signal(data, origin_reason, now=now)
        if rules is not None:
            sized = size_limit_order(rules if isinstance(rules, SymbolRules) else SymbolRules.from_exchange_info(rules), data["side"], price, quantity, envelope=self.risk_envelope, fee_rate=fee_rate, available_inventory=(self._snapshot(now).owned_inventory.get(data["symbol"], ZERO) if data["side"] == "SELL" else None))
            if self._deadline_expired(deadline_monotonic):
                return self._reject_signal(data, "AUTO_DEADLINE_EXPIRED", now=now)
            if not sized.valid:
                return self._reject_signal(data, ";".join(sized.reasons), now=now)
            price_value, qty_value, notional, fee_value = sized.price, sized.quantity, sized.notional, sized.fee_reserve
        else:
            price_value, qty_value = _dec(price), _dec(quantity)
            notional, fee_value = price_value * qty_value, price_value * qty_value * _dec(fee_rate)
        market_reason = self._strict_market_rejection(data, price_value, market, now=now)
        if market_reason is not None:
            return self._reject_signal(data, market_reason, now=now)
        existing = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE signal_id=?", (data["signal_id"],)).fetchone()
        if existing is not None:
            return self._intent_response(existing) or {}
        state = self.control()
        blocked = {DISABLED, DISARMED, KILLED} if data["intent"] == "EXIT" else {DISABLED, PAUSED, DISARMED, KILLED}
        if not state.get("authorized") or state.get("state") in blocked:
            return self._reject_signal(data, "CONTROL_" + str(state.get("state", DISABLED)), now=now)
        snapshot = risk_snapshot if risk_snapshot is not None else self._snapshot(now)
        if self._deadline_expired(deadline_monotonic):
            return self._reject_signal(data, "AUTO_DEADLINE_EXPIRED", now=now)
        if data["intent"] == "ENTRY":
            peer_block_reason = self._probe_entry_block_reason(now)
            if peer_block_reason is not None:
                return self._reject_signal(data, peer_block_reason, now=now)
            assessment = assess_entry(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee_value, now=_utc(now or self.clock()))
        else:
            assessment = assess_exit(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee_value, now=_utc(now or self.clock()), symbol=data["symbol"], quantity=qty_value, minimum_notional=(rules.min_notional if isinstance(rules, SymbolRules) else None))
        if self._deadline_expired(deadline_monotonic):
            return self._reject_signal(data, "AUTO_DEADLINE_EXPIRED", now=now)
        if not assessment.allowed:
            return self._reject_signal(data, ";".join(assessment.reasons), now=now)
        return self._reserve_and_send(data, price_value, qty_value, notional, fee_value, time_in_force, now=now, minimum_notional=(rules.min_notional if isinstance(rules, SymbolRules) else None), market=market, deadline_monotonic=deadline_monotonic)

    submit = submit_signal
    execute = submit_signal
    place_order = submit_signal

    def _reject_signal(self, data: Mapping[str, Any], reason: str, *, now: Any = None) -> dict[str, Any]:
        current = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE signal_id=?", (data["signal_id"],)).fetchone()
        if current is not None:
            return self._intent_response(current) or {}
        now_iso = _iso(now or self.clock())
        intent_id = uuid.uuid4().hex
        client_id = self._client_order_id(data)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                generation = int(self.control().get("generation", 0))
                self._conn.execute(
                    "INSERT INTO binance_execution_signals(signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,decision_interval,decision_at,intent,side,reason,exit_policy_json,binding_json,provenance_json,strategy_ref_json,signal_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["decision_interval"], data["decision_at"], data["intent"], data["side"], data.get("reason"), _json(data.get("exit_policy")), _json(data.get("binding")), _json(data.get("provenance")), _json(data.get("strategy_ref")), _json(data), now_iso),
                )
                self._conn.execute(
                    "INSERT INTO binance_execution_order_intents(intent_id,signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,intent,side,price,quantity,notional,fee_reserve,client_order_id,state,generation,reason,exit_policy_json,binding_json,provenance_json,strategy_ref_json,updated_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (intent_id, data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["intent"], data["side"], "0", "0", "0", "0", client_id, REJECTED, generation, reason, _json(data.get("exit_policy")), _json(data.get("binding")), _json(data.get("provenance")), _json(data.get("strategy_ref")), now_iso, _json({"reason": reason})),
                )
                self._transition(intent_id, None, REJECTED, generation, reason, {"reason": reason})
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}

    def _reserve_and_send(
        self,
        data: Mapping[str, Any],
        price: Decimal,
        quantity: Decimal,
        notional: Decimal,
        fee: Decimal,
        tif: str,
        *,
        now: Any = None,
        minimum_notional: Any | None = None,
        market: Any = None,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        now_iso = _iso(now or self.clock())
        intent_id = uuid.uuid4().hex
        client_id = self._client_order_id(data)
        rejection_reason: str | None = None
        generation = 0
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._deadline_expired(deadline_monotonic):
                    rejection_reason = "AUTO_DEADLINE_EXPIRED"
                    self._conn.rollback()
                else:
                    existing = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE signal_id=? OR client_order_id=?", (data["signal_id"], client_id)).fetchone()
                    if existing is not None:
                        self._conn.rollback()
                        return self._intent_response(existing) or {}
                    ctl = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
                    generation = int(ctl["generation"] if ctl else 0)
                    state = str(ctl["state"]) if ctl is not None else DISABLED
                    authorized = bool(ctl["authorized"]) if ctl is not None else False
                    blocked = {DISABLED, DISARMED, KILLED} if data["intent"] == "EXIT" else {DISABLED, PAUSED, DISARMED, KILLED}
                    dynamic_rejection = self._dynamic_entry_rejection(data) if data["intent"] == "ENTRY" else None
                    entry_origin_rejection = self._entry_origin_rejection(data) if data["intent"] == "ENTRY" else None
                    exit_rejection = self._exit_origin_rejection(data) if data["intent"] == "EXIT" else None
                    peer_block_reason = self._probe_entry_block_reason(now) if data["intent"] == "ENTRY" else None
                    if not authorized or state in blocked:
                        rejection_reason = "CONTROL_" + state
                        self._conn.rollback()
                    elif dynamic_rejection is not None:
                        rejection_reason = dynamic_rejection
                        self._conn.rollback()
                    elif entry_origin_rejection is not None:
                        rejection_reason = entry_origin_rejection
                        self._conn.rollback()
                    elif peer_block_reason is not None:
                        rejection_reason = peer_block_reason
                        self._conn.rollback()
                    elif exit_rejection is not None:
                        rejection_reason = exit_rejection
                        self._conn.rollback()
                    elif self._deadline_expired(deadline_monotonic):
                        rejection_reason = "AUTO_DEADLINE_EXPIRED"
                        self._conn.rollback()
                    else:
                        snapshot = self._snapshot(now)
                        if self._deadline_expired(deadline_monotonic):
                            rejection_reason = "AUTO_DEADLINE_EXPIRED"
                            self._conn.rollback()
                        else:
                            if data["intent"] == "ENTRY":
                                assessment = assess_entry(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee, now=_utc(now or self.clock()))
                            else:
                                assessment = assess_exit(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee, now=_utc(now or self.clock()), symbol=data["symbol"], quantity=quantity, minimum_notional=minimum_notional)
                            if not assessment.allowed:
                                rejection_reason = ";".join(assessment.reasons)
                                self._conn.rollback()
                            else:
                                self._conn.execute(
                                    "INSERT INTO binance_execution_signals(signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,decision_interval,decision_at,intent,side,reason,exit_policy_json,binding_json,provenance_json,strategy_ref_json,signal_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["decision_interval"], data["decision_at"], data["intent"], data["side"], data.get("reason"), _json(data.get("exit_policy")), _json(data.get("binding")), _json(data.get("provenance")), _json(data.get("strategy_ref")), _json(data), now_iso),
                                )
                                self._conn.execute(
                                    "INSERT INTO binance_execution_order_intents(intent_id,signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,intent,side,price,quantity,notional,fee_reserve,client_order_id,state,generation,owner_id,lease_expires_at,exit_policy_json,binding_json,provenance_json,strategy_ref_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (intent_id, data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["intent"], data["side"], _dstr(price), _dstr(quantity), _dstr(notional), _dstr(fee), client_id, INTENT, generation, self.owner_id, _iso(_utc(self.clock()) + timedelta(seconds=self.lease_seconds)), _json(data.get("exit_policy")), _json(data.get("binding")), _json(data.get("provenance")), _json(data.get("strategy_ref")), now_iso),
                                )
                                self._transition(intent_id, None, INTENT, generation, "created", {})
                                self._conn.execute(
                                    "INSERT INTO binance_execution_risk_reservations(reservation_id,intent_id,symbol,side,amount,reserved_quantity,fee_reserve,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                                    (uuid.uuid4().hex, intent_id, data["symbol"], data["side"], _dstr(notional), _dstr(quantity) if data["side"] == "SELL" else "0", _dstr(fee), "HELD", now_iso),
                                )
                                self._transition(intent_id, INTENT, RESERVED, generation, "risk reserved", {"amount": _dstr(notional), "fee_reserve": _dstr(fee)})
                                self._conn.execute("UPDATE binance_execution_order_intents SET state=?,updated_at=? WHERE intent_id=?", (RESERVED, now_iso, intent_id))
                                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        if rejection_reason is not None:
            return self._reject_signal(data, rejection_reason, now=now)
        if self._deadline_expired(deadline_monotonic):
            return self._expire_unsent(intent_id, "AUTO_DEADLINE_EXPIRED", generation=generation)
        self._fault("before_network_submit")
        if self._deadline_expired(deadline_monotonic):
            return self._expire_unsent(intent_id, "AUTO_DEADLINE_EXPIRED", generation=generation)
        return self._network_submit(
            intent_id,
            data,
            price,
            quantity,
            tif,
            generation,
            market=market,
            deadline_monotonic=deadline_monotonic,
        )

    def _transition(self, intent_id: str, from_state: str | None, to_state: str, generation: int, reason: str, payload: Any) -> None:
        self._conn.execute("INSERT OR IGNORE INTO binance_execution_order_transitions(intent_id,from_state,to_state,generation,observed_at,reason,payload_json) VALUES(?,?,?,?,?,?,?)", (intent_id, from_state, to_state, generation, _iso(self.clock()), reason, _json(payload)))

    def _expire_unsent(self, intent_id: str, reason: str, *, generation: int | None = None) -> dict[str, Any]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                if row is None:
                    self._conn.rollback()
                    return {}
                expected_generation = int(row["generation"]) if generation is None else int(generation)
                if (
                    str(row["state"]) not in {RESERVED, SUBMITTING}
                    or int(row["generation"]) != expected_generation
                    or str(row["owner_id"] or "") != self.owner_id
                ):
                    current = self._intent_response(row) or {}
                    self._conn.rollback()
                    return current
                now = _iso(self.clock())
                changed = self._conn.execute(
                    "UPDATE binance_execution_order_intents SET state=?,reason=?,raw_json=?,updated_at=? "
                    "WHERE intent_id=? AND generation=? AND state IN ('RESERVED','SUBMITTING') AND owner_id=?",
                    (REJECTED, reason, _json({"reason": reason, "known_unsent": True}), now, intent_id, expected_generation, self.owner_id),
                )
                if changed.rowcount != 1:
                    current = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                    self._conn.rollback()
                    return self._intent_response(current) or {}
                self._transition(intent_id, str(row["state"]), REJECTED, expected_generation, reason, {"known_unsent": True})
                self._conn.execute("UPDATE binance_execution_risk_reservations SET status='RELEASED',released_at=? WHERE intent_id=? AND status='HELD'", (now, intent_id))
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}

    def _final_submit_fence(self, intent_id: str, data: Mapping[str, Any], generation: int, deadline_monotonic: float | None) -> str | None:
        """Run the last durable/authentication fence directly before the sink."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                if row is None:
                    self._conn.rollback()
                    return "MISSING"
                if self._deadline_expired(deadline_monotonic):
                    self._conn.rollback()
                    return "AUTO_DEADLINE_EXPIRED"
                if (
                    str(row["state"]) != SUBMITTING
                    or int(row["generation"]) != int(generation)
                    or str(row["owner_id"] or "") != self.owner_id
                ):
                    self._conn.rollback()
                    return "TERMINAL"
                if not self._fence(generation, entries=data["intent"] == "ENTRY"):
                    self._conn.rollback()
                    return "FENCE_REJECTED"
                if data["intent"] == "ENTRY":
                    peer_block_reason = self._probe_entry_block_reason(self.clock())
                    if peer_block_reason is not None:
                        self._conn.rollback()
                        return peer_block_reason
                    reason = self._dynamic_entry_rejection(data)
                    if reason is not None:
                        self._conn.rollback()
                        return reason
                elif data["intent"] == "EXIT":
                    reason = self._exit_origin_rejection(data)
                    if reason is not None:
                        self._conn.rollback()
                        return reason
                if self._deadline_expired(deadline_monotonic):
                    self._conn.rollback()
                    return "AUTO_DEADLINE_EXPIRED"
                self._conn.commit()
                return None
            except BaseException:
                self._conn.rollback()
                raise

    def _network_submit(
        self,
        intent_id: str,
        data: Mapping[str, Any],
        price: Decimal,
        quantity: Decimal,
        tif: str,
        generation: int,
        *,
        market: Any = None,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            if self._deadline_expired(deadline_monotonic):
                self._conn.rollback()
                return self._expire_unsent(intent_id, "AUTO_DEADLINE_EXPIRED", generation=generation)
            row = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                self._conn.rollback()
                return {}
            if not self._claim(row, generation):
                current = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                self._conn.rollback()
                return self._intent_response(current) or {}
            self._transition(intent_id, RESERVED, SUBMITTING, generation, "before venue call", {})
            self._conn.commit()
        if not self._fence(generation, entries=data["intent"] == "ENTRY"):
            return self._set_unknown(intent_id, "fence rejected before venue call", generation=generation)
        self._fault("before_venue_call")
        if self._deadline_expired(deadline_monotonic):
            return self._expire_unsent(intent_id, "AUTO_DEADLINE_EXPIRED", generation=generation)
        final_reason = self._final_submit_fence(intent_id, data, generation, deadline_monotonic)
        if final_reason is not None:
            if final_reason == "FENCE_REJECTED":
                return self._set_unknown(intent_id, "fence rejected before venue call", generation=generation)
            if final_reason in {"TERMINAL", "MISSING"}:
                current = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                return self._intent_response(current) or {}
            return self._expire_unsent(intent_id, final_reason, generation=generation)
        if self._deadline_expired(deadline_monotonic):
            return self._expire_unsent(intent_id, "AUTO_DEADLINE_EXPIRED", generation=generation)
        market_reason = self._strict_market_rejection(data, price, market, now=self.clock())
        if market_reason is not None:
            return self._expire_unsent(intent_id, market_reason, generation=generation)
        try:
            if self.venue is None:
                raise RuntimeError("no Binance venue configured")
            method = getattr(self.venue, "place_limit_order", None) or getattr(self.venue, "place_order", None)
            if method is None:
                raise RuntimeError("venue has no place_limit_order")
            kwargs = {"symbol": data["symbol"], "side": data["side"], "quantity": _dstr(quantity), "price": _dstr(price), "time_in_force": tif}
            if deadline_monotonic is not None:
                kwargs["deadline_monotonic"] = deadline_monotonic
            if isinstance(self.venue, BinanceSpotRESTClient):
                kwargs["newClientOrderId"] = self._client_order_id(data)
            else:
                kwargs.update({"new_client_order_id": self._client_order_id(data), "newClientOrderId": self._client_order_id(data), "client_order_id": self._client_order_id(data), "orig_client_order_id": self._client_order_id(data)})
            result = _call(method, kwargs)
        except BaseException as exc:
            return self._set_unknown(intent_id, f"venue exception: {type(exc).__name__}", generation=generation)
        control = self.control()
        if int(control.get("generation", -1)) != int(generation) or control.get("state") == KILLED or control.get("kill_requested"):
            return self._set_unknown(intent_id, "control generation changed during venue call", generation=generation, result=result)
        return self._apply_order_result(intent_id, result, generation)

    def _claim(self, row: sqlite3.Row, generation: int) -> bool:
        now = _utc(self.clock())
        lease = _iso(now + timedelta(seconds=self.lease_seconds))
        result = self._conn.execute(
            "UPDATE binance_execution_order_intents SET state=?,submitted_at=?,owner_id=?,lease_expires_at=?,updated_at=? "
            "WHERE intent_id=? AND generation=? AND state=? AND (owner_id IS NULL OR owner_id=? OR lease_expires_at IS NULL OR lease_expires_at<?)",
            (SUBMITTING, _iso(now), self.owner_id, lease, _iso(now), row["intent_id"], int(generation), RESERVED, self.owner_id, _iso(now)),
        )
        return result.rowcount == 1
    def _fence(self, generation: int, *, entries: bool = True) -> bool:
        """Require the durable control row to remain authorized for this sink."""
        row = self._conn.execute(
            "SELECT * FROM binance_execution_control WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(row["generation"]) != int(generation)
            or bool(row["kill_requested"])
            or str(row["state"]) in {DISABLED, DISARMED, KILLED}
        ):
            return False
        if entries and str(row["state"]) != ARMED:
            return False
        current = (
            self.environment,
            self._binding_hash(),
            self.risk_envelope.canonical_hash,
            self._credential_hash(),
            self.entry_policy_hash or "",
        )
        persisted = (
            str(row["environment"]),
            str(row["binding_hash"]),
            str(row["envelope_hash"]),
            str(row["credential_hash"]),
            str(row["entry_policy_hash"] or ""),
        )
        return bool(row["authorized"]) and current == persisted

    @staticmethod
    def _merge_order_state(
        current: str,
        observed: str,
        *,
        original_quantity: Decimal,
        observed_filled: Decimal,
    ) -> str:
        """Merge an observation without allowing a newer state to regress."""
        current = str(current).upper()
        observed = str(observed).upper()
        if current in TERMINAL:
            # Once an authoritative terminal state is durable, a delayed
            # submission/query response must not resurrect or rewrite it.
            return current
        if current == PARTIALLY_FILLED:
            if original_quantity > ZERO and observed_filled >= original_quantity:
                return FILLED
            if observed in {CANCELED, EXPIRED}:
                return observed
            return PARTIALLY_FILLED
        if observed in {CANCELED, EXPIRED}:
            return (
                FILLED
                if original_quantity > ZERO and observed_filled >= original_quantity
                else observed
            )
        if observed == FILLED and original_quantity > ZERO and observed_filled >= original_quantity:
            return FILLED
        if observed == PARTIALLY_FILLED or observed_filled > ZERO:
            return PARTIALLY_FILLED
        rank = {
            INTENT: 0,
            RESERVED: 1,
            SUBMITTING: 2,
            UNKNOWN: 3,
            ACKNOWLEDGED: 4,
            PARTIALLY_FILLED: 5,
            FILLED: 6,
            CANCELED: 6,
            EXPIRED: 6,
            REJECTED: 6,
        }
        return observed if rank.get(observed, 0) >= rank.get(current, 0) else current

    def _filled_quantity_locked(self, intent_id: str) -> Decimal:
        rows = self._conn.execute(
            "SELECT quantity FROM binance_execution_fills WHERE intent_id=?",
            (intent_id,),
        ).fetchall()
        return sum((_dec(row[0]) for row in rows), ZERO)
    def _exact_filled_quantity_locked(
        self,
        intent_id: str,
        exchange_order_id: Any,
    ) -> Decimal:
        """Sum only durable trade rows carrying this exact order identity."""
        expected = str(exchange_order_id)
        rows = self._conn.execute(
            "SELECT quantity,payload_json FROM binance_execution_fills "
            "WHERE intent_id=?",
            (intent_id,),
        ).fetchall()
        covered = ZERO
        for row in rows:
            payload = _load(row["payload_json"], {})
            if (
                isinstance(payload, Mapping)
                and payload.get("orderId") is not None
                and str(payload["orderId"]) == expected
            ):
                covered += _dec(row["quantity"])
        return covered

    def _persist_sell_reservation_remainder_locked(
        self,
        intent: Mapping[str, Any],
        filled_quantity: Decimal,
    ) -> bool:
        if str(intent["side"]).upper() != "SELL":
            return False
        remainder = max(ZERO, _dec(intent["quantity"]) - filled_quantity)
        current = self._conn.execute(
            "SELECT reserved_quantity "
            "FROM binance_execution_risk_reservations "
            "WHERE intent_id=? AND UPPER(side)='SELL' AND status='HELD'",
            (intent["intent_id"],),
        ).fetchone()
        if current is None or _dec(current["reserved_quantity"]) == remainder:
            return False
        self._conn.execute(
            "UPDATE binance_execution_risk_reservations "
            "SET reserved_quantity=? "
            "WHERE intent_id=? AND UPPER(side)='SELL' AND status='HELD'",
            (_dstr(remainder), intent["intent_id"]),
        )
        return True


    def _insert_fills_locked(
        self,
        intent: sqlite3.Row,
        fills: Iterable[Mapping[str, Any]],
        *,
        exchange_order_id: str | None = None,
    ) -> int:
        count = 0
        for fill in fills:
            qty = _dec(fill.get("qty", fill.get("quantity", fill.get("executedQty", 0))))
            if qty <= ZERO:
                continue
            trade_id = _exchange_trade_id(fill)
            if trade_id is None:
                trade_id = canonical_sha256({"intent_id": intent["intent_id"], "fill": dict(fill)})[:32]
            trade_id = _scoped_trade_id(intent["symbol"], trade_id)
            if self._conn.execute(
                "SELECT 1 FROM binance_execution_fills WHERE trade_id=?",
                (trade_id,),
            ).fetchone() is not None:
                continue
            price = _dec(fill.get("price", intent["price"]))
            quote = _dec(fill.get("quoteQty", fill.get("quote_quantity", qty * price)))
            commission = _dec(fill.get("commission", fill.get("fee", 0)))
            asset = fill.get("commissionAsset", fill.get("commission_asset", fill.get("fee_asset")))
            timestamp = fill.get("time", fill.get("trade_time", fill.get("timestamp", self.clock())))
            self._conn.execute(
                "INSERT INTO binance_execution_fills("
                "trade_id,intent_id,client_order_id,exchange_order_id,symbol,side,"
                "quantity,price,quote_quantity,commission,commission_asset,trade_time,payload_json"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    trade_id,
                    intent["intent_id"],
                    intent["client_order_id"],
                    exchange_order_id or intent["exchange_order_id"],
                    intent["symbol"],
                    intent["side"],
                    _dstr(qty),
                    _dstr(price),
                    _dstr(quote),
                    _dstr(commission),
                    None if asset is None else str(asset).upper(),
                    _iso(timestamp),
                    _json(fill),
                ),
            )
            count += 1
        return count

    def _set_unknown(
        self,
        intent_id: str,
        reason: str,
        *,
        generation: int | None = None,
        result: Any = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return {}
                if (
                    row["state"] in TERMINAL
                    or (
                        generation is not None
                        and int(row["generation"]) != int(generation)
                    )
                ):
                    current = self._intent_response(row) or {}
                    self._conn.rollback()
                    return current
                now = _iso(self.clock())
                self._conn.execute(
                    "UPDATE binance_execution_order_intents "
                    "SET state=?,reason=?,raw_json=?,updated_at=? WHERE intent_id=?",
                    (
                        UNKNOWN,
                        reason,
                        _json({"reason": reason, "result": _payload(result)}),
                        now,
                        intent_id,
                    ),
                )
                self._transition(
                    intent_id,
                    str(row["state"]),
                    UNKNOWN,
                    int(row["generation"]),
                    reason,
                    {"result": _payload(result)},
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self._intent_response(
            self._conn.execute(
                "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
        ) or {}
    def _strict_response_identity_reason(self, intent_id: str, payload: Any) -> str | None:
        if self.environment != BINANCE_SPOT_TESTNET:
            return None
        row = self._conn.execute(
            "SELECT client_order_id,symbol,side,exchange_order_id FROM binance_execution_order_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None or not isinstance(payload, Mapping):
            return "ORDER_RESPONSE_IDENTITY_MISMATCH"
        client = payload.get("clientOrderId", payload.get("client_order_id", payload.get("origClientOrderId")))
        if str(client or "") != str(row["client_order_id"]):
            return "ORDER_RESPONSE_CLIENT_ID_MISMATCH"
        try:
            if _symbol(payload.get("symbol")) != _symbol(row["symbol"]):
                return "ORDER_RESPONSE_SYMBOL_MISMATCH"
        except ValueError:
            return "ORDER_RESPONSE_SYMBOL_MISMATCH"
        if str(payload.get("side", "")).upper() != str(row["side"]).upper():
            return "ORDER_RESPONSE_SIDE_MISMATCH"
        response_order_id = _canonical_order_id(payload.get("orderId", payload.get("order_id")))
        if response_order_id is None:
            return "ORDER_RESPONSE_ORDER_ID_MISMATCH"
        stored_order_id = _canonical_order_id(row["exchange_order_id"])
        if stored_order_id is not None and response_order_id != stored_order_id:
            return "ORDER_RESPONSE_ORDER_ID_MISMATCH"
        return None


    def _apply_order_result(
        self,
        intent_id: str,
        result: Any,
        generation: int,
        *,
        require_authoritative: bool = False,
    ) -> dict[str, Any]:
        status = _status(result)
        payload = _payload(result)
        raw_status = _authoritative_order_state(payload)
        if require_authoritative:
            if status != "OK":
                return self._set_unknown(
                    intent_id,
                    "unresolved authoritative order response",
                    generation=generation,
                    result=result,
                )
            if raw_status is None:
                return self._set_unknown(
                    intent_id,
                    "malformed authoritative order response",
                    generation=generation,
                    result=result,
                )
        identity_reason = self._strict_response_identity_reason(intent_id, payload)
        if identity_reason is not None:
            return self._set_unknown(
                intent_id,
                identity_reason,
                generation=generation,
                result=result,
            )
        if status == UNKNOWN:
            return self._set_unknown(
                intent_id,
                "ambiguous venue result",
                generation=generation,
                result=result,
            )
        elif status == "RATE_LIMIT":
            # A rate limit says nothing about whether the venue accepted the
            # request.  Keep the reservation held and reconcile by client ID.
            return self._set_unknown(
                intent_id,
                "venue rate limited",
                generation=generation,
                result=result,
            )
        elif status not in {"OK", REJECTED}:
            return self._set_unknown(
                intent_id,
                "unresolved venue result",
                generation=generation,
                result=result,
            )

        if status == REJECTED:
            observed = REJECTED
            reason = "venue rejected"
        elif raw_status is None:
            # A successful transport response is not an order acknowledgement
            # unless its payload carries a recognized exchange order state.
            return self._set_unknown(
                intent_id,
                "malformed venue order response",
                generation=generation,
                result=result,
            )
        else:
            state = raw_status
            observed = {
                "CANCELLED": CANCELED,
                "CANCELED": CANCELED,
                "EXPIRED": EXPIRED,
                "FILLED": FILLED,
                "PARTIALLY_FILLED": PARTIALLY_FILLED,
                "ACKNOWLEDGED": ACKNOWLEDGED,
                "NEW": ACKNOWLEDGED,
            }[state]
            reason = "venue acknowledged"

        explicit_fills = _explicit_fill_rows(payload)
        if self.environment == BINANCE_SPOT_TESTNET and explicit_fills:
            expected_order_id = _canonical_order_id(payload.get("orderId")) if isinstance(payload, Mapping) else None
            intent_identity = self._conn.execute(
                "SELECT symbol,client_order_id FROM binance_execution_order_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            expected_symbol = (
                str(intent_identity["symbol"]).upper()
                if intent_identity is not None
                else ""
            )
            expected_client_order_id = (
                str(intent_identity["client_order_id"])
                if intent_identity is not None
                else ""
            )
            if expected_order_id is None or intent_identity is None or any(
                _canonical_order_id(fill.get("orderId")) != expected_order_id
                or str(fill.get("symbol", "")).upper().replace("/", "").replace("-", "").replace("_", "")
                != expected_symbol
                or (
                    (fill.get("clientOrderId") or fill.get("origClientOrderId")) not in (None, "")
                    and str(fill.get("clientOrderId") or fill.get("origClientOrderId"))
                    != expected_client_order_id
                )
                for fill in explicit_fills
            ):
                return self._set_unknown(
                    intent_id,
                    "TRADE_RESPONSE_IDENTITY_MISMATCH",
                    generation=generation,
                    result=result,
                )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return {}
                if int(row["generation"]) != int(generation):
                    current = self._intent_response(row) or {}
                    self._conn.rollback()
                    return current
                exchange_order_id = (
                    _canonical_order_id(payload.get("orderId"))
                    if self.environment == BINANCE_SPOT_TESTNET and isinstance(payload, Mapping)
                    else (
                        str(payload.get("orderId"))
                        if isinstance(payload, Mapping) and payload.get("orderId") is not None
                        else None
                    )
                )
                inserted = self._insert_fills_locked(
                    row,
                    explicit_fills,
                    exchange_order_id=exchange_order_id,
                )
                original = _dec(row["quantity"])
                cumulative = _cumulative_quantity(payload)
                durable_filled = self._filled_quantity_locked(intent_id)
                reservation_changed = self._persist_sell_reservation_remainder_locked(
                    row,
                    durable_filled,
                )
                observed_filled = max(cumulative, durable_filled)
                terminal_under_evidenced = (
                    observed in {CANCELED, EXPIRED}
                    and cumulative > durable_filled
                )
                if terminal_under_evidenced:
                    # An aggregate terminal response is not proof of fills.
                    # Keep it unresolved until myTrades supplies rows covering
                    # the exchange cumulative quantity.
                    target = UNKNOWN
                    transition_reason = "terminal fill evidence unresolved"
                else:
                    target = self._merge_order_state(
                        str(row["state"]),
                        observed,
                        original_quantity=original,
                        observed_filled=observed_filled,
                    )
                    transition_reason = reason
                changed = target != str(row["state"])
                if not changed and inserted == 0 and exchange_order_id is None and not reservation_changed:
                    current = self._intent_response(row) or {}
                    self._conn.rollback()
                    return current

                now = _iso(self.clock())
                self._conn.execute(
                    "UPDATE binance_execution_order_intents "
                    "SET state=?,exchange_order_id=COALESCE(?,exchange_order_id),"
                    "acknowledged_at=?,reason=?,raw_json=?,updated_at=? WHERE intent_id=?",
                    (
                        target,
                        exchange_order_id,
                        now,
                        transition_reason,
                        _json(payload),
                        now,
                        intent_id,
                    ),
                )
                if changed:
                    self._transition(
                        intent_id,
                        str(row["state"]),
                        target,
                        generation,
                        transition_reason,
                        payload,
                    )
                self._fault("before_order_result_commit")
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        # The transaction above is the durable fill/state commit.  Keep a
        # deterministic crash boundary here so a restart exercises the same
        # release recovery path as a process dying after commit.
        self._fault("after_order_result_commit")
        if inserted:
            self._recompute_position(str(row["symbol"]))
        self._release_if_done(intent_id)
        return (
            self._intent_response(
                self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
            )
            or {}
        )

    # ---- reconciliation and fills -------------------------------------------------
    def _filled_evidence_sufficient(self, row: Mapping[str, Any]) -> bool:
        """Return whether durable trade rows cover an authoritative FILLED."""

        if str(row["state"]) != FILLED:
            return False
        payload = _load(row["raw_json"], {}) or {}
        expected = _cumulative_quantity(payload)
        if expected <= ZERO:
            expected = _dec(row["quantity"])
        current = self._conn.execute(
            "SELECT quantity FROM binance_execution_fills WHERE intent_id=?",
            (row["intent_id"],),
        ).fetchall()
        filled = sum((_dec(item[0]) for item in current), ZERO)
        return expected > ZERO and filled >= expected

    def reconcile(self, *, symbol: str | None = None, account: Mapping[str, Any] | None = None, deadline_monotonic: float | None = None) -> dict[str, Any]:
        key = _symbol(symbol) if symbol else "ALL"
        attempted = _iso(self.clock())
        self._set_reconciliation(key, "RUNNING", attempted_at=attempted, heartbeat_at=attempted)
        if self._deadline_expired(deadline_monotonic):
            self._set_reconciliation(key, "FAILURE", attempted_at=attempted, completed_at=_iso(self.clock()), heartbeat_at=_iso(self.clock()), error="AUTO_DEADLINE_EXPIRED")
            return {"status": "FAILURE", "error": "AUTO_DEADLINE_EXPIRED", "count": 0}
        if account is not None:
            self.update_account(account)
        elif self.venue is not None and hasattr(self.venue, "account"):
            try:
                if self._deadline_expired(deadline_monotonic):
                    raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                account_kwargs: dict[str, Any] = {}
                if deadline_monotonic is not None:
                    account_kwargs["deadline_monotonic"] = deadline_monotonic
                response = _call(self.venue.account, account_kwargs)
                if self._deadline_expired(deadline_monotonic):
                    raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                if _status(response) not in {"OK", "ACKNOWLEDGED"}:
                    raise RuntimeError("account response " + _status(response))
                payload = _payload(response)
                if isinstance(payload, Mapping):
                    self.update_account(payload, epoch=payload.get("epoch", payload.get("accountEpoch")))
            except BaseException as exc:
                reason = type(exc).__name__
                self.pause("RECONCILIATION_FAILURE")
                self._set_reconciliation(
                    key,
                    "FAILURE",
                    attempted_at=attempted,
                    completed_at=_iso(self.clock()),
                    heartbeat_at=_iso(self.clock()),
                    error=reason,
                    details={"phase": "account"},
                )
                return {"status": "FAILURE", "error": reason}
        intents = self._conn.execute(
            "SELECT * FROM binance_execution_order_intents "
            "WHERE state IN ('RESERVED','SUBMITTING','UNKNOWN','ACKNOWLEDGED',"
            "'PARTIALLY_FILLED','FILLED') AND (?='ALL' OR symbol=?) "
            "ORDER BY updated_at,intent_id",
            (key, key),
        ).fetchall()
        if self.venue is None:
            self.pause("RECONCILIATION_FAILURE")
            self._set_reconciliation(
                key,
                "FAILURE",
                attempted_at=attempted,
                completed_at=_iso(self.clock()),
                heartbeat_at=_iso(self.clock()),
                error="no venue",
            )
            return {"status": "FAILURE", "count": 0}
        seen = 0
        failure: str | None = None
        reset_detected = False
        for row in intents:
            if str(row["state"]) == FILLED and self._filled_evidence_sufficient(row):
                # Fill/state commit and reservation release are deliberately
                # separate durable steps.  A restart can land here after a
                # crash between them, so always run the normal release path
                # before skipping venue reconciliation.
                self._release_if_done(str(row["intent_id"]))
                continue
            try:
                query = getattr(self.venue, "query_order", None)
                if query is None:
                    raise RuntimeError("venue has no query_order")
                identifier = (
                    {"order_id": row["exchange_order_id"]}
                    if row["exchange_order_id"] not in (None, "")
                    else {"orig_client_order_id": row["client_order_id"]}
                )
                if self._deadline_expired(deadline_monotonic):
                    raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                query_kwargs = {"symbol": row["symbol"], **identifier}
                if deadline_monotonic is not None:
                    query_kwargs["deadline_monotonic"] = deadline_monotonic
                response = _call(query, query_kwargs)
                if self._deadline_expired(deadline_monotonic):
                    raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                status = _status(response)
                payload = _payload(response)
                code = payload.get("code") if isinstance(payload, Mapping) else None
                authoritative_missing = status == REJECTED and (
                    str(code) == "-2013"
                    or (
                        isinstance(payload, Mapping)
                        and payload.get("authoritative_missing")
                    )
                )
                if authoritative_missing:
                    reset_detected = True
                    if self.environment == BINANCE_SPOT_TESTNET and str(row["state"]) in {
                        RESERVED, SUBMITTING, ACKNOWLEDGED, UNKNOWN
                    }:
                        self._set_unknown(
                            row["intent_id"],
                            "TESTNET_RESET_HISTORY_MISSING",
                            generation=int(row["generation"]),
                            result=response,
                        )
                    self._set_reconciliation(
                        "TESTNET_RESET",
                        "RESET",
                        details={
                            "symbol": row["symbol"],
                            "client_order_id": row["client_order_id"],
                            "authoritative_missing": True,
                            "reason": "TESTNET_RESET_HISTORY_MISSING",
                        },
                    )
                    continue
                if status == UNKNOWN:
                    if str(row["state"]) in {RESERVED, SUBMITTING}:
                        self._set_unknown(
                            row["intent_id"],
                            "ambiguous reconciliation result",
                            generation=int(row["generation"]),
                            result=response,
                        )
                    # UNKNOWN is intentionally not retried blindly; preserve
                    # the unresolved row while keeping reconciliation callable.
                    continue
                if status != "OK":
                    self._set_unknown(
                        row["intent_id"],
                        "unresolved reconciliation response",
                        generation=int(row["generation"]),
                        result=response,
                    )
                    if failure is None:
                        failure = "OrderResponse" + status.title()
                    continue
                if _authoritative_order_state(payload) is None:
                    self._set_unknown(
                        row["intent_id"],
                        "malformed authoritative reconciliation response",
                        generation=int(row["generation"]),
                        result=response,
                    )
                    if failure is None:
                        failure = "MalformedOrderResponse"
                    continue
                applied = self._apply_order_result(
                    row["intent_id"],
                    response,
                    int(row["generation"]),
                    require_authoritative=True,
                )
                if self.environment == BINANCE_SPOT_TESTNET:
                    applied_reason = str(applied.get("reason", ""))
                    if applied.get("state") == UNKNOWN and (
                        applied_reason.startswith("ORDER_RESPONSE_")
                        or applied_reason == "TRADE_RESPONSE_IDENTITY_MISMATCH"
                    ):
                        seen += 1
                        continue
                authoritative_state = _authoritative_order_state(payload)
                # A strategy order may first learn its exchange identity from
                # this authoritative query (for example after a restart that
                # only retained the client order ID).  Read the committed
                # identity after applying the order result; the row selected
                # for this reconciliation pass may be stale.
                current = self._conn.execute(
                    "SELECT exchange_order_id,symbol FROM binance_execution_order_intents "
                    "WHERE intent_id=?",
                    (row["intent_id"],),
                ).fetchone()
                expected_order_id = current["exchange_order_id"] if current is not None else None
                if expected_order_id in (None, "") and isinstance(payload, Mapping):
                    expected_order_id = payload.get("orderId")
                order_id = expected_order_id
                trades_method = getattr(self.venue, "my_trades", None)
                if trades_method is not None and expected_order_id not in (None, ""):
                    if self._deadline_expired(deadline_monotonic):
                        raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                    trades_kwargs = {"symbol": row["symbol"], "order_id": order_id}
                    if deadline_monotonic is not None:
                        trades_kwargs["deadline_monotonic"] = deadline_monotonic
                    trades = _call(trades_method, trades_kwargs)
                    if self._deadline_expired(deadline_monotonic):
                        raise TimeoutError("AUTO_DEADLINE_EXPIRED")
                    trade_status = _status(trades)
                    if trade_status != "OK":
                        raise RuntimeError("myTrades response " + trade_status)
                    trade_rows = _explicit_fill_rows(_payload(trades))
                    expected_order_id = str(expected_order_id)
                    if self.environment == BINANCE_SPOT_TESTNET:
                        # TESTNET reconciliation must bind every trade row to
                        # the exact canonical order, symbol, and optional
                        # client identity before it can become evidence.
                        expected_canonical_order_id = _canonical_order_id(expected_order_id)
                        matching_trade_rows = [
                            fill
                            for fill in trade_rows
                            if _canonical_order_id(fill.get("orderId")) == expected_canonical_order_id
                            and expected_canonical_order_id is not None
                            and str(fill.get("symbol", "")).upper().replace("/", "").replace("-", "").replace("_", "")
                            == str(row["symbol"]).upper()
                            and (
                                fill.get("clientOrderId") in (None, "")
                                or str(fill.get("clientOrderId")) == str(row["client_order_id"])
                            )
                        ]
                    else:
                        # PAPER venues historically return lightweight trade
                        # rows carrying only the exchange order identity.
                        matching_trade_rows = [
                            fill
                            for fill in trade_rows
                            if fill.get("orderId") is not None
                            and str(fill["orderId"]) == expected_order_id
                        ]
                    self._ingest_fills(
                        row["intent_id"],
                        matching_trade_rows,
                    )
                    if authoritative_state in {
                        "CANCELED",
                        "EXPIRED",
                        "FILLED",
                    }:
                        # Re-apply the authoritative terminal observation only
                        # after durable trade rows cover the exchange
                        # cumulative quantity.  A strict TESTNET response
                        # requires exact remote-order identity; PAPER keeps
                        # the historical intent-scoped fill accounting.
                        cumulative = _cumulative_quantity(payload)
                        durable_filled = (
                            self._exact_filled_quantity_locked(
                                row["intent_id"],
                                expected_order_id,
                            )
                            if self.environment == BINANCE_SPOT_TESTNET
                            else self._filled_quantity_locked(row["intent_id"])
                        )
                        if cumulative > ZERO and durable_filled >= cumulative:
                            self._recompute_position(str(row["symbol"]))
                            self._apply_order_result(
                                row["intent_id"],
                                response,
                                int(row["generation"]),
                                require_authoritative=True,
                            )
                seen += 1
            except BaseException as exc:
                if failure is None:
                    failure = type(exc).__name__
                continue
        if self._detect_reset():
            reset_detected = True
        self._recompute_all_positions()
        completed = _iso(self.clock())
        if failure is not None:
            self.pause("RECONCILIATION_FAILURE")
            self._set_reconciliation(
                key,
                "FAILURE",
                attempted_at=attempted,
                completed_at=completed,
                heartbeat_at=completed,
                error=failure,
                details={"phase": "order"},
            )
            return {
                "status": "FAILURE",
                "count": seen,
                "heartbeat_at": completed,
                "error": failure,
            }
        if reset_detected:
            reason = (
                "TESTNET_RESET_HISTORY_MISSING"
                if self.environment == BINANCE_SPOT_TESTNET
                else "TESTNET_RESET"
            )
            if self.control().get("pause_reason") != reason:
                self.pause(reason)
            self._set_reconciliation(
                key,
                "RESET",
                attempted_at=attempted,
                completed_at=completed,
                heartbeat_at=completed,
                details={"reason": reason},
            )
            if self.environment == BINANCE_SPOT_TESTNET:
                return {
                    "status": "PAUSED",
                    "reason": reason,
                    "reset_reason": reason,
                    "reset_detected": True,
                    "control_path_paused": True,
                    "execution_paused": True,
                    "count": seen,
                    "heartbeat_at": completed,
                }
            return {"status": "RESET", "count": seen, "heartbeat_at": completed}
        self._set_reconciliation(
            key,
            "SUCCESS",
            attempted_at=attempted,
            completed_at=completed,
            heartbeat_at=completed,
        )
        return {"status": "SUCCESS", "count": seen, "heartbeat_at": completed}

    poll = reconcile
    reconcile_orders = reconcile

    def _detect_reset(self) -> bool:
        current = self._conn.execute("SELECT epoch FROM binance_execution_account WHERE singleton=1").fetchone()
        if current is None or current[0] is None:
            return False
        epoch = str(current[0])
        previous = self._conn.execute("SELECT details_json FROM binance_execution_reconciliation WHERE key='ACCOUNT_EPOCH'").fetchone()
        old = _load(previous[0], {}) if previous else {}
        old_epoch = old.get("epoch") if isinstance(old, Mapping) else None
        if old_epoch is not None and str(old_epoch) != epoch:
            self.pause("TESTNET_RESET")
            self._set_reconciliation("ACCOUNT_EPOCH", "RESET", details={"epoch": epoch, "previous_epoch": old_epoch})
            self._conn.execute("UPDATE binance_execution_reconciliation SET details_json=? WHERE key='ACCOUNT_EPOCH'", (_json({"epoch": epoch, "previous_epoch": old_epoch}),))
            self._conn.commit()
            return True
        self._set_reconciliation("ACCOUNT_EPOCH", "CURRENT", details={"epoch": epoch})
        return False

    def _ingest_fills(self, intent_id: str, fills: Iterable[Mapping[str, Any]]) -> int:
        fill_rows = tuple(fills)
        if not fill_rows:
            return 0
        symbol: str | None = None
        count = 0
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                intent = self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if intent is None:
                    self._conn.rollback()
                    return 0
                symbol = str(intent["symbol"])
                count = self._insert_fills_locked(intent, fill_rows)
                current = self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if current is not None:
                    original = _dec(current["quantity"])
                    filled = self._filled_quantity_locked(intent_id)
                    self._persist_sell_reservation_remainder_locked(
                        current,
                        filled,
                    )
                    if count:
                        observed = FILLED if original > ZERO and filled >= original else PARTIALLY_FILLED
                        target = self._merge_order_state(
                            str(current["state"]),
                            observed,
                            original_quantity=original,
                            observed_filled=filled,
                        )
                        if target != str(current["state"]):
                            now = _iso(self.clock())
                            self._conn.execute(
                                "UPDATE binance_execution_order_intents SET state=?,updated_at=? WHERE intent_id=?",
                                (target, now, intent_id),
                            )
                            self._transition(
                                intent_id,
                                str(current["state"]),
                                target,
                                int(current["generation"]),
                                "fill reconciliation",
                                {"filled_quantity": _dstr(filled)},
                            )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        if count and symbol is not None:
            self._recompute_position(symbol)
            self._release_if_done(intent_id)
        return count

    record_fills = _ingest_fills
    ingest_fills = _ingest_fills

    def _update_fill_state(self, intent_id: str) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return
                original = _dec(row["quantity"])
                filled = self._filled_quantity_locked(intent_id)
                observed = FILLED if original > ZERO and filled >= original else PARTIALLY_FILLED
                target = self._merge_order_state(
                    str(row["state"]),
                    observed,
                    original_quantity=original,
                    observed_filled=filled,
                )
                if target != str(row["state"]):
                    now = _iso(self.clock())
                    self._conn.execute(
                        "UPDATE binance_execution_order_intents SET state=?,updated_at=? WHERE intent_id=?",
                        (target, now, intent_id),
                    )
                    self._transition(
                        intent_id,
                        str(row["state"]),
                        target,
                        int(row["generation"]),
                        "fill reconciliation",
                        {"filled_quantity": _dstr(filled)},
                    )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def _release_if_done(self, intent_id: str) -> None:
        row = self._conn.execute(
            "SELECT state,quantity,raw_json,intent_id FROM binance_execution_order_intents "
            "WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            return
        state = str(row["state"])
        if state == FILLED and not self._filled_evidence_sufficient(row):
            # Exchange FILLED is not durable fill evidence; retain exposure and
            # keep this order in reconciliation until myTrades supplies rows.
            return
        if state in {REJECTED, FILLED, CANCELED, EXPIRED}:
            self._conn.execute(
                "UPDATE binance_execution_risk_reservations "
                "SET status='RELEASED',released_at=? "
                "WHERE intent_id=? AND status='HELD'",
                (_iso(self.clock()), intent_id),
            )
            self._conn.commit()
        elif state == PARTIALLY_FILLED:
            # Keep the reservation for the unresolved remainder/worst case.
            pass

    # ---- position ledger ----------------------------------------------------------
    def _recompute_all_positions(self) -> None:
        symbols = [row[0] for row in self._conn.execute("SELECT DISTINCT symbol FROM binance_execution_fills").fetchall()]
        for symbol in symbols:
            self._recompute_position(str(symbol))

    def _fee_quote(self, fill: sqlite3.Row, commission: Decimal, asset: str | None, price: Decimal, payload: Mapping[str, Any]) -> tuple[Decimal, bool]:
        if commission <= ZERO:
            return ZERO, False
        asset = str(asset or "").upper()
        if asset == QUOTE_ASSET:
            return commission, False
        if asset in {"BASE", str(fill["symbol"])[0:-4]}:
            return commission * price, False
        for key in ("commissionQuote", "feeQuote", "fee_quote", "commission_quote"):
            if payload.get(key) is not None:
                return _dec(payload[key]), False
        marks = payload.get("fee_marks") or payload.get("marks")
        if isinstance(marks, Mapping):
            for mark_asset, mark_price in marks.items():
                if str(mark_asset).upper() == asset:
                    return commission * _dec(mark_price), False
        # Third-asset/unknown fees must not be silently treated as zero.
        return ZERO, True

    def _recompute_position(self, symbol: str) -> BinancePosition | None:
        fills = self._conn.execute("SELECT * FROM binance_execution_fills WHERE symbol=? ORDER BY trade_time,rowid", (symbol,)).fetchall()
        quantity = ZERO
        cost = ZERO
        realized = ZERO
        fees_quote = ZERO
        unknown = False
        origin: tuple[Any, ...] | None = None
        origin_epoch: str | None = None
        candidate_id = None
        binding_hash = None
        binding = None
        provenance = None
        strategy_ref = None
        exit_policy = None
        for fill in fills:
            qty = _dec(fill["quantity"])
            price = _dec(fill["price"])
            quote = _dec(fill["quote_quantity"], qty * price)
            commission = _dec(fill["commission"])
            payload = _load(fill["payload_json"], {}) or {}
            fee_quote, fee_unknown = self._fee_quote(fill, commission, fill["commission_asset"], price, payload)
            fees_quote += fee_quote
            unknown = unknown or fee_unknown
            intent = self._conn.execute(
                "SELECT intent_id,candidate_id,binding_hash,binding_json,provenance_json,strategy_ref_json,exit_policy_json "
                "FROM binance_execution_order_intents WHERE intent_id=?",
                (fill["intent_id"],),
            ).fetchone()
            fill_origin = None
            if intent is not None:
                fill_origin = (
                    intent["candidate_id"],
                    intent["binding_hash"],
                    _load(intent["binding_json"], None),
                    _load(intent["provenance_json"], None),
                    _load(intent["strategy_ref_json"], None),
                    _load(intent["exit_policy_json"], None),
                )
            if str(fill["side"]).upper() == "BUY":
                if quantity <= ZERO:
                    origin = fill_origin
                    origin_epoch = str(intent["intent_id"]) if intent is not None else None
                    if origin is not None:
                        candidate_id, binding_hash, binding, provenance, strategy_ref, exit_policy = origin
                elif origin is not None and fill_origin is not None and not self._mapping_equal(fill_origin, origin):
                    unknown = True
                base_asset = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else symbol
                commission_asset = str(fill["commission_asset"] or "").upper()
                base_fee = commission_asset in {base_asset, "BASE"}
                net_qty = qty - (commission if base_fee else ZERO)
                quantity += max(ZERO, net_qty)
                cost += quote + (fee_quote if not base_fee else ZERO)
            else:
                if quantity > ZERO and (origin is None or (fill_origin is not None and not self._mapping_equal(fill_origin, origin))):
                    unknown = True
                base_asset = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else symbol
                commission_asset = str(fill["commission_asset"] or "").upper()
                base_fee = commission_asset in {base_asset, "BASE"}
                sell_qty = qty + (commission if base_fee else ZERO)
                consumed = min(quantity, sell_qty)
                proceeds = quote - (fee_quote if not base_fee else ZERO)
                if consumed == quantity and quantity > ZERO:
                    allocated_cost = cost
                    realized += proceeds - cost
                elif quantity > ZERO:
                    realized += (proceeds * quantity - cost * consumed) / quantity
                    allocated_cost = cost * consumed / quantity
                else:
                    allocated_cost = ZERO
                    realized += proceeds
                quantity = max(ZERO, quantity - consumed)
                cost = max(ZERO, cost - allocated_cost)
                if quantity <= ZERO:
                    origin = None
                    origin_epoch = None
                    candidate_id = binding_hash = binding = provenance = strategy_ref = exit_policy = None
        if quantity <= ZERO:
            candidate_id = binding_hash = binding = provenance = strategy_ref = origin_epoch = None
            exit_policy = None
        existing = self._conn.execute("SELECT mark_price FROM binance_execution_positions WHERE symbol=?", (symbol,)).fetchone()
        mark = _dec(existing[0]) if existing and existing[0] is not None else None
        unrealized = None if mark is None or unknown else mark * quantity - cost
        valuation = "UNKNOWN" if unknown else "KNOWN"
        average = cost / quantity if quantity > ZERO else ZERO
        record = BinancePosition(symbol, quantity, cost, average, mark, realized, unrealized, fees_quote, valuation, candidate_id, binding_hash, binding, provenance, strategy_ref, origin_epoch, exit_policy)
        with self._lock:
            self._conn.execute(
                "INSERT INTO binance_execution_positions(symbol,quantity,cost_basis,average_cost,mark_price,realized_pnl,unrealized_pnl,fees_quote,valuation_status,candidate_id,binding_hash,origin_epoch,exit_policy_json,binding_json,provenance_json,strategy_ref_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(symbol) DO UPDATE SET quantity=excluded.quantity,cost_basis=excluded.cost_basis,average_cost=excluded.average_cost,mark_price=excluded.mark_price,realized_pnl=excluded.realized_pnl,unrealized_pnl=excluded.unrealized_pnl,fees_quote=excluded.fees_quote,valuation_status=excluded.valuation_status,candidate_id=excluded.candidate_id,binding_hash=excluded.binding_hash,origin_epoch=excluded.origin_epoch,exit_policy_json=excluded.exit_policy_json,binding_json=excluded.binding_json,provenance_json=excluded.provenance_json,strategy_ref_json=excluded.strategy_ref_json,updated_at=excluded.updated_at",
                (symbol, _dstr(quantity), _dstr(cost), _dstr(average), None if mark is None else _dstr(mark), _dstr(realized), None if unrealized is None else _dstr(unrealized), _dstr(fees_quote), valuation, candidate_id, binding_hash, origin_epoch, _json(exit_policy), _json(binding), _json(provenance), _json(strategy_ref), _iso(self.clock())),
            )
            self._conn.commit()
        if unknown and self.control().get("state") == ARMED:
            self.pause("UNKNOWN_FEE_VALUATION")
        return record

    def mark(self, symbol: str, price: Any) -> dict[str, Any]:
        symbol = _symbol(symbol)
        self._conn.execute("UPDATE binance_execution_positions SET mark_price=?,updated_at=? WHERE symbol=?", (_dstr(price), _iso(self.clock()), symbol))
        self._conn.commit()
        position = self._recompute_position(symbol)
        return position.as_dict() if position else {}

    update_mark = mark

    def position(self, symbol: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM binance_execution_positions WHERE symbol=?", (_symbol(symbol),)).fetchone()
        if row is None:
            return None
        result = dict(row)
        for key in ("exit_policy_json", "binding_json", "provenance_json", "strategy_ref_json"):
            if key in result:
                result[key[:-5]] = _load(result.pop(key), None)
        result["originating_binding"] = result.get("binding")
        result["originating_provenance"] = result.get("provenance")
        result["originating_strategy_ref"] = result.get("strategy_ref")
        return result

    get_position = position

    def positions(self) -> list[dict[str, Any]]:
        return [self.position(row[0]) for row in self._conn.execute("SELECT symbol FROM binance_execution_positions ORDER BY symbol").fetchall()]

    def fills(self, *, symbol: str | None = None, client_order_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM binance_execution_fills"
        args: list[Any] = []
        conditions = []
        if symbol:
            conditions.append("symbol=?")
            args.append(_symbol(symbol))
        if client_order_id:
            conditions.append("client_order_id=?")
            args.append(client_order_id)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY trade_time,trade_id"
        return [dict(row) for row in self._conn.execute(query, args).fetchall()]

    def orders(self, *, state: str | None = None, symbol: str | None = None) -> list[dict[str, Any]]:
        reservation_aliases = {
            column: "_risk_reservation_" + column for column in _RISK_RESERVATION_COLUMNS
        }
        query = (
            "SELECT i.*, "
            + ",".join(
                f"r.{column} AS {alias}"
                for column, alias in reservation_aliases.items()
            )
            + " FROM binance_execution_order_intents AS i "
            "LEFT JOIN binance_execution_risk_reservations AS r ON r.intent_id=i.intent_id"
        )
        args: list[Any] = []
        conditions = []
        if state:
            conditions.append("i.state=?")
            args.append(str(state).upper())
        if symbol:
            conditions.append("i.symbol=?")
            args.append(_symbol(symbol))
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY i.updated_at,i.intent_id"
        orders = []
        for row in self._conn.execute(query, args).fetchall():
            result = dict(row)
            reservation = None
            if result["_risk_reservation_intent_id"] is not None:
                reservation = {
                    column: result[alias]
                    for column, alias in reservation_aliases.items()
                }
            for alias in reservation_aliases.values():
                result.pop(alias, None)
            result["risk_reservation"] = reservation
            orders.append(result)
        return orders

    def cancel(
        self,
        client_order_id: str | None = None,
        *,
        symbol: str,
        reason: str = "operator cancel",
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        client_order_id = client_order_id or orig_client_order_id
        if not client_order_id:
            raise ValueError("client_order_id is required")
        row = self._conn.execute(
            "SELECT * FROM binance_execution_order_intents "
            "WHERE client_order_id=? AND symbol=?",
            (client_order_id, _symbol(symbol)),
        ).fetchone()
        if row is None:
            raise ValueError("order is not AXIOM-owned")
        if row["state"] in TERMINAL:
            return self._intent_response(row) or {}
        if self.venue is None:
            self.pause("CANCEL_FAILURE")
            return self._set_unknown(
                row["intent_id"],
                "no venue for cancellation",
                generation=int(row["generation"]),
            )
        try:
            method = getattr(self.venue, "cancel_owned_order", None) or getattr(
                self.venue, "cancel_order", None
            )
            if method is None:
                raise RuntimeError("venue has no owned cancellation method")
            identifier = (
                {"order_id": row["exchange_order_id"]}
                if row["exchange_order_id"] not in (None, "")
                else {"orig_client_order_id": client_order_id}
            )
            response = _call(method, {"symbol": row["symbol"], **identifier})
        except BaseException as exc:
            self.pause("CANCEL_FAILURE")
            return self._set_unknown(
                row["intent_id"],
                f"cancel exception: {type(exc).__name__}",
                generation=int(row["generation"]),
            )
        status = _status(response)
        payload = _payload(response)
        if status != "OK" or _authoritative_order_state(payload) is None:
            self.pause("CANCEL_FAILURE")
            return self._set_unknown(
                row["intent_id"],
                "unresolved cancellation response",
                generation=int(row["generation"]),
                result=response,
            )
        self._apply_order_result(
            row["intent_id"],
            response,
            int(row["generation"]),
            require_authoritative=True,
        )
        self._release_if_done(row["intent_id"])
        return (
            self._intent_response(
                self._conn.execute(
                    "SELECT * FROM binance_execution_order_intents WHERE intent_id=?",
                    (row["intent_id"],),
                ).fetchone()
            )
            or {}
        )

    cancel_order = cancel
    cancel_owned_order = cancel

    # ---- heartbeat/schema introspection ------------------------------------------
    def _set_reconciliation(self, key: str, status: str, *, attempted_at: Any = None, completed_at: Any = None, heartbeat_at: Any = None, error: str | None = None, details: Any = None) -> None:
        self._conn.execute("INSERT INTO binance_execution_reconciliation(key,status,attempted_at,completed_at,heartbeat_at,error,details_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status=excluded.status,attempted_at=COALESCE(excluded.attempted_at,binance_execution_reconciliation.attempted_at),completed_at=COALESCE(excluded.completed_at,binance_execution_reconciliation.completed_at),heartbeat_at=COALESCE(excluded.heartbeat_at,binance_execution_reconciliation.heartbeat_at),error=excluded.error,details_json=excluded.details_json", (key, status, attempted_at, completed_at, heartbeat_at, error, _json(details or {})))
        self._conn.commit()

    def check_connectivity(self, *, deadline_monotonic: float | None = None) -> dict[str, Any]:
        checked = _iso(self.clock())
        if self._deadline_expired(deadline_monotonic):
            return {"status": "FAILURE", "error": "AUTO_DEADLINE_EXPIRED"}
        if self.venue is None or not hasattr(self.venue, "account"):
            result = {"status": "FAILURE", "error": "no venue"}
            self._conn.execute("UPDATE binance_execution_connectivity SET status=?,checked_at=?,heartbeat_at=?,error=?,updated_at=? WHERE singleton=1", ("FAILURE", checked, checked, result["error"], checked))
            self._conn.commit()
            return result
        try:
            connectivity_kwargs: dict[str, Any] = {}
            if deadline_monotonic is not None:
                connectivity_kwargs["deadline_monotonic"] = deadline_monotonic
            response = _call(self.venue.account, connectivity_kwargs)
            if self._deadline_expired(deadline_monotonic):
                return {"status": "FAILURE", "error": "AUTO_DEADLINE_EXPIRED"}
            status = _status(response)
            payload = _payload(response)
            ok = status in {"OK", "ACKNOWLEDGED"}
            if ok and isinstance(payload, Mapping):
                self.update_account(payload, epoch=payload.get("epoch", payload.get("accountEpoch")))
            result = {"status": "OK" if ok else status, "checked_at": checked}
            self._conn.execute("UPDATE binance_execution_connectivity SET status=?,checked_at=?,heartbeat_at=?,error=?,updated_at=? WHERE singleton=1", (result["status"], checked, checked, None if ok else status, checked))
        except BaseException as exc:
            result = {"status": "FAILURE", "error": type(exc).__name__, "checked_at": checked}
            self._conn.execute("UPDATE binance_execution_connectivity SET status=?,checked_at=?,heartbeat_at=?,error=?,updated_at=? WHERE singleton=1", ("FAILURE", checked, checked, type(exc).__name__, checked))
        self._conn.commit()
        return result

    connectivity = check_connectivity

    def heartbeat(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM binance_execution_reconciliation ORDER BY heartbeat_at DESC LIMIT 1").fetchone()
        connectivity = self._conn.execute("SELECT * FROM binance_execution_connectivity WHERE singleton=1").fetchone()
        return {"reconciliation": dict(row) if row else None, "connectivity": dict(connectivity) if connectivity else None}

    health = heartbeat

    def schema(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM binance_execution_schema_lock WHERE singleton=1").fetchone()
        return dict(row) if row else {"namespace": self.namespace, "schema_version": self.schema_version}

    def close(self) -> None:
        if self._owns_store:
            self.store.close()


# Friendly aliases used by integrations and hidden canary harnesses.
BinanceExecution = BinanceExecutionService
BinanceSpotExecutionService = BinanceExecutionService
OrderExecutionService = BinanceExecutionService

__all__ = [
    "BinanceExecutionService", "BinanceExecution", "BinanceSpotExecutionService", "OrderExecutionService",
    "BinancePosition", "ENABLE_CONFIRMATION", "ENABLE_TESTNET_CONFIRMATION", "INTENT", "RESERVED",
    "SUBMITTING", "ACKNOWLEDGED", "REJECTED", "UNKNOWN", "PARTIALLY_FILLED", "FILLED", "CANCELED",
    "EXPIRED", "DISABLED", "ARMED", "PAUSED", "DISARMED", "KILLED",
]
