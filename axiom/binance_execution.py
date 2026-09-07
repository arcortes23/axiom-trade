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
)
from .storage import AxiomStore

UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
QUOTE_ASSET = "USDT"
SCHEMA_VERSION = "binance-execution-v1"
ENABLE_CONFIRMATION = "ENABLE BINANCE AUTO CANARY"

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
ACTIVE = frozenset({INTENT, RESERVED, SUBMITTING, ACKNOWLEDGED, UNKNOWN, PARTIALLY_FILLED})

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
    if value is None:
        return default
    try:
        return json.loads(str(value))
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
            if text in {"REJECTED", "ERROR", "FAILED", "FAILURE"}:
                return REJECTED
            if text in {"UNKNOWN", "TIMEOUT", "DISCONNECTED"}:
                return UNKNOWN
            if text in {"NEW", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CANCELLED", "EXPIRED"}:
                return "OK"
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


def _payload(result: Any) -> Any:
    if isinstance(result, BinanceSpotResult):
        return result.payload
    if isinstance(result, Mapping):
        payload = result.get("payload")
        return payload if payload is not None else result
    return getattr(result, "payload", result)


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
            "exit_policy": self.exit_policy,
        }


class BinanceExecutionService:
    """Durable Spot execution coordinator.

    The constructor accepts either an :class:`AxiomStore`, a SQLite path, or a
    sqlite connection.  A fake venue is intentionally enough for tests and
    development; the real REST client is passed as ``venue``.
    """

    namespace = "BINANCE_SPOT_EXECUTION"
    schema_version = SCHEMA_VERSION

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
        owner_id: str | None = None,
        lease_seconds: int | float = 30,
        clock: Callable[[], Any] | None = None,
        account: Mapping[str, Any] | None = None,
        db_path: str | None = None,
        runtime_profile: BinanceRuntimeProfile | None = None,
    ) -> None:
        if db_path is not None:
            store = db_path
        if profile is None:
            profile = runtime_profile
        self._owns_store = False
        if isinstance(store, AxiomStore):
            self.store = store
            self._conn = store.connection
        elif isinstance(store, sqlite3.Connection):
            self.store = AxiomStore(connection=store)
            self._owns_store = True
            self._conn = store
        else:
            self.store = AxiomStore(str(store))
            self._owns_store = True
            self._conn = self.store.connection
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.venue = venue if venue is not None else adapter
        self.profile = profile
        self.environment = _env(environment if environment is not None else (profile.environment if profile else PAPER))
        if profile is not None:
            if not isinstance(profile, BinanceRuntimeProfile):
                raise TypeError("profile must be BinanceRuntimeProfile")
            if self.environment != profile.environment.value:
                raise ValueError("profile/environment mismatch")
            if self.environment == BINANCE_SPOT_LIVE:
                raise ValueError("development profile cannot use Binance LIVE authenticated transport")
        self.binding = binding
        self.risk_envelope = envelope or risk_envelope
        if not isinstance(self.risk_envelope, BinanceRiskEnvelope):
            raise TypeError("risk_envelope must be BinanceRiskEnvelope")
        self.credential_store = credential_store
        self.credentials = credentials
        self.credential_ref = credential_ref
        self.owner_id = str(owner_id or ("worker-" + uuid.uuid4().hex))
        self.lease_seconds = max(1, int(lease_seconds))
        self.clock = clock or (lambda: datetime.now(UTC))
        self.account_state: dict[str, Any] = dict(account or {})
        self._init_schema()
        self._check_persisted_boundary()

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
                    credential_hash TEXT NOT NULL, authorized INTEGER NOT NULL DEFAULT 0,
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
                    submitted_at TEXT, acknowledged_at TEXT, updated_at TEXT NOT NULL,
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
                    fee_reserve TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
                    released_at TEXT
                );
                CREATE TABLE IF NOT EXISTS binance_execution_positions (
                    symbol TEXT PRIMARY KEY, quantity TEXT NOT NULL, cost_basis TEXT NOT NULL,
                    average_cost TEXT NOT NULL, mark_price TEXT, realized_pnl TEXT NOT NULL,
                    unrealized_pnl TEXT, fees_quote TEXT NOT NULL, valuation_status TEXT NOT NULL,
                    candidate_id TEXT, binding_hash TEXT, exit_policy_json TEXT,
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
            now = _iso(self.clock())
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_schema_lock(singleton,namespace,schema_version,created_at) VALUES(1,?,?,?)",
                (self.namespace, self.schema_version, now),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_control(singleton,state,generation,environment,binding_hash,envelope_hash,credential_hash,authorized,kill_requested,updated_at) VALUES(1,?,?,?,?,?,?,?,?,?)",
                (DISABLED, 0, self.environment, self._binding_hash(), self.risk_envelope.canonical_hash, self._credential_hash(), 0, 0, now),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO binance_execution_connectivity(singleton,status,updated_at) VALUES(1,'UNKNOWN',?)",
                (now,),
            )
            self._conn.commit()

    def _binding_hash(self) -> str:
        value = self.binding
        if isinstance(value, CryptoExecutionBinding):
            return value.binding_hash
        if isinstance(value, Mapping):
            return str(value.get("binding_hash") or canonical_sha256(dict(value)))
        if value is None:
            return canonical_sha256({"binding": None})
        return canonical_sha256(str(value))

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
        current = (self.environment, self._binding_hash(), self.risk_envelope.canonical_hash, self._credential_hash())
        persisted = (str(row["environment"]), str(row["binding_hash"]), str(row["envelope_hash"]), str(row["credential_hash"]))
        if current != persisted:
            self._conn.execute(
                "UPDATE binance_execution_control SET authorized=0,state=?,generation=generation+1,binding_hash=?,envelope_hash=?,credential_hash=?,environment=?,updated_at=? WHERE singleton=1",
                (DISABLED, current[1], current[2], current[3], current[0], _iso(self.clock())),
            )
            self._conn.commit()

    # ---- control ------------------------------------------------------------------
    def control(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
        if row is None:
            return {"state": DISABLED, "authorized": False}
        current = (self.environment, self._binding_hash(), self.risk_envelope.canonical_hash, self._credential_hash())
        authorized = bool(row["authorized"]) and tuple(str(row[key]) for key in ("environment", "binding_hash", "envelope_hash", "credential_hash")) == current
        return {**dict(row), "authorized": authorized, "generation": int(row["generation"]), "kill_requested": bool(row["kill_requested"])}

    status = control

    def enable_auto_canary(self, confirmation: str = "", *, actor: str = "operator") -> dict[str, Any]:
        if confirmation != ENABLE_CONFIRMATION:
            raise PermissionError("exact confirmation required: ENABLE BINANCE AUTO CANARY")
        if self.environment == BINANCE_SPOT_LIVE or (self.profile is not None and self.profile.environment.value == BINANCE_SPOT_LIVE):
            raise PermissionError("development profile refuses Binance LIVE")
        now = _iso(self.clock())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute("SELECT generation FROM binance_execution_control WHERE singleton=1").fetchone()
            generation = int(row[0] if row else 0) + 1
            self._conn.execute(
                "UPDATE binance_execution_control SET state=?,generation=?,environment=?,binding_hash=?,envelope_hash=?,credential_hash=?,authorized=1,kill_requested=0,pause_reason=NULL,updated_at=? WHERE singleton=1",
                (ARMED, generation, self.environment, self._binding_hash(), self.risk_envelope.canonical_hash, self._credential_hash(), now),
            )
            self._conn.execute("INSERT INTO binance_execution_actions(action_id,action,actor,confirmation,generation,created_at,payload_json) VALUES(?,?,?,?,?,?,?)", (uuid.uuid4().hex, "ENABLE", actor, confirmation, generation, now, _json({"environment": self.environment})))
            self._conn.commit()
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
        if confirmation not in {ENABLE_CONFIRMATION, "RESET BINANCE AUTO CANARY"}:
            raise PermissionError("exact confirmation required")
        return self.enable_auto_canary(ENABLE_CONFIRMATION, actor=actor)

    def _fence(self, generation: int, *, entries: bool = True) -> bool:
        row = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
        if row is None or int(row["generation"]) != int(generation) or bool(row["kill_requested"]) or str(row["state"]) in {DISABLED, DISARMED, KILLED}:
            return False
        if entries and str(row["state"]) != ARMED:
            return False
        current = (self.environment, self._binding_hash(), self.risk_envelope.canonical_hash, self._credential_hash())
        persisted = (str(row["environment"]), str(row["binding_hash"]), str(row["envelope_hash"]), str(row["credential_hash"]))
        return bool(row["authorized"]) and current == persisted


    # ---- account/risk -------------------------------------------------------------
    def update_account(self, account: Mapping[str, Any], *, epoch: Any = None, observed_at: Any = None) -> dict[str, Any]:
        # Account payloads are exchange data, but redact accidental credentials
        # before they enter the durable execution namespace.
        body = dict(_load(_json(dict(account)), {}) or {})
        if epoch is None:
            epoch = body.get("epoch", body.get("accountEpoch", body.get("account_epoch")))
        now = _iso(observed_at or self.clock())
        with self._lock:
            self._conn.execute("INSERT INTO binance_execution_account(singleton,account_json,epoch,observed_at) VALUES(1,?,?,?) ON CONFLICT(singleton) DO UPDATE SET account_json=excluded.account_json,epoch=excluded.epoch,observed_at=excluded.observed_at", (_json(body), None if epoch is None else str(epoch), now))
            self._conn.commit()
        self.account_state = body
        return body

    set_account = update_account

    def _snapshot(self, now: Any = None) -> RiskSnapshot:
        account = dict(self.account_state)
        row = self._conn.execute("SELECT account_json FROM binance_execution_account WHERE singleton=1").fetchone()
        if row is not None:
            account.update(_load(row[0], {}) or {})
        pending_rows = self._conn.execute("SELECT symbol,side,quantity,price,state,fee_reserve,exchange_order_id FROM binance_execution_order_intents WHERE state IN ('RESERVED','SUBMITTING','ACKNOWLEDGED','UNKNOWN','PARTIALLY_FILLED')").fetchall()
        pending = [dict(row) for row in pending_rows]
        positions = self._conn.execute("SELECT symbol,quantity FROM binance_execution_positions").fetchall()
        inventory = dict(account.get("owned_inventory", account.get("inventory", {})) or {})
        for row in positions:
            if _dec(row["quantity"]) > ZERO:
                inventory[str(row["symbol"])] = str(row["quantity"])
        account.setdefault("reserved_exposure", "0")
        account.setdefault("positions", len([v for k, v in inventory.items() if str(k).upper() != QUOTE_ASSET and _dec(v) > ZERO]))
        account["owned_inventory"] = inventory
        account["pending_orders"] = pending
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
        output["decision_at"] = _iso(signal["decision_at"])
        output["reason"] = str(signal.get("reason", ""))
        output["exit_policy"] = signal.get("exit_policy")
        output["binding_hash"] = str(signal.get("binding_hash") or self._binding_hash())
        output["opportunity_id"] = None if signal.get("opportunity_id") is None else str(signal["opportunity_id"])
        return output

    def _client_order_id(self, signal: Mapping[str, Any]) -> str:
        raw = "AXIOM-" + canonical_sha256({"signal_id": str(signal["signal_id"]), "symbol": signal["symbol"], "binding_hash": signal.get("binding_hash")})[:24]
        return raw[:36]

    def _intent_response(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        for key in ("raw_json", "exit_policy_json"):
            if key in result:
                result[key[:-5] if key.endswith("_json") else key] = _load(result[key], None)
        reservation = self._conn.execute("SELECT * FROM binance_execution_risk_reservations WHERE intent_id=?", (result["intent_id"],)).fetchone()
        result["risk_reservation"] = dict(reservation) if reservation is not None else None
        return result

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
    ) -> dict[str, Any]:
        data = self._normalize_signal({**dict(signal), **({"opportunity_id": opportunity_id} if opportunity_id is not None else {})})
        if data["binding_hash"] != self._binding_hash():
            return self._reject_signal(data, "BINDING_CHANGED", now=now)
        if rules is not None:
            sized = size_limit_order(rules if isinstance(rules, SymbolRules) else SymbolRules.from_exchange_info(rules), data["side"], price, quantity, envelope=self.risk_envelope, fee_rate=fee_rate, available_inventory=(self._snapshot(now).owned_inventory.get(data["symbol"], ZERO) if data["side"] == "SELL" else None))
            if not sized.valid:
                return self._reject_signal(data, ";".join(sized.reasons), now=now)
            price_value, qty_value, notional, fee_value = sized.price, sized.quantity, sized.notional, sized.fee_reserve
        else:
            price_value, qty_value = _dec(price), _dec(quantity)
            notional, fee_value = price_value * qty_value, price_value * qty_value * _dec(fee_rate)
        existing = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE signal_id=?", (data["signal_id"],)).fetchone()
        if existing is not None:
            return self._intent_response(existing) or {}
        state = self.control()
        blocked = {DISABLED, DISARMED, KILLED} if data["intent"] == "EXIT" else {DISABLED, PAUSED, DISARMED, KILLED}
        if not state.get("authorized") or state.get("state") in blocked:
            return self._reject_signal(data, "CONTROL_" + str(state.get("state", DISABLED)), now=now)
        snapshot = risk_snapshot if risk_snapshot is not None else self._snapshot(now)
        if data["intent"] == "ENTRY":
            assessment = assess_entry(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee_value, now=_utc(now or self.clock()))
        else:
            assessment = assess_exit(snapshot, self.risk_envelope, requested_notional=notional, reserved_fee=fee_value, now=_utc(now or self.clock()), symbol=data["symbol"], quantity=qty_value, minimum_notional=(rules.min_notional if isinstance(rules, SymbolRules) else None))
        if not assessment.allowed:
            return self._reject_signal(data, ";".join(assessment.reasons), now=now)
        return self._reserve_and_send(data, price_value, qty_value, notional, fee_value, time_in_force, now=now)

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
                self._conn.execute("INSERT INTO binance_execution_signals(signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,decision_interval,decision_at,intent,side,reason,exit_policy_json,signal_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["decision_interval"], data["decision_at"], data["intent"], data["side"], data.get("reason"), _json(data.get("exit_policy")), _json(data), now_iso))
                self._conn.execute("INSERT INTO binance_execution_order_intents(intent_id,signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,intent,side,price,quantity,notional,fee_reserve,client_order_id,state,generation,reason,exit_policy_json,updated_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (intent_id, data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["intent"], data["side"], "0", "0", "0", "0", client_id, REJECTED, int(self.control().get("generation", 0)), reason, _json(data.get("exit_policy")), now_iso, _json({"reason": reason})))
                self._transition(intent_id, None, REJECTED, int(self.control().get("generation", 0)), reason, {"reason": reason})
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}

    def _reserve_and_send(self, data: Mapping[str, Any], price: Decimal, quantity: Decimal, notional: Decimal, fee: Decimal, tif: str, *, now: Any = None) -> dict[str, Any]:
        now_iso = _iso(now or self.clock())
        intent_id = uuid.uuid4().hex
        client_id = self._client_order_id(data)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE signal_id=? OR client_order_id=?", (data["signal_id"], client_id)).fetchone()
            if existing is not None:
                self._conn.rollback()
                return self._intent_response(existing) or {}
            ctl = self._conn.execute("SELECT * FROM binance_execution_control WHERE singleton=1").fetchone()
            generation = int(ctl["generation"] if ctl else 0)
            self._conn.execute("INSERT INTO binance_execution_signals(signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,decision_interval,decision_at,intent,side,reason,exit_policy_json,signal_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["decision_interval"], data["decision_at"], data["intent"], data["side"], data.get("reason"), _json(data.get("exit_policy")), _json(data), now_iso))
            self._conn.execute("INSERT INTO binance_execution_order_intents(intent_id,signal_id,opportunity_id,candidate_id,binding_hash,symbol,environment,intent,side,price,quantity,notional,fee_reserve,client_order_id,state,generation,owner_id,lease_expires_at,exit_policy_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (intent_id, data["signal_id"], data.get("opportunity_id"), data["candidate_id"], data["binding_hash"], data["symbol"], data["environment"], data["intent"], data["side"], _dstr(price), _dstr(quantity), _dstr(notional), _dstr(fee), client_id, INTENT, generation, self.owner_id, _iso(_utc(self.clock()) + timedelta(seconds=self.lease_seconds)), _json(data.get("exit_policy")), now_iso))
            self._transition(intent_id, None, INTENT, generation, "created", {})
            self._conn.execute("INSERT INTO binance_execution_risk_reservations(reservation_id,intent_id,symbol,side,amount,fee_reserve,status,created_at) VALUES(?,?,?,?,?,?,?,?)", (uuid.uuid4().hex, intent_id, data["symbol"], data["side"], _dstr(notional), _dstr(fee), "HELD", now_iso))
            self._transition(intent_id, INTENT, RESERVED, generation, "risk reserved", {"amount": _dstr(notional), "fee_reserve": _dstr(fee)})
            self._conn.execute("UPDATE binance_execution_order_intents SET state=?,updated_at=? WHERE intent_id=?", (RESERVED, now_iso, intent_id))
            self._conn.commit()
        return self._network_submit(intent_id, data, price, quantity, tif, generation)

    def _transition(self, intent_id: str, from_state: str | None, to_state: str, generation: int, reason: str, payload: Any) -> None:
        self._conn.execute("INSERT OR IGNORE INTO binance_execution_order_transitions(intent_id,from_state,to_state,generation,observed_at,reason,payload_json) VALUES(?,?,?,?,?,?,?)", (intent_id, from_state, to_state, generation, _iso(self.clock()), reason, _json(payload)))

    def _network_submit(self, intent_id: str, data: Mapping[str, Any], price: Decimal, quantity: Decimal, tif: str, generation: int) -> dict[str, Any]:
        now_iso = _iso(self.clock())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                self._conn.rollback()
                return {}
            if not self._claim(row, generation):
                self._conn.rollback()
                return self._intent_response(row) or {}
            self._conn.execute("UPDATE binance_execution_order_intents SET state=?,submitted_at=?,updated_at=? WHERE intent_id=?", (SUBMITTING, now_iso, now_iso, intent_id))
            self._transition(intent_id, RESERVED, SUBMITTING, generation, "before venue call", {})
            self._conn.commit()
        # Fence in a separate, immediately-preceding read transaction; venue is outside SQLite.
        if not self._fence(generation, entries=data["intent"] == "ENTRY"):
            return self._set_unknown(intent_id, "fence rejected before venue call")
        try:
            if self.venue is None:
                raise RuntimeError("no Binance venue configured")
            method = getattr(self.venue, "place_limit_order", None) or getattr(self.venue, "place_order", None)
            if method is None:
                raise RuntimeError("venue has no place_limit_order")
            kwargs = {"symbol": data["symbol"], "side": data["side"], "quantity": _dstr(quantity), "price": _dstr(price), "time_in_force": tif}
            if isinstance(self.venue, BinanceSpotRESTClient):
                kwargs["newClientOrderId"] = self._client_order_id(data)
            else:
                kwargs.update({"new_client_order_id": self._client_order_id(data), "newClientOrderId": self._client_order_id(data), "client_order_id": self._client_order_id(data), "orig_client_order_id": self._client_order_id(data)})
            result = _call(method, kwargs)
        except BaseException as exc:
            return self._set_unknown(intent_id, f"venue exception: {type(exc).__name__}")
        control = self.control()
        if int(control.get("generation", -1)) != int(generation) or control.get("state") == KILLED or control.get("kill_requested"):
            return self._set_unknown(intent_id, "control generation changed during venue call", result=result)
        return self._apply_order_result(intent_id, result, generation)

    def _claim(self, row: sqlite3.Row, generation: int) -> bool:
        now = _utc(self.clock())
        lease = _iso(now + timedelta(seconds=self.lease_seconds))
        result = self._conn.execute("UPDATE binance_execution_order_intents SET owner_id=?,lease_expires_at=?,generation=? WHERE intent_id=? AND (owner_id IS NULL OR owner_id=? OR lease_expires_at IS NULL OR lease_expires_at<?)", (self.owner_id, lease, generation, row["intent_id"], self.owner_id, _iso(now)))
        return result.rowcount == 1

    def _set_unknown(self, intent_id: str, reason: str, *, result: Any = None) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT state,generation FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                return {}
            if row[0] in TERMINAL:
                return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("UPDATE binance_execution_order_intents SET state=?,reason=?,raw_json=?,updated_at=? WHERE intent_id=?", (UNKNOWN, reason, _json({"reason": reason, "result": _payload(result)}), _iso(self.clock()), intent_id))
            self._transition(intent_id, str(row[0]), UNKNOWN, int(row[1]), reason, {"result": _payload(result)})
            self._conn.commit()
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}

    def _apply_order_result(self, intent_id: str, result: Any, generation: int) -> dict[str, Any]:
        status = _status(result)
        payload = _payload(result)
        if status == UNKNOWN:
            return self._set_unknown(intent_id, "ambiguous venue result", result=result)
        if status in {REJECTED, "RATE_LIMIT"}:
            target = REJECTED
            reason = "venue rejected" if status == REJECTED else "venue rate limited"
        else:
            raw_status = str(payload.get("status", "NEW")).upper() if isinstance(payload, Mapping) else "NEW"
            target = {
                "CANCELLED": CANCELED,
                "CANCELED": CANCELED,
                "EXPIRED": EXPIRED,
                "FILLED": FILLED,
                "PARTIALLY_FILLED": PARTIALLY_FILLED,
            }.get(raw_status, ACKNOWLEDGED)
            cumulative = _cumulative_quantity(payload)
            if cumulative > ZERO:
                quantity_row = self._conn.execute(
                    "SELECT quantity FROM binance_execution_order_intents WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                original = _dec(quantity_row[0]) if quantity_row is not None else ZERO
                target = FILLED if original > ZERO and cumulative >= original else PARTIALLY_FILLED
            reason = "venue acknowledged"
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute("SELECT state,symbol,client_order_id FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                self._conn.rollback()
                return {}
            self._conn.execute("UPDATE binance_execution_order_intents SET state=?,exchange_order_id=?,acknowledged_at=?,reason=?,raw_json=?,updated_at=? WHERE intent_id=?", (target, str(payload.get("orderId")) if isinstance(payload, Mapping) and payload.get("orderId") is not None else None, _iso(self.clock()), reason, _json(payload), _iso(self.clock()), intent_id))
            self._transition(intent_id, str(row["state"]), target, generation, reason, payload)
            self._conn.commit()
        self._ingest_fills(intent_id, _explicit_fill_rows(payload))
        self._release_if_done(intent_id)
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()) or {}

    # ---- reconciliation and fills -------------------------------------------------
    def reconcile(self, *, symbol: str | None = None, account: Mapping[str, Any] | None = None) -> dict[str, Any]:
        key = _symbol(symbol) if symbol else "ALL"
        attempted = _iso(self.clock())
        self._set_reconciliation(key, "RUNNING", attempted_at=attempted, heartbeat_at=attempted)
        if account is not None:
            self.update_account(account)
        elif self.venue is not None and hasattr(self.venue, "account"):
            try:
                response = _call(self.venue.account, {})
                if _status(response) in {"OK", "ACKNOWLEDGED"}:
                    payload = _payload(response)
                    if isinstance(payload, Mapping):
                        self.update_account(payload, epoch=payload.get("epoch", payload.get("accountEpoch")))
            except BaseException as exc:
                self._set_reconciliation(key, "FAILURE", attempted_at=attempted, completed_at=_iso(self.clock()), error=type(exc).__name__)
                return {"status": "FAILURE", "error": type(exc).__name__}
        intents = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE state IN ('UNKNOWN','SUBMITTING','ACKNOWLEDGED','PARTIALLY_FILLED') AND (?='ALL' OR symbol=?) ORDER BY updated_at,intent_id", (key, key)).fetchall()
        if self.venue is None:
            self._set_reconciliation(key, "FAILURE", attempted_at=attempted, completed_at=_iso(self.clock()), error="no venue")
            return {"status": "FAILURE", "count": 0}
        seen = 0
        for row in intents:
            try:
                query = getattr(self.venue, "query_order", None)
                if query is None:
                    raise RuntimeError("venue has no query_order")
                response = _call(query, {"symbol": row["symbol"], "orig_client_order_id": row["client_order_id"], "client_order_id": row["client_order_id"], "order_id": row["exchange_order_id"]})
                status = _status(response)
                if status == UNKNOWN:
                    continue
                if status == REJECTED:
                    payload = _payload(response)
                    code = payload.get("code") if isinstance(payload, Mapping) else None
                    if str(code) == "-2013" or (isinstance(payload, Mapping) and payload.get("authoritative_missing")):
                        self.pause("TESTNET_RESET")
                        self._set_reconciliation("TESTNET_RESET", "RESET", details={"symbol": row["symbol"], "client_order_id": row["client_order_id"], "authoritative_missing": True})
                    continue
                self._apply_order_result(row["intent_id"], response, int(row["generation"]))
                payload = _payload(response)
                order_id = payload.get("orderId") if isinstance(payload, Mapping) else row["exchange_order_id"]
                trades_method = getattr(self.venue, "my_trades", None)
                if trades_method is not None:
                    trades = _call(trades_method, {"symbol": row["symbol"], "order_id": order_id})
                    if _status(trades) != UNKNOWN:
                        self._ingest_fills(row["intent_id"], _explicit_fill_rows(_payload(trades)))
                seen += 1
            except BaseException:
                continue
        self._detect_reset()
        self._recompute_all_positions()
        completed = _iso(self.clock())
        self._set_reconciliation(key, "SUCCESS", attempted_at=attempted, completed_at=completed, heartbeat_at=completed)
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
        intent = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
        if intent is None:
            return 0
        count = 0
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            for fill in fills:
                qty = _dec(fill.get("qty", fill.get("quantity", fill.get("executedQty", 0))))
                if qty <= ZERO:
                    continue
                trade_id = _exchange_trade_id(fill)
                if trade_id is None:
                    trade_id = canonical_sha256({"intent_id": intent_id, "fill": dict(fill)})[:32]
                if self._conn.execute("SELECT 1 FROM binance_execution_fills WHERE trade_id=?", (trade_id,)).fetchone() is not None:
                    continue
                price = _dec(fill.get("price", intent["price"]))
                quote = _dec(fill.get("quoteQty", fill.get("quote_quantity", qty * price)))
                commission = _dec(fill.get("commission", fill.get("fee", 0)))
                asset = fill.get("commissionAsset", fill.get("commission_asset", fill.get("fee_asset")))
                timestamp = fill.get("time", fill.get("trade_time", fill.get("timestamp", self.clock())))
                self._conn.execute("INSERT INTO binance_execution_fills(trade_id,intent_id,client_order_id,exchange_order_id,symbol,side,quantity,price,quote_quantity,commission,commission_asset,trade_time,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (trade_id, intent_id, intent["client_order_id"], intent["exchange_order_id"], intent["symbol"], intent["side"], _dstr(qty), _dstr(price), _dstr(quote), _dstr(commission), None if asset is None else str(asset).upper(), _iso(timestamp), _json(fill)))
                count += 1
            self._conn.commit()
        if count:
            self._recompute_position(str(intent["symbol"]))
            self._update_fill_state(intent_id)
            self._release_if_done(intent_id)
        return count

    record_fills = _ingest_fills
    ingest_fills = _ingest_fills
    def _update_fill_state(self, intent_id: str) -> None:
        row = self._conn.execute("SELECT state,side,quantity FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
        if row is None:
            return
        filled_rows = self._conn.execute("SELECT quantity FROM binance_execution_fills WHERE intent_id=?", (intent_id,)).fetchall()
        qty = sum((_dec(item[0]) for item in filled_rows), ZERO)
        original = _dec(row["quantity"])
        target = FILLED if qty >= original and original > ZERO else PARTIALLY_FILLED
        if str(row["state"]) != REJECTED:
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                current = self._conn.execute("SELECT state,generation FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
                if current is not None:
                    current_state = str(current[0])
                    if current_state != FILLED or target == FILLED:
                        if current_state != target:
                            self._conn.execute("UPDATE binance_execution_order_intents SET state=?,updated_at=? WHERE intent_id=?", (target, _iso(self.clock()), intent_id))
                            self._transition(intent_id, current_state, target, int(current[1]), "fill reconciliation", {"filled_quantity": _dstr(qty)})
                self._conn.commit()

    def _release_if_done(self, intent_id: str) -> None:
        row = self._conn.execute("SELECT state FROM binance_execution_order_intents WHERE intent_id=?", (intent_id,)).fetchone()
        if row is None:
            return
        state = str(row[0])
        if state in {REJECTED, FILLED, CANCELED, EXPIRED}:
            self._conn.execute("UPDATE binance_execution_risk_reservations SET status='RELEASED',released_at=? WHERE intent_id=? AND status='HELD'", (_iso(self.clock()), intent_id))
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
        if isinstance(marks, Mapping) and asset in marks:
            return commission * _dec(marks[asset]), False
        # Third-asset/unknown fees must not be silently treated as zero.
        return ZERO, True

    def _recompute_position(self, symbol: str) -> BinancePosition | None:
        symbol = _symbol(symbol)
        fills = self._conn.execute("SELECT * FROM binance_execution_fills WHERE symbol=? ORDER BY trade_time,trade_id", (symbol,)).fetchall()
        quantity = ZERO
        cost = ZERO
        realized = ZERO
        fees_quote = ZERO
        unknown = False
        candidate_id = None
        binding_hash = None
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
            intent = self._conn.execute("SELECT candidate_id,binding_hash,exit_policy_json FROM binance_execution_order_intents WHERE intent_id=?", (fill["intent_id"],)).fetchone()
            if str(fill["side"]).upper() == "BUY":
                if candidate_id is None and intent is not None:
                    candidate_id, binding_hash, exit_policy = intent[0], intent[1], _load(intent[2])
                base_asset = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else symbol
                net_qty = qty - (commission if str(fill["commission_asset"] or "").upper() in {base_asset, "BASE"} else ZERO)
                quantity += max(ZERO, net_qty)
                cost += quote + (commission if str(fill["commission_asset"] or "").upper() == QUOTE_ASSET else ZERO)
            else:
                base_asset = symbol[:-len(QUOTE_ASSET)] if symbol.endswith(QUOTE_ASSET) else symbol
                sell_qty = qty + (commission if str(fill["commission_asset"] or "").upper() in {base_asset, "BASE"} else ZERO)
                consumed = min(quantity, sell_qty)
                proceeds = quote - (commission if str(fill["commission_asset"] or "").upper() == QUOTE_ASSET else ZERO)
                if consumed == quantity and quantity > ZERO:
                    allocated_cost = cost
                    realized += proceeds - cost
                elif quantity > ZERO:
                    # Keep the fee-adjusted allocation as one rational Decimal
                    # expression; dividing the average first loses a digit at
                    # the active Decimal precision.
                    realized += (proceeds * quantity - cost * consumed) / quantity
                    allocated_cost = cost * consumed / quantity
                else:
                    allocated_cost = ZERO
                    realized += proceeds
                quantity = max(ZERO, quantity - consumed)
                cost = max(ZERO, cost - allocated_cost)
        existing = self._conn.execute("SELECT mark_price FROM binance_execution_positions WHERE symbol=?", (symbol,)).fetchone()
        mark = _dec(existing[0]) if existing and existing[0] is not None else None
        unrealized = None if mark is None or unknown else mark * quantity - cost
        valuation = "UNKNOWN" if unknown else "KNOWN"
        average = cost / quantity if quantity > ZERO else ZERO
        if quantity <= ZERO and realized == ZERO and not unknown:
            # Keep a zero row for an explicitly traded symbol; it makes dust and
            # restart behavior inspectable without inventing inventory.
            pass
        record = BinancePosition(symbol, quantity, cost, average, mark, realized, unrealized, fees_quote, valuation, candidate_id, binding_hash, exit_policy)
        with self._lock:
            self._conn.execute("INSERT INTO binance_execution_positions(symbol,quantity,cost_basis,average_cost,mark_price,realized_pnl,unrealized_pnl,fees_quote,valuation_status,candidate_id,binding_hash,exit_policy_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET quantity=excluded.quantity,cost_basis=excluded.cost_basis,average_cost=excluded.average_cost,mark_price=excluded.mark_price,realized_pnl=excluded.realized_pnl,unrealized_pnl=excluded.unrealized_pnl,fees_quote=excluded.fees_quote,valuation_status=excluded.valuation_status,candidate_id=COALESCE(binance_execution_positions.candidate_id,excluded.candidate_id),binding_hash=COALESCE(binance_execution_positions.binding_hash,excluded.binding_hash),exit_policy_json=COALESCE(binance_execution_positions.exit_policy_json,excluded.exit_policy_json),updated_at=excluded.updated_at", (symbol, _dstr(quantity), _dstr(cost), _dstr(average), None if mark is None else _dstr(mark), _dstr(realized), None if unrealized is None else _dstr(unrealized), _dstr(fees_quote), valuation, candidate_id, binding_hash, _json(exit_policy), _iso(self.clock())))
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
        result["exit_policy"] = _load(result.pop("exit_policy_json", None))
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
        query += " ORDER BY trade_time,trade_id"
        return [dict(row) for row in self._conn.execute(query, args).fetchall()]

    def orders(self, *, state: str | None = None, symbol: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM binance_execution_order_intents"
        args: list[Any] = []
        conditions = []
        if state:
            conditions.append("state=?")
            args.append(str(state).upper())
        if symbol:
            conditions.append("symbol=?")
            args.append(_symbol(symbol))
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY updated_at,intent_id"
        return [dict(row) for row in self._conn.execute(query, args).fetchall()]

    def cancel(self, client_order_id: str | None = None, *, symbol: str, reason: str = "operator cancel", orig_client_order_id: str | None = None) -> dict[str, Any]:
        client_order_id = client_order_id or orig_client_order_id
        if not client_order_id:
            raise ValueError("client_order_id is required")
        row = self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE client_order_id=? AND symbol=?", (client_order_id, _symbol(symbol))).fetchone()
        if row is None:
            raise ValueError("order is not AXIOM-owned")
        if row["state"] in TERMINAL:
            return self._intent_response(row) or {}
        if self.venue is None:
            return self._set_unknown(row["intent_id"], "no venue for cancellation")
        try:
            method = getattr(self.venue, "cancel_owned_order", None) or getattr(self.venue, "cancel_order", None)
            if method is None:
                raise RuntimeError("venue has no owned cancellation method")
            response = _call(method, {"symbol": row["symbol"], "orig_client_order_id": client_order_id, "client_order_id": client_order_id, "order_id": row["exchange_order_id"]})
        except BaseException as exc:
            return self._set_unknown(row["intent_id"], f"cancel exception: {type(exc).__name__}")
        if _status(response) == UNKNOWN:
            return self._set_unknown(row["intent_id"], "ambiguous cancellation result", result=response)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            current = self._conn.execute("SELECT state,generation FROM binance_execution_order_intents WHERE intent_id=?", (row["intent_id"],)).fetchone()
            if current is not None:
                self._conn.execute("UPDATE binance_execution_order_intents SET state=?,reason=?,updated_at=? WHERE intent_id=?", (CANCELED, reason, _iso(self.clock()), row["intent_id"]))
                self._transition(row["intent_id"], str(current[0]), CANCELED, int(current[1]), reason, _payload(response))
            self._conn.commit()
        self._release_if_done(row["intent_id"])
        return self._intent_response(self._conn.execute("SELECT * FROM binance_execution_order_intents WHERE intent_id=?", (row["intent_id"],)).fetchone()) or {}

    cancel_order = cancel
    cancel_owned_order = cancel

    # ---- heartbeat/schema introspection ------------------------------------------
    def _set_reconciliation(self, key: str, status: str, *, attempted_at: Any = None, completed_at: Any = None, heartbeat_at: Any = None, error: str | None = None, details: Any = None) -> None:
        self._conn.execute("INSERT INTO binance_execution_reconciliation(key,status,attempted_at,completed_at,heartbeat_at,error,details_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status=excluded.status,attempted_at=COALESCE(excluded.attempted_at,binance_execution_reconciliation.attempted_at),completed_at=COALESCE(excluded.completed_at,binance_execution_reconciliation.completed_at),heartbeat_at=COALESCE(excluded.heartbeat_at,binance_execution_reconciliation.heartbeat_at),error=excluded.error,details_json=excluded.details_json", (key, status, attempted_at, completed_at, heartbeat_at, error, _json(details or {})))
        self._conn.commit()

    def check_connectivity(self) -> dict[str, Any]:
        checked = _iso(self.clock())
        if self.venue is None or not hasattr(self.venue, "account"):
            result = {"status": "FAILURE", "error": "no venue"}
            self._conn.execute("UPDATE binance_execution_connectivity SET status=?,checked_at=?,heartbeat_at=?,error=?,updated_at=? WHERE singleton=1", ("FAILURE", checked, checked, result["error"], checked))
            self._conn.commit()
            return result
        try:
            response = _call(self.venue.account, {})
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
    "BinancePosition", "ENABLE_CONFIRMATION", "INTENT", "RESERVED", "SUBMITTING", "ACKNOWLEDGED",
    "REJECTED", "UNKNOWN", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "DISABLED", "ARMED",
    "PAUSED", "DISARMED", "KILLED",
]
