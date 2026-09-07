"""Bounded autonomous Binance Spot orchestration.

This module is intentionally a small coordinator.  It does not own a venue,
refresh research, or call any other market family.  All dependencies are
injected (and are used through duck typing) so a worker can be restarted over
an existing execution ledger without importing an execution implementation.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import inspect
import json
import threading
import uuid
from typing import Any, Callable, Iterable, Mapping, Sequence

from .binance_execution import ARMED, DISABLED, DISARMED, KILLED, PAUSED
from .binance_market import BoundedBinanceMarketCollector, BinanceMarketSnapshot
from .binance_risk import SymbolRules
from .binance_signals import BinanceSignalEngine
from .crypto_universe import UniverseSnapshot, load_crypto_universe
from .domain import ensure_utc, utc_now
from .storage import AxiomStore

UTC = timezone.utc
ZERO = Decimal("0")

_SECRET_WORDS = (
    "secret", "password", "token", "credential", "api_key", "apikey",
    "authorization", "private_key", "passphrase", "keyring",
)


def _dec(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def _symbol(value: Any) -> str:
    return str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _iso(value: Any) -> str:
    candidate = value if isinstance(value, datetime) else datetime.now(UTC)
    return ensure_utc(candidate).isoformat()


def _value(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _safe(value: Any, *, depth: int = 0) -> Any:
    """Bound and redact payloads written to the autonomous namespace."""
    if depth > 7:
        return "<truncated>"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in list(value.items())[:256]:
            name = str(key)
            lowered = name.lower().replace("-", "_")
            if any(word in lowered for word in _SECRET_WORDS):
                continue
            result[name] = _safe(child, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe(child, depth=depth + 1) for child in list(value)[:256]]
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            return _safe(value.as_dict(), depth=depth + 1)
        except Exception:
            pass
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _safe(value.to_dict(), depth=depth + 1)
        except Exception:
            pass
    if is_dataclass(value):
        try:
            return _safe(asdict(value), depth=depth + 1)
        except Exception:
            pass
    return str(value)


def _json(value: Any) -> str:
    return json.dumps(_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a fake or production dependency without requiring one signature."""
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        params = signature.parameters
        if not any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
            kwargs = {key: value for key, value in kwargs.items() if key in params}
    return method(*args, **kwargs)


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            result = value.as_dict()
            return result if isinstance(result, Mapping) else None
        except Exception:
            return None
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            result = value.to_dict()
            return result if isinstance(result, Mapping) else None
        except Exception:
            return None
    return None


def _quantity(position: Any) -> Decimal:
    values = [
        _value(
            position,
            "quantity",
            "qty",
            "position_quantity",
            "base_quantity",
            "net_quantity",
            default=None,
        ),
        _value(position, "owned_quantity", "owned_qty", default=None),
    ]
    if not any(value is not None for value in values) and not isinstance(position, (Mapping, list, tuple, set, frozenset)):
        values.append(position)
    for value in values:
        quantity = _dec(value)
        if quantity > ZERO:
            return quantity
    return ZERO



def _flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "n", "none", "null"}
    return bool(value)


def _axiom_owned(position: Any) -> bool:
    foreign = _value(
        position,
        "foreign",
        "is_foreign",
        "foreign_position",
        "external",
        "is_external",
        "non_axiom",
        "not_axiom_owned",
        default=None,
    )
    if foreign is not None and _flag(foreign):
        return False
    marker = _value(position, "axiom_owned", "is_axiom_owned", "owned_by_axiom", "owned", default=None)
    if marker is not None:
        return _flag(marker)
    owner = _value(position, "owner", "owner_name", "ownership", "source", default=None)
    if owner is None:
        # ``execution.positions()`` is an AXIOM ledger projection by contract.
        return True
    if isinstance(owner, Mapping):
        owner = _value(owner, "name", "id", "owner", "system", default=None)
    owner_text = str(owner or "").strip().upper().replace("-", "_").replace(" ", "_")
    if "AXIOM" in owner_text:
        return True
    # Venue/source labels (for example ``BINANCE``) do not establish foreign
    # ownership.  Only an explicit foreign marker opts a positive position out.
    return not (
        owner_text in {"FOREIGN", "EXTERNAL", "OTHER", "NON_AXIOM", "NOT_AXIOM"}
        or "FOREIGN" in owner_text
        or "EXTERNAL" in owner_text
        or "NON_AXIOM" in owner_text
        or "NOT_AXIOM" in owner_text
    )



def _score(value: Any) -> Decimal:
    return _dec(value)


class BinanceAutonomousWorker:
    """Run one serialized, bounded Binance Spot decision cycle.

    The worker is deliberately conservative: it reconciles before looking at
    entries, never enables execution, and treats missing or malformed evidence
    as a no-trade condition.  It accepts either real services or tiny fakes;
    no venue or network object is constructed here.
    """

    namespace = "BINANCE_SPOT_AUTONOMOUS"
    schema_version = "binance-auto-v1"
    worker_name = "binance-auto"

    def __init__(
        self,
        store: AxiomStore,
        execution: Any | None = None,
        *,
        collector: Any | None = None,
        provider: Any | None = None,
        universe: UniverseSnapshot | Mapping[str, Any] | Any | None = None,
        universe_snapshot: UniverseSnapshot | Mapping[str, Any] | None = None,
        universe_loader: Callable[..., Any] | None = None,
        universe_builder: Any | None = None,
        qualification: Any | None = None,
        qualification_service: Any | None = None,
        signal_engine_factory: Callable[..., Any] | None = None,
        signal_engine: Any | None = None,
        strategy: Any | None = None,
        interval_seconds: float = 60.0,
        interval: str = "1d",
        limit: int = 1000,
        max_actionable: int = 3,
        max_candidates: int | None = None,
        max_entries_per_cycle: int = 1,
        fill_quantity: Any = Decimal("1"),
        fee_rate: Any = Decimal("0"),
        time_in_force: str = "IOC",
        depth: int = 20,
        collector_kwargs: Mapping[str, Any] | None = None,
        clock: Callable[[], Any] = utc_now,
        sleeper: Callable[[float], Any] | None = None,
        stop_event: threading.Event | None = None,
        worker_id: str | None = None,
        profile: Any | None = None,
    ) -> None:
        if not isinstance(store, AxiomStore):
            raise TypeError("store must be AxiomStore")
        self.store = store
        self.execution = execution
        self.clock = clock
        self.sleeper = sleeper or (lambda seconds: self._stop_event.wait(seconds))
        self._stop_event = stop_event or threading.Event()
        self._decision_lock = threading.Lock()
        self.worker_id = str(worker_id or self.worker_name)
        self.interval_seconds = float(interval_seconds)
        if self.interval_seconds < 0 or self.interval_seconds != self.interval_seconds or self.interval_seconds == float("inf"):
            raise ValueError("interval_seconds must be finite and non-negative")
        self.interval = str(interval).strip() or "1d"
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be positive")
        self.limit = int(limit)
        self.max_actionable = max(1, int(max_candidates if max_candidates is not None else max_actionable))
        self.max_entries_per_cycle = max(0, int(max_entries_per_cycle))
        self.fill_quantity = _dec(fill_quantity, Decimal("1"))
        if self.fill_quantity <= ZERO:
            raise ValueError("fill_quantity must be positive")
        self.fee_rate = _dec(fee_rate)
        if self.fee_rate < ZERO:
            raise ValueError("fee_rate must be non-negative")
        self.time_in_force = str(time_in_force or "IOC")
        self.depth = max(1, int(depth))
        self.profile = profile
        self.strategy = strategy
        self.qualification = qualification or qualification_service
        self.signal_engine_factory = signal_engine_factory
        self.signal_engine = signal_engine
        self._universe_loader = universe_loader
        self._universe_source = universe_snapshot if universe_snapshot is not None else (universe_builder if universe_builder is not None else universe)
        self._collector = collector
        self._provider = provider
        self._collector_kwargs = dict(collector_kwargs or {})
        self._cycle_number = 0
        self._last_status: dict[str, Any] = {"status": "IDLE", "worker_name": self.worker_id}
        self._event_sink: list[dict[str, Any]] | None = None
        self._init_schema()
        self._load_restart_state()

    # ---- autonomous namespace -------------------------------------------------
    def _init_schema(self) -> None:
        with self.store._lock:
            conn = self.store.connection
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_auto_schema_lock (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    namespace TEXT NOT NULL, schema_version TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), worker_name TEXT NOT NULL,
                    status TEXT NOT NULL, cycle_number INTEGER NOT NULL DEFAULT 0,
                    cycle_id TEXT, no_trade_reason TEXT, pause_reason TEXT,
                    state_json TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_cycles (
                    cycle_id TEXT PRIMARY KEY, worker_name TEXT NOT NULL, cycle_number INTEGER NOT NULL,
                    started_at TEXT NOT NULL, completed_at TEXT, status TEXT NOT NULL,
                    control_state TEXT, universe_id TEXT, universe_version TEXT, universe_hash TEXT,
                    dataset_version TEXT, ranking_run_id TEXT, selected_candidate TEXT,
                    selected_symbol TEXT, entry_count INTEGER NOT NULL DEFAULT 0,
                    exit_count INTEGER NOT NULL DEFAULT 0, no_trade_reason TEXT,
                    error TEXT, payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_events (
                    event_id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, observed_at TEXT NOT NULL,
                    event_type TEXT NOT NULL, status TEXT NOT NULL, symbol TEXT,
                    candidate_id TEXT, signal_id TEXT, reason TEXT, payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_auto_cycles_time
                    ON binance_auto_cycles(worker_name, started_at, cycle_id);
                CREATE INDEX IF NOT EXISTS idx_binance_auto_events_cycle
                    ON binance_auto_events(cycle_id, observed_at, event_id);
                CREATE INDEX IF NOT EXISTS idx_binance_auto_events_signal
                    ON binance_auto_events(signal_id, event_type, status);
                """
            )
            now = _iso(self.clock())
            conn.execute(
                "INSERT OR IGNORE INTO binance_auto_schema_lock(singleton,namespace,schema_version,created_at) VALUES(1,?,?,?)",
                (self.namespace, self.schema_version, now),
            )
            conn.commit()

    def _load_restart_state(self) -> None:
        with self.store._lock:
            row = self.store.connection.execute(
                "SELECT cycle_number,state_json,status FROM binance_auto_state WHERE singleton=1 AND worker_name=?",
                (self.worker_id,),
            ).fetchone()
        if row is not None:
            self._cycle_number = int(row[0] or 0)
            try:
                self._last_status = json.loads(row[1])
            except (TypeError, ValueError, json.JSONDecodeError):
                self._last_status = {"status": str(row[2])}

    def _persist_state(self, result: Mapping[str, Any]) -> None:
        state = _safe(dict(result))
        now = _iso(self.clock())
        with self.store._lock:
            self.store.connection.execute(
                "INSERT INTO binance_auto_state(singleton,worker_name,status,cycle_number,cycle_id,no_trade_reason,pause_reason,state_json,updated_at) VALUES(1,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET worker_name=excluded.worker_name,status=excluded.status,cycle_number=excluded.cycle_number,cycle_id=excluded.cycle_id,no_trade_reason=excluded.no_trade_reason,pause_reason=excluded.pause_reason,state_json=excluded.state_json,updated_at=excluded.updated_at",
                (
                    self.worker_id,
                    str(result.get("status", "IDLE")), self._cycle_number, result.get("cycle_id"),
                    result.get("no_trade_reason"), result.get("pause_reason"), _json(state), now,
                ),
            )
            self.store.connection.commit()
        self._last_status = dict(state) if isinstance(state, Mapping) else {"status": str(result.get("status", "IDLE"))}

    def _persist_cycle(self, result: Mapping[str, Any], started_at: str, completed_at: str) -> None:
        provenance = result.get("provenance") if isinstance(result.get("provenance"), Mapping) else {}
        ranking = result.get("ranking") if isinstance(result.get("ranking"), Mapping) else {}
        selected = ranking.get("selected") if isinstance(ranking.get("selected"), Mapping) else {}
        with self.store._lock:
            self.store.connection.execute(
                "INSERT OR REPLACE INTO binance_auto_cycles(cycle_id,worker_name,cycle_number,started_at,completed_at,status,control_state,universe_id,universe_version,universe_hash,dataset_version,ranking_run_id,selected_candidate,selected_symbol,entry_count,exit_count,no_trade_reason,error,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    result["cycle_id"], self.worker_id, self._cycle_number, started_at, completed_at,
                    result.get("status", "NO_TRADE"), result.get("control_state"), provenance.get("universe_id"),
                    provenance.get("universe_version"), provenance.get("snapshot_hash"), provenance.get("dataset_version"),
                    ranking.get("ranking_run_id"), selected.get("candidate_id"), selected.get("symbol"),
                    len(result.get("entries", ()) or ()), len(result.get("exits", ()) or ()),
                    result.get("no_trade_reason"), result.get("error"), _json(result),
                ),
            )
            self.store.connection.commit()

    def _event(self, cycle_id: str, event_type: str, status: str, *, now: Any, symbol: Any = None, candidate_id: Any = None, signal_id: Any = None, reason: Any = None, payload: Any = None) -> dict[str, Any]:
        event = {
            "event_id": uuid.uuid4().hex,
            "cycle_id": cycle_id,
            "observed_at": _iso(now),
            "event_type": str(event_type),
            "status": str(status),
            "symbol": _symbol(symbol) if symbol else None,
            "candidate_id": None if candidate_id is None else str(candidate_id),
            "signal_id": None if signal_id is None else str(signal_id),
            "reason": None if reason is None else str(reason),
            "payload": payload,
        }
        if self._event_sink is not None:
            self._event_sink.append(_safe(event))
        with self.store._lock:
            self.store.connection.execute(
                "INSERT INTO binance_auto_events(event_id,cycle_id,observed_at,event_type,status,symbol,candidate_id,signal_id,reason,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (event["event_id"], cycle_id, event["observed_at"], event["event_type"], event["status"], event["symbol"], event["candidate_id"], event["signal_id"], event["reason"], _json(event["payload"])),
            )
            self.store.connection.commit()
        return event

    def _signal_seen(self, signal_id: str) -> bool:
        if not signal_id:
            return False
        with self.store._lock:
            row = self.store.connection.execute(
                "SELECT 1 FROM binance_auto_events WHERE signal_id=? AND event_type IN ('ENTRY_SUBMIT','EXIT_SUBMIT') AND status IN ('SUBMITTED','ACCEPTED','UNKNOWN','ACKNOWLEDGED','FILLED','PARTIALLY_FILLED') LIMIT 1",
                (signal_id,),
            ).fetchone()
        return row is not None
    def _control(self) -> dict[str, Any]:
        if self.execution is None:
            return {"state": DISABLED, "authorized": False}
        method = getattr(self.execution, "control", None) or getattr(self.execution, "status", None)
        try:
            value = _call(method) if callable(method) else method
        except BaseException as exc:
            return {"state": PAUSED, "authorized": False, "pause_reason": type(exc).__name__}
        if isinstance(value, Mapping):
            result = dict(value)
            if "state" not in result and result.get("control_state") is not None:
                result["state"] = result["control_state"]
            result.setdefault("state", DISABLED)
            result.setdefault("authorized", False)
            return result
        return {"state": DISABLED, "authorized": False}

    def _pause(self, reason: str) -> None:
        if self.execution is not None:
            method = getattr(self.execution, "pause", None)
            if callable(method):
                try:
                    _call(method, str(reason))
                except Exception:
                    pass

    def _reconcile(self, now: datetime) -> tuple[dict[str, Any], str | None]:
        if self.execution is None:
            return ({"status": "FAILURE", "error": "execution service unavailable"}, "EXECUTION_UNAVAILABLE")
        method = getattr(self.execution, "reconcile", None) or getattr(self.execution, "poll", None)
        if not callable(method):
            return ({"status": "FAILURE", "error": "reconcile unavailable"}, "RECONCILIATION_UNAVAILABLE")
        try:
            result = _call(method)
            result = dict(result) if isinstance(result, Mapping) else {"status": "SUCCESS", "result": result}
            status = str(result.get("status", "SUCCESS")).upper()
            if status in {"FAILURE", "ERROR", "UNKNOWN", "PAUSED"}:
                reason = "RECONCILIATION_" + status
                self._pause(reason)
                return result, reason
            return result, None
        except BaseException as exc:
            reason = "RECONCILIATION_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            return {"status": "FAILURE", "error": type(exc).__name__}, reason

    def _connectivity(self) -> str | None:
        if self.execution is None:
            return "EXECUTION_UNAVAILABLE"
        method = getattr(self.execution, "check_connectivity", None) or getattr(self.execution, "connectivity", None)
        if not callable(method):
            return None
        try:
            result = _call(method)
            status = str(result.get("status", "OK") if isinstance(result, Mapping) else "OK").upper()
            if status not in {"OK", "SUCCESS", "ACKNOWLEDGED"}:
                reason = "CONNECTIVITY_" + status
                self._pause(reason)
                return reason
        except BaseException as exc:
            reason = "CONNECTIVITY_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            return reason
        return None

    def _load_universe(self, now: datetime) -> UniverseSnapshot | None:
        source = self._universe_source
        if self._universe_loader is not None:
            value = _call(self._universe_loader, now=now)
        elif isinstance(source, UniverseSnapshot):
            value = source
        elif isinstance(source, Mapping):
            value = UniverseSnapshot.from_record(source, universe_id=source.get("universe_id"))
        elif source is not None:
            loader = getattr(source, "load_persisted", None) or getattr(source, "load", None) or getattr(source, "snapshot", None)
            if callable(loader):
                value = _call(loader)
            elif callable(source):
                value = _call(source, now=now)
            else:
                value = source
        else:
            value = load_crypto_universe(self.store)
        if value is None:
            return None
        if isinstance(value, UniverseSnapshot):
            return value
        if isinstance(value, Mapping):
            return UniverseSnapshot.from_record(value, universe_id=value.get("universe_id"))
        raise TypeError("universe loader did not return UniverseSnapshot")

    def _collector_for(self, snapshot: UniverseSnapshot) -> Any:
        if self._collector is not None:
            return self._collector
        if self._provider is None:
            raise RuntimeError("market collector/provider unavailable")
        kwargs = dict(self._collector_kwargs)
        kwargs.setdefault("max_workers", 4)
        kwargs.setdefault("depth", self.depth)
        kwargs.setdefault("fill_quantity", float(self.fill_quantity))
        kwargs.setdefault("clock", self.clock)
        return BoundedBinanceMarketCollector(self._provider, snapshot, **kwargs)

    def _collect(self, snapshot: UniverseSnapshot, exit_symbols: Sequence[str], now: datetime) -> list[Any]:
        collector = self._collector_for(snapshot)
        collect = getattr(collector, "collect", None)
        if not callable(collect):
            raise RuntimeError("collector has no collect method")
        result = _call(
            collect, snapshot, interval=self.interval, limit=self.limit, exit_symbols=tuple(exit_symbols),
            # The collector marks symbols listed in ``exit_symbols`` as
            # reconciliation/exit-only itself.  The worker must not put the
            # entire selected universe into that mode: successful
            # reconciliation is a prerequisite for normal entries, not a
            # reason to disable them.
            reconciliation=False, now=now,
        )
        if isinstance(result, Mapping):
            status = str(result.get("status", "SUCCESS")).upper()
            if status in {"FAILURE", "ERROR", "PAUSED"} or result.get("error"):
                raise RuntimeError("market collection " + status)
            if "records" in result or "snapshots" in result:
                records = result.get("records", result.get("snapshots", ()))
                return list(records) if isinstance(records, (list, tuple)) else []
            if result.get("symbol") is not None:
                return [result]
            # A few collectors naturally return ``{symbol: record}`` rather
            # than wrapping records in a list.  Preserve that symbol when the
            # record itself is only a market payload.
            records: list[Any] = []
            for symbol, record in result.items():
                if not isinstance(record, Mapping):
                    continue
                item = dict(record)
                item.setdefault("symbol", symbol)
                records.append(item)
            return records
        return list(result or ())

    # ---- positions and signals ------------------------------------------------
    def _positions(self) -> dict[str, Any]:
        if self.execution is None:
            return {}
        method = getattr(self.execution, "positions", None)
        if callable(method):
            try:
                result = _call(method)
            except Exception:
                return {}
        else:
            result = _value(self.execution, "positions", default={})

        nested_keys = ("positions", "rows", "items", "position", "data")
        metadata_keys = {
            "symbol", "pair", "market", "market_symbol", "asset", "asset_symbol",
            "status", "count", "total", "page", "page_size", "next",
        }

        def rows(value: Any, symbol_hint: str = "", depth: int = 0) -> Iterable[tuple[str, Any]]:
            if depth > 6:
                return
            if isinstance(value, Mapping):
                explicit_symbol = _symbol(
                    _value(value, "symbol", "pair", "market_symbol", "asset_symbol", default=symbol_hint)
                )
                nested = _value(value, *nested_keys, default=None)
                if explicit_symbol:
                    # A list item such as ``{"symbol": "BTCUSDT",
                    # "quantity": "1"}`` is already a position row.
                    if _quantity(value) > ZERO:
                        yield explicit_symbol, value
                    # Some adapters wrap that row in ``position``/``data``.
                    # Keep the outer symbol as a hint while walking inward.
                    if nested is not None:
                        yield from rows(nested, explicit_symbol, depth + 1)
                    return
                if nested is not None:
                    yield from rows(nested, symbol_hint, depth + 1)
                    return
                # Also accept a mapping keyed by symbol, including one nested
                # inside a list: ``[{"BTCUSDT": {"quantity": "1"}}]``.
                for key, child in value.items():
                    name = str(key).strip().lower()
                    if name in metadata_keys or name in nested_keys:
                        continue
                    child_symbol = _symbol(key)
                    if isinstance(child, (Mapping, list, tuple)):
                        yield from rows(child, child_symbol or symbol_hint, depth + 1)
                return
            if isinstance(value, (list, tuple)):
                for item in value:
                    yield from rows(item, symbol_hint, depth + 1)
                return
            mapped = _as_mapping(value)
            if mapped is not None:
                yield from rows(mapped, symbol_hint, depth + 1)
                return
            explicit_symbol = _symbol(_value(value, "symbol", "pair", default=symbol_hint))
            if explicit_symbol:
                yield explicit_symbol, value

        output: dict[str, Any] = {}
        for symbol, row in rows(result):
            if _axiom_owned(row) and _quantity(row) > ZERO:
                output[symbol] = row
        return output

    def _unresolved_exit_symbols(self) -> set[str]:
        """Return symbols with an owned SELL whose reservation is still held."""
        if self.execution is None:
            return set()
        method = getattr(self.execution, "orders", None)
        if not callable(method):
            return set()
        try:
            rows = _call(method)
        except Exception:
            return set()
        result: set[str] = set()
        for row in rows or ():
            if not isinstance(row, Mapping):
                continue
            if str(row.get("intent", "")).upper() != "EXIT":
                continue
            if str(row.get("side", "")).upper() != "SELL":
                continue
            reservation = row.get("risk_reservation")
            if not isinstance(reservation, Mapping) or str(reservation.get("status", "")).upper() != "HELD":
                continue
            symbol = _symbol(row.get("symbol"))
            if symbol:
                result.add(symbol)
        return result

    def _origin_binding(self, position: Any) -> dict[str, Any]:
        for name in ("originating_binding", "entry_binding", "binding"):
            value = _value(position, name, default=None)
            if isinstance(value, Mapping):
                return dict(value)
        value = _value(position, "binding_json", default=None)
        if isinstance(value, Mapping):
            return dict(value)
        candidate = _value(position, "candidate_id", "originating_candidate_id", default="")
        symbol = _symbol(_value(position, "symbol", default=""))
        return {
            "candidate_id": str(candidate or ("position-" + symbol)),
            "symbol": symbol,
            "binding_hash": _value(position, "binding_hash", "originating_binding_hash", default=""),
            "environment": "PAPER",
            "timeframe": self.interval,
            "exit_policy": _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default={}) or {},
        }

    def _engine(self, binding: Mapping[str, Any], *, position: Any = None, intent: str = "ENTRY", row: Mapping[str, Any] | None = None) -> Any:
        policy = _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default=None) if position is not None else None
        if not isinstance(policy, Mapping):
            policy = _value(binding, "exit_policy", default={}) or {}
        strategy = _value(binding, "strategy", "strategy_document", default=None) or self.strategy
        if position is not None and isinstance(policy, Mapping):
            strategy = policy.get("strategy") or policy.get("strategy_definition") or strategy
        kwargs = {
            "binding": binding, "strategy": strategy, "environment": _value(binding, "environment", default="PAPER"),
            "decision_interval": _value(binding, "timeframe", "interval", default=self.interval),
            "exit_policy": dict(policy) if isinstance(policy, Mapping) else {},
            "positions": { _symbol(_value(position, "symbol", default="")): position } if position is not None else {},
            "clock": self.clock,
            "intent": intent,
            "row": row,
        }
        factory = self.signal_engine_factory
        if factory is not None:
            try:
                return _call(factory, **kwargs)
            except TypeError:
                for args in ((binding,), (binding, strategy), (binding, position)):
                    try:
                        return factory(*args)
                    except TypeError:
                        continue
                raise
        if self.signal_engine is not None:
            if callable(self.signal_engine) and not hasattr(self.signal_engine, "evaluate"):
                return _call(self.signal_engine, **kwargs)
            return self.signal_engine
        if strategy is None:
            raise ValueError("strategy unavailable for BinanceSignalEngine")
        return BinanceSignalEngine(
            binding, strategy, environment=kwargs["environment"], decision_interval=kwargs["decision_interval"],
            exit_policy=kwargs["exit_policy"], positions=kwargs["positions"], clock=self.clock,
        )

    def _evaluate(self, engine: Any, market: Any, *, now: datetime, positions: Mapping[str, Any]) -> tuple[Mapping[str, Any] | None, str]:
        method = getattr(engine, "evaluate", None) or getattr(engine, "decide", None)
        if not callable(method):
            raise RuntimeError("signal engine has no evaluate method")
        value = _call(method, market, now=now, positions=positions)
        if value is None:
            return None, str(_value(engine, "no_trade_reason", default="NO_SIGNAL") or "NO_SIGNAL")
        result = _as_mapping(value)
        if result is None:
            raise TypeError("signal engine returned a non-mapping signal")
        return result, ""

    def _market_map(self, records: Iterable[Any]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if isinstance(records, Mapping):
            items = records.items()
        else:
            items = ((None, record) for record in records)
        for key, record in items:
            symbol = _symbol(_value(record, "symbol", default=key or ""))
            if symbol:
                if key and isinstance(record, Mapping) and not _value(record, "symbol", default=None):
                    record = {**record, "symbol": symbol}
                output[symbol] = record
        return output

    def _feasible(self, market: Any) -> bool:
        if bool(_value(market, "error", default=None)) or bool(_value(market, "timed_out", default=False)):
            return False
        if not bool(_value(market, "tradable", default=False)):
            return False
        status = _value(market, "status", default=None)
        if status is not None and str(status).strip() and str(status).upper() != "TRADING":
            return False
        exchange_info = _value(market, "exchange_info", default=None)
        if isinstance(exchange_info, Mapping):
            info_status = str(exchange_info.get("status", "")).upper()
            if info_status and info_status != "TRADING":
                return False
            if exchange_info.get("isSpotTradingAllowed") is False:
                return False
        rules = _value(market, "symbol_rules", "rules", default=None)
        if rules is not None:
            rule_status = _value(rules, "status", default=None)
            if rule_status is not None and str(rule_status).strip() and str(rule_status).upper() != "TRADING":
                return False
            if _value(rules, "spot_trading_allowed", default=True) is False:
                return False
        if not bool(_value(market, "new_entry_allowed", default=False)):
            return False
        if not bool(_value(market, "ticker_fresh", "fresh_ticker", default=False)) or not bool(_value(market, "book_fresh", "fresh_book", default=False)):
            return False
        depth = _value(market, "depth", default={})
        if not isinstance(depth, Mapping) or depth.get("fresh") is False:
            return False
        try:
            if int(depth.get("bid_levels", 0) or 0) < 1 or int(depth.get("ask_levels", 0) or 0) < 1:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        spread = _value(market, "spread", default=None)
        if spread is None or _dec(spread) <= ZERO:
            return False
        evidence = _value(market, "fill_evidence", default={})
        if not isinstance(evidence, Mapping):
            return False
        buy, sell = evidence.get("buy"), evidence.get("sell")
        return isinstance(buy, Mapping) and isinstance(sell, Mapping) and bool(buy.get("complete")) and bool(sell.get("complete"))

    def _rules(self, market: Any) -> SymbolRules:
        existing = _value(market, "symbol_rules", "rules", default=None)
        if isinstance(existing, SymbolRules):
            return existing
        info = _value(market, "exchange_info", default=None)
        if isinstance(info, Mapping):
            try:
                return SymbolRules.from_exchange_info(info)
            except (TypeError, ValueError):
                pass
        return SymbolRules(symbol=_symbol(_value(market, "symbol", default="")), status="TRADING", spot_trading_allowed=True)

    def _price(self, market: Any, side: str) -> Decimal:
        evidence = _value(market, "fill_evidence", default={})
        side_data = evidence.get("buy" if side == "BUY" else "sell", {}) if isinstance(evidence, Mapping) else {}
        price = _dec(side_data.get("price") if isinstance(side_data, Mapping) else None)
        if price > ZERO:
            return price
        ticker = _value(market, "ticker", default=None)
        return _dec(_value(ticker, "ask" if side == "BUY" else "bid", "last", default=ZERO))

    def _submit(self, signal: Mapping[str, Any], market: Any, *, position: Any = None, now: datetime, cycle_id: str, event_type: str, candidate_id: Any = None) -> tuple[dict[str, Any], bool]:
        signal_id = str(signal.get("signal_id") or signal.get("id") or "")
        symbol = _symbol(signal.get("symbol") or _value(market, "symbol", default=""))
        if not signal_id:
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, reason="SIGNAL_ID_MISSING", payload=signal)
            return {"status": "BLOCKED", "reason": "SIGNAL_ID_MISSING"}, False
        if self._signal_seen(signal_id):
            self._event(cycle_id, event_type, "DEDUPLICATED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason="DUPLICATE_INTERVAL", payload=signal)
            return {"status": "DEDUPLICATED", "reason": "DUPLICATE_INTERVAL", "signal_id": signal_id}, False
        side = str(signal.get("side", "SELL" if signal.get("intent") == "EXIT" else "BUY")).upper()
        price = self._price(market, side)
        quantity = _quantity(position) if position is not None else self.fill_quantity
        if price <= ZERO or quantity <= ZERO:
            reason = "CONSERVATIVE_PRICE_OR_QUANTITY_UNAVAILABLE"
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "BLOCKED", "reason": reason}, False
        if self.execution is None or not callable(getattr(self.execution, "submit_signal", None)):
            reason = "EXECUTION_UNAVAILABLE"
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "BLOCKED", "reason": reason}, False
        try:
            result = _call(
                self.execution.submit_signal, signal, price=price, quantity=quantity, rules=self._rules(market),
                fee_rate=self.fee_rate, time_in_force=self.time_in_force, now=now, opportunity_id=candidate_id,
            )
            output = dict(result) if isinstance(result, Mapping) else {"status": "SUBMITTED", "result": result}
        except BaseException as exc:
            reason = "SUBMIT_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            self._event(cycle_id, event_type, "PAUSED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "PAUSED", "reason": reason}, False
        state = str(output.get("state", output.get("status", "SUBMITTED"))).upper()
        blocked_reason = str(output.get("reason", ""))
        accepted = state not in {"REJECTED", "BLOCKED", "PAUSED", "DISABLED", "DISARMED", "KILLED"} and not blocked_reason.startswith("CONTROL_")
        self._event(cycle_id, event_type, "ACCEPTED" if accepted else "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=blocked_reason or state, payload={"signal": signal, "result": output})
        return output, accepted

    # ---- ranking ---------------------------------------------------------------
    def _rank(self, feasibility: Mapping[str, Any], now: datetime) -> dict[str, Any]:
        if self.qualification is None:
            return {"selection_status": "NONE", "rankings": [], "reason": "QUALIFICATION_UNAVAILABLE"}
        rank = getattr(self.qualification, "rank_and_select", None) or getattr(self.qualification, "rank", None)
        if callable(rank):
            result = _call(rank, feasibility, limit=self.max_actionable, now=now)
            projected = _as_mapping(result)
            if projected is not None:
                return dict(projected)
            metadata = getattr(result, "meta", None)
            output = dict(metadata) if isinstance(metadata, Mapping) else {}
            output["rankings"] = list(result or ())
            return output
        qualify = getattr(self.qualification, "qualify_all", None)
        if callable(qualify):
            qualified = _call(qualify, feasibility, now=now)
        else:
            qualified = ()
        action = getattr(self.qualification, "actionable_rankings", None)
        rows = _call(action, self.max_actionable) if callable(action) else qualified
        return {"selection_status": "CURRENT" if rows else "NONE", "rankings": list(rows or ())}

    def _qualification_current(self, ranking: Mapping[str, Any]) -> bool:
        status = str(ranking.get("selection_status", ranking.get("status", ""))).upper()
        if status in {"STALE", "NONE", "INVALID", "PAUSED"}:
            return False
        current = getattr(self.qualification, "status", None) if self.qualification is not None else None
        if current is not None:
            try:
                value = _call(current) if callable(current) else current
                if isinstance(value, Mapping):
                    current_status = str(value.get("selection_status", value.get("status", ""))).upper()
                    if current_status and current_status not in {"CURRENT", "QUALIFIED"}:
                        return False
                elif value is not None:
                    current_status = str(value).upper()
                    if current_status and current_status not in {"CURRENT", "QUALIFIED"}:
                        return False
            except Exception:
                return False
        return True

    def _candidates(self, ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows: list[Any] = []
        for key in ("rankings", "actionable_rankings", "fallbacks"):
            values = ranking.get(key)
            if isinstance(values, (list, tuple)):
                rows.extend(values)
        selected = ranking.get("selected") or ranking.get("winner")
        if selected:
            rows.insert(0, selected)
        result: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                continue
            row = dict(raw)
            symbol = _symbol(row.get("symbol"))
            candidate = str(row.get("candidate_id", ""))
            if not symbol or not candidate:
                continue
            if row.get("qualified") is False or row.get("actionable") is False:
                continue
            key = (candidate, symbol)
            if key in seen:
                continue
            seen.add(key)
            row["symbol"] = symbol
            result.append(row)

        def order_key(row: Mapping[str, Any]) -> tuple[int, Decimal, str, str]:
            try:
                rank = int(row.get("rank") or 10**9)
            except (TypeError, ValueError, OverflowError):
                rank = 10**9
            return rank, -_score(row.get("total_score")), str(row.get("candidate_id")), str(row.get("symbol"))

        result.sort(key=order_key)
        # The qualification limit counts fallbacks, while ``selected`` /
        # ``winner`` is returned separately by the production ranker.  Keep
        # that selected row plus the bounded fallback set so an infeasible
        # winner cannot hide the next actionable candidate.  Without a
        # selection, retain the original actionable bound.
        candidate_limit = self.max_actionable + (1 if selected else 0)
        return result[:candidate_limit]

    # ---- cycle/run -------------------------------------------------------------
    def cycle(self, *, now: datetime | None = None) -> dict[str, Any]:
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "worker_name": self.worker_id, "reason": "CYCLE_ALREADY_IN_PROGRESS"}
        started = ensure_utc(now or self.clock())
        self._cycle_number += 1
        cycle_id = "cycle-" + uuid.uuid4().hex
        result: dict[str, Any] = {
            "schema_version": self.schema_version, "worker_name": self.worker_id, "cycle_id": cycle_id,
            "cycle_number": self._cycle_number, "status": "NO_TRADE", "control_state": None,
            "entries": [], "exits": [], "events": [], "no_trade_reason": "NO_ACTIONABLE_SIGNAL",
            "pause_reason": None, "error": None, "provenance": {}, "ranking": {}, "reconciliation": {},
        }
        self._event_sink = []
        try:
            # This is deliberately first, including DISABLED/DISARMED/KILLED.
            reconciliation, reconcile_reason = self._reconcile(started)
            result["reconciliation"] = reconciliation
            if reconcile_reason:
                result["pause_reason"] = reconcile_reason
            result["control_state"] = self._control().get("state")
            snapshot = self._load_universe(started)
            if snapshot is None:
                # A missing persisted snapshot is an ordinary no-trade input
                # only after reconciliation succeeds.  A real reconciliation
                # failure remains paused and must not be hidden by this gate.
                if reconcile_reason:
                    result["status"] = "PAUSED"
                    return result
                # Reconciliation/control have already run, but no market,
                # provider, or qualification service may be touched.
                result["status"] = "NO_TRADE"
                result["no_trade_reason"] = "NO_UNIVERSE"
                result["pause_reason"] = None
                result["error"] = None
                return result
            connectivity_reason = self._connectivity()
            if connectivity_reason and not result.get("pause_reason"):
                result["pause_reason"] = connectivity_reason
            positions = self._positions()
            unresolved_exits = self._unresolved_exit_symbols()
            exit_symbols = tuple(sorted(set(positions) - unresolved_exits))
            result["provenance"] = {
                "universe_id": snapshot.universe_id, "universe_version": snapshot.version,
                "snapshot_hash": snapshot.snapshot_hash, "dataset_version": snapshot.version,
                "status": snapshot.status, "selected_symbols": list(snapshot.selected_symbols),
            }
            records = self._collect(snapshot, exit_symbols, started)
            market_failures = [
                market for market in records
                if bool(_value(market, "error", default=None)) or bool(_value(market, "timed_out", default=False))
            ]
            if market_failures and not result.get("pause_reason"):
                result["pause_reason"] = "MARKET_COLLECTION_FAILURE"
                self._pause(result["pause_reason"])
            markets = self._market_map(records)

            # Exits are evaluated and submitted before any rank/entry work.
            for symbol in exit_symbols:
                position = positions[symbol]
                market = markets.get(symbol)
                if market is None:
                    reason = "EXIT_MARKET_EVIDENCE_UNAVAILABLE"
                    self._event(cycle_id, "EXIT_EVALUATION", "BLOCKED", now=started, symbol=symbol, candidate_id=_value(position, "candidate_id", default=None), reason=reason, payload={"position": position})
                    result["exits"].append({
                        "symbol": symbol,
                        "intent": "EXIT",
                        "status": "BLOCKED",
                        "result": {"status": "BLOCKED", "reason": reason},
                        "reason": reason,
                    })
                    continue
                binding = self._origin_binding(position)
                try:
                    engine = self._engine(binding, position=position, intent="EXIT")
                    signal, reason = self._evaluate(engine, market, now=started, positions=positions)
                    if signal is None:
                        reason = reason or "NO_SIGNAL"
                        self._event(cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=binding.get("candidate_id"), reason=reason, payload={"binding": binding, "position": position})
                        result["exits"].append({
                            "symbol": symbol,
                            "intent": "EXIT",
                            "status": "NO_TRADE",
                            "result": {"status": "NO_TRADE", "reason": reason},
                            "reason": reason,
                        })
                        continue
                    signal_intent = str(signal.get("intent", "EXIT")).upper()
                    signal_side = str(signal.get("side", "SELL" if signal_intent == "EXIT" else "BUY")).upper()
                    if signal_intent != "EXIT" or signal_side != "SELL":
                        reason = "NON_EXIT_SIGNAL"
                        self._event(cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=binding.get("candidate_id"), reason=reason, payload=signal)
                        result["exits"].append({
                            "symbol": symbol,
                            "intent": "EXIT",
                            "status": "NO_TRADE",
                            "result": {"status": "NO_TRADE", "reason": reason},
                            "reason": reason,
                        })
                        continue
                    submitted, accepted = self._submit(signal, market, position=position, now=started, cycle_id=cycle_id, event_type="EXIT_SUBMIT", candidate_id=binding.get("candidate_id"))
                    result["exits"].append({
                        "symbol": symbol,
                        "intent": "EXIT",
                        "status": "SUBMITTED" if accepted else "BLOCKED",
                        "result": submitted,
                        "signal_id": signal.get("signal_id"),
                    })
                except BaseException as exc:
                    reason = "EXIT_EVALUATION_" + type(exc).__name__.upper()
                    self._event(cycle_id, "EXIT_EVALUATION", "BLOCKED", now=started, symbol=symbol, candidate_id=binding.get("candidate_id"), reason=reason, payload={"error": str(exc), "binding": binding})
                    result["exits"].append({
                        "symbol": symbol,
                        "intent": "EXIT",
                        "status": "BLOCKED",
                        "result": {"status": "BLOCKED", "reason": reason},
                        "reason": reason,
                    })

            feasibility = {symbol: self._feasible(market) for symbol, market in markets.items()}
            result["feasibility"] = feasibility
            try:
                ranking = self._rank(feasibility, started)
            except BaseException as exc:
                self._pause("QUALIFICATION_TRANSPORT_" + type(exc).__name__.upper())
                ranking = {"selection_status": "PAUSED", "rankings": [], "reason": "QUALIFICATION_FAILURE", "error": type(exc).__name__}
                result["pause_reason"] = "QUALIFICATION_FAILURE"
            result["ranking"] = _safe(ranking)
            control = self._control()
            result["control_state"] = control.get("state")
            entry_gate_reason = None
            if result.get("pause_reason"):
                entry_gate_reason = str(result["pause_reason"])
            elif str(snapshot.status).upper() != "CURRENT":
                entry_gate_reason = "UNIVERSE_STALE"
            elif not self._qualification_current(ranking):
                entry_gate_reason = "QUALIFICATION_SELECTION_NOT_CURRENT"
            elif str(control.get("state", DISABLED)).upper() != ARMED or not bool(control.get("authorized", False)):
                entry_gate_reason = "CONTROL_" + str(control.get("state", DISABLED)).upper()
            entries_attempted = 0
            if entry_gate_reason is None:
                for row in self._candidates(ranking):
                    if entries_attempted >= self.max_entries_per_cycle:
                        break
                    candidate, symbol = str(row["candidate_id"]), _symbol(row["symbol"])
                    market = markets.get(symbol)
                    if symbol in positions:
                        reason = "OWNED_POSITION_NO_AVERAGING"
                    elif market is None:
                        reason = "MARKET_EVIDENCE_UNAVAILABLE"
                    elif not feasibility.get(symbol, False):
                        reason = "EXECUTION_EVIDENCE_INFEASIBLE"
                    else:
                        reason = ""
                    if reason:
                        self._event(cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=candidate, reason=reason, payload=row)
                        result["no_trade_reason"] = reason
                        continue
                    binding = _value(row, "binding", default=row)
                    if not isinstance(binding, Mapping):
                        binding = row
                    try:
                        engine = self._engine(dict(binding), intent="ENTRY", row=row)
                        signal, signal_reason = self._evaluate(engine, market, now=started, positions=positions)
                        if signal is None:
                            reason = signal_reason or "NO_SIGNAL"
                            self._event(cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=candidate, reason=reason, payload=row)
                            result["no_trade_reason"] = reason
                            continue
                        if str(signal.get("intent", "ENTRY")).upper() != "ENTRY" or str(signal.get("side", "BUY")).upper() != "BUY":
                            reason = "NON_ENTRY_SIGNAL"
                            self._event(cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=candidate, reason=reason, payload=signal)
                            result["no_trade_reason"] = reason
                            continue
                        self._event(cycle_id, "ENTRY_EVALUATION", "EVALUATED", now=started, symbol=symbol, candidate_id=candidate, signal_id=signal.get("signal_id"), reason="SIGNAL_READY", payload=signal)
                        submitted, accepted = self._submit(signal, market, now=started, cycle_id=cycle_id, event_type="ENTRY_SUBMIT", candidate_id=candidate)
                        if accepted:
                            entries_attempted += 1
                            result["entries"].append({"candidate_id": candidate, "symbol": symbol, "intent": "ENTRY", "status": "SUBMITTED", "result": submitted, "signal_id": signal.get("signal_id")})
                            result["no_trade_reason"] = ""
                        else:
                            reason = str(submitted.get("reason", "ENTRY_REJECTED"))
                            if reason == "DUPLICATE_INTERVAL":
                                result["entries"].append({"candidate_id": candidate, "symbol": symbol, "intent": "ENTRY", "status": "DEDUPLICATED", "result": submitted, "signal_id": signal.get("signal_id")})
                                result["no_trade_reason"] = reason
                                break
                            result["no_trade_reason"] = reason
                    except BaseException as exc:
                        reason = "ENTRY_EVALUATION_" + type(exc).__name__.upper()
                        self._event(cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started, symbol=symbol, candidate_id=candidate, reason=reason, payload={"error": str(exc), "row": row})
                        result["no_trade_reason"] = reason
            else:
                result["no_trade_reason"] = entry_gate_reason
                self._event(cycle_id, "ENTRY_GATE", "NO_TRADE", now=started, reason=entry_gate_reason, payload={"control": control, "ranking": ranking})
            if any(item.get("status") == "SUBMITTED" for item in result["entries"]) or any(item.get("status") == "SUBMITTED" for item in result["exits"]):
                result["status"] = "ACTIONED"
            elif result.get("pause_reason"):
                result["status"] = "PAUSED"
            else:
                result["status"] = "NO_TRADE"
        except BaseException as exc:
            result["status"] = "PAUSED"
            result["pause_reason"] = "CYCLE_" + type(exc).__name__.upper()
            result["error"] = type(exc).__name__
            self._pause(str(result["pause_reason"]))
        finally:
            completed = ensure_utc(self.clock())
            result["events"] = list(self._event_sink or ())
            self._event_sink = None
            try:
                self._persist_cycle(result, _iso(started), _iso(completed))
                self._persist_state(result)
            except Exception:
                # A persistence failure is visible in process state, but must
                # never make a long-running worker thread die.
                self._last_status = dict(result)
            self._decision_lock.release()
        return result

    def run(self, max_cycles: int | None = None) -> list[dict[str, Any]]:
        if max_cycles is not None and (isinstance(max_cycles, bool) or int(max_cycles) < 0):
            raise ValueError("max_cycles must be non-negative or None")
        target = None if max_cycles is None else int(max_cycles)
        results: list[dict[str, Any]] = []
        while target is None or len(results) < target:
            if self._stop_event.is_set():
                break
            try:
                results.append(self.cycle())
            except BaseException as exc:
                self._pause("RUN_" + type(exc).__name__.upper())
                results.append({"status": "PAUSED", "reason": type(exc).__name__})
            if target is not None and len(results) >= target:
                break
            if self._stop_event.is_set():
                break
            if self.interval_seconds > 0:
                try:
                    self.sleeper(self.interval_seconds)
                except BaseException as exc:
                    self._pause("SLEEP_" + type(exc).__name__.upper())
                    break
        return results

    def stop(self) -> None:
        self._stop_event.set()

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            row = self.store.connection.execute("SELECT * FROM binance_auto_state WHERE singleton=1").fetchone()
            cycle = self.store.connection.execute("SELECT * FROM binance_auto_cycles WHERE worker_name=? ORDER BY cycle_number DESC,cycle_id DESC LIMIT 1", (self.worker_id,)).fetchone()
        result = dict(self._last_status)
        result.update({"worker_name": self.worker_id, "namespace": self.namespace, "schema_version": self.schema_version, "cycle_number": self._cycle_number})
        if row is not None:
            result.update({"status": row["status"], "no_trade_reason": row["no_trade_reason"], "pause_reason": row["pause_reason"], "updated_at": row["updated_at"]})
        if cycle is not None:
            result["last_cycle"] = {"cycle_id": cycle["cycle_id"], "status": cycle["status"], "started_at": cycle["started_at"], "completed_at": cycle["completed_at"], "entry_count": cycle["entry_count"], "exit_count": cycle["exit_count"], "no_trade_reason": cycle["no_trade_reason"], "error": cycle["error"]}
        return _safe(result)


__all__ = ["BinanceAutonomousWorker"]
