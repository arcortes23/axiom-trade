"""Safe, bounded operator facade for the isolated Binance Spot canary.

This module is intentionally independent of :mod:`axiom.operator`.  It never
imports or invokes Polymarket/Hermes code, and it exposes only the small action
allowlist needed by a development Binance dashboard.  All durable writes use
the ``binance_operator_*`` namespace; action records are append-only and carry
both UTC and Asia/Manila display timestamps.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

try:  # These imports are deliberately Binance-only.
    from .binance_execution import (
        ARMED,
        BINANCE_SPOT_LIVE,
        DISARMED,
        DISABLED,
        ENABLE_CONFIRMATION,
        KILLED,
        PAPER,
        PAUSED,
        UNKNOWN,
    )
except ImportError:  # pragma: no cover - direct module loading fallback
    ARMED = "ARMED"
    BINANCE_SPOT_LIVE = "BINANCE_SPOT_LIVE"
    DISARMED = "DISARMED"
    DISABLED = "DISABLED"
    ENABLE_CONFIRMATION = "ENABLE BINANCE AUTO CANARY"
    KILLED = "KILLED"
    PAPER = "PAPER"
    PAUSED = "PAUSED"
    UNKNOWN = "UNKNOWN"

try:
    from .binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE, SymbolRules, size_limit_order
except ImportError:  # pragma: no cover - direct module loading fallback
    DEFAULT_BINANCE_RISK_ENVELOPE = None
    SymbolRules = None
    size_limit_order = None


UTC = timezone.utc
try:
    PHT = ZoneInfo("Asia/Manila")
except Exception:  # pragma: no cover - zoneinfo is available on supported Python
    PHT = timezone(timedelta(hours=8))

ACTION_NAMES = frozenset(
    {
        "CONNECTIVITY_CHECK",
        "ORDER_VALIDATION_TEST",
        "ENABLE",
        "PAUSE",
        "RESUME",
        "DISARM",
        "KILL",
        "RESTART",
    }
)
EXACT_ENABLE_PHRASE = ENABLE_CONFIRMATION
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 25
DEFAULT_STALE_SECONDS = 300.0
_SECRET_WORDS = re.compile(
    r"(?:secret|password|passwd|token|api[_-]?key|apikey|private[_-]?key|private|mnemonic|passphrase|authorization|bearer|credential)",
    re.I,
)
_FORBIDDEN_TRANSPORT_WORDS = re.compile(r"(?:polymarket|hermes)", re.I)
_HOST_KEYS = frozenset({"host", "hostname", "base_url", "baseurl", "url", "endpoint", "origin", "path"})


class BinanceOperatorError(ValueError):
    """A request was rejected before it reached any service or transport."""

    def __init__(self, reason: str, message: str | None = None) -> None:
        self.reason = str(reason)
        super().__init__(message or self.reason)


def _utc(value: Any = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return datetime.now(UTC)
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def _iso(value: Any = None) -> str:
    return _utc(value).isoformat()


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if isinstance(value, bool):
        return default
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def _decimal_text(value: Any, default: Decimal = Decimal("0")) -> str:
    """Render an exact, stable USDT amount with at least two decimals.

    Display formatting must not quantize values: execution and risk
    projections can carry more precision than cents, and the audit view must
    preserve it verbatim.
    """
    text = format(_decimal(value, default), "f")
    if "." not in text:
        return f"{text}.00"
    _, fraction = text.split(".", 1)
    if len(fraction) < 2:
        text += "0" * (2 - len(fraction))
    return text


def _jsonable(value: Any, *, key: str | None = None, redact: bool = True) -> Any:
    """Convert arbitrary fake-service values to bounded, secret-free JSON.

    Secret-shaped scalar values are replaced, but container values are walked
    recursively.  In particular, a safe credential projection must remain a
    mapping so callers can still inspect ``configured`` and ``reference_hash``.
    """
    secret_key = bool(
        key
        and _SECRET_WORDS.search(str(key))
        and str(key).lower() not in {"credential_hash", "credential_ref_hash", "reference_hash"}
    )
    if secret_key and redact and not isinstance(value, Mapping) and not isinstance(value, (list, tuple, set, frozenset)):
        return "<redacted>"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v, key=str(k), redact=redact) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item, key=key if secret_key else None, redact=redact) for item in value]
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _jsonable(value.to_dict(), key=key, redact=redact)
        except Exception:
            pass
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            return _jsonable(value.as_dict(), key=key, redact=redact)
        except Exception:
            pass
    if hasattr(value, "value") and not isinstance(value, (str, bytes, bytearray)):
        return _jsonable(value.value, key=key, redact=redact)
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and (value != value or value in {float("inf"), float("-inf")}):
            return None
        return value
    return str(value)[:512]


def _contains_secret_shape(value: Any, *, parent_key: str = "") -> bool:
    if parent_key and _SECRET_WORDS.search(parent_key):
        return True
    if isinstance(value, Mapping):
        return any(_contains_secret_shape(child, parent_key=str(key)) for key, child in value.items())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_contains_secret_shape(child, parent_key=parent_key) for child in value)
    return False


def _contains_forbidden_transport(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_FORBIDDEN_TRANSPORT_WORDS.search(value))
    if isinstance(value, Mapping):
        return any(_contains_forbidden_transport(key) or _contains_forbidden_transport(child) for key, child in value.items())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_contains_forbidden_transport(child) for child in value)
    return False


def _timestamp_projection(value: Any) -> dict[str, str]:
    stamp = _utc(value)
    return {"utc": stamp.isoformat(), "pht": stamp.astimezone(PHT).isoformat()}


def _id_summary(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= 24:
        return text
    return f"{text[:10]}…{text[-10:]}"


def _summary_record(record: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(_jsonable(record))
    for key, value in list(result.items()):
        if value is not None and "id" in str(key).lower() and isinstance(value, str) and len(value) > 24:
            result[f"{key}_summary"] = _id_summary(value)
            result[key] = _id_summary(value)
    return result


def _as_mapping(value: Any) -> dict[str, Any]:
    projected = _jsonable(value)
    return dict(projected) if isinstance(projected, Mapping) else {}


def _call(method: Any, payload: Mapping[str, Any] | None = None) -> Any:
    """Call a duck-typed fake/service without broad retrying side effects."""
    if not callable(method):
        raise TypeError("service method is not callable")
    body = dict(payload or {})
    try:
        return method(**body)
    except TypeError as first:
        # Simple fakes often accept one payload object.  This fallback is only
        # used after Python rejected the keyword shape, not after a network call.
        try:
            return method(body)
        except TypeError:
            raise first


class BinanceCanaryControlPlane:
    """Authoritative, Binance-only operator boundary and dashboard projection.

    ``execution``, ``qualification`` and ``worker`` are intentionally duck
    typed.  This keeps the dashboard independent from concrete worker startup
    and makes local fakes useful without importing any Polymarket services.
    """

    action_table = "binance_operator_actions"

    def __init__(
        self,
        store: Any,
        execution: Any,
        qualification: Any | None = None,
        worker: Any | None = None,
        profile: Any | None = None,
        *,
        clock: Callable[[], Any] | None = None,
        stale_after_seconds: float = DEFAULT_STALE_SECONDS,
    ) -> None:
        self.store = store
        self.execution = execution
        self.qualification = qualification
        self.worker = worker
        self.profile = profile if profile is not None else getattr(execution, "profile", None)
        self.clock = clock or getattr(execution, "clock", None) or (lambda: datetime.now(UTC))
        try:
            stale = float(stale_after_seconds)
        except (TypeError, ValueError):
            stale = DEFAULT_STALE_SECONDS
        self.stale_after_seconds = max(1.0, stale)
        self._lock = threading.RLock()
        self._conn = self._resolve_connection()
        self._init_schema()
        self._validate_profile()

    def _resolve_connection(self) -> sqlite3.Connection:
        for candidate in (
            self.store if isinstance(self.store, sqlite3.Connection) else None,
            getattr(self.store, "connection", None),
            getattr(self.store, "_conn", None),
            getattr(self.execution, "_conn", None),
        ):
            if isinstance(candidate, sqlite3.Connection):
                candidate.row_factory = sqlite3.Row
                return candidate
        connection = sqlite3.connect(":memory:", check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_operator_actions (
                    action_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL DEFAULT '',
                    attempted_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    timestamp_pht TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    result_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_operator_actions_time
                    ON binance_operator_actions(timestamp_utc, action_id);
                """
            )
            columns = {
                str(row[1])
                for row in self._conn.execute("PRAGMA table_info(binance_operator_actions)").fetchall()
            }
            if "target" not in columns:
                self._conn.execute(
                    "ALTER TABLE binance_operator_actions ADD COLUMN target TEXT NOT NULL DEFAULT ''"
                )
            self._conn.commit()

    def _validate_profile(self) -> None:
        host = self._profile_value("host", "127.0.0.1")
        if str(host) != "127.0.0.1":
            raise BinanceOperatorError("LOOPBACK_HOST_REQUIRED")

    def _profile_value(self, key: str, default: Any = None) -> Any:
        profile = self.profile
        if isinstance(profile, Mapping):
            return profile.get(key, default)
        return getattr(profile, key, default) if profile is not None else default

    def _now(self) -> datetime:
        try:
            return _utc(self.clock())
        except Exception:
            return datetime.now(UTC)

    def _environment(self) -> str:
        profile_value = self._profile_value("environment", None)
        execution_value = getattr(self.execution, "environment", None)
        value = profile_value if profile_value is not None else (execution_value or PAPER)
        profile_text = str(getattr(profile_value, "value", profile_value) or "").upper()
        execution_text = str(getattr(execution_value, "value", execution_value) or "").upper()
        if "LIVE" in profile_text or "LIVE" in execution_text:
            return BINANCE_SPOT_LIVE
        return str(getattr(value, "value", value) or PAPER).upper()

    def _is_live(self) -> bool:
        return self._environment() in {"LIVE", BINANCE_SPOT_LIVE} or "LIVE" in self._environment()

    def _profile_projection(self) -> dict[str, Any]:
        profile = self.profile
        if profile is not None and hasattr(profile, "projection") and callable(profile.projection):
            try:
                projected = _as_mapping(profile.projection())
            except Exception:
                projected = {}
        elif isinstance(profile, Mapping):
            projected = _as_mapping(profile)
        else:
            projected = {}
        projected.setdefault("feature", "binance_spot")
        projected.setdefault("feature_instance", "binance-dev")
        projected.setdefault("runtime", "binance-dev")
        projected.setdefault("runtime_identity", "binance-dev")
        projected.setdefault("environment", self._environment())
        projected.setdefault("host", "127.0.0.1")
        projected.setdefault("port", 8081)
        projected.setdefault("transport", "binance_spot")
        raw_db = projected.get("db_path") or getattr(self.store, "path", None)
        if raw_db is not None:
            projected["db_path"] = str(raw_db)
        root = projected.get("worktree_root") or self._profile_value("worktree_root")
        if root is not None:
            projected["worktree_root"] = str(root)
        projected.setdefault("revision", self._profile_value("revision", os.environ.get("AXIOM_REVISION", "unknown")))
        return _as_mapping(projected)

    def _credential_projection(self, control: Mapping[str, Any]) -> dict[str, Any]:
        configured = False
        reference_hash: str | None = None
        try:
            value = getattr(self.execution, "_credential_value", None)
            credentials = value() if callable(value) else getattr(self.execution, "credentials", None)
            if isinstance(credentials, Mapping):
                configured = bool(credentials.get("api_key") and credentials.get("api_secret"))
            elif credentials is not None:
                configured = bool(getattr(credentials, "api_key", None) and getattr(credentials, "api_secret", None))
        except Exception:
            configured = False
        raw_hash = control.get("credential_hash")
        if raw_hash:
            # The execution service stores the hash of this exact marker when
            # credentials are absent.  It is safe to compare, never expose keys.
            absent_hash = hashlib.sha256(b'{"configured":false}').hexdigest()
            configured = configured or str(raw_hash) != absent_hash
        ref = getattr(self.execution, "credential_ref", None)
        if ref is not None:
            try:
                reference_hash = str(ref.stable_id()) if hasattr(ref, "stable_id") else hashlib.sha256(str(getattr(ref, "identity", ref)).encode()).hexdigest()
            except Exception:
                reference_hash = None
        return {
            "configured": bool(configured),
            "reference_hash": reference_hash,
        }

    def _execution_control(self) -> dict[str, Any]:
        method = getattr(self.execution, "control", None) or getattr(self.execution, "status", None)
        try:
            raw = method() if callable(method) else {}
            projected = _jsonable(raw, redact=False)
            return dict(projected) if isinstance(projected, Mapping) else {}
        except Exception:
            return {"state": DISABLED, "authorized": False, "error": "CONTROL_UNAVAILABLE"}

    def _execution_schema(self) -> dict[str, Any]:
        method = getattr(self.execution, "schema", None)
        try:
            return _as_mapping(method() if callable(method) else {})
        except Exception:
            return {}

    def _heartbeat(self) -> dict[str, Any]:
        method = getattr(self.execution, "heartbeat", None)
        try:
            return _as_mapping(method() if callable(method) else {})
        except Exception:
            return {}

    def _connectivity_projection(self, heartbeat: Mapping[str, Any]) -> dict[str, Any]:
        connectivity = heartbeat.get("connectivity")
        if not isinstance(connectivity, Mapping):
            connectivity = {}
        connectivity = dict(_jsonable(connectivity))
        checked = connectivity.get("checked_at") or connectivity.get("heartbeat_at")
        timestamp = _timestamp_projection(checked) if checked else None
        stale = True
        if checked:
            stale = (self._now() - _utc(checked)).total_seconds() > self.stale_after_seconds
        raw_status = str(connectivity.get("status") or "UNKNOWN").upper()
        ready = raw_status in {"OK", "READY", "SUCCESS"} and not stale
        return {
            **connectivity,
            "status": raw_status,
            "checked_at": timestamp,
            "heartbeat_at": _timestamp_projection(connectivity["heartbeat_at"]) if connectivity.get("heartbeat_at") else None,
            "stale": bool(stale),
            "readiness": "READY" if ready else ("STALE" if raw_status in {"OK", "READY", "SUCCESS"} and stale else "NOT_READY"),
            "ready": ready,
        }

    def _worker_projection(self) -> dict[str, Any]:
        if self.worker is None:
            return {"status": "NOT_CONFIGURED", "restart": {"visible": False}}
        method = getattr(self.worker, "status", None)
        try:
            state = _as_mapping(method() if callable(method) else {})
        except Exception as exc:
            state = {"status": "ERROR", "error": type(exc).__name__}
        state["restart"] = {"visible": callable(getattr(self.worker, "stop", None)) or callable(getattr(self.worker, "restart", None))}
        return state

    def _qualification_projection(self) -> dict[str, Any]:
        service = self.qualification
        if service is None:
            return {
                "eligibility": [],
                "rankable": [],
                "current_vs_stale": "NONE",
                "selection": None,
                "family": None,
                "reason": "QUALIFICATION_NOT_CONFIGURED",
            }
        try:
            status_value = getattr(service, "status", None)
            status = _as_mapping(status_value() if callable(status_value) else status_value)
        except Exception as exc:
            status = {"status": "ERROR", "reason": type(exc).__name__.upper()}
        try:
            current_method = getattr(service, "current_selection", None)
            current = current_method() if callable(current_method) else status.get("current_selection")
        except Exception:
            current = None
        try:
            rank_method = getattr(service, "actionable_rankings", None)
            rankable = rank_method(5) if callable(rank_method) else status.get("actionable_rankings", [])
        except Exception:
            rankable = []
        if not isinstance(rankable, Sequence) or isinstance(rankable, (str, bytes)):
            rankable = []
        current_mapping = _as_mapping(current) if current is not None else None
        status_name = str(status.get("selection_status") or status.get("status") or ("CURRENT" if current_mapping else "NONE")).upper()
        selected = current_mapping or status.get("selected") or status.get("winner")
        selected = _as_mapping(selected) if isinstance(selected, Mapping) else None
        family = None
        if selected:
            family = selected.get("family") or selected.get("strategy_family") or selected.get("model_family")
        eligibility = status.get("eligibility") or status.get("qualifications") or status.get("qualified_count", 0)
        return {
            "eligibility": _jsonable(eligibility),
            "eligible_count": status.get("qualified_count"),
            "rankable": [_summary_record(_as_mapping(item)) for item in list(rankable)[:5]],
            "rankable_count": status.get("ranking_count", len(rankable)),
            "current_vs_stale": status_name,
            "selection_status": status_name,
            "selection_valid": bool(status.get("selection_valid", status_name == "CURRENT")),
            "selection": _summary_record(selected) if selected else None,
            "family": family,
            "reason": str(status.get("reason") or ("" if selected else "NO_CURRENT_SELECTION")),
            "reasons": _jsonable(status.get("reasons", [])),
        }

    def _records(self, name: str) -> list[dict[str, Any]]:
        method = getattr(self.execution, name, None)
        try:
            value = method() if callable(method) else []
        except Exception:
            value = []
        if isinstance(value, Mapping):
            for key in ("items", "records", name):
                candidate = value.get(key)
                if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
                    value = candidate
                    break
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            return []
        # A dashboard projection must remain bounded even when a fake or
        # third-party service returns an unbounded history.
        return [
            _as_mapping(item)
            for item in list(value)[:MAX_PAGE_SIZE]
            if isinstance(item, Mapping) or hasattr(item, "to_dict") or hasattr(item, "as_dict")
        ]

    def _sql_records(self, table: str, *, limit: int = 100) -> list[dict[str, Any]]:
        try:
            rows = self._conn.execute(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT ?", (int(limit),)).fetchall()
            return [dict(row) for row in rows]
        except Exception:
            return []

    def _latest_signal(self, worker_state: Mapping[str, Any]) -> dict[str, Any] | None:
        rows = self._sql_records("binance_execution_signals", limit=1)
        if rows:
            row = rows[0]
            raw = row.get("signal_json")
            if isinstance(raw, str):
                try:
                    decoded = json.loads(raw)
                    if isinstance(decoded, Mapping):
                        row = {**row, **decoded}
                except (TypeError, ValueError):
                    pass
            return _summary_record(row)
        for key in ("latest_signal", "signal"):
            value = worker_state.get(key)
            if isinstance(value, Mapping):
                return _summary_record(value)
        return None

    def _budget_projection(self, positions: Sequence[Mapping[str, Any]], orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        control = self._execution_control()
        envelope = getattr(self.execution, "risk_envelope", None) or DEFAULT_BINANCE_RISK_ENVELOPE
        def get(name: str, fallback: Any) -> Any:
            return getattr(envelope, name, fallback) if envelope is not None else fallback
        exposure = sum((_decimal(row.get("cost_basis", row.get("notional", 0))) for row in positions), Decimal("0"))
        reservations = sum((_decimal(row.get("notional", 0)) + _decimal(row.get("fee_reserve", 0)) for row in orders if str(row.get("state", "")).upper() not in {"REJECTED", "FILLED", "CANCELED", "CANCELLED", "EXPIRED"}), Decimal("0"))
        realized = sum((_decimal(row.get("realized_pnl")) for row in positions), Decimal("0"))
        unrealized = sum((_decimal(row.get("unrealized_pnl")) for row in positions), Decimal("0"))
        fees = sum((_decimal(row.get("fees_quote", row.get("commission", 0))) for row in positions), Decimal("0"))
        active_positions = sum(1 for row in positions if _decimal(row.get("quantity")) > 0)
        submissions = sum(1 for row in orders if row.get("submitted_at") or str(row.get("state", "")).upper() in {"SUBMITTING", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "UNKNOWN"})
        limits = {
            "entry_notional": _decimal_text(get("entry_notional", 10)),
            "max_aggregate_exposure": _decimal_text(get("max_aggregate_exposure", 30)),
            "max_reserved_exposure": _decimal_text(get("max_reserved_exposure", 30)),
            "realized_loss_entry_stop": _decimal_text(get("realized_loss_entry_stop", 5)),
            "equity_loss_entry_stop": _decimal_text(get("equity_loss_entry_stop", 5)),
            "max_positions": int(get("max_positions", 5)),
            "max_submissions_per_day": int(get("max_submissions_per_day", 20)),
        }
        remaining = {
            "entry_notional": _decimal_text(max(Decimal("0"), _decimal(limits["entry_notional"]) - reservations)),
            "max_aggregate_exposure": _decimal_text(max(Decimal("0"), _decimal(limits["max_aggregate_exposure"]) - exposure)),
            "max_reserved_exposure": _decimal_text(max(Decimal("0"), _decimal(limits["max_reserved_exposure"]) - reservations)),
            "realized_loss_entry_stop": _decimal_text(max(Decimal("0"), _decimal(limits["realized_loss_entry_stop"]) + min(Decimal("0"), realized))),
            "equity_loss_entry_stop": _decimal_text(max(Decimal("0"), _decimal(limits["equity_loss_entry_stop"]) + min(Decimal("0"), realized + unrealized))),
            "max_positions": max(0, int(limits["max_positions"]) - active_positions),
            "max_submissions_per_day": max(0, int(limits["max_submissions_per_day"]) - submissions),
        }
        return {
            "limits": limits,
            "remaining": remaining,
            "net_pnl": _decimal_text(realized + unrealized - fees),
            "realized_pnl": _decimal_text(realized),
            "unrealized_pnl": _decimal_text(unrealized),
            "fees": _decimal_text(fees),
            "exposure": _decimal_text(exposure),
            "reservations": _decimal_text(reservations),
            "control_state": control.get("state", DISABLED),
        }

    def _paged(self, rows: Sequence[Mapping[str, Any]], page: int, page_size: int) -> dict[str, Any]:
        total = len(rows)
        start = (page - 1) * page_size
        selected = list(rows[start : start + page_size])
        details = [_as_mapping(item) for item in selected]
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_more": start + page_size < total,
            "items": [_summary_record(item) for item in selected],
            "detail_records": [_jsonable(item) for item in details],
        }

    def status(self) -> dict[str, Any]:
        """Return a read-only, bounded dashboard projection."""
        with self._lock:
            control = self._execution_control()
            heartbeat = self._heartbeat()
            connectivity = self._connectivity_projection(heartbeat)
            worker_state = self._worker_projection()
            positions = self._records("positions")
            orders = self._records("orders")
            fills = self._records("fills")
            if not positions:
                positions = self._sql_records("binance_execution_positions")
            if not orders:
                orders = self._sql_records("binance_execution_order_intents")
            if not fills:
                fills = self._sql_records("binance_execution_fills")
            qualification = self._qualification_projection()
            latest_signal = self._latest_signal(worker_state)
            unknown = [row for row in orders if str(row.get("state", "")).upper() == UNKNOWN]
            recent_actions = self.list_actions(limit=5)
            risk = self._budget_projection(positions, orders)
            now_projection = _timestamp_projection(self._now())
            return _as_mapping(
                {
                    "timestamp": now_projection,
                    "profile": self._profile_projection(),
                    "development_profile": self._profile_projection(),
                    "transport": {"binance": "ENABLED", "polymarket": "DISABLED", "hermes": "DISABLED"},
                    "polymarket_transport": "DISABLED",
                    "credentials": self._credential_projection(control),
                    "connectivity": connectivity,
                    "readiness": {
                        "status": connectivity.get("readiness", "NOT_READY"),
                        "ready": bool(connectivity.get("ready")),
                        "stale": bool(connectivity.get("stale", True)),
                        "checked_at": connectivity.get("checked_at"),
                    },
                    "qualification": qualification,
                    "eligibility": qualification.get("eligibility"),
                    "rankable": qualification.get("rankable"),
                    "current_vs_stale": qualification.get("current_vs_stale"),
                    "selection": qualification.get("selection"),
                    "family": qualification.get("family"),
                    "reason": qualification.get("reason"),
                    "latest_signal": latest_signal,
                    "no_trade_reason": (
                        (latest_signal.get("no_trade_reason") or latest_signal.get("reason"))
                        if isinstance(latest_signal, Mapping) and str(latest_signal.get("status", "")).upper() in {"NO_TRADE", "BLOCKED", "REJECTED"}
                        else (None if latest_signal else qualification.get("reason") or "NO_SIGNAL")
                    ),
                    "positions": self._paged(positions, 1, 5),
                    "risk": risk,
                    "budgets": risk,
                    "reconciliation": heartbeat.get("reconciliation"),
                    "heartbeat": heartbeat,
                    "control": control,
                    "pause": control.get("state") == PAUSED,
                    "disarmed": control.get("state") == DISARMED,
                    "killed": control.get("state") == KILLED or bool(control.get("kill_requested")),
                    "worker": worker_state,
                    "restart": {**worker_state.get("restart", {"visible": False}), "last_action": (recent_actions[0] if recent_actions else None)},
                    "actions": recent_actions,
                    "live_execution": False if self._is_live() else self._environment() != PAPER,
                }
            )

    def snapshot(self, page_size: int = DEFAULT_PAGE_SIZE, page: int = 1, **kwargs: Any) -> dict[str, Any]:
        """Return status plus bounded pages and complete IDs in detail records."""
        try:
            size = int(page_size)
            number = int(page)
        except (TypeError, ValueError) as exc:
            raise ValueError("page and page_size must be integers") from exc
        if isinstance(page_size, bool) or isinstance(page, bool) or size < 1 or number < 1:
            raise ValueError("page and page_size must be positive")
        size = min(size, MAX_PAGE_SIZE)
        status = self.status()
        positions = self._records("positions") or self._sql_records("binance_execution_positions")
        orders = self._records("orders") or self._sql_records("binance_execution_order_intents")
        fills = self._records("fills") or self._sql_records("binance_execution_fills")
        unknown = [row for row in orders if str(row.get("state", "")).upper() == UNKNOWN]
        return {
            "status": status,
            "page": number,
            "page_size": size,
            "positions": self._paged(positions, number, size),
            "orders": self._paged(orders, number, size),
            "fills": self._paged(fills, number, size),
            "unknown": self._paged(unknown, number, size),
            "action": kwargs.get("action"),
        }

    def _validate_request(self, action: str, payload: Mapping[str, Any]) -> None:
        if action not in ACTION_NAMES:
            raise BinanceOperatorError("ACTION_NOT_ALLOWED")
        if _contains_forbidden_transport(action) or _contains_forbidden_transport(payload):
            raise BinanceOperatorError("TRANSPORT_NOT_ALLOWED")
        if _contains_secret_shape(payload):
            raise BinanceOperatorError("SECRET_PAYLOAD_REJECTED")
        expected_host = str(self._profile_value("host", "127.0.0.1"))

        def inspect_mapping(value: Any) -> None:
            if not isinstance(value, Mapping):
                if isinstance(value, (list, tuple, set, frozenset)):
                    for child in value:
                        inspect_mapping(child)
                return
            for key, child in value.items():
                name = str(key).lower()
                if name in _HOST_KEYS:
                    if name == "host" and str(child) == expected_host:
                        pass
                    elif name == "host":
                        raise BinanceOperatorError("HOST_NOT_ALLOWED")
                    else:
                        raise BinanceOperatorError("ARBITRARY_ENDPOINT_REJECTED")
                inspect_mapping(child)

        inspect_mapping(payload)
    def _envelope_projection(self) -> dict[str, Any]:
        envelope = getattr(self.execution, "risk_envelope", None) or DEFAULT_BINANCE_RISK_ENVELOPE
        if envelope is None:
            return {}
        if hasattr(envelope, "as_dict") and callable(envelope.as_dict):
            return _as_mapping(envelope.as_dict())
        if hasattr(envelope, "to_dict") and callable(envelope.to_dict):
            return _as_mapping(envelope.to_dict())
        return _as_mapping(envelope)

    def _order_validation(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        environment = self._environment()
        symbol = str(payload.get("symbol") or "").replace("/", "").replace("-", "").upper()
        side = str(payload.get("side") or "BUY").upper()
        price = _decimal(payload.get("price"))
        quantity = _decimal(payload.get("quantity"))
        local: dict[str, Any] = {
            "venue": "BINANCE_SPOT",
            "environment": environment,
            "symbol": symbol,
            "side": side,
            "price": _decimal_text(price),
            "quantity": _decimal_text(quantity),
            "notional": _decimal_text(price * quantity),
            "valid": bool(symbol and side in {"BUY", "SELL"} and price > 0 and quantity > 0),
            "reasons": [],
            "placed": False,
        }
        if not symbol:
            local["reasons"].append("SYMBOL_REQUIRED")
        if price <= 0:
            local["reasons"].append("PRICE_MUST_BE_POSITIVE")
        if quantity <= 0:
            local["reasons"].append("QUANTITY_MUST_BE_POSITIVE")
        envelope = getattr(self.execution, "risk_envelope", None) or DEFAULT_BINANCE_RISK_ENVELOPE
        entry_limit = _decimal(getattr(envelope, "entry_notional", 10))
        if side == "BUY" and price * quantity > entry_limit:
            local["valid"] = False
            local["reasons"].append("ENTRY_NOTIONAL_LIMIT")
        rules = payload.get("rules")
        if rules is not None and size_limit_order is not None:
            try:
                sized = size_limit_order(
                    SymbolRules.from_exchange_info(rules) if not isinstance(rules, SymbolRules) else rules,
                    side,
                    price,
                    quantity,
                    envelope=getattr(self.execution, "risk_envelope", None) or DEFAULT_BINANCE_RISK_ENVELOPE,
                    fee_rate=payload.get("fee_rate", 0),
                    available_inventory=payload.get("available_inventory"),
                )
                local.update(
                    {
                        "valid": bool(sized.valid),
                        "price": _decimal_text(sized.price),
                        "quantity": _decimal_text(sized.quantity),
                        "notional": _decimal_text(sized.notional),
                        "fee_reserve": _decimal_text(sized.fee_reserve),
                        "reasons": list(sized.reasons),
                    }
                )
            except Exception as exc:
                local["valid"] = False
                local["reasons"].append(type(exc).__name__.upper())
        # A test endpoint is optional and only permitted for explicit non-PAPER
        # environments.  No method named place/submit/order is ever considered.
        venue = getattr(self.execution, "venue", None)
        test_method = None
        if environment != PAPER and venue is not None:
            for name in ("order_test", "test_order", "order_validation_test"):
                candidate = getattr(venue, name, None)
                if callable(candidate):
                    test_method = candidate
                    break
        if test_method is not None:
            result = _call(test_method, {"symbol": symbol, "side": side, "price": local["price"], "quantity": local["quantity"]})
            local["venue_test"] = _as_mapping(result)
            local["venue_test_called"] = True
        else:
            local["venue_test_called"] = False
        return local

    def _restart(self) -> dict[str, Any]:
        if self.worker is None:
            return {"status": "NOT_CONFIGURED", "restart_visible": False}
        restart = getattr(self.worker, "restart", None)
        if callable(restart):
            return {"status": "REQUESTED", "restart_visible": True, "result": _as_mapping(restart())}
        stop = getattr(self.worker, "stop", None)
        if callable(stop):
            stopped = _as_mapping(stop()) if callable(stop) else {}
            return {"status": "STOPPED", "restart_visible": True, "stopped": stopped, "worker": self._worker_projection()}
        return {"status": "UNSUPPORTED", "restart_visible": False, "worker": self._worker_projection()}

    def _execute(self, action: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_request(action, payload)
        if action == "CONNECTIVITY_CHECK":
            method = getattr(self.execution, "check_connectivity", None) or getattr(self.execution, "connectivity", None)
            if not callable(method):
                raise BinanceOperatorError("CONNECTIVITY_UNAVAILABLE")
            # Payload is deliberately ignored: connectivity has no write or
            # endpoint override and therefore cannot become browser-controlled.
            return {"connectivity": _as_mapping(method())}
        if action == "ORDER_VALIDATION_TEST":
            return {"validation": self._order_validation(payload)}
        if action == "ENABLE":
            envelope = self._envelope_projection()
            if self._is_live():
                raise BinanceOperatorError("DEVELOPMENT_LIVE_REFUSED")
            confirmation = payload.get("confirmation", payload.get("confirm"))
            if confirmation != EXACT_ENABLE_PHRASE:
                raise BinanceOperatorError("EXACT_CONFIRMATION_REQUIRED")
            method = getattr(self.execution, "enable_auto_canary", None) or getattr(self.execution, "enable", None)
            if not callable(method):
                raise BinanceOperatorError("ENABLE_UNAVAILABLE")
            return {"envelope": envelope, "control": _as_mapping(_call(method, {"confirmation": EXACT_ENABLE_PHRASE}))}
        if action == "PAUSE":
            method = getattr(self.execution, "pause", None)
            if not callable(method):
                raise BinanceOperatorError("PAUSE_UNAVAILABLE")
            return {"control": _as_mapping(_call(method, {"reason": str(payload.get("reason") or "operator pause")})), "entries_paused": True, "exits_allowed": True}
        if action == "RESUME":
            if self._is_live():
                raise BinanceOperatorError("DEVELOPMENT_LIVE_REFUSED")
            confirmation = payload.get("confirmation", payload.get("confirm"))
            if confirmation != EXACT_ENABLE_PHRASE:
                raise BinanceOperatorError("EXACT_CONFIRMATION_REQUIRED")
            method = getattr(self.execution, "resume", None) or getattr(self.execution, "enable_auto_canary", None) or getattr(self.execution, "enable", None)
            if not callable(method):
                raise BinanceOperatorError("RESUME_UNAVAILABLE")
            return {"control": _as_mapping(_call(method, {"confirmation": EXACT_ENABLE_PHRASE})), "entries_paused": False}
        if action in {"DISARM", "KILL"}:
            method = getattr(self.execution, action.lower(), None)
            if not callable(method):
                raise BinanceOperatorError(action + "_UNAVAILABLE")
            before = self._records("positions")
            result = _as_mapping(_call(method, {"reason": str(payload.get("reason") or f"operator {action.lower()}")}))
            remaining = [row for row in before if _decimal(row.get("quantity")) > 0]
            return {"control": result, "remaining_positions": len(remaining), "position_warning": bool(remaining), "reconciliation_continues": True}
        if action == "RESTART":
            return {"restart": self._restart()}
        raise BinanceOperatorError("ACTION_NOT_ALLOWED")

    def _persist_action(
        self,
        action: str,
        payload: Mapping[str, Any],
        *,
        attempted: datetime,
        completed: datetime,
        success: bool,
        reason: str,
        result: Mapping[str, Any],
    ) -> str:
        action_id = uuid.uuid4().hex
        attempted_projection = _timestamp_projection(attempted)
        completed_projection = _timestamp_projection(completed)
        encoded_payload = json.dumps(_jsonable(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        encoded_result = json.dumps(_jsonable(result), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO binance_operator_actions(action_id,action,target,attempted_at,completed_at,created_at,timestamp,timestamp_utc,timestamp_pht,success,reason,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        action_id,
                        action,
                        str(payload.get("target") or "")[:256],
                        attempted_projection["utc"],
                        completed_projection["utc"],
                        completed_projection["utc"],
                        completed_projection["utc"],
                        completed_projection["utc"],
                        completed_projection["pht"],
                        int(bool(success)),
                        str(reason or "")[:256],
                        encoded_payload,
                        encoded_result,
                    ),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return action_id

    def action(self, action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Attempt one allowlisted action and durably audit success or failure."""
        action_value = str(action or "").strip().upper()
        body = dict(payload or {}) if isinstance(payload or {}, Mapping) else {}
        attempted = self._now()
        success = False
        reason = ""
        result: dict[str, Any] = {}
        try:
            result = _as_mapping(self._execute(action_value, body))
            success = True
        except BinanceOperatorError as exc:
            reason = exc.reason
        except PermissionError as exc:
            reason = str(exc).upper().replace(" ", "_")[:256] or "PERMISSION_DENIED"
        except Exception as exc:
            reason = type(exc).__name__.upper()
        completed = self._now()
        timestamp = _timestamp_projection(completed)
        response: dict[str, Any] = {
            "ok": success,
            "action": action_value,
            "attempted_at": _timestamp_projection(attempted),
            "completed_at": _timestamp_projection(completed),
            "timestamp": timestamp,
            "timestamp_utc": timestamp["utc"],
            "timestamp_pht": timestamp["pht"],
            "paper_default": self._environment() == PAPER,
            "live_execution": False,
        }
        if success:
            response["result"] = result
        else:
            response["reason"] = reason or "ACTION_FAILED"
            if action_value == "ENABLE":
                response["envelope"] = self._envelope_projection()
        try:
            action_id = self._persist_action(action_value or "INVALID", body, attempted=attempted, completed=completed, success=success, reason=reason, result=response)
            response["action_id"] = action_id
        except Exception as exc:
            response["ok"] = False
            response["reason"] = "ACTION_AUDIT_FAILED"
            response["audit_error"] = type(exc).__name__
        return response

    execute = action

    def list_actions(self, *, limit: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
        bounded = max(0, min(int(limit), MAX_PAGE_SIZE))
        try:
            rows = self._conn.execute("SELECT * FROM binance_operator_actions ORDER BY timestamp_utc DESC,action_id DESC LIMIT ?", (bounded,)).fetchall()
        except Exception:
            return []
        return [_as_mapping(dict(row)) for row in rows]


__all__ = [
    "ACTION_NAMES",
    "EXACT_ENABLE_PHRASE",
    "BinanceCanaryControlPlane",
    "BinanceOperatorError",
]
