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
import inspect
import json
import os
import re
import sqlite3
import threading
import time
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
        ENABLE_TESTNET_CONFIRMATION,
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
    ENABLE_TESTNET_CONFIRMATION = "ENABLE BINANCE TESTNET AUTO CANARY"
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
TESTNET_PROBE_CONFIRMATION = "RUN BINANCE TESTNET EXECUTION PROBE"
EXACT_PROBE_PHRASE = TESTNET_PROBE_CONFIRMATION
EXACT_TESTNET_PROBE_PHRASE = TESTNET_PROBE_CONFIRMATION
EXACT_EXECUTION_PROBE_PHRASE = TESTNET_PROBE_CONFIRMATION
TESTNET_EXECUTION_PROBE_CONFIRMATION = TESTNET_PROBE_CONFIRMATION
EXACT_TESTNET_ENABLE_PHRASE = ENABLE_TESTNET_CONFIRMATION
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 25
DEFAULT_STALE_SECONDS = 300.0
TESTNET_ACTION_NAMES = frozenset(
    {
        "CONNECTIVITY_CHECK",
        "ORDER_VALIDATION_TEST",
        "EXECUTION_PROBE",
        "RECONCILE_PROBE",
        "ENABLE",
        "PAUSE",
        "RESUME",
        "DISARM",
        "KILL",
    }
)
_SECRET_WORDS = re.compile(
    r"(?:secret|password|passwd|token|api[_-]?key|apikey|private[_-]?key|private|mnemonic|passphrase|authorization|bearer|credential)",
    re.I,
)
_CREDENTIAL_HASH_RE = re.compile(r"\A[0-9a-fA-F]{64}\Z")
_ABSENT_CREDENTIAL_HASH = hashlib.sha256(b'{"configured":false}').hexdigest()
_REDACTED_MARKER = "<redacted>"
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
    """Convert arbitrary service values to bounded, secret-free JSON.

    A secret-shaped key owns the entire value below it.  Redacting only
    scalar leaves is unsafe because nested mappings/lists can still contain
    credentials.  The one intentional exception is the credential projection,
    which is constructed explicitly by :meth:`_credential_projection` rather
    than passed through this generic serializer.
    """
    key_text = str(key) if key is not None else ""
    lowered = key_text.lower()
    secret_key = bool(key_text and _SECRET_WORDS.search(key_text))
    if secret_key:
        return "<redacted>"

    # Persisted JSON columns are still untrusted data.  Decode them before
    # projection so nested secret-shaped keys cannot survive as an opaque
    # string.  Keep the column's string shape for callers that expect SQL
    # records, while replacing unsafe leaves in the encoded value.
    if lowered.endswith("_json") and isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = None
        if decoded is not None:
            projected = _jsonable(decoded, key=key_text[:-5] or None, redact=redact)
            try:
                return json.dumps(
                    projected,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
            except (TypeError, ValueError):
                return "<redacted>"

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v, key=str(k), redact=redact) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item, redact=redact) for item in value]
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


def _validated_credential_hash(value: Any) -> str | None:
    """Accept only an opaque SHA-256 credential fingerprint."""
    if not isinstance(value, str) or value == _REDACTED_MARKER:
        return None
    if _CREDENTIAL_HASH_RE.fullmatch(value) is None:
        return None
    return value.casefold()


def _explicit_credentials_configured(value: Any) -> bool:
    """Read configuration only from explicit scalar credential fields."""
    if isinstance(value, Mapping):
        api_key = value.get("api_key")
        api_secret = value.get("api_secret")
    elif value is not None:
        api_key = getattr(value, "api_key", None)
        api_secret = getattr(value, "api_secret", None)
    else:
        return False
    return all(
        isinstance(item, str) and bool(item) and item != _REDACTED_MARKER
        for item in (api_key, api_secret)
    )


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
            configured = _explicit_credentials_configured(credentials)
        except Exception:
            configured = False
        if not configured:
            # ``_execution_control`` intentionally redacts this secret-shaped
            # field.  Only inspect a raw value as an opaque, exact SHA-256
            # fingerprint; never carry it into a projection.
            raw_hash = _validated_credential_hash(control.get("credential_hash"))
            if raw_hash is None:
                method = getattr(self.execution, "control", None) or getattr(self.execution, "status", None)
                try:
                    raw_control = method() if callable(method) else {}
                    raw_hash = _validated_credential_hash(
                        raw_control.get("credential_hash")
                        if isinstance(raw_control, Mapping)
                        else None
                    )
                except Exception:
                    raw_hash = None
            if raw_hash is not None:
                configured = raw_hash != _ABSENT_CREDENTIAL_HASH
        ref = getattr(self.execution, "credential_ref", None)
        if ref is not None:
            try:
                stable_id = getattr(ref, "stable_id", None)
                raw_reference = stable_id() if callable(stable_id) else None
                reference_hash = _validated_credential_hash(raw_reference)
            except Exception:
                reference_hash = None
            # A valid reference fingerprint can establish configured state,
            # while malformed references must not override independent,
            # explicitly configured credentials or control state.
            if reference_hash is not None:
                configured = True
        # This is the sole credential allowlist boundary.  Never merge or
        # serialize arbitrary credential mappings into a projection.
        return {
            "configured": bool(configured),
            "reference_hash": reference_hash,
        }

    def _execution_control(self) -> dict[str, Any]:
        method = getattr(self.execution, "control", None) or getattr(self.execution, "status", None)
        try:
            raw = method() if callable(method) else {}
            projected = _jsonable(raw)
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

        quote_reservations = Decimal("0")
        base_reservations: dict[str, Decimal] = {}
        for row in orders:
            reservation = row.get("risk_reservation")
            if (
                not isinstance(reservation, Mapping)
                or str(reservation.get("status", "")).upper() != "HELD"
            ):
                continue
            side = str(row.get("side") or "BUY").upper()
            if side == "SELL":
                symbol = str(row.get("symbol") or "").strip().upper()[:32]
                quantity = _decimal(reservation.get("reserved_quantity"))
                if symbol and quantity > 0:
                    base_reservations[symbol] = base_reservations.get(symbol, Decimal("0")) + quantity
                # A SELL reserves base inventory, not quote buying power.
                continue
            quote_reservations += _decimal(row.get("notional", row.get("amount", 0))) + _decimal(row.get("fee_reserve", 0))

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
            "entry_notional": _decimal_text(max(Decimal("0"), _decimal(limits["entry_notional"]) - quote_reservations)),
            "max_aggregate_exposure": _decimal_text(max(Decimal("0"), _decimal(limits["max_aggregate_exposure"]) - exposure)),
            "max_reserved_exposure": _decimal_text(max(Decimal("0"), _decimal(limits["max_reserved_exposure"]) - quote_reservations)),
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
            "reservations": _decimal_text(quote_reservations),
            "quote_reservations": _decimal_text(quote_reservations),
            "base_reservations": {
                symbol: _decimal_text(quantity)
                for symbol, quantity in sorted(base_reservations.items())[:100]
            },
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
            projection = _as_mapping(
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
            projection["credentials"] = self._credential_projection(control)
            return projection

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

class BinanceTestnetControlPlane:
    """Operator boundary for the isolated Binance Spot TESTNET gate.

    The gate owns all authenticated/network work.  This facade only dispatches
    explicit gate operations, projects already-persisted gate state, and
    optionally controls an injected strategy component.  In particular,
    validation and probes never pass through strategy signal submission.
    """

    strict_testnet = True
    action_names = TESTNET_ACTION_NAMES
    action_table = "binance_testnet_operator_actions"
    environment = "BINANCE_SPOT_TESTNET"
    source = "binance_testnet_operator"
    probe_kind = "TESTNET EXECUTION PROBE"
    max_history = 256

    def __init__(
        self,
        gate: Any,
        *,
        execution: Any | None = None,
        worker: Any | None = None,
        clock: Callable[[], Any] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        store: Any | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        if gate is None:
            raise TypeError("gate is required")
        if store is not None and connection is not None:
            raise TypeError("provide either audit store or audit connection, not both")
        self.gate = gate
        self.execution = execution
        self.worker = worker
        self.clock = clock or getattr(gate, "clock", None) or (lambda: datetime.now(UTC))
        self.monotonic_clock = monotonic_clock or time.monotonic
        self._lock = threading.RLock()
        self.audit_store = store
        # ``store`` remains an explicit audit-only alias; it is never inferred
        # from the gate, whose connection is independently owned by runtime.
        self.store = store
        self._owns_audit_connection = False
        self._closed = False
        self._autonomous_window: dict[str, Any] | None = None
        self._conn = self._resolve_connection(store=store, connection=connection)
        self._init_schema()

    def _resolve_connection(
        self,
        *,
        store: Any | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Connection:
        candidates = (
            connection,
            store if isinstance(store, sqlite3.Connection) else None,
            getattr(store, "connection", None),
            getattr(store, "_conn", None),
        )
        for candidate in candidates:
            if isinstance(candidate, sqlite3.Connection):
                candidate.row_factory = sqlite3.Row
                return candidate
        if store is not None or connection is not None:
            raise TypeError("audit store must provide a sqlite3 connection")
        # Standalone control-plane fakes still get an isolated, owned audit
        # connection rather than borrowing the gate's transaction boundary.
        owned = sqlite3.connect(":memory:", check_same_thread=False)
        owned.row_factory = sqlite3.Row
        self._owns_audit_connection = True
        return owned

    def close(self) -> None:
        """Close only an internally-owned audit connection, idempotently."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._owns_audit_connection:
                try:
                    self._conn.close()
                except sqlite3.Error:
                    pass


    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_testnet_operator_actions (
                    action_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    environment TEXT NOT NULL,
                    source TEXT NOT NULL,
                    probe_kind TEXT NOT NULL,
                    attempted_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    timestamp_pht TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    result_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_testnet_operator_actions_time
                    ON binance_testnet_operator_actions(timestamp_utc, action_id);
                """
            )
            columns = {
                str(row[1])
                for row in self._conn.execute(
                    "PRAGMA table_info(binance_testnet_operator_actions)"
                ).fetchall()
            }
            for name, definition in (
                ("environment", "TEXT NOT NULL DEFAULT 'BINANCE_SPOT_TESTNET'"),
                ("source", "TEXT NOT NULL DEFAULT 'binance_testnet_operator'"),
                ("probe_kind", "TEXT NOT NULL DEFAULT 'TESTNET EXECUTION PROBE'"),
            ):
                if name not in columns:
                    self._conn.execute(
                        f"ALTER TABLE binance_testnet_operator_actions ADD COLUMN {name} {definition}"
                    )
            self._conn.commit()

    def _now(self) -> datetime:
        try:
            return _utc(self.clock())
        except Exception:
            return datetime.now(UTC)

    @staticmethod
    def _status_value(value: Any, default: str = "BLOCKED") -> str:
        if isinstance(value, Mapping):
            value = value.get("status", default)
        text = str(getattr(value, "value", value) or default).upper()
        return text[:64]

    def _safe_call(self, method: Any, payload: Mapping[str, Any] | None = None) -> Any:
        if not callable(method):
            raise BinanceOperatorError("ACTION_UNAVAILABLE")
        body = dict(payload or {})
        if not body:
            return method()
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            signature = None
        if signature is not None and not any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        ):
            body = {key: value for key, value in body.items() if key in signature.parameters}
        try:
            return method(**body)
        except TypeError as first:
            try:
                return method(body)
            except TypeError:
                raise first

    def _gate_projection(self, name: str, fallback: Mapping[str, Any]) -> dict[str, Any]:
        method = getattr(self.gate, name, None)
        if not callable(method):
            return dict(fallback)
        try:
            return _as_mapping(method())
        except Exception as exc:
            return {
                "status": "BLOCKED",
                "reason": f"{name.upper()}_UNAVAILABLE",
                "error": type(exc).__name__,
            }

    def _profile_projection(self) -> dict[str, Any]:
        profile = getattr(self.gate, "profile", None)
        try:
            method = getattr(profile, "projection", None)
            value = method() if callable(method) else profile
        except Exception:
            value = {}
        projected = _as_mapping(value)
        projected["environment"] = self.environment
        projected.setdefault("host", "127.0.0.1")
        projected.setdefault("port", 8082)
        projected.setdefault("runtime_identity", "binance-testnet")
        projected.setdefault("feature_instance", "binance-testnet")
        projected.setdefault("identity", "binance-testnet")
        projected.setdefault("db_path", "runtime-data/binance-testnet.sqlite")
        projected.setdefault("transport", "binance_spot_testnet")
        return projected

    def _credential_projection(self) -> dict[str, Any]:
        method = getattr(self.gate, "_credential_projection", None)
        raw: Mapping[str, Any] = {}
        if callable(method):
            try:
                candidate = method()
                raw = candidate if isinstance(candidate, Mapping) else {}
            except Exception:
                raw = {}
        configured = bool(raw.get("configured"))
        if not configured:
            credentials = getattr(self.gate, "credentials", None)
            configured = _explicit_credentials_configured(credentials)
        return {
            "configured": configured,
            "api_key_configured": configured,
            "api_secret_configured": configured,
            "secret_values_exposed": False,
        }

    def _strategy_status(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for label, component in (("execution", self.execution), ("worker", self.worker)):
            # An execution coordinator is not autonomous state unless the
            # worker explicitly supports persisted strategy hydration and can
            # execute a cycle; keep execution-only runtimes blocked.
            if component is None or (label == "execution" and self._strategy_target() is None):
                continue
            method = getattr(component, "status", None) or getattr(component, "control", None)
            if not callable(method):
                continue
            try:
                state = _as_mapping(method())
            except Exception as exc:
                state = {"status": "ERROR", "error": type(exc).__name__}
            result[label] = state
        if "execution" in result:
            merged = dict(result["execution"])
            if "worker" in result:
                merged["worker"] = result["worker"]
            return merged
        if "worker" in result:
            return dict(result["worker"])
        return {}

    def _strategy_target(self) -> Any | None:
        """Return the control target only for an executable persisted-strategy worker.

        A configured ``strategy`` attribute is not an execution capability:
        production workers hydrate the exact strategy/binding from the frozen
        qualification row at cycle time.  The worker therefore has to
        explicitly advertise that persisted-strategy path and expose a cycle
        entry point.  This also keeps an execution-only or arbitrary injected
        transport blocked.
        """
        worker = self.worker
        if worker is None:
            return None
        capability = getattr(worker, "supports_persisted_strategy", None)
        cycle = getattr(worker, "cycle", None)
        if not callable(capability) or not callable(cycle):
            return None
        try:
            if capability() is not True:
                return None
        except Exception:
            return None
        return self.execution if self.execution is not None else worker

    def _strategy_method(self, name: str) -> Any:
        for component in (self.execution, self.worker):
            method = getattr(component, name, None) if component is not None else None
            if callable(method):
                return method
        return None

    def _strategy_evidence(self, strategy: Mapping[str, Any]) -> dict[str, Any]:
        def first_mapping(*keys: str) -> dict[str, Any] | None:
            for key in keys:
                value = strategy.get(key)
                if isinstance(value, Mapping):
                    return _summary_record(value)
            return None

        selected = first_mapping("selected_candidate", "selection", "candidate")
        signal = first_mapping("current_signal", "latest_signal", "signal")
        reason = strategy.get("no_trade_reason") or strategy.get("reason")
        return {
            "selected_candidate": selected,
            "current_signal": signal,
            "no_trade_reason": str(reason)[:256] if reason is not None else None,
        }

    def _autonomous_projection(
        self,
        strategy: Mapping[str, Any],
        connectivity: Mapping[str, Any],
        validation: Mapping[str, Any],
        gate_projection: Mapping[str, Any],
        credentials: Mapping[str, Any],
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        target_present = self._strategy_target() is not None
        raw_state = str(strategy.get("state") or strategy.get("control_state") or strategy.get("status") or "").upper()
        enabled = bool(strategy.get("enabled")) or raw_state in {"ARMED", "ENABLED", "RUNNING"}

        # Autonomous control must report the earliest actionable safety gate.
        # In particular, a missing credential configuration is actionable even
        # when no optional strategy component was injected.
        if not bool(credentials.get("configured")):
            state, blocked, enabled = "BLOCKED", "CREDENTIALS_NOT_CONFIGURED", False
        elif self._status_value(connectivity) != "PASS":
            state, blocked, enabled = "BLOCKED", "CONNECTIVITY_NOT_PASS", False
        elif self._status_value(validation) != "PASS":
            state, blocked, enabled = "BLOCKED", "VALIDATION_NOT_PASS", False
        elif not target_present:
            state, blocked, enabled = "BLOCKED", "STRATEGY_NOT_CONFIGURED", False
        else:
            state = raw_state or ("ENABLED" if enabled else "DISABLED")
            blocked = None
            if not enabled:
                blocked = evidence["no_trade_reason"] or strategy.get("blocked_reason")
        risk = strategy.get("risk_envelope")
        if not isinstance(risk, Mapping):
            risk = gate_projection.get("risk_envelope")
        if not isinstance(risk, Mapping):
            risk = {}
        bounded = strategy.get("bounded_window")
        if not isinstance(bounded, Mapping):
            bounded = self._autonomous_window
        return {
            "enabled": enabled,
            "state": state,
            "blocked_reason": str(blocked)[:256] if blocked else None,
            **evidence,
            "risk_envelope": _as_mapping(risk),
            "bounded_window": _as_mapping(bounded) if isinstance(bounded, Mapping) else bounded,
        }

    def status(self) -> dict[str, Any]:
        """Return only persisted/projection state; never initiate gate I/O."""
        with self._lock:
            connectivity = self._gate_projection(
                "connectivity_status",
                {"status": "BLOCKED", "reason": "NOT_CHECKED"},
            )
            validation = self._gate_projection(
                "validation_status",
                {"status": "BLOCKED", "reason": "NOT_CHECKED"},
            )
            probe = self._gate_projection(
                "probe_status",
                {"status": "BLOCKED", "reason": "NOT_STARTED"},
            )
            credentials = self._credential_projection()
            strategy = self._strategy_status()
            evidence = self._strategy_evidence(strategy)
            # The status path intentionally reads no gate dashboard helper:
            # projection helpers are duck-typed and may be implemented by an
            # embedding runtime.  Only the three persisted, read-only gate
            # status methods above are allowed here.
            gate_projection: dict[str, Any] = {}
            envelope = getattr(self.gate, "risk_envelope", None) or DEFAULT_BINANCE_RISK_ENVELOPE
            if envelope is not None:
                try:
                    gate_projection["risk_envelope"] = (
                        _as_mapping(envelope.as_dict())
                        if callable(getattr(envelope, "as_dict", None))
                        else _as_mapping(envelope)
                    )
                except Exception:
                    pass
            profile = self._profile_projection()
            result = {
                "title": "BINANCE SPOT TESTNET",
                "strict_testnet": True,
                "environment": self.environment,
                "source": self.source,
                "probe_kind": self.probe_kind,
                "timestamp": _timestamp_projection(self._now()),
                "profile": profile,
                "credentials": credentials,
                "connectivity": connectivity,
                "validation": validation,
                "probe": probe,
                "isolation": {
                    "schema_namespace": "binance_testnet_*",
                    "operator_action_table": self.action_table,
                    "strategy_ledgers_touched": False,
                    "probe_separate_from_strategy": True,
                    "polymarket_transport": "DISABLED",
                },
                "strategy_evidence": evidence,
                "autonomous": self._autonomous_projection(
                    strategy, connectivity, validation, gate_projection, credentials, evidence
                ),
                "actions": self.list_actions(limit=5),
            }
            # Preserve safe gate-level fields (notably risk permissions) without
            # allowing gate probe data to become strategy evidence.
            for key in ("risk_envelope", "permissions"):
                if key in gate_projection:
                    result[key] = _jsonable(gate_projection[key])
            return _as_mapping(result)


    def snapshot(self, **kwargs: Any) -> dict[str, Any]:
        size = kwargs.get("page_size", DEFAULT_PAGE_SIZE)
        try:
            size = max(1, min(int(size), MAX_PAGE_SIZE))
        except (TypeError, ValueError):
            size = DEFAULT_PAGE_SIZE
        result = self.status()
        result["strict_testnet"] = True
        result["page_size"] = size
        result["actions"] = self.list_actions(limit=size)
        if "action" in kwargs:
            result["action"] = kwargs["action"]
        return result

    def _validate_request(self, action: str, payload: Mapping[str, Any]) -> None:
        if action not in self.action_names:
            raise BinanceOperatorError("ACTION_NOT_ALLOWED")
        if _contains_secret_shape(payload):
            raise BinanceOperatorError("SECRET_PAYLOAD_REJECTED")
        if _contains_forbidden_transport(payload) or _contains_forbidden_transport(action):
            raise BinanceOperatorError("TRANSPORT_NOT_ALLOWED")
        encoded = json.dumps(_jsonable(payload), ensure_ascii=False).lower()
        if "/sapi" in encoded or "mainnet" in encoded or "api.binance.com" in encoded:
            raise BinanceOperatorError("TESTNET_TRANSPORT_ONLY")

    def _require_phrase(self, payload: Mapping[str, Any], phrase: str) -> None:
        confirmation = payload.get("confirmation", payload.get("confirm"))
        if confirmation != phrase:
            raise BinanceOperatorError("EXACT_CONFIRMATION_REQUIRED")

    def _deadline_checkpoint(self, deadline_monotonic: float | None) -> None:
        if deadline_monotonic is None:
            return
        try:
            expired = self.monotonic_clock() >= float(deadline_monotonic)
        except Exception:
            expired = True
        if expired:
            raise BinanceOperatorError("AUTO_DEADLINE_EXPIRED")

    def authorize_bounded_auto(
        self,
        confirmation: str,
        window_seconds: int,
        *,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        """Authorize one bounded autonomous run for the CLI runtime only."""
        with self._lock:
            # All checks happen before the first durable execution mutation.
            self._deadline_checkpoint(deadline_monotonic)
            if confirmation != ENABLE_TESTNET_CONFIRMATION:
                raise BinanceOperatorError("EXACT_CONFIRMATION_REQUIRED")
            if (
                not isinstance(window_seconds, int)
                or isinstance(window_seconds, bool)
                or not 30 <= window_seconds <= 900
            ):
                raise BinanceOperatorError("BOUNDED_WINDOW_REQUIRED")
            credentials = self._credential_projection()
            self._deadline_checkpoint(deadline_monotonic)
            if not bool(credentials.get("configured")):
                raise BinanceOperatorError("CREDENTIALS_NOT_CONFIGURED")
            if self._strategy_target() is None:
                raise BinanceOperatorError("STRATEGY_NOT_CONFIGURED")
            self._deadline_checkpoint(deadline_monotonic)
            connectivity = self._gate_projection("connectivity_status", {})
            self._deadline_checkpoint(deadline_monotonic)
            validation = self._gate_projection("validation_status", {})
            self._deadline_checkpoint(deadline_monotonic)
            if self._status_value(connectivity) != "PASS":
                raise BinanceOperatorError("CONNECTIVITY_NOT_PASS")
            if self._status_value(validation) != "PASS":
                raise BinanceOperatorError("VALIDATION_NOT_PASS")
            method = (
                self._strategy_method("enable_auto_canary")
                or self._strategy_method("enable")
                or self._strategy_method("start")
            )
            if method is None:
                raise BinanceOperatorError("ENABLE_UNAVAILABLE")
            # This is the durable mutation boundary.  Do not invoke the
            # strategy once the absolute deadline has elapsed.
            self._deadline_checkpoint(deadline_monotonic)
            result = _as_mapping(
                self._safe_call(
                    method,
                    {
                        "confirmation": ENABLE_TESTNET_CONFIRMATION,
                        "deadline_monotonic": deadline_monotonic,
                    },
                )
            )
            self._autonomous_window = {"seconds": window_seconds, "bounded": True}
            return {"control": result, "autonomous": self._autonomous_window}

    def _execute(self, action: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_request(action, payload)
        # ENABLE and RESUME are deliberately not API actions.  Both could arm
        # a strategy without a bounded execution window; only runtime.auto()
        # may call authorize_bounded_auto() after taking the profile lock.
        if action in {"ENABLE", "RESUME"}:
            raise BinanceOperatorError("BOUNDED_AUTO_REQUIRES_CLI")
        if action == "CONNECTIVITY_CHECK":
            return _as_mapping(
                self._safe_call(getattr(self.gate, "check_connectivity", None))
            )
        if action == "ORDER_VALIDATION_TEST":
            symbol = payload.get("symbol")
            body = {"symbol": symbol} if symbol else {}
            return _as_mapping(
                self._safe_call(getattr(self.gate, "validate_order", None), body)
            )
        if action == "EXECUTION_PROBE":
            self._require_phrase(payload, TESTNET_PROBE_CONFIRMATION)
            symbol = payload.get("symbol")
            body = {"symbol": symbol} if symbol else {}
            return _as_mapping(
                self._safe_call(getattr(self.gate, "execute_probe", None), body)
            )
        if action == "RECONCILE_PROBE":
            return _as_mapping(
                self._safe_call(getattr(self.gate, "reconcile_probe", None))
            )
        if action in {"PAUSE", "DISARM", "KILL"}:
            if self._strategy_target() is None:
                raise BinanceOperatorError("STRATEGY_NOT_CONFIGURED")
            method = self._strategy_method(action.lower())
            if method is None:
                raise BinanceOperatorError(action + "_UNAVAILABLE")
            result = _as_mapping(
                self._safe_call(method, {"reason": str(payload.get("reason") or f"operator {action.lower()}")})
            )
            return {"control": result}
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
        encoded_payload = json.dumps(
            _jsonable(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        encoded_result = json.dumps(
            _jsonable(result), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        with self._lock:
            self._conn.execute(
                "INSERT INTO binance_testnet_operator_actions(action_id,action,environment,source,probe_kind,attempted_at,completed_at,timestamp_utc,timestamp_pht,success,reason,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    action_id,
                    action,
                    self.environment,
                    self.source,
                    self.probe_kind,
                    attempted_projection["utc"],
                    completed_projection["utc"],
                    completed_projection["utc"],
                    completed_projection["pht"],
                    int(bool(success)),
                    str(reason or "")[:256],
                    encoded_payload,
                    encoded_result,
                ),
            )
            self._conn.execute(
                "DELETE FROM binance_testnet_operator_actions WHERE rowid NOT IN (SELECT rowid FROM binance_testnet_operator_actions ORDER BY timestamp_utc DESC,action_id DESC LIMIT ?)",
                (self.max_history,),
            )
            self._conn.commit()
        return action_id

    def action(self, action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            return self._action_unlocked(action, payload)

    def _action_unlocked(self, action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
        stamp = _timestamp_projection(completed)
        response: dict[str, Any] = {
            "ok": success,
            "action": action_value,
            "environment": self.environment,
            "source": self.source,
            "probe_kind": self.probe_kind,
            "attempted_at": _timestamp_projection(attempted),
            "completed_at": _timestamp_projection(completed),
            "timestamp": stamp,
            "timestamp_utc": stamp["utc"],
            "timestamp_pht": stamp["pht"],
        }
        if success:
            response["result"] = result
        else:
            response["reason"] = reason or "ACTION_FAILED"
        try:
            response["action_id"] = self._persist_action(
                action_value or "INVALID",
                body,
                attempted=attempted,
                completed=completed,
                success=success,
                reason=reason,
                result=response,
            )
        except Exception as exc:
            response["ok"] = False
            response["reason"] = "ACTION_AUDIT_FAILED"
            response["audit_error"] = type(exc).__name__
        return _as_mapping(response)

    execute = action

    def list_actions(self, *, limit: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
        try:
            bounded = max(0, min(int(limit), MAX_PAGE_SIZE))
        except (TypeError, ValueError):
            bounded = DEFAULT_PAGE_SIZE
        try:
            rows = self._conn.execute(
                "SELECT * FROM binance_testnet_operator_actions ORDER BY timestamp_utc DESC,action_id DESC LIMIT ?",
                (bounded,),
            ).fetchall()
        except Exception:
            return []
        return [_as_mapping(dict(row)) for row in rows]

__all__ = [
    "ACTION_NAMES",
    "TESTNET_ACTION_NAMES",
    "TESTNET_PROBE_CONFIRMATION",
    "EXACT_PROBE_PHRASE",
    "EXACT_TESTNET_PROBE_PHRASE",
    "EXACT_EXECUTION_PROBE_PHRASE",
    "TESTNET_EXECUTION_PROBE_CONFIRMATION",
    "EXACT_TESTNET_ENABLE_PHRASE",
    "BinanceCanaryControlPlane",
    "BinanceTestnetControlPlane",
    "BinanceOperatorError",
]
