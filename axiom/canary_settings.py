"""Durable operator settings for the Polymarket canary.

Settings are deliberately separate from the live canary control row.  A draft is
an immutable proposed configuration; activation is a compare-and-swap operation
which advances a generation and records an audit event.  Nothing in this module
opens credentials, creates a venue, or submits an order.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import sqlite3
import uuid
from zoneinfo import ZoneInfo
from typing import Any

from .domain import ensure_utc, utc_now
from .storage import AxiomStore

UTC = timezone.utc
PHT = ZoneInfo("Asia/Manila")

# These are intentionally visible engineering bounds.  They are validation
# bounds, not silently applied clamps.  Values outside them are rejected.
ENGINEERING_BOUNDS: dict[str, tuple[Any, Any]] = {
    "money": (Decimal("0.01"), Decimal("1000000.00")),
    "positions": (1, 10000),
    "submissions": (1, 100000),
    "slippage_bps": (0, 10000),
}

# The first Polymarket canary's effective limits.  Keep these values in lockstep
# with the legacy control defaults: a missing settings row must not widen risk.
DEFAULT_CANARY_SETTINGS: dict[str, Any] = {
    "target_notional_usd": "1.00",
    "max_exposure_usd": "5.00",
    "max_daily_loss_usd": "2.00",
    "max_open_positions": 3,
    "max_orders_per_day": 5,
    "max_slippage_bps": 100,
    "max_all_in_buy_usd": "1.00",
    "max_fee_reserve_usd": "0.01",
    "max_gross_daily_buy_usd": "5.00",
    "max_aggregate_open_cost_usd": "5.00",
    "max_aggregate_exposure_usd": "5.00",
    "max_positions": 3,
    "max_submitted_orders_per_day": 5,
    "realized_loss_entry_stop_usd": "2.00",
    "equity_loss_entry_stop_usd": "2.00",
    "per_market_buy_cap_usd": None,
    "per_event_buy_cap_usd": None,
    "cumulative_buy_cap_usd": None,
}

_DECIMAL_FIELDS = {
    "target_notional_usd",
    "max_exposure_usd",
    "max_daily_loss_usd",
    "max_all_in_buy_usd",
    "max_fee_reserve_usd",
    "max_gross_daily_buy_usd",
    "max_aggregate_open_cost_usd",
    "max_aggregate_exposure_usd",
    "realized_loss_entry_stop_usd",
    "equity_loss_entry_stop_usd",
    "per_market_buy_cap_usd",
    "per_event_buy_cap_usd",
    "cumulative_buy_cap_usd",
}
_INTEGER_FIELDS = {
    "max_open_positions",
    "max_orders_per_day",
    "max_positions",
    "max_submitted_orders_per_day",
    "max_slippage_bps",
}
_OPTIONAL_FIELDS = {
    "per_market_buy_cap_usd",
    "per_event_buy_cap_usd",
    "cumulative_buy_cap_usd",
}

# Accept names used by existing controls and by the execution/dashboard owners,
# but persist exactly one canonical name for deterministic hashes.
_ALIASES: dict[str, str] = {
    "max_all_in_buy_fee_reserve_usd": "max_all_in_buy_usd",
    "max_all_in_buy_reserve_usd": "max_all_in_buy_usd",
    "max_all_in_buy_fee_reserve": "max_all_in_buy_usd",
    "max_buy_reserve_usd": "max_all_in_buy_usd",
    "buy_fee_reserve_usd": "max_fee_reserve_usd",
    "max_buy_fee_reserve_usd": "max_fee_reserve_usd",
    "max_buy_fee_reserve": "max_fee_reserve_usd",
    "gross_daily_buy_usd": "max_gross_daily_buy_usd",
    "max_gross_daily_buy": "max_gross_daily_buy_usd",
    "daily_buy_cap_usd": "max_gross_daily_buy_usd",
    "aggregate_open_cost_usd": "max_aggregate_open_cost_usd",
    "max_aggregate_open_cost": "max_aggregate_open_cost_usd",
    "open_cost_usd": "max_aggregate_open_cost_usd",
    "aggregate_exposure_usd": "max_aggregate_exposure_usd",
    "max_aggregate_exposure": "max_aggregate_exposure_usd",
    "max_exposure": "max_aggregate_exposure_usd",
    "positions": "max_positions",
    "max_open_positions": "max_positions",
    "orders_per_day": "max_submitted_orders_per_day",
    "max_submissions_per_day": "max_submitted_orders_per_day",
    "submitted_orders_per_day": "max_submitted_orders_per_day",
    "total_submitted_orders_per_day": "max_submitted_orders_per_day",
    "max_total_submitted_orders_per_day": "max_submitted_orders_per_day",
    "daily_submission_limit": "max_submitted_orders_per_day",
    "realized_loss_stop_usd": "realized_loss_entry_stop_usd",
    "realized_loss_entry_stop": "realized_loss_entry_stop_usd",
    "equity_loss_stop_usd": "equity_loss_entry_stop_usd",
    "equity_loss_entry_stop": "equity_loss_entry_stop_usd",
    "per_market_cap_usd": "per_market_buy_cap_usd",
    "per_event_cap_usd": "per_event_buy_cap_usd",
    "cumulative_cap_usd": "cumulative_buy_cap_usd",
    "slippage_bps": "max_slippage_bps",
    "max_execution_deviation_bps": "max_slippage_bps",
}

# An incoming legacy field and its expanded equivalent may both be supplied.
# They are only accepted when equal; silently choosing one would hide a risk
# review mistake.
_LEGACY_EQUIVALENTS = {
    "target_notional_usd": "max_all_in_buy_usd",
    "max_exposure_usd": "max_aggregate_exposure_usd",
    "max_open_positions": "max_positions",
    "max_orders_per_day": "max_submitted_orders_per_day",
    "max_daily_loss_usd": "realized_loss_entry_stop_usd",
}


class CanarySettingsConflict(ValueError):
    """A compare-and-swap generation or configuration conflict."""


class CanarySettingsValidationError(ValueError):
    """A malformed or unsafe settings value."""


def _canonical(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (tuple, list)):
        return [_canonical(v) for v in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _decimal(value: Any, *, field: str, optional: bool = False, scale: int = 2) -> str | None:
    if optional and (value is None or value == ""):
        return None
    if isinstance(value, (bool, float)) or value is None:
        raise CanarySettingsValidationError(f"{field} must be an exact decimal string or Decimal")
    if not isinstance(value, (str, int, Decimal)):
        raise CanarySettingsValidationError(f"{field} must be an exact decimal string or Decimal")
    if isinstance(value, str) and not value.strip():
        raise CanarySettingsValidationError(f"{field} must be a finite decimal")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CanarySettingsValidationError(f"{field} must be a finite decimal") from exc
    if not number.is_finite() or number < 0:
        raise CanarySettingsValidationError(f"{field} must be finite and non-negative")
    # Decimal exponent can be positive for values such as 1E+2.  It is exact,
    # while values requiring more fractional places are not accepted.
    fractional_places = max(0, -number.as_tuple().exponent)
    if fractional_places > scale:
        raise CanarySettingsValidationError(f"{field} has more than {scale} decimal places")
    lower, upper = ENGINEERING_BOUNDS["money"]
    if number < lower or number > upper:
        raise CanarySettingsValidationError(f"{field} must be between {lower} and {upper}")
    return format(number, "f")


def _integer(value: Any, *, field: str, kind: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise CanarySettingsValidationError(f"{field} must be an integer")
    try:
        number = Decimal(str(value).strip()) if isinstance(value, str) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CanarySettingsValidationError(f"{field} must be an integer") from exc
    if not number.is_finite() or number != number.to_integral_value():
        raise CanarySettingsValidationError(f"{field} must be an integer")
    result = int(number)
    lower, upper = ENGINEERING_BOUNDS[kind]
    if result < lower or result > upper:
        raise CanarySettingsValidationError(f"{field} must be between {lower} and {upper}")
    return result


def _timestamp(value: datetime | None) -> datetime:
    if value is None:
        return ensure_utc(utc_now())
    if not isinstance(value, datetime):
        raise TypeError("clock must return datetime")
    return ensure_utc(value)


def _actor(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("actor_id", value.get("id", value.get("name")))
    text = str(value or "").strip()
    if not text or len(text) > 256:
        raise ValueError("actor is required")
    return text


def _next_pht_reset(now: datetime) -> datetime:
    local = now.astimezone(PHT)
    next_local = (local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return next_local.astimezone(UTC)


class CanarySettingsService:
    """Versioned DRAFT/ACTIVE settings and read-only risk projections.

    ``save_draft`` merges a partial edit with the active settings and creates an
    immutable draft. ``activate_draft`` is a generation-fenced atomic swap;
    callers must pass the generation observed in their snapshot.
    """

    def __init__(
        self,
        store: AxiomStore,
        clock: Callable[[], datetime] | None = None,
        *,
        initialize: bool = True,
    ) -> None:
        if store is None:
            raise TypeError("store is required")
        if not isinstance(initialize, bool):
            raise TypeError("initialize must be a boolean")
        self.store = store
        self.clock = clock or utc_now
        self._initialize = initialize
        if initialize:
            self._ensure_default_active()

    @property
    def engineering_bounds(self) -> dict[str, dict[str, Any]]:
        return {
            name: {"minimum": _canonical(low), "maximum": _canonical(high)}
            for name, (low, high) in ENGINEERING_BOUNDS.items()
        }

    @staticmethod
    def _known_field(name: str) -> str:
        key = str(name).strip()
        if key in DEFAULT_CANARY_SETTINGS:
            return key
        if key in _ALIASES:
            return _ALIASES[key]
        raise CanarySettingsValidationError(f"unknown canary setting: {key}")

    def _normalize(self, values: Mapping[str, Any], *, base: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(values, Mapping):
            raise CanarySettingsValidationError("settings must be an object")
        source = dict(base or DEFAULT_CANARY_SETTINGS)
        supplied: dict[str, Any] = {}
        for raw_name, raw_value in values.items():
            name = self._known_field(str(raw_name))
            if name in supplied and supplied[name] != raw_value:
                raise CanarySettingsValidationError(f"conflicting values for {name}")
            supplied[name] = raw_value
        for legacy, expanded in _LEGACY_EQUIVALENTS.items():
            if legacy in supplied and expanded not in supplied:
                supplied[expanded] = supplied[legacy]
            elif expanded in supplied and legacy not in supplied:
                supplied[legacy] = supplied[expanded]
        # The dashboard intentionally exposes one Open exposure control.  Keep
        # the persisted open-cost fence coupled to that control when callers
        # edit exposure without also submitting the hidden invariant.  Direct
        # callers may still provide both fields, in which case validation below
        # rejects an unsafe mismatch instead of silently choosing one.
        if (
            "max_aggregate_exposure_usd" in supplied
            and "max_aggregate_open_cost_usd" not in supplied
        ):
            supplied["max_aggregate_open_cost_usd"] = supplied[
                "max_aggregate_exposure_usd"
            ]
        source.update(supplied)
        normalized: dict[str, Any] = {}
        for name in DEFAULT_CANARY_SETTINGS:
            value = source.get(name)
            if name in _DECIMAL_FIELDS:
                normalized[name] = _decimal(value, field=name, optional=name in _OPTIONAL_FIELDS)
            else:
                normalized[name] = _integer(value, field=name, kind="slippage_bps" if name == "max_slippage_bps" else "positions" if name in {"max_open_positions", "max_positions"} else "submissions")
        # Expanded and legacy controls must agree.  Equality is checked after
        # exact normalization so ``1`` and ``1.00`` do not produce false diffs.
        for legacy, expanded in _LEGACY_EQUIVALENTS.items():
            if normalized[legacy] != normalized[expanded]:
                raise CanarySettingsValidationError(f"{legacy} must equal {expanded}")
        if Decimal(normalized["max_fee_reserve_usd"]) > Decimal(normalized["max_all_in_buy_usd"]):
            raise CanarySettingsValidationError("max_fee_reserve_usd cannot exceed max_all_in_buy_usd")
        if Decimal(normalized["max_all_in_buy_usd"]) > Decimal(normalized["max_gross_daily_buy_usd"]):
            raise CanarySettingsValidationError("max_all_in_buy_usd cannot exceed max_gross_daily_buy_usd")
        if Decimal(normalized["max_aggregate_open_cost_usd"]) > Decimal(normalized["max_aggregate_exposure_usd"]):
            raise CanarySettingsValidationError("open cost cannot exceed aggregate exposure")
        for optional in _OPTIONAL_FIELDS:
            cap = normalized[optional]
            if cap is not None and Decimal(cap) < Decimal(normalized["max_all_in_buy_usd"]):
                raise CanarySettingsValidationError(f"{optional} cannot be smaller than one all-in buy commitment")
        return normalized

    @staticmethod
    def _hash(values: Mapping[str, Any]) -> str:
        return hashlib.sha256(_json(values).encode("utf-8")).hexdigest()

    def _legacy_limit_sources(self) -> list[dict[str, Any]]:
        """Read legacy risk envelopes without changing their control state.

        The first settings migration can run against a database that has the
        old operator configuration and/or the live canary control row but no
        versioned ACTIVE settings.  Both rows are evidence of the previously
        effective envelope.  Unknown fields (including authorization flags) are
        intentionally ignored; this migration creates settings only and never
        enables a control row.
        """
        connection = self.store.connection
        try:
            tables = {
                str(row["name"])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        except sqlite3.Error:
            return []
        sources: list[dict[str, Any]] = []
        if "operator_config" in tables:
            try:
                row = connection.execute(
                    "SELECT value_json FROM operator_config "
                    "WHERE config_key='canary-risk-limits'"
                ).fetchone()
            except sqlite3.Error:
                row = None
            if row is not None:
                try:
                    payload = json.loads(str(row["value_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise CanarySettingsValidationError(
                        "legacy operator risk limits are malformed"
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise CanarySettingsValidationError(
                        "legacy operator risk limits must be an object"
                    )
                sources.append(dict(payload))
        if "canary_control" in tables:
            try:
                row = connection.execute(
                    "SELECT limits_json FROM canary_control WHERE singleton=1"
                ).fetchone()
            except sqlite3.Error:
                row = None
            if row is not None:
                try:
                    payload = json.loads(str(row["limits_json"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise CanarySettingsValidationError(
                        "legacy canary control limits are malformed"
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise CanarySettingsValidationError(
                        "legacy canary control limits must be an object"
                    )
                sources.append(dict(payload))
        return sources

    def _conservative_migration_values(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Derive first ACTIVE values as the minimum of known legacy limits."""
        values = dict(DEFAULT_CANARY_SETTINGS)
        observed: list[dict[str, Any]] = []
        candidates: dict[str, list[Any]] = {}
        for source in self._legacy_limit_sources():
            normalized_source: dict[str, Any] = {}
            for raw_name, raw_value in source.items():
                try:
                    name = self._known_field(str(raw_name))
                except CanarySettingsValidationError:
                    continue
                try:
                    if name in _DECIMAL_FIELDS:
                        value = _decimal(
                            raw_value,
                            field=name,
                            optional=name in _OPTIONAL_FIELDS,
                        )
                    else:
                        value = _integer(
                            raw_value,
                            field=name,
                            kind=(
                                "slippage_bps"
                                if name == "max_slippage_bps"
                                else "positions"
                                if name in {"max_open_positions", "max_positions"}
                                else "submissions"
                            ),
                        )
                except CanarySettingsValidationError as exc:
                    # A recognized legacy limit that cannot be represented
                    # exactly is unsafe to migrate: proceeding would widen it
                    # to a new default.
                    raise CanarySettingsValidationError(
                        f"legacy canary limit {raw_name!s} is invalid"
                    ) from exc
                if value is None:
                    continue
                normalized_source[name] = value
                candidates.setdefault(name, []).append(value)
            # The old names represented the same budget dimensions as the
            # expanded settings.  Preserve a tighter value on both names.
            for legacy, expanded in _LEGACY_EQUIVALENTS.items():
                if legacy in normalized_source:
                    value = normalized_source[legacy]
                    normalized_source[expanded] = value
                    candidates.setdefault(expanded, []).append(value)
                elif expanded in normalized_source:
                    value = normalized_source[expanded]
                    normalized_source[legacy] = value
                    candidates.setdefault(legacy, []).append(value)
            if normalized_source:
                observed.append(normalized_source)
        for name, entries in candidates.items():
            if name in _DECIMAL_FIELDS:
                values[name] = min(
                    (Decimal(str(item)) for item in entries),
                    default=Decimal(str(values[name])),
                )
                values[name] = format(values[name], "f")
            elif name in _INTEGER_FIELDS:
                values[name] = min(
                    (int(item) for item in entries),
                    default=int(values[name]),
                )
        # Legacy envelopes may expose only one side of a newer cross-budget
        # relation.  Tighten the dependent setting rather than widening the
        # legacy restriction or manufacturing an invalid ACTIVE row.
        values["max_all_in_buy_usd"] = format(
            min(
                Decimal(str(values["max_all_in_buy_usd"])),
                Decimal(str(values["max_gross_daily_buy_usd"])),
                *(
                    Decimal(str(values[name]))
                    for name in _OPTIONAL_FIELDS
                    if values.get(name) is not None
                ),
            ),
            "f",
        )
        values["target_notional_usd"] = format(
            min(
                Decimal(str(values["target_notional_usd"])),
                Decimal(str(values["max_all_in_buy_usd"])),
            ),
            "f",
        )
        values["max_fee_reserve_usd"] = format(
            min(
                Decimal(str(values["max_fee_reserve_usd"])),
                Decimal(str(values["max_all_in_buy_usd"])),
            ),
            "f",
        )
        values["max_aggregate_open_cost_usd"] = format(
            min(
                Decimal(str(values["max_aggregate_open_cost_usd"])),
                Decimal(str(values["max_aggregate_exposure_usd"])),
            ),
            "f",
        )
        for optional in _OPTIONAL_FIELDS:
            if values.get(optional) is not None:
                values[optional] = format(Decimal(str(values[optional])), "f")
        # Keep legacy and expanded spellings exactly equal after taking the
        # minimum, then run the normal cross-budget validation once.
        for legacy, expanded in _LEGACY_EQUIVALENTS.items():
            values[expanded] = values[legacy] = (
                min(
                    Decimal(str(values[legacy])),
                    Decimal(str(values[expanded])),
                )
                if legacy in _DECIMAL_FIELDS
                else min(int(values[legacy]), int(values[expanded]))
            )
            if legacy in _DECIMAL_FIELDS:
                values[legacy] = values[expanded] = format(
                    Decimal(str(values[legacy])), "f"
                )
        return self._normalize(values), observed

    def _ensure_default_active(self) -> None:
        # Avoid opening a writer transaction for ordinary reads or service
        # construction once the active row exists.  The check is repeated
        # inside the immediate transaction so concurrent first-run callers
        # still serialize the complete migration.
        if self.store.load_canary_setting_config(status="ACTIVE") is not None:
            return
        # Serialize the check, config insert, and genesis audit together.  A
        # second process opening the same database must observe either the
        # complete migration or no migration, never an ACTIVE row without its
        # audit event.
        with self.store.transaction(immediate=True):
            existing = self.store.load_canary_setting_config(status="ACTIVE")
            if existing is not None:
                return
            # An empty settings history is the only valid first-run state.
            # Never recreate defaults after an established envelope has lost
            # its ACTIVE row; that would silently reset risk to migration
            # values instead of surfacing unavailable authority.
            prior_config = self.store.connection.execute(
                "SELECT 1 FROM canary_setting_configs LIMIT 1"
            ).fetchone()
            prior_audit = self.store.connection.execute(
                "SELECT 1 FROM canary_setting_audit LIMIT 1"
            ).fetchone()
            if prior_config is not None or prior_audit is not None:
                return
            values, observed_legacy = self._conservative_migration_values()
            digest = self._hash(values)
            stamp = _timestamp(self.clock())
            config_id = "cfg-default-" + digest[:16]
            self.store.save_canary_setting_config(
                config_id=config_id,
                state="ACTIVE",
                generation=1,
                config_hash=digest,
                values=values,
                actor="system:migration",
                timestamp=stamp,
                activated_at=stamp,
            )
            self.store.record_canary_setting_audit(
                config_id=config_id,
                action="MIGRATION_DEFAULT_ACTIVE",
                actor="system:migration",
                previous_config_id=None,
                previous_config_hash=None,
                new_config_id=config_id,
                new_config_hash=digest,
                previous_generation=0,
                new_generation=1,
                timestamp=stamp,
                detail={
                    "reason": "preserve legacy canary limits",
                    "legacy_sources": observed_legacy,
                    "authorization_state_untouched": True,
                    "counting_review_required": any(
                        "max_orders_per_day" in source
                        or "max_submitted_orders_per_day" in source
                        for source in observed_legacy
                    ),
                },
            )

    @staticmethod
    def _public_record(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if record is None:
            return None
        values = dict(record.get("values") or record.get("settings") or {})
        return _canonical({
            "config_id": str(record.get("config_id")),
            "state": str(record.get("state", record.get("status", ""))).upper(),
            "generation": int(record.get("generation", 0)),
            "config_hash": str(record.get("config_hash", "")),
            "settings": _canonical(values),
            "values": _canonical(values),
            "actor": record.get("actor"),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "activated_at": record.get("activated_at"),
        })

    def _active_record(self) -> dict[str, Any]:
        # Active-settings reads are strictly read-only.  First-run migration
        # belongs to service initialization, never to a status projection.
        row = self.store.load_canary_setting_config(status="ACTIVE")
        if row is None:
            raise RuntimeError("active canary settings are unavailable")
        return dict(row)

    def _require_writable(self) -> None:
        if not self._initialize:
            raise CanarySettingsConflict("settings service is read-only")

    def _control_record(self) -> dict[str, Any]:
        """Return the persisted control fence, when the legacy row exists."""
        try:
            row = self.store.connection.execute(
                "SELECT * FROM canary_control WHERE singleton=1"
            ).fetchone()
        except sqlite3.Error:
            return {}
        return dict(row) if row is not None else {}

    @staticmethod
    def _optional_hash(value: Any, field: str) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if len(text) != 64 or any(char not in "0123456789abcdefABCDEF" for char in text):
            raise CanarySettingsConflict(f"{field} must be a SHA-256 hex digest")
        return text

    @staticmethod
    def _optional_generation(value: Any, field: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise CanarySettingsConflict(f"{field} must be a positive integer")
        return value

    def _check_fences(
        self,
        active: Mapping[str, Any],
        *,
        expected_config_hash: str | None,
        expected_control_generation: int | None,
    ) -> dict[str, Any]:
        if expected_config_hash is not None and str(active.get("config_hash", "")) != expected_config_hash:
            raise CanarySettingsConflict("active settings hash changed")
        control = self._control_record()
        if expected_control_generation is not None:
            actual = control.get("control_generation")
            try:
                actual_generation = int(actual)
            except (TypeError, ValueError):
                actual_generation = None
            if actual_generation != expected_control_generation:
                raise CanarySettingsConflict("canary control generation changed")
        return control

    def save_draft(
        self,
        values: Mapping[str, Any],
        actor: Any,
        *,
        expected_generation: int | None = None,
        expected_config_hash: Any | None = None,
        expected_control_generation: int | None = None,
    ) -> dict[str, Any]:
        actor_id = _actor(actor)
        self._require_writable()
        expected_generation = self._optional_generation(expected_generation, "expected_generation")
        expected_hash = self._optional_hash(expected_config_hash, "expected_config_hash")
        expected_control_generation = self._optional_generation(
            expected_control_generation,
            "expected_control_generation",
        )
        stamp = _timestamp(self.clock())
        with self.store.transaction(immediate=True):
            active = self._active_record()
            if expected_generation is not None and int(active.get("generation", 0)) != expected_generation:
                raise CanarySettingsConflict("settings generation changed")
            control = self._check_fences(
                active,
                expected_config_hash=expected_hash,
                expected_control_generation=expected_control_generation,
            )
            base = dict(active.get("values") or {})
            normalized = self._normalize(values, base=base)
            digest = self._hash(normalized)
            config_id = f"cfg-{stamp.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}"
            self.store.save_canary_setting_config(
                config_id=config_id,
                state="DRAFT",
                generation=int(active.get("generation", 1)),
                config_hash=digest,
                values=normalized,
                actor=actor_id,
                timestamp=stamp,
                activated_at=None,
            )
            changed_fields = sorted({self._known_field(str(k)) for k in values})
            counting_review_required = bool(
                {"max_orders_per_day", "max_submitted_orders_per_day"} & set(changed_fields)
            )
            self.store.record_canary_setting_audit(
                config_id=config_id,
                action="DRAFT_CREATED",
                actor=actor_id,
                previous_config_id=active.get("config_id"),
                previous_config_hash=active.get("config_hash"),
                new_config_id=config_id,
                new_config_hash=digest,
                previous_generation=int(active.get("generation", 1)),
                new_generation=int(active.get("generation", 1)),
                timestamp=stamp,
                detail={
                    "changed_fields": changed_fields,
                    "counting_review_required": counting_review_required,
                    # Bind the draft to the exact active/control snapshot that
                    # was shown to the operator.  Activation recovers these
                    # fences from this immutable audit event.
                    "reviewed_active_generation": int(active.get("generation", 1)),
                    "reviewed_active_config_hash": active.get("config_hash"),
                    # Presence is intentional: ``None`` means the operator
                    # reviewed a database with no control row, not that the
                    # control fence was omitted.  If a row is created before
                    # activation, that review is stale and must be redone.
                    "reviewed_control_present": bool(control),
                    "reviewed_control_generation": control.get("control_generation"),
                    "control_generation": control.get("control_generation"),
                },
            )
            draft = self.store.load_canary_setting_config(config_id=config_id)
        return self._public_record(draft) or {}

    def activate_draft(
        self,
        config_id: Any,
        actor: Any,
        expected_generation: Any,
        *,
        expected_config_hash: Any | None = None,
        expected_control_generation: Any | None = None,
    ) -> dict[str, Any]:
        self._require_writable()
        actor_id = _actor(actor)
        if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation < 1:
            raise CanarySettingsConflict("expected_generation must be a positive integer")
        expected_hash = self._optional_hash(expected_config_hash, "expected_config_hash")
        expected_control_generation = self._optional_generation(
            expected_control_generation,
            "expected_control_generation",
        )
        identifier = str(config_id or "").strip()
        if not identifier:
            raise CanarySettingsConflict("config_id is required")
        stamp = _timestamp(self.clock())
        try:
            with self.store.transaction(immediate=True):
                active = self._active_record()
                if int(active.get("generation", 0)) != expected_generation:
                    raise CanarySettingsConflict("settings generation changed")
                self._check_fences(
                    active,
                    expected_config_hash=expected_hash,
                    expected_control_generation=expected_control_generation,
                )
                draft = self.store.load_canary_setting_config(
                    config_id=identifier,
                    state="DRAFT",
                )
                if draft is None:
                    raise CanarySettingsConflict("settings config is not a draft")
                try:
                    if int(draft.get("generation", 0)) != int(active.get("generation", 0)):
                        raise CanarySettingsConflict("draft settings generation changed")
                except (TypeError, ValueError) as exc:
                    raise CanarySettingsConflict("draft settings generation is invalid") from exc
                # OperatorControlPlane carries the legacy config/generation
                # payload. Recover the immutable review fences from the
                # DRAFT_CREATED audit event so control-generation and active
                # hash races cannot silently activate stale edits.
                draft_audit = self.store.connection.execute(
                    "SELECT detail_json,previous_config_hash,previous_generation "
                    "FROM canary_setting_audit "
                    "WHERE config_id=? AND action='DRAFT_CREATED' "
                    "ORDER BY timestamp DESC,audit_id DESC LIMIT 1",
                    (identifier,),
                ).fetchone()
                if draft_audit is None:
                    raise CanarySettingsConflict("draft review fence is missing")
                try:
                    review_detail = json.loads(draft_audit["detail_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise CanarySettingsConflict("draft review fence is invalid") from exc
                if not isinstance(review_detail, Mapping):
                    raise CanarySettingsConflict("draft review fence is invalid")
                reviewed_hash = review_detail.get(
                    "reviewed_active_config_hash",
                    draft_audit["previous_config_hash"],
                )
                if str(reviewed_hash or "") != str(active.get("config_hash") or ""):
                    raise CanarySettingsConflict("active settings hash changed")
                reviewed_generation = review_detail.get(
                    "reviewed_active_generation",
                    draft_audit["previous_generation"],
                )
                try:
                    if int(reviewed_generation) != int(active.get("generation", 0)):
                        raise CanarySettingsConflict("settings generation changed")
                except (TypeError, ValueError) as exc:
                    raise CanarySettingsConflict("draft review generation is invalid") from exc
                reviewed_control_present = review_detail.get(
                    "reviewed_control_present"
                )
                reviewed_control_generation = review_detail.get(
                    "reviewed_control_generation",
                    review_detail.get("control_generation"),
                )
                # Older DRAFT_CREATED records predate the explicit presence
                # fence but persisted ``control_generation``.  A null value
                # is an intentional review of no control row; preserve that
                # meaning instead of treating it as an omitted fence.
                legacy_no_control_review = (
                    reviewed_control_present is None
                    and reviewed_control_generation is None
                    and (
                        "reviewed_control_generation" in review_detail
                        or "control_generation" in review_detail
                    )
                )
                current_control = self._control_record()
                if reviewed_control_present is False or legacy_no_control_review:
                    if current_control:
                        raise CanarySettingsConflict("canary control generation changed")
                elif reviewed_control_present is True:
                    if not current_control:
                        raise CanarySettingsConflict("canary control generation changed")
                    try:
                        current_control_generation = int(
                            current_control.get("control_generation")
                        )
                    except (TypeError, ValueError):
                        raise CanarySettingsConflict("canary control generation changed") from None
                    if (
                        reviewed_control_generation is None
                        or current_control_generation != int(reviewed_control_generation)
                    ):
                        raise CanarySettingsConflict("canary control generation changed")
                elif reviewed_control_generation is not None:
                    # Compatibility for pre-presence-fence drafts: retain the
                    # old generation check when an explicit generation exists.
                    try:
                        current_control_generation = int(
                            current_control.get("control_generation")
                        )
                    except (TypeError, ValueError):
                        raise CanarySettingsConflict("canary control generation changed") from None
                    if current_control_generation != int(reviewed_control_generation):
                        raise CanarySettingsConflict("canary control generation changed")
                draft_values = dict(draft.get("values") or {})
                try:
                    draft_digest = self._hash(self._normalize(draft_values))
                except CanarySettingsValidationError as exc:
                    raise CanarySettingsConflict("draft settings are invalid") from exc
                if str(draft.get("config_hash", "")) != draft_digest:
                    raise CanarySettingsConflict("draft settings hash does not match values")
                row = self.store.activate_canary_setting_config(
                    config_id=identifier,
                    actor=actor_id,
                    expected_generation=expected_generation,
                    timestamp=stamp,
                )
        except CanarySettingsConflict:
            raise
        except ValueError as exc:
            raise CanarySettingsConflict(str(exc)) from exc
        return self._public_record(row) or {}

    def active_limits(self) -> dict[str, Any]:
        """Return a fresh JSON-safe active settings projection.

        Legacy keys are retained because the existing canary consumes those
        names; expanded keys are included for execution and dashboard owners.
        """
        active = self._active_record()
        return _canonical(dict(active.get("values") or {}))
    @staticmethod
    def _candidate_values(candidate: Mapping[str, Any] | None) -> dict[str, Any]:
        if candidate is None:
            return {}
        if not isinstance(candidate, Mapping):
            raise CanarySettingsValidationError("tighter_candidate must be an object")
        body = candidate.get("limits", candidate.get("settings", candidate))
        if not isinstance(body, Mapping):
            raise CanarySettingsValidationError("tighter_candidate limits must be an object")
        return dict(body)

    @staticmethod
    def _usage_defaults() -> dict[str, Any]:
        return {
            "submitted_orders": 0,
            "buy_filled_usd": "0.00",
            "buy_pending_usd": "0.00",
            "buy_unknown_usd": "0.00",
            "gross_daily_buy_usd": "0.00",
            "all_in_buy_reserved_usd": "0.00",
            "aggregate_open_cost_usd": "0.00",
            "aggregate_exposure_usd": "0.00",
            "open_positions": 0,
            "realized_loss_usd": "0.00",
            "today_realized_pnl_usd": "0.00",
            "equity_loss_usd": "0.00",
            "equity_status": "UNKNOWN",
            "per_market_buy_usd": {},
            "per_event_buy_usd": {},
            "cumulative_buy_usd": "0.00",
            "external_flow_usd": "0.00",
        }
    def _unavailable_snapshot(self, observed: datetime) -> dict[str, Any]:
        """Return an explicit read-only unknown projection without seeding."""
        control = self._control_record()
        try:
            control_generation = int(control["control_generation"])
        except (KeyError, TypeError, ValueError):
            control_generation = None
        control_projection = {
            "state": str(control.get("state", "")).upper() or None,
            "generation": control_generation,
            "settings_config_id": control.get("settings_config_id"),
            "settings_generation": control.get("settings_generation"),
        }
        draft = self.store.load_canary_setting_config(state="DRAFT")
        return {
            "status": "UNKNOWN",
            "settings_available": False,
            "settings_unavailable_reason": "ACTIVE_SETTINGS_UNAVAILABLE",
            "observed_at": observed.isoformat(),
            "config_id": None,
            "config_hash": None,
            "risk_breaker": None,
            "generation": None,
            "control_generation": control_generation,
            "control_state": control_projection["state"],
            "control": _canonical(control_projection),
            "active": None,
            "draft": self._public_record(draft),
            "effective_limits": None,
            "candidate_constraints": None,
            "usage": _canonical(self._usage_defaults()),
            "remaining": {},
            "entry_over_limit_dimensions": [],
            "entry_block_reasons": ["active canary settings are unavailable"],
            "pht_next_reset": _next_pht_reset(observed).isoformat(),
            "next_reset_at_pht": _next_pht_reset(observed).isoformat(),
            "engineering_bounds": self.engineering_bounds,
            "entry_block_only": True,
            "liquidation_allowed_when_tighter": True,
            "collateral": {
                "asset": "pUSD",
                "unit": "pUSD",
                "base_unit": "micro-pUSD",
                "base_unit_scale": 6,
                "chain_id": 137,
                "collateral_token": "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
                "token_address": "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
            },
        }

    def snapshot(
        self,
        now: datetime | None = None,
        tighter_candidate: Mapping[str, Any] | None = None,
        *,
        usage: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        observed = _timestamp(now or self.clock())
        if not self._initialize and self.store.load_canary_setting_config(status="ACTIVE") is None:
            return self._unavailable_snapshot(observed)
        active = self._active_record()
        control = self._control_record()
        draft_row = self.store.load_canary_setting_config(state="DRAFT")
        active_values = dict(active.get("values") or {})
        candidate_raw = self._candidate_values(tighter_candidate)
        candidate: dict[str, Any] = {}
        if candidate_raw:
            candidate = self._normalize(candidate_raw, base=active_values)
        effective = dict(active_values)
        if candidate:
            for name in effective:
                if name in _DECIMAL_FIELDS:
                    if candidate.get(name) is not None:
                        if effective.get(name) is None:
                            effective[name] = candidate[name]
                        else:
                            effective[name] = min(Decimal(effective[name]), Decimal(candidate[name]))
                elif name in _INTEGER_FIELDS:
                    effective[name] = min(int(effective[name]), int(candidate[name]))
        raw_usage = dict(self.store.canary_risk_accounting(observed))
        if usage is not None:
            raw_usage.update(dict(usage))
        used = self._usage_defaults()
        used.update(raw_usage)
        for name in ("buy_filled_usd", "buy_pending_usd", "buy_unknown_usd", "gross_daily_buy_usd", "all_in_buy_reserved_usd", "aggregate_open_cost_usd", "aggregate_exposure_usd", "realized_loss_usd", "today_realized_pnl_usd", "equity_loss_usd", "cumulative_buy_usd", "external_flow_usd"):
            value = used.get(name, "0")
            used[name] = format(Decimal(str(value)), "f")
        equity_status = str(used.get("equity_status") or "UNKNOWN").strip().upper()
        if equity_status not in {"KNOWN", "CURRENT", "OBSERVED", "UNKNOWN", "MISSING", "STALE"}:
            equity_status = "UNKNOWN"
        used["equity_status"] = equity_status
        risk_breaker = str(used.get("risk_breaker") or "").strip().upper() or None
        used["risk_breaker"] = risk_breaker
        # ``max_all_in_buy_usd`` is the fee-inclusive maximum for one BUY,
        # not a shared aggregate budget.  Aggregate usage is reported through
        # the independent gross/open/exposure dimensions below.
        remaining = {
            "submitted_orders": max(0, int(effective["max_submitted_orders_per_day"]) - int(used["submitted_orders"])),
            "all_in_buy_usd": format(Decimal(effective["max_all_in_buy_usd"]), "f"),
            "gross_daily_buy_usd": format(max(Decimal("0"), Decimal(effective["max_gross_daily_buy_usd"]) - Decimal(used["gross_daily_buy_usd"])), "f"),
            "aggregate_open_cost_usd": format(max(Decimal("0"), Decimal(effective["max_aggregate_open_cost_usd"]) - Decimal(used["aggregate_open_cost_usd"])), "f"),
            "aggregate_exposure_usd": format(max(Decimal("0"), Decimal(effective["max_aggregate_exposure_usd"]) - Decimal(used["aggregate_exposure_usd"])), "f"),
            "positions": max(0, int(effective["max_positions"]) - int(used["open_positions"])),
            "realized_loss_usd": format(max(Decimal("0"), Decimal(effective["realized_loss_entry_stop_usd"]) - Decimal(used["realized_loss_usd"])), "f"),
            "equity_loss_usd": format(max(Decimal("0"), Decimal(effective["equity_loss_entry_stop_usd"]) - Decimal(used["equity_loss_usd"])), "f"),
            "slippage_bps": effective["max_slippage_bps"],
        }
        # Values smaller than a held commitment affect entries only.  The
        # projection says this explicitly so a UI cannot imply liquidation is
        # blocked by a tighter draft/candidate.
        over_limit_dimensions: list[dict[str, Any]] = []
        for dimension, usage_name, limit_name in (
            ("submitted_orders", "submitted_orders", "max_submitted_orders_per_day"),
            ("gross_daily_buy_usd", "gross_daily_buy_usd", "max_gross_daily_buy_usd"),
            ("aggregate_open_cost_usd", "aggregate_open_cost_usd", "max_aggregate_open_cost_usd"),
            ("aggregate_exposure_usd", "aggregate_exposure_usd", "max_aggregate_exposure_usd"),
            ("positions", "open_positions", "max_positions"),
            ("realized_loss_usd", "realized_loss_usd", "realized_loss_entry_stop_usd"),
            ("equity_loss_usd", "equity_loss_usd", "equity_loss_entry_stop_usd"),
        ):
            used_value = (
                int(used[usage_name])
                if usage_name in {"submitted_orders", "open_positions"}
                else Decimal(str(used[usage_name]))
            )
            limit_value = (
                int(effective[limit_name])
                if limit_name in _INTEGER_FIELDS
                else Decimal(str(effective[limit_name]))
            )
            if used_value > limit_value:
                over_limit_dimensions.append({
                    "dimension": dimension,
                    "used": format(used_value, "f") if isinstance(used_value, Decimal) else used_value,
                    "limit": format(limit_value, "f") if isinstance(limit_value, Decimal) else limit_value,
                })
        try:
            control_generation = int(control["control_generation"])
        except (KeyError, TypeError, ValueError):
            control_generation = None
        control_projection = {
            "state": str(control.get("state", "")).upper() or None,
            "generation": control_generation,
            "settings_config_id": control.get("settings_config_id"),
            "settings_generation": control.get("settings_generation"),
        }
        entry_block_reasons: list[str] = []
        if over_limit_dimensions:
            entry_block_reasons.append("existing commitments exceed current entry limits")
        if risk_breaker:
            entry_block_reasons.append(f"durable risk breaker active: {risk_breaker}")
        if equity_status in {"UNKNOWN", "MISSING", "STALE"}:
            entry_block_reasons.append("authoritative equity evidence is unavailable or stale")
        return {
            "status": "CURRENT",
            "settings_available": True,
            "observed_at": observed.isoformat(),
            "config_id": active.get("config_id"),
            "config_hash": active.get("config_hash"),
            "generation": int(active.get("generation", 0)),
            "control_generation": control_generation,
            "control_state": control_projection["state"],
            "control": _canonical(control_projection),
            "active": self._public_record(active),
            "draft": self._public_record(draft_row),
            "effective_limits": _canonical(effective),
            "risk_breaker": risk_breaker,
            "candidate_constraints": _canonical(candidate) if candidate else None,
            "usage": _canonical(used),
            "remaining": _canonical(remaining),
            "entry_over_limit_dimensions": _canonical(over_limit_dimensions),
            "entry_block_reasons": _canonical(entry_block_reasons),
            "pht_next_reset": _next_pht_reset(observed).isoformat(),
            "next_reset_at_pht": _next_pht_reset(observed).isoformat(),
            "engineering_bounds": self.engineering_bounds,
            "entry_block_only": True,
            "liquidation_allowed_when_tighter": True,
            "collateral": {
                "asset": "pUSD",
                "unit": "pUSD",
                "base_unit": "micro-pUSD",
                "base_unit_scale": 6,
                "chain_id": 137,
                "collateral_token": "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
                "token_address": "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
            },
        }

    def reset_cumulative_buy_usage(
        self,
        actor: Any,
        *,
        disarmed: bool = False,
        expected_generation: int | None = None,
        expected_config_hash: Any | None = None,
        expected_control_generation: Any | None = None,
    ) -> dict[str, Any]:
        actor_id = _actor(actor)
        self._require_writable()
        if disarmed is not True:
            raise CanarySettingsConflict("cumulative reset requires explicit disarmed=True")
        expected_generation = self._optional_generation(expected_generation, "expected_generation")
        expected_hash = self._optional_hash(expected_config_hash, "expected_config_hash")
        expected_control_generation = self._optional_generation(
            expected_control_generation,
            "expected_control_generation",
        )
        stamp = _timestamp(self.clock())
        with self.store.transaction(immediate=True):
            active = self._active_record()
            if expected_generation is not None and int(active["generation"]) != expected_generation:
                raise CanarySettingsConflict("settings generation changed")
            control = self._check_fences(
                active,
                expected_config_hash=expected_hash,
                expected_control_generation=expected_control_generation,
            )
            # An explicit argument cannot override the persisted authorization
            # state.  DISABLED is deliberately not treated as DISARMED.
            if control and str(control.get("state", "")).upper() != "DISARMED":
                raise CanarySettingsConflict("cumulative reset requires DISARMED canary control")
            self.store.record_canary_setting_audit(
                config_id=str(active["config_id"]),
                action="CUMULATIVE_USAGE_RESET",
                actor=actor_id,
                previous_config_id=str(active["config_id"]),
                previous_config_hash=str(active["config_hash"]),
                new_config_id=str(active["config_id"]),
                new_config_hash=str(active["config_hash"]),
                previous_generation=int(active["generation"]),
                new_generation=int(active["generation"]),
                timestamp=stamp,
                detail={
                    "disarmed": True,
                    "baseline": "zero",
                    "cumulative_reset_at": stamp.isoformat(),
                    "control_generation": control.get("control_generation"),
                },
            )
            control_generation = control.get("control_generation")
        return {
            "reset": True,
            "config_id": active["config_id"],
            "generation": int(active["generation"]),
            "control_generation": control_generation,
            "timestamp": stamp.isoformat(),
        }


__all__ = [
    "CanarySettingsConflict",
    "CanarySettingsService",
    "CanarySettingsValidationError",
    "DEFAULT_CANARY_SETTINGS",
    "ENGINEERING_BOUNDS",
]
