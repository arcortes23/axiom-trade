"""Bounded autonomous canary scheduling, isolated from collection and research."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
import hashlib
import json
import logging
import math
import threading
import uuid
from typing import Any, Callable, Mapping

from .canary import (
    AUTONOMOUS_MICRO_LIVE,
    CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS,
    CanaryBlocked,
    CanaryService,
    PolymarketClobV2Venue,
    _UNSET,
    _canary_adverse_acknowledged,
    _canary_authorization_selection_policy_hash,
    _canary_authorization_current_selection_hash,
)
from .domain import ensure_utc, utc_now
from .ranker import CandidateCanaryRanker
from .lifecycle import _canonical_scope_gate_error
from .storage import AxiomStore
from .canary_positions import (
    reconcile_pending,
    manage_positions,
    _ensure_schema,
    _sync_entry_lots,
)

_LOGGER = logging.getLogger(__name__)


class AutonomousCanaryWorker:
    """Run one serialized rank/signal/submit decision at a bounded cadence."""
    _SUBMISSION_SUCCESS_STATUSES = frozenset(
        {
            "SUBMITTED",
            "ACCEPTED",
            "MATCHED",
            "PARTIALLY_FILLED",
            "SETTLED",
        }
    )
    _MAX_RETRY_DELAY_SECONDS = 30.0

    def __init__(
        self,
        store: AxiomStore,
        *,
        interval_seconds: float = 60.0,
        clock=utc_now,
        venue_factory: Callable[[], Any] | None = None,
        allow_test_venue: bool = False,
    ) -> None:
        self.store = store
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.clock = clock
        self.venue_factory = venue_factory or (lambda: PolymarketClobV2Venue(allow_environment=False))
        self.allow_test_venue = bool(allow_test_venue)
        self._decision_lock = threading.Lock()
        self._consecutive_failures = 0
        self._next_retry_at: datetime | None = None
        self.controller_owner_id = (
            f"autonomous-canary:{uuid.uuid4().hex[:20]}"
        )
        self._controller_lease: Mapping[str, Any] | None = None
        self._unknown_signal_ids: set[str] = set()
        self._rolling_cursor: dict[str, int] = {}
        self._rolling_overlap_cursor: dict[str, tuple[str, str]] = {}
        self._last_scan_skip_reasons: dict[str, int] = {
            reason: 0 for reason in self._SCAN_SKIP_REASONS
        }
    def _acquire_controller_lease(self, timestamp: datetime) -> Mapping[str, Any] | None:
        acquire = getattr(self.store, "acquire_canary_controller_lease", None)
        if not callable(acquire):
            return None
        # A worker that is still running owns the same generation across
        # decision windows.  Renew it before attempting a fresh acquisition;
        # otherwise every direct tick would race itself and lose authority.
        if isinstance(self._controller_lease, Mapping):
            renewed = self._renew_controller_lease(timestamp)
            if renewed is not None:
                return renewed
            self._controller_lease = None
        try:
            lease = acquire(
                owner_id=self.controller_owner_id,
                lease_seconds=max(60, int(self.interval_seconds * 2)),
                now=timestamp,
            )
        except (TypeError, ValueError, RuntimeError):
            return None
        if not isinstance(lease, Mapping):
            return None
        owner = str(lease.get("owner_id") or "").strip()
        generation = lease.get("generation")
        if owner != self.controller_owner_id or generation in (None, ""):
            return None
        self._controller_lease = dict(lease)
        return self._controller_lease

    def _renew_controller_lease(self, timestamp: datetime) -> Mapping[str, Any] | None:
        lease = self._controller_lease
        renew = getattr(self.store, "renew_canary_controller_lease", None)
        if not callable(renew) or not isinstance(lease, Mapping):
            return lease if isinstance(lease, Mapping) else None
        try:
            renewed = renew(
                owner_id=self.controller_owner_id,
                generation=lease.get("generation"),
                lease_seconds=max(60, int(self.interval_seconds * 2)),
                now=timestamp,
            )
        except (TypeError, ValueError, RuntimeError):
            return None
        if not isinstance(renewed, Mapping):
            return None
        self._controller_lease = dict(renewed)
        return self._controller_lease

    def _release_controller_lease(
        self,
        timestamp: datetime,
        *,
        reason: str = "worker_stop",
    ) -> None:
        lease = self._controller_lease
        release = getattr(self.store, "release_canary_controller_lease", None)
        if callable(release) and isinstance(lease, Mapping):
            try:
                release(
                    owner_id=self.controller_owner_id,
                    generation=lease.get("generation"),
                    now=timestamp,
                    reason=reason,
                )
            except (TypeError, ValueError, RuntimeError):
                pass
        self._controller_lease = None

    @staticmethod
    def _rolling_execution_mode(selection: Mapping[str, Any]) -> str:
        """Resolve the persisted execution policy without broadening authority."""
        candidates = (
            selection.get("execution_authorization_mode"),
            selection.get("authorization_mode"),
            selection.get("execution_mode"),
            selection.get("execution_policy"),
            selection.get("operating_policy"),
            selection.get("exploratory_policy"),
            selection.get("setup_policy"),
            selection.get("policy_mode"),
        )
        raw: Any = next((value for value in candidates if value not in (None, "")), None)
        if isinstance(raw, Mapping):
            raw = raw.get("mode") or raw.get("name") or raw.get("type")
        mode = str(raw or "EVIDENCE_SELECTED").strip().upper()
        if mode in {
            "EXPLORATORY",
            "EXPLORATORY_LIVE",
            "EXPLORATORY_MICRO_CANARY",
            "MICRO_CANARY",
        }:
            return "EXPLORATORY_MICRO_CANARY"
        return "EVIDENCE_SELECTED"


    @staticmethod
    def _position_obligations(service: CanaryService) -> bool:
        """Return whether this tick needs an authenticated position read."""
        _ensure_schema(service)
        connection = service.store.connection
        confirmed_inventory = False
        with service.store._lock:
            pending = connection.execute(
                "SELECT 1 FROM canary_position_requests "
                "WHERE status IN ('PREPARED','SUBMITTING','SUBMITTED','ACKNOWLEDGED',"
                "'OPEN','UNKNOWN','MATCHED','FILLED','PARTIAL','PARTIALLY_FILLED',"
                "'EXIT_REQUESTED','RECONCILE_PENDING') LIMIT 1"
            ).fetchone()
            ledger = connection.execute(
                "SELECT 1 FROM canary_ledger "
                "WHERE UPPER(side) IN ('BUY','SELL') "
                "AND UPPER(status) IN ('RESERVED','SUBMITTING','SUBMITTED','ACCEPTED',"
                "'ACKNOWLEDGED','UNKNOWN','PARTIAL','PARTIALLY_FILLED','MATCHED','OPEN') "
                "LIMIT 1"
            ).fetchone()
            reservations = connection.execute(
                "SELECT 1 FROM canary_risk_reservations "
                "WHERE UPPER(status) IN ('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED',"
                "'UNKNOWN','PARTIAL','PARTIALLY_FILLED','OPEN','SUBMITTED') "
                "LIMIT 1"
            ).fetchone()
            attempts = connection.execute(
                "SELECT 1 FROM canary_submission_attempts AS attempt "
                "LEFT JOIN canary_risk_reservations AS reservation "
                "ON reservation.intent_id=attempt.intent_id "
                "WHERE UPPER(attempt.status) IN ('ATTEMPTED','SUBMITTING','SUBMITTED','ACCEPTED',"
                "'ACKNOWLEDGED','UNKNOWN','PARTIAL','PARTIALLY_FILLED','MATCHED','OPEN') "
                "AND (reservation.reservation_id IS NULL OR UPPER(reservation.status) IN "
                "('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED','UNKNOWN','PARTIAL',"
                "'PARTIALLY_FILLED','OPEN','SUBMITTED')) LIMIT 1"
            ).fetchone()
            lots = connection.execute(
                "SELECT 1 FROM canary_position_lots "
                "WHERE UPPER(COALESCE(status,'')) IN "
                "('OPEN','EXIT_PENDING','MANAGEMENT_BLOCKED') LIMIT 1"
            ).fetchone()
            accounting_reader = getattr(service.store, "canary_risk_accounting", None)
            if callable(accounting_reader):
                try:
                    accounting = accounting_reader(ensure_utc(service.clock()))
                except Exception:
                    accounting = {}
                quantities = (
                    accounting.get("open_quantity_by_market")
                    if isinstance(accounting, Mapping)
                    else None
                )
                if isinstance(quantities, Mapping):
                    for quantity in quantities.values():
                        try:
                            if Decimal(str(quantity)) > 0:
                                confirmed_inventory = True
                                break
                        except (InvalidOperation, TypeError, ValueError, ArithmeticError):
                            continue
        return any(
            item is not None
            for item in (pending, ledger, reservations, attempts, lots)
        ) or confirmed_inventory
    @staticmethod
    def _unresolved_position_obligations(service: CanaryService) -> bool:
        """Return whether a durable order identity still needs reconciliation."""
        _ensure_schema(service)
        connection = service.store.connection
        orphan_inventory = False
        with service.store._lock:
            ledger = connection.execute(
                "SELECT 1 FROM canary_ledger "
                "WHERE UPPER(side) IN ('BUY','SELL') "
                "AND UPPER(status) IN ('RESERVED','SUBMITTING','SUBMITTED','ACCEPTED',"
                "'ACKNOWLEDGED','UNKNOWN','PARTIAL','PARTIALLY_FILLED','MATCHED','OPEN') "
                "AND NULLIF(TRIM(COALESCE(exchange_order_id,'')),'') IS NULL LIMIT 1"
            ).fetchone()
            reservations = connection.execute(
                "SELECT 1 FROM canary_risk_reservations "
                "WHERE UPPER(status) IN ('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED',"
                "'UNKNOWN','PARTIAL','PARTIALLY_FILLED','OPEN','SUBMITTED') LIMIT 1"
            ).fetchone()
            attempts = connection.execute(
                "SELECT 1 FROM canary_submission_attempts AS attempt "
                "LEFT JOIN canary_risk_reservations AS reservation "
                "ON reservation.intent_id=attempt.intent_id "
                "WHERE UPPER(attempt.status) IN ('ATTEMPTED','SUBMITTING','SUBMITTED','ACCEPTED',"
                "'ACKNOWLEDGED','UNKNOWN','PARTIAL','PARTIALLY_FILLED','MATCHED','OPEN') "
                "AND (reservation.reservation_id IS NULL OR UPPER(reservation.status) IN "
                "('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED','UNKNOWN','PARTIAL',"
                "'PARTIALLY_FILLED','OPEN','SUBMITTED')) LIMIT 1"
            ).fetchone()
            accounting_reader = getattr(service.store, "canary_risk_accounting", None)
            if callable(accounting_reader):
                try:
                    accounting = accounting_reader(ensure_utc(service.clock()))
                except Exception:
                    accounting = {}
                quantities = (
                    accounting.get("open_quantity_by_market")
                    if isinstance(accounting, Mapping)
                    else None
                )
                if isinstance(quantities, Mapping):
                    lot_rows = connection.execute(
                        "SELECT market_id,quantity,sold_quantity "
                        "FROM canary_position_lots "
                        "WHERE status IN ('OPEN','EXIT_PENDING')"
                    ).fetchall()
                    projected: dict[str, Decimal] = {}
                    for row in lot_rows:
                        market_id = str(row["market_id"] or "").strip()
                        if not market_id:
                            continue
                        try:
                            quantity = Decimal(str(row["quantity"] or "0"))
                            sold = Decimal(str(row["sold_quantity"] or "0"))
                        except (InvalidOperation, TypeError, ValueError, ArithmeticError):
                            continue
                        projected[market_id] = projected.get(market_id, Decimal("0")) + max(
                            Decimal("0"), quantity - sold
                        )
                    for market_id, quantity in quantities.items():
                        try:
                            canonical = Decimal(str(quantity))
                        except (InvalidOperation, TypeError, ValueError, ArithmeticError):
                            continue
                        if canonical > projected.get(str(market_id), Decimal("0")):
                            orphan_inventory = True
                            break
        return any(item is not None for item in (ledger, reservations, attempts)) or orphan_inventory
    _SCAN_CAP = 10
    _ROLLING_FORCE_EXIT_QUERY_LIMIT = 80
    _ROLLING_CURSOR_MAX = 64
    _SIGNAL_REASON_CODES = (
        "READY_SIGNAL",
        "NO_STRATEGY_SIGNAL",
        "STRATEGY_EVALUATED_DECLINED",
        "SIGNAL_PRODUCED",
        "MODEL_INPUT_MISSING",
        "WARMING_UP",
        "INSUFFICIENT_LOOKBACK",
        "NO_FORWARD_SNAPSHOT",
        "STALE_FORWARD_EVIDENCE",
        "MARKET_CLOSED",
        "MARKET_FILTER_MISMATCH",
        "RESEARCH_ONLY",
        "INVALID_POLICY",
        "DEFERRED_MARKETS",
        "SCOPE_RESOLUTION_MISSING",
        "SCOPE_RESOLUTION_SCOPE_MISMATCH",
        "SCOPE_RESOLUTION_STALE",
        "LEGACY_SCOPE_SUCCESSOR_REQUIRED",
        "SCOPE_RESOLUTION_ZERO_MATCHES",
        "SCOPE_RESOLUTION_TOKEN_MISMATCH",
        "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
    )
    _REASON_COUNT_CAP = 10_000
    _EXACT_DATA_BLOCKER_REASONS = frozenset(
        {
            "NO_FORWARD_SNAPSHOT",
            "STALE_FORWARD_EVIDENCE",
            "MODEL_INPUT_MISSING",
            "WARMING_UP",
            "INSUFFICIENT_LOOKBACK",
            "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
            "MARKET_CLOSED",
            "MARKET_FILTER_MISMATCH",
            "DEFERRED_MARKETS",
            "SCOPE_RESOLUTION_MISSING",
            "SCOPE_RESOLUTION_SCOPE_MISMATCH",
            "SCOPE_RESOLUTION_STALE",
            "SCOPE_RESOLUTION_ZERO_MATCHES",
            "SCOPE_RESOLUTION_TOKEN_MISMATCH",
        }
    )


    @classmethod
    def _normalize_reason_counts(cls, value: Any) -> dict[str, int]:
        parsed: Any = value
        if isinstance(value, str):
            try:
                parsed = json.loads(value or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = {}
        result = {reason: 0 for reason in cls._SIGNAL_REASON_CODES}
        if not isinstance(parsed, Mapping):
            return result
        for reason in cls._SIGNAL_REASON_CODES:
            try:
                count = int(parsed.get(reason, 0) or 0)
            except (TypeError, ValueError, OverflowError):
                count = 0
            result[reason] = min(cls._REASON_COUNT_CAP, max(0, count))
        return result

    @classmethod
    def _evaluation_reason(cls, evaluation: Any, signal: Any) -> str:
        fallback: str | None = None
        for source in (evaluation, signal):
            if isinstance(source, Mapping):
                reason = str(source.get("reason_code") or "").strip().upper()
                if reason not in cls._SIGNAL_REASON_CODES:
                    continue
                if reason in cls._EXACT_DATA_BLOCKER_REASONS:
                    return reason
                fallback = fallback or reason
        if fallback:
            return fallback
        if isinstance(signal, Mapping) and str(signal.get("status") or "").upper() == "READY":
            return "READY_SIGNAL"
        return "NO_STRATEGY_SIGNAL"


    @classmethod
    def _evaluation_blocker(
        cls,
        evaluation: Any,
        signal: Any = None,
    ) -> str | None:
        if not isinstance(evaluation, Mapping):
            evaluation = {}
        evaluation_reason = str(evaluation.get("reason_code") or "").strip().upper()
        if not evaluation_reason and isinstance(signal, Mapping):
            evaluation_reason = str(signal.get("reason_code") or "").strip().upper()
        evidence = evaluation.get("evidence")
        if not isinstance(evidence, Mapping):
            evidence = {}
        # A binding failure is more specific than the collector-health
        # projection that surrounds it.  Preserve the executable-document
        # blocker instead of masking it with the generic health outcome.
        binding_reason = str(evidence.get("binding_reason") or "").strip().upper()
        if (
            evaluation_reason == "COLLECTOR_CANDIDATE_HEALTH_BLOCKED"
            and binding_reason == "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE"
        ):
            return binding_reason
        # The evaluator's reason is the authoritative data prerequisite.  Do
        # not replace a precise missing/stale/warming/model/lookback outcome
        # with an evidence detail (or the generic no-edge result).
        if evaluation_reason in cls._EXACT_DATA_BLOCKER_REASONS:
            return evaluation_reason
        if not evidence:
            return None
        authority_error = str(evidence.get("authority_error") or "").strip()
        required_health_reason = str(
            evidence.get("required_health_reason_code") or ""
        ).strip().upper()
        if (
            authority_error
            and evaluation_reason == "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        ):
            return evaluation_reason
        for key in ("binding_reason", "authority_reason_code"):
            if (
                key == "authority_reason_code"
                and (
                    evaluation_reason
                    not in {
                        "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
                        "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                    }
                    or (
                        evaluation_reason
                        == "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
                        and required_health_reason
                    )
                )
            ):
                continue
            reason = str(evidence.get(key) or "").strip().upper()
            if reason and not reason.startswith("REQUIRED_MARKETS_"):
                return reason
        if required_health_reason and (
            not required_health_reason.startswith("REQUIRED_MARKETS_")
            and required_health_reason
            not in {
                "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
                "CANDIDATE_MARKET_CLOSED",
            }
        ):
            return required_health_reason
        return None


    @classmethod
    def _increment_reason_count(
        cls,
        counts: dict[str, int],
        reason: str,
    ) -> None:
        if reason not in cls._SIGNAL_REASON_CODES:
            reason = "NO_STRATEGY_SIGNAL"
        counts[reason] = min(
            cls._REASON_COUNT_CAP,
            max(0, int(counts.get(reason, 0))) + 1,
        )

    _SCAN_FIELDS = (
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
        "signal_scan_cycle_id",
        "signal_scan_candidate_universe_hash",
        "signal_scan_cycle_started_at",
        "signal_scan_cycle_completed_at",
        "signal_scan_cycle_complete",
        "signal_scan_checked_this_cycle",
        "signal_scan_remaining_this_cycle",
        "signal_scan_coverage_percentage",
        "signal_scan_skip_reasons_json",
        "signal_scan_reason_counts_json",
        "signal_scan_status",
    )
    _SCAN_SKIP_REASONS = (
        "INVALID_RANKING_BINDING",
        "QUALIFICATION_INVALID",
        "DUPLICATE_CLUSTER_DEFERRED",
        "CYCLE_REMAINDER",
        "LEGACY_SCOPE_SUCCESSOR_REQUIRED",
    )

    @staticmethod
    def _finite(value: Any, default: float = 0.0) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        return parsed if math.isfinite(parsed) else default

    @staticmethod
    def _mapping_metric(
        signal: Mapping[str, Any],
        evidence: Mapping[str, Any],
        *names: str,
        default: float = 0.0,
    ) -> float:
        for source in (signal, evidence):
            for name in names:
                if name in source:
                    return AutonomousCanaryWorker._finite(source.get(name), default)
        return default

    @classmethod
    def _ready_priority(
        cls,
        candidate: Mapping[str, Any],
        signal: Mapping[str, Any],
    ) -> tuple[Any, ...]:
        try:
            rank = int(candidate.get("rank"))
        except (TypeError, ValueError):
            rank = 0
        rank_key = rank if rank > 0 else 1_000_000_000
        evidence = signal.get("evidence")
        evidence = evidence if isinstance(evidence, Mapping) else {}
        execution = cls._mapping_metric(
            signal,
            evidence,
            "current_execution_feasibility",
            "execution_feasibility",
            "execution_feasibility_score",
            default=0.0,
        )
        liquidity = cls._mapping_metric(
            signal,
            evidence,
            "fresh_liquidity_feasibility",
            "liquidity_feasibility",
            "liquidity",
            "liquidity_score",
            default=0.0,
        )
        slippage = cls._mapping_metric(
            signal,
            evidence,
            "slippage_bps",
            "expected_slippage_bps",
            "slippage",
            default=float("inf"),
        )
        return (
            rank_key,
            -execution,
            -liquidity,
            slippage,
            str(candidate.get("candidate_id") or ""),
        )

    @classmethod
    def _diversity_order(cls, rows: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        clusters: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            clusters.setdefault(str(row.get("cluster_key") or ""), []).append(row)

        def candidate_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
            try:
                rank = int(row.get("rank"))
            except (TypeError, ValueError):
                rank = 0
            score = cls._finite(row.get("total_score"), float("-inf"))
            return (
                rank if rank > 0 else 1_000_000_000,
                -score,
                str(row.get("candidate_id") or ""),
            )

        representatives: list[Mapping[str, Any]] = []
        followers: list[Mapping[str, Any]] = []
        for cluster in sorted(clusters):
            members = clusters[cluster]
            marked = [item for item in members if int(item.get("cluster_representative") or 0) == 1]
            primary = sorted(marked or members, key=candidate_key)[0]
            representatives.append(primary)
            followers.extend(item for item in members if item is not primary)
        representatives.sort(key=candidate_key)
        followers.sort(
            key=lambda row: (
                -cls._finite(row.get("total_score"), float("-inf")),
                str(row.get("candidate_id") or ""),
            )
        )
        return representatives + followers

    def _current_rankings(
        self,
        ranker: CandidateCanaryRanker,
        *,
        ranking_run_id: str | None,
        timestamp: datetime,
    ) -> list[Mapping[str, Any]]:
        """Return current ranking rows plus independently scanable candidates."""
        self._last_scan_skip_reasons = {
            reason: 0 for reason in self._SCAN_SKIP_REASONS
        }
        try:
            rows = ranker.rankings(limit=10_000)
        except Exception:
            self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
            return []
        persisted_ids = {
            str(row.get("candidate_id") or "").strip()
            for row in rows
            if isinstance(row, Mapping) and str(row.get("candidate_id") or "").strip()
        }
        valid: list[tuple[Mapping[str, Any], int]] = []
        accepted_ids: set[str] = set()
        representative_clusters: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            lifecycle = self.store.load_candidate_lifecycle(candidate_id)
            payload = (
                ranker.service._merged_lifecycle_payload(lifecycle)
                if isinstance(lifecycle, Mapping)
                else None
            )
            scope_error = _canonical_scope_gate_error(
                str(lifecycle.get("stage") or "") if isinstance(lifecycle, Mapping) else "",
                payload if isinstance(payload, Mapping) else {},
            )
            if scope_error is not None:
                skip_reason = (
                    scope_error
                    if scope_error in self._SCAN_SKIP_REASONS
                    else "INVALID_RANKING_BINDING"
                )
                self._last_scan_skip_reasons[skip_reason] += 1
                continue
            with self.store._lock:
                eligibility = self.store.connection.execute(
                    "SELECT candidate_id,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            binding = ranker.service._eligibility_binding_result(
                candidate_id,
                eligibility,
                verify_attestation=False,
            )
            qualification_hash = str(binding.get("qualification_hash") or "").strip()
            if not binding.get("bound") or not qualification_hash:
                self._last_scan_skip_reasons["QUALIFICATION_INVALID"] += 1
                continue
            if str(row.get("qualification_hash") or "").strip() != qualification_hash:
                self._last_scan_skip_reasons["QUALIFICATION_INVALID"] += 1
                continue
            try:
                accepted = ranker.validate_persisted_ranking(
                    row,
                    ranking_run_id=str(ranking_run_id or ""),
                    now=timestamp,
                )
            except Exception:
                accepted = False
            if not accepted:
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            try:
                persisted_rank = int(row.get("rank"))
            except (TypeError, ValueError, OverflowError):
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            if persisted_rank < 0:
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            try:
                representative_marker = int(row.get("cluster_representative"))
            except (TypeError, ValueError, OverflowError):
                self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                continue
            reason = str(row.get("reason") or "").strip().upper()
            if persisted_rank == 0:
                rankless_reason = (
                    reason.startswith("RANKING_EVIDENCE_MISSING")
                    or reason in {
                        "RANKING_EVIDENCE_BELOW_MINIMUM_SAMPLE",
                        "RANKING_EVIDENCE_ZERO_TRADES",
                        "FROZEN_HASH_MISSING",
                    }
                )
                if representative_marker != 0 or not (
                    reason == "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                    or rankless_reason
                ):
                    self._last_scan_skip_reasons["INVALID_RANKING_BINDING"] += 1
                    continue
            elif representative_marker == 1:
                representative_clusters.add(str(row.get("cluster_key") or ""))
            valid.append((row, persisted_rank))
            accepted_ids.add(candidate_id)

        # A current ranking row is preferred, but ranking is not the
        # qualification boundary.  Include every eligible lifecycle candidate
        # that has no current positive ranking row so its signal is checked
        # and its check is durably covered by the active scan cycle.
        try:
            fallback_rows = ranker.eligible_scan_rows(
                now=timestamp,
                ranking_run_id=ranking_run_id,
            )
        except Exception:
            fallback_rows = []
        for row in fallback_rows:
            candidate_id = str(row.get("candidate_id") or "").strip()
            if (
                not candidate_id
                or candidate_id in persisted_ids
                or candidate_id in accepted_ids
            ):
                continue
            lifecycle = self.store.load_candidate_lifecycle(candidate_id)
            payload = (
                ranker.service._merged_lifecycle_payload(lifecycle)
                if isinstance(lifecycle, Mapping)
                else None
            )
            scope_error = _canonical_scope_gate_error(
                str(lifecycle.get("stage") or "") if isinstance(lifecycle, Mapping) else "",
                payload if isinstance(payload, Mapping) else {},
            )
            if scope_error is not None:
                skip_reason = (
                    scope_error
                    if scope_error in self._SCAN_SKIP_REASONS
                    else "INVALID_RANKING_BINDING"
                )
                self._last_scan_skip_reasons[skip_reason] += 1
                continue
            qualification_hash = str(row.get("qualification_hash") or "").strip()
            if not qualification_hash:
                self._last_scan_skip_reasons["QUALIFICATION_INVALID"] += 1
                continue
            valid.append((row, 0))
            accepted_ids.add(candidate_id)

        def is_unranked_reason(row: Mapping[str, Any]) -> bool:
            reason = str(row.get("reason") or "").strip().upper()
            return reason.startswith("RANKING_EVIDENCE_MISSING") or reason in {
                "FROZEN_HASH_MISSING",
                "RANKING_EVIDENCE_BELOW_MINIMUM_SAMPLE",
                "RANKING_EVIDENCE_ZERO_TRADES",
            }

        ordered_rows = [
            row
            for row, persisted_rank in valid
            if persisted_rank > 0
            or str(row.get("cluster_key") or "") in representative_clusters
            or is_unranked_reason(row)
        ]
        self._last_scan_skip_reasons["DUPLICATE_CLUSTER_DEFERRED"] = min(
            10_000,
            sum(
                1
                for row, persisted_rank in valid
                if (
                    persisted_rank == 0
                    and str(row.get("cluster_key") or "") in representative_clusters
                    and not is_unranked_reason(row)
                )
            ),
        )
        return ordered_rows

    @staticmethod
    def _actionable_rank(
        row: Mapping[str, Any],
        ordered: list[Mapping[str, Any]],
    ) -> int | None:
        """Return research rank, or deterministic scan rank for a follower."""
        try:
            persisted_rank = int(row.get("rank"))
        except (TypeError, ValueError, OverflowError):
            return None
        if persisted_rank > 0:
            return persisted_rank
        if persisted_rank != 0:
            return None
        candidate_id = str(row.get("candidate_id") or "").strip()
        for position, candidate in enumerate(ordered, start=1):
            if candidate is row or (
                candidate_id
                and str(candidate.get("candidate_id") or "").strip() == candidate_id
            ):
                return position
        return None


    def _scan_state(self) -> dict[str, Any]:
        with self.store._lock:
            row = self.store.connection.execute(
                "SELECT signal_scan_cursor,signal_scan_ranking_run_id,"
                "next_signal_scan_start_rank,next_signal_scan_end_rank,"
                "signal_scan_cycle_id,signal_scan_candidate_universe_hash,"
                "signal_scan_cycle_started_at,signal_scan_cycle_completed_at,"
                "signal_scan_cycle_complete,signal_scan_checked_this_cycle,"
                "signal_scan_remaining_this_cycle,signal_scan_coverage_percentage,"
                "signal_scan_skip_reasons_json,signal_scan_reason_counts_json,"
                "signal_scan_status "
                "FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
        return dict(row) if row is not None else {}

    def _record_start(self, service: CanaryService, timestamp: datetime) -> None:
        """Persist the running heartbeat before evaluating any decision."""
        try:
            service.record_autonomous_decision(
                next_decision="EVALUATING_CANDIDATES",
                worker_status="RUNNING",
                timestamp=timestamp,
                last_tick_started_at=timestamp.isoformat(),
                consecutive_failures=self._consecutive_failures,
                next_retry_at=(
                    self._next_retry_at.isoformat()
                    if self._next_retry_at is not None
                    else None
                ),
                candidates_evaluated=0,
                signals_generated=0,
                orders_attempted=0,
            )
        except Exception:
            # A transient dashboard publication failure must not terminate the
            # decision loop.  The completion record below retries persistence.
            return

    def _record_finish(
        self,
        service: CanaryService,
        *,
        timestamp: datetime,
        next_decision: str,
        blocker: str | None = None,
        signal_id: str | None = None,
        worker_status: str = "IDLE",
        candidates_evaluated: int = 0,
        signals_generated: int = 0,
        orders_attempted: int = 0,
        candidates_ranked: int = 0,
        candidates_signal_checked: int = 0,
        candidates_no_signal: int = 0,
        actionable_candidates_found: int = 0,
        selected_actionable_candidate: str | None = None,
        selected_actionable_rank: int | None = None,
        selected_actionable_score: float | None = None,
        signal_scan_cursor: int | None | object = _UNSET,
        signal_scan_ranking_run_id: str | None | object = _UNSET,
        next_signal_scan_start_rank: int | None | object = _UNSET,
        next_signal_scan_end_rank: int | None | object = _UNSET,
        signal_scan_cycle_id: str | None | object = _UNSET,
        signal_scan_candidate_universe_hash: str | None | object = _UNSET,
        signal_scan_skip_reasons_json: str | None | object = _UNSET,
        signal_scan_reason_counts_json: str | None | object = _UNSET,
        signal_scan_cycle_started_at: str | None | object = _UNSET,
        signal_scan_cycle_completed_at: str | None | object = _UNSET,
        signal_scan_cycle_complete: int | bool | None | object = _UNSET,
        signal_scan_checked_this_cycle: int | None | object = _UNSET,
        signal_scan_remaining_this_cycle: int | None | object = _UNSET,
        signal_scan_coverage_percentage: float | None | object = _UNSET,
        signal_scan_status: str | None | object = _UNSET,
        signal_scan_checked_keys: list[Mapping[str, Any]] | None = None,
        error_code: str | None = None,
    ) -> None:
        failed = error_code is not None
        if failed:
            self._consecutive_failures += 1
            delay = min(
                self._MAX_RETRY_DELAY_SECONDS,
                max(1.0, 2.0 ** max(0, self._consecutive_failures - 1)),
            )
            self._next_retry_at = timestamp + timedelta(seconds=delay)
        else:
            self._consecutive_failures = 0
            self._next_retry_at = None

        def nonnegative(value: Any) -> int | None | object:
            if value is _UNSET:
                return _UNSET
            try:
                parsed = int(value) if value is not None else None
            except (TypeError, ValueError):
                parsed = None
            return max(0, parsed) if parsed is not None else None

        def text(value: Any) -> str | None | object:
            if value is _UNSET:
                return _UNSET
            return str(value) if value is not None else None

        def complete(value: Any) -> int | None | object:
            if value is _UNSET:
                return _UNSET
            return int(bool(value)) if value is not None else None

        def coverage(value: Any) -> float | None | object:
            if value is _UNSET:
                return _UNSET
            try:
                parsed = float(value) if value is not None else None
            except (TypeError, ValueError):
                parsed = None
            return (
                min(100.0, max(0.0, parsed))
                if parsed is not None and math.isfinite(parsed)
                else 0.0
            )

        def reason_counts(value: Any) -> str | None | object:
            if value is _UNSET:
                return _UNSET
            normalized = self._normalize_reason_counts(value)
            return json.dumps(normalized, sort_keys=True, separators=(",", ":"))[:4096]

        def skip_reasons(value: Any) -> str | None | object:
            if value is _UNSET:
                return _UNSET
            return str(value or "{}")[:4096]

        try:
            service.record_autonomous_decision(
                next_decision=next_decision,
                blocker=blocker,
                signal_id=signal_id,
                worker_status=worker_status,
                timestamp=timestamp,
                last_tick_completed_at=timestamp.isoformat(),
                last_successful_tick=timestamp.isoformat() if not failed else None,
                last_error_code=error_code,
                consecutive_failures=self._consecutive_failures,
                next_retry_at=(
                    self._next_retry_at.isoformat()
                    if self._next_retry_at is not None
                    else None
                ),
                candidates_evaluated=max(0, int(candidates_evaluated)),
                signals_generated=max(0, int(signals_generated)),
                orders_attempted=max(0, int(orders_attempted)),
                candidates_ranked=max(0, int(candidates_ranked)),
                signal_scan_cycle_id=text(signal_scan_cycle_id),
                signal_scan_candidate_universe_hash=text(
                    signal_scan_candidate_universe_hash
                ),
                signal_scan_cycle_started_at=text(signal_scan_cycle_started_at),
                signal_scan_cycle_completed_at=text(signal_scan_cycle_completed_at),
                signal_scan_cycle_complete=complete(signal_scan_cycle_complete),
                signal_scan_checked_this_cycle=nonnegative(
                    signal_scan_checked_this_cycle
                ),
                signal_scan_remaining_this_cycle=nonnegative(
                    signal_scan_remaining_this_cycle
                ),
                signal_scan_coverage_percentage=coverage(
                    signal_scan_coverage_percentage
                ),
                signal_scan_skip_reasons_json=skip_reasons(
                    signal_scan_skip_reasons_json
                ),
                signal_scan_reason_counts_json=reason_counts(
                    signal_scan_reason_counts_json
                ),
                signal_scan_status=text(signal_scan_status),
                signal_scan_checked_keys=signal_scan_checked_keys,
                candidates_signal_checked=max(0, int(candidates_signal_checked)),
                candidates_no_signal=max(0, int(candidates_no_signal)),
                actionable_candidates_found=max(0, int(actionable_candidates_found)),
                selected_actionable_candidate=selected_actionable_candidate,
                selected_actionable_rank=selected_actionable_rank,
                selected_actionable_score=selected_actionable_score,
                signal_scan_cursor=nonnegative(signal_scan_cursor),
                signal_scan_ranking_run_id=text(signal_scan_ranking_run_id),
                next_signal_scan_start_rank=nonnegative(
                    next_signal_scan_start_rank
                ),
                next_signal_scan_end_rank=nonnegative(next_signal_scan_end_rank),
            )
        except Exception:
            return

    def _legacy_tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Rank once, then inspect one bounded diversity-aware signal window."""
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "decision": "DECISION_ALREADY_IN_PROGRESS"}
        timestamp = ensure_utc(now or self.clock())
        service: CanaryService | None = None
        ranking: Mapping[str, Any] = {}
        candidates_evaluated = 0
        signals_generated = 0
        orders_attempted = 0
        position_reconciliation: Mapping[str, Any] = {}
        position_management: Mapping[str, Any] = {}
        position_blocker: str | None = None
        candidates_ranked = 0
        candidates_signal_checked = 0
        candidates_no_signal = 0
        actionable_candidates_found = 0
        selected_actionable_candidate: str | None = None
        selected_actionable_rank: int | None = None
        selected_actionable_score: float | None = None
        try:
            previous_scan_projection = self._scan_state()
        except Exception:
            previous_scan_projection = {}

        def previous_int(name: str, default: int | None = None) -> int | None:
            try:
                value = previous_scan_projection.get(name)
                return int(value) if value is not None else default
            except (TypeError, ValueError):
                return default

        signal_scan_cursor = previous_int("signal_scan_cursor", 0) or 0
        signal_scan_ranking_run_id = (
            str(previous_scan_projection.get("signal_scan_ranking_run_id") or "").strip()
            or None
        )
        next_signal_scan_start_rank = previous_int("next_signal_scan_start_rank")
        next_signal_scan_end_rank = previous_int("next_signal_scan_end_rank")
        signal_scan_cycle_id = (
            str(previous_scan_projection.get("signal_scan_cycle_id") or "").strip()
            or None
        )
        signal_scan_candidate_universe_hash = (
            str(
                previous_scan_projection.get(
                    "signal_scan_candidate_universe_hash"
                )
                or ""
            ).strip()
            or None
        )
        signal_scan_cycle_started_at = (
            str(previous_scan_projection.get("signal_scan_cycle_started_at") or "")
            or None
        )
        signal_scan_cycle_completed_at = (
            str(previous_scan_projection.get("signal_scan_cycle_completed_at") or "")
            or None
        )
        signal_scan_cycle_complete = int(
            bool(previous_int("signal_scan_cycle_complete", 0))
        )
        signal_scan_checked_this_cycle = previous_int(
            "signal_scan_checked_this_cycle", 0
        ) or 0
        signal_scan_remaining_this_cycle = previous_int(
            "signal_scan_remaining_this_cycle", 0
        ) or 0
        signal_scan_coverage_percentage = self._finite(
            previous_scan_projection.get("signal_scan_coverage_percentage"),
            0.0,
        )
        signal_scan_skip_reasons_json = str(
            previous_scan_projection.get("signal_scan_skip_reasons_json") or "{}"
        )
        signal_scan_reason_counts = self._normalize_reason_counts(
            previous_scan_projection.get("signal_scan_reason_counts_json")
        )
        signal_scan_status = str(
            previous_scan_projection.get("signal_scan_status") or "UNKNOWN"
        )
        signal_scan_checked_keys: list[Mapping[str, Any]] = []
        scan_checked_set: set[tuple[str, str]] = set()
        scan_universe_keys: set[tuple[str, str]] = set()
        ordered: list[Mapping[str, Any]] = []
        scan_rows: list[Mapping[str, Any]] = []
        update_scan_projection_fn: Callable[..., None] | None = None
        scan_payload_fn: Callable[[], dict[str, Any]] | None = None
        try:
            service = CanaryService(self.store, clock=self.clock)
            # Position tables are part of the worker's durable startup
            # contract.  Initialize them before checking control state so a
            # disarmed worker can reconstruct inventory without constructing a
            # venue or attempting an exit.
            _ensure_schema(service)
            self._record_start(service, timestamp)
            control = service.authoritative_status()
            control_state = str(control.get("micro_live_canary") or "").upper()
            controller_lease = self._acquire_controller_lease(timestamp)
            if isinstance(controller_lease, Mapping):
                generation = controller_lease.get("generation")
                setattr(service, "controller_owner_id", self.controller_owner_id)
                setattr(service, "controller_lease_generation", generation)
                setattr(service, "controller_generation", generation)
            if control_state == "DISARMED":
                # Rebuild the durable ownership projection from confirmed
                # ledger/risk fills while disarmed.  This is deliberately
                # venue-free: no network read or exit submission is allowed
                # until the operator arms the worker again.
                _sync_entry_lots(service, timestamp)
            position_venue: Any | None = None
            # A disarmed worker must not construct a venue or invoke any sink.
            # Durable position obligations remain visible, but are resumed only
            # after the operator arms the controller again.
            if control_state != "DISARMED" and self._position_obligations(service):
                try:
                    service.require_current_credential_binding()
                except CanaryBlocked as exc:
                    reason = str(exc)
                    position_reconciliation = {
                        "status": "DEGRADED",
                        "reconciled": 0,
                        "blocked": 1,
                        "requests": [{"status": "UNKNOWN", "reason": reason}],
                        "entries": [],
                    }
                    position_management = {
                        "status": "BLOCKED",
                        "submitted": 0,
                        "blocked": reason,
                        "positions": [],
                    }
                    position_blocker = reason
                else:
                    position_venue = self.venue_factory()
                    position_reconciliation = reconcile_pending(
                        service,
                        position_venue,
                        allow_test_venue=self.allow_test_venue,
                    )
                    position_management = manage_positions(
                        service,
                        position_venue,
                        allow_test_venue=self.allow_test_venue,
                    )
                    if str(position_reconciliation.get("status") or "").upper() == "DEGRADED":
                        position_blocker = "CANARY_RECONCILIATION_PROVIDER_ERROR"
                    elif (
                        str(position_management.get("status") or "").upper() == "BLOCKED"
                        and position_management.get("blocked")
                    ):
                        # Do not admit new BUY capacity while an originating
                        # policy's required exit is blocked.
                        position_blocker = "CANARY_POSITION_MANAGEMENT_BLOCKED"
                    elif self._unresolved_position_obligations(service):
                        # Reservations and attempts without a durable order
                        # identity cannot be safely retried or released here.
                        position_blocker = "CANARY_POSITION_OBLIGATION_UNRESOLVED"
            else:
                position_reconciliation = {
                    "status": "IDLE",
                    "reconciled": 0,
                    "blocked": 0,
                    "requests": [],
                    "entries": [],
                }
                position_management = {
                    "status": "IDLE",
                    "submitted": 0,
                    "blocked": [],
                    "positions": [],
                }
            ranker = CandidateCanaryRanker(
                self.store,
                service=service,
                clock=self.clock,
            )
            ranking = {}
            for attempt in range(3):
                try:
                    ranking = ranker.evaluate_and_select(timestamp)
                    break
                except Exception as exc:
                    if (
                        getattr(exc, "error_code", None)
                        != "LIFECYCLE_SNAPSHOT_CHANGED"
                        or attempt >= 2
                    ):
                        raise
            signal_scan_ranking_run_id = (
                str(ranking.get("ranking_run_id") or "").strip()
                if isinstance(ranking, Mapping)
                else None
            ) or None
            rows = self._current_rankings(
                ranker,
                ranking_run_id=signal_scan_ranking_run_id,
                timestamp=timestamp,
            )
            ordered = self._diversity_order(rows)
            try:
                candidates_ranked = max(0, int(ranking.get("rankable_count")))
            except (TypeError, ValueError, OverflowError):
                candidates_ranked = len(
                    ranking.get("rankings")
                    if isinstance(ranking.get("rankings"), list)
                    else []
                )
            try:
                authoritative_eligible_count = max(
                    0,
                    int(ranking.get("eligible_count")),
                )
            except (TypeError, ValueError, OverflowError):
                # Older ranking payloads may omit the count.  A non-empty
                # independently scanable universe still proves eligibility;
                # an empty one preserves the true-empty behavior.
                authoritative_eligible_count = len(ordered)
            try:
                authoritative_rankable_count = max(
                    0,
                    int(ranking.get("rankable_count")),
                )
            except (TypeError, ValueError, OverflowError):
                authoritative_rankable_count = candidates_ranked
            no_persisted_ranking_fallback = (
                authoritative_eligible_count > 0
                and authoritative_rankable_count == 0
                and not signal_scan_ranking_run_id
                and not ranking.get("rankings")
            )
            # When ranking has no persisted rows, retain the fallback scan's
            # compatibility no-signal outcome.  The durable projection still
            # records any remaining candidates for the next tick.

            previous = self._scan_state()
            previous_cycle_id = str(previous.get("signal_scan_cycle_id") or "").strip()
            try:
                previous_complete = int(previous.get("signal_scan_cycle_complete") or 0) == 1
            except (TypeError, ValueError):
                previous_complete = False
            if previous_cycle_id and not previous_complete:
                signal_scan_cycle_id = previous_cycle_id
                signal_scan_cycle_started_at = (
                    str(previous.get("signal_scan_cycle_started_at") or timestamp.isoformat())
                )
                signal_scan_reason_counts = self._normalize_reason_counts(
                    previous.get("signal_scan_reason_counts_json")
                )
                with self.store._lock:
                    checked_rows = self.store.connection.execute(
                        "SELECT candidate_id,qualification_hash "
                        "FROM canary_signal_scan_checked WHERE cycle_id=?",
                        (signal_scan_cycle_id,),
                    ).fetchall()
                scan_checked_set = {
                    (
                        str(item["candidate_id"] or "").strip(),
                        str(item["qualification_hash"] or "").strip(),
                    )
                    for item in checked_rows
                    if str(item["candidate_id"] or "").strip()
                    and str(item["qualification_hash"] or "").strip()
                }
            else:
                signal_scan_cycle_id = "scan-" + uuid.uuid4().hex[:24]
                signal_scan_cycle_started_at = timestamp.isoformat()
                signal_scan_reason_counts = self._normalize_reason_counts({})
                scan_checked_set = set()
            scan_universe_keys = {
                (
                    str(row.get("candidate_id") or "").strip(),
                    str(row.get("qualification_hash") or "").strip(),
                )
                for row in ordered
                if str(row.get("candidate_id") or "").strip()
                and str(row.get("qualification_hash") or "").strip()
            }
            scan_checked_set.intersection_update(scan_universe_keys)
            signal_scan_candidate_universe_hash = hashlib.sha256(
                json.dumps(
                    sorted(
                        (
                            {
                                "candidate_id": candidate_id,
                                "qualification_hash": qualification_hash,
                            }
                            for candidate_id, qualification_hash in scan_universe_keys
                        ),
                        key=lambda item: (
                            item["candidate_id"],
                            item["qualification_hash"],
                        ),
                    ),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()

            def remaining_rows() -> list[Mapping[str, Any]]:
                return [
                    row
                    for row in ordered
                    if (
                        str(row.get("candidate_id") or "").strip(),
                        str(row.get("qualification_hash") or "").strip(),
                    )
                    not in scan_checked_set
                ]

            remaining = remaining_rows()
            scan_rows = remaining[: self._SCAN_CAP]
            if scan_rows:
                first_remaining = next(
                    (
                        index
                        for index, row in enumerate(ordered)
                        if row is scan_rows[0]
                    ),
                    0,
                )
                scan_start = first_remaining
                scan_end = min(first_remaining + self._SCAN_CAP, len(ordered))
            else:
                scan_start = scan_end = 0
            signal_scan_cursor = scan_start
            next_signal_scan_start_rank = scan_start
            next_signal_scan_end_rank = scan_end

            def update_scan_projection(*, force_complete: bool = False, actionable: bool = False) -> None:
                nonlocal signal_scan_checked_this_cycle
                nonlocal signal_scan_remaining_this_cycle
                nonlocal signal_scan_coverage_percentage
                nonlocal signal_scan_cycle_complete
                nonlocal signal_scan_cycle_completed_at
                nonlocal signal_scan_cursor
                nonlocal next_signal_scan_start_rank
                nonlocal next_signal_scan_end_rank
                nonlocal signal_scan_skip_reasons_json
                nonlocal signal_scan_status
                remaining_now = remaining_rows()
                signal_scan_checked_this_cycle = len(scan_checked_set)
                signal_scan_remaining_this_cycle = len(remaining_now)
                total = len(scan_universe_keys)
                # An empty universe is not a successfully checked universe.
                # Keep its prerequisite progress measurable as 0/0 and
                # distinguish it from a completed no-edge scan.
                signal_scan_coverage_percentage = (
                    100.0 * signal_scan_checked_this_cycle / total if total else 0.0
                )
                complete = force_complete or not remaining_now
                signal_scan_cycle_complete = int(complete)
                signal_scan_cycle_completed_at = (
                    timestamp.isoformat() if complete else None
                )
                if remaining_now:
                    first = next(
                        (
                            index
                            for index, row in enumerate(ordered)
                            if row is remaining_now[0]
                        ),
                        0,
                    )
                    signal_scan_cursor = first
                    next_signal_scan_start_rank = first
                    next_signal_scan_end_rank = min(first + self._SCAN_CAP, len(ordered))
                else:
                    signal_scan_cursor = 0
                    next_signal_scan_start_rank = 0
                    next_signal_scan_end_rank = (
                        min(self._SCAN_CAP, len(ordered)) if actionable else 0
                    )
                skips = {
                    reason: min(
                        10_000,
                        max(0, int(self._last_scan_skip_reasons.get(reason, 0))),
                    )
                    for reason in self._SCAN_SKIP_REASONS
                }
                skips["CYCLE_REMAINDER"] = min(
                    10_000,
                    max(0, signal_scan_remaining_this_cycle),
                )
                signal_scan_skip_reasons_json = json.dumps(
                    skips, sort_keys=True, separators=(",", ":")
                )
                signal_scan_status = (
                    "NO_ELIGIBLE_CANDIDATES"
                    if authoritative_eligible_count == 0
                    else "COMPLETE_NO_SIGNAL"
                    if complete and not actionable
                    else "COMPLETE_ACTIONABLE"
                    if complete
                    else "IN_PROGRESS"
                )

            update_scan_projection_fn = update_scan_projection
            update_scan_projection()

            def scan_payload() -> dict[str, Any]:
                return {
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
                    "signal_scan_cycle_id": signal_scan_cycle_id,
                    "signal_scan_candidate_universe_hash": signal_scan_candidate_universe_hash,
                    "signal_scan_cycle_started_at": signal_scan_cycle_started_at,
                    "signal_scan_cycle_completed_at": signal_scan_cycle_completed_at,
                    "signal_scan_cycle_complete": signal_scan_cycle_complete,
                    "signal_scan_checked_this_cycle": signal_scan_checked_this_cycle,
                    "signal_scan_remaining_this_cycle": signal_scan_remaining_this_cycle,
                    "signal_scan_coverage_percentage": signal_scan_coverage_percentage,
                    "signal_scan_skip_reasons_json": json.loads(
                        signal_scan_skip_reasons_json
                    ),
                    "signal_scan_reason_counts_json": dict(signal_scan_reason_counts),
                    "signal_scan_status": signal_scan_status,
                    "signal_scan_checked_keys": signal_scan_checked_keys,
                }
            scan_payload_fn = scan_payload

            def finish(
                *,
                next_decision: str,
                blocker: str | None = None,
                signal_id: str | None = None,
                worker_status: str = "IDLE",
                error_code: str | None = None,
            ) -> None:
                payload = scan_payload()
                payload["signal_scan_skip_reasons_json"] = signal_scan_skip_reasons_json
                payload["signal_scan_reason_counts_json"] = json.dumps(
                    signal_scan_reason_counts,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision=next_decision,
                    blocker=blocker,
                    signal_id=signal_id,
                    worker_status=worker_status,
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                    orders_attempted=orders_attempted,
                    error_code=error_code,
                    **payload,
                )

            def result(**extra: Any) -> dict[str, Any]:
                return {
                    **extra,
                    "position_reconciliation": dict(position_reconciliation),
                    "orders_attempted": orders_attempted,
                    "position_management": dict(position_management),
                    "position_blocker": position_blocker,
                    **scan_payload(),
                    "ranking": ranking,
                }

            if control.get("micro_live_canary") == "KILLED":
                finish(
                    next_decision="KILL_LATCHED",
                    blocker="CANARY_KILLED",
                    worker_status="KILLED",
                )
                return result(
                    status="BLOCKED",
                    decision="KILL_LATCHED",
                    blocker="CANARY_KILLED",
                )
            if control.get("micro_live_canary") != AUTONOMOUS_MICRO_LIVE:
                finish(
                    next_decision="ENABLE AUTO CANARY",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                )
                return result(
                    status="DISABLED",
                    decision="AUTONOMOUS_CANARY_DISABLED",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                )
            if position_blocker is not None:
                finish(
                    next_decision="WAIT_FOR_POSITION_RECONCILIATION",
                    blocker=position_blocker,
                    worker_status="DEGRADED",
                    error_code="CANARY_POSITION_RECONCILIATION_FAILED",
                )
                return result(
                    status="BLOCKED",
                    decision="POSITION_RECONCILIATION_BLOCKED",
                    blocker=position_blocker,
                )
            if authoritative_eligible_count == 0 or not ordered:
                update_scan_projection(force_complete=True)
                if authoritative_eligible_count == 0:
                    blocker = "NO_ELIGIBLE_CANDIDATES"
                    finish(
                        next_decision="WAIT_FOR_RANKABLE_CANDIDATE",
                        blocker=blocker,
                    )
                    return result(
                        status="BLOCKED",
                        decision=blocker,
                        blocker=blocker,
                    )

                # Eligibility is authoritative and independent from ranking
                # evidence.  An eligible universe with no rankable rows is
                # a truthful no-signal outcome, not an empty universe.
                blocker = "NO_ACTIONABLE_SIGNAL"
                finish(
                    next_decision=(
                        "WAIT_FOR_RANKABLE_CANDIDATE"
                        if authoritative_rankable_count == 0
                        else "WAIT_FOR_FRESH_ACTIONABLE_SIGNAL"
                    ),
                    blocker=blocker,
                )
                return result(
                    status="NO_SIGNAL",
                    decision=blocker,
                    blocker=blocker,
                )

            ready: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
            unknown_signal_id: str | None = None
            unknown_seen = False
            evaluation_blocker: str | None = None
            for row in scan_rows:
                candidate_id = str(row.get("candidate_id") or "").strip()
                qualification_hash = str(row.get("qualification_hash") or "").strip()
                evaluation = service.evaluate_signal(
                    candidate_id,
                    cycle_id=signal_scan_cycle_id,
                )
                signal = (
                    evaluation.get("signal")
                    if isinstance(evaluation, Mapping)
                    else None
                )
                candidates_evaluated += 1
                reason = self._evaluation_reason(evaluation, signal)
                candidate_blocker = self._evaluation_blocker(evaluation, signal)
                if (
                    evaluation_blocker is None
                    or (
                        candidate_blocker in self._EXACT_DATA_BLOCKER_REASONS
                        and evaluation_blocker not in self._EXACT_DATA_BLOCKER_REASONS
                    )
                ):
                    evaluation_blocker = candidate_blocker or evaluation_blocker
                self._increment_reason_count(signal_scan_reason_counts, reason)
                candidates_signal_checked += 1
                scan_checked_set.add((candidate_id, qualification_hash))
                signal_scan_checked_keys.append(
                    {
                        "cycle_id": signal_scan_cycle_id,
                        "candidate_id": candidate_id,
                        "qualification_hash": qualification_hash,
                        "checked_at": timestamp.isoformat(),
                        "rank_at_check": self._actionable_rank(row, ordered),
                        "ranking_run_id": signal_scan_ranking_run_id,
                    }
                )
                if isinstance(signal, Mapping):
                    signals_generated += 1
                status = (
                    str(signal.get("status") or "").upper()
                    if isinstance(signal, Mapping)
                    else "NONE"
                )
                signal_id = (
                    str(signal.get("signal_id") or "").strip()
                    if isinstance(signal, Mapping)
                    else ""
                )
                if status == "UNKNOWN":
                    unknown_seen = True
                    if signal_id:
                        unknown_signal_id = unknown_signal_id or signal_id
                        self._unknown_signal_ids.add(signal_id)
                    candidates_no_signal += 1
                    continue
                if (
                    reason == "READY_SIGNAL"
                    and status == "READY"
                    and signal_id
                    and signal_id not in self._unknown_signal_ids
                    and str(signal.get("candidate_id") or candidate_id) == candidate_id
                ):
                    ready.append((row, signal))
                    continue
                candidates_no_signal += 1


            actionable_candidates_found = len(ready)
            if actionable_candidates_found:
                row, signal = min(
                    ready,
                    key=lambda item: self._ready_priority(item[0], item[1]),
                )
                selected_actionable_candidate = str(row.get("candidate_id") or "")
                selected_actionable_rank = self._actionable_rank(row, ordered)
                selected_actionable_score = self._finite(
                    row.get("total_score"),
                    0.0,
                )
                signal_id = str(signal.get("signal_id") or "")
                update_scan_projection(actionable=True)
            else:
                signal_id = unknown_signal_id
                # Projection must be updated before interpreting the outcome:
                # a bounded window with work left is not a genuine no-edge
                # decision.
                update_scan_projection()
                if unknown_seen:
                    blocker = "UNKNOWN_NO_RETRY"
                    decision = blocker
                    next_decision = "WAIT_FOR_FRESH_ACTIONABLE_SIGNAL"
                    result_status = "BLOCKED"
                elif (
                    signal_scan_status == "IN_PROGRESS"
                    and not no_persisted_ranking_fallback
                ):
                    blocker = evaluation_blocker or "SCAN_IN_PROGRESS"
                    decision = blocker
                    next_decision = "CONTINUE_SIGNAL_SCAN"
                    result_status = "BLOCKED" if evaluation_blocker else "IN_PROGRESS"
                elif evaluation_blocker and not no_persisted_ranking_fallback:
                    blocker = evaluation_blocker
                    decision = blocker
                    next_decision = "WAIT_FOR_DATA_REFRESH"
                    result_status = "BLOCKED"
                else:
                    blocker = "NO_ACTIONABLE_SIGNAL"
                    decision = blocker
                    next_decision = "WAIT_FOR_FRESH_ACTIONABLE_SIGNAL"
                    result_status = "NO_SIGNAL"
                finish(
                    next_decision=next_decision,
                    blocker=blocker,
                    signal_id=signal_id,
                    worker_status="DEGRADED" if unknown_seen else "IDLE",
                    error_code=(
                        "CANARY_SUBMISSION_UNKNOWN"
                        if unknown_seen
                        else None
                    ),
                )
                return result(
                    status=result_status,
                    decision=decision,
                    blocker=blocker,
                    candidate_id=(
                        str(scan_rows[0].get("candidate_id") or "")
                        if scan_rows
                        else None
                    ),
                    signal_id=signal_id,
                )

            if not service.credentials.configured(allow_environment=False):
                finish(
                    next_decision="WAIT_FOR_CREDENTIALS",
                    blocker="CREDENTIALS_NOT_CONFIGURED",
                    signal_id=signal_id,
                )
                return result(
                    status="BLOCKED",
                    decision="CREDENTIALS_NOT_CONFIGURED",
                    blocker="CREDENTIALS_NOT_CONFIGURED",
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                )
            try:
                service.bind_autonomous_actionable_candidate(
                    selected_actionable_candidate,
                    ranking_run_id=signal_scan_ranking_run_id or "",
                    signal_id=signal_id,
                )
            except CanaryBlocked as exc:
                blocker = str(exc)
                finish(
                    next_decision="WAIT_FOR_NEXT_DECISION",
                    blocker=blocker,
                    signal_id=signal_id,
                )
                return result(
                    status="BLOCKED",
                    decision=blocker,
                    blocker=blocker,
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                )
            if position_venue is None:
                try:
                    service.require_current_credential_binding()
                    position_venue = self.venue_factory()
                except CanaryBlocked as exc:
                    blocker = str(exc)
                    finish(
                        next_decision="WAIT_FOR_CREDENTIALS",
                        blocker=blocker,
                        signal_id=signal_id,
                    )
                    return result(
                        status="BLOCKED",
                        decision=blocker,
                        blocker=blocker,
                        candidate_id=selected_actionable_candidate,
                        signal_id=signal_id,
                    )
            venue = position_venue
            try:
                orders_attempted = 1
                submission = service.submit_signal(
                    signal_id,
                    venue=venue,
                    allow_test_venue=self.allow_test_venue,
                )
            except CanaryBlocked as exc:
                blocker = str(exc)
                if blocker == "CANARY_SUBMISSION_UNKNOWN":
                    self._unknown_signal_ids.add(signal_id)
                decision = "UNKNOWN_NO_RETRY" if blocker == "CANARY_SUBMISSION_UNKNOWN" else blocker
                finish(
                    next_decision=(
                        "UNKNOWN_NO_RETRY"
                        if decision == "UNKNOWN_NO_RETRY"
                        else "WAIT_FOR_NEXT_DECISION"
                    ),
                    blocker=decision,
                    signal_id=signal_id,
                    worker_status=(
                        "DEGRADED"
                        if blocker == "CANARY_SUBMISSION_UNKNOWN"
                        else "IDLE"
                    ),
                    error_code=(
                        "CANARY_SUBMISSION_UNKNOWN"
                        if blocker == "CANARY_SUBMISSION_UNKNOWN"
                        else None
                    ),
                )
                return result(
                    status="BLOCKED",
                    decision=decision,
                    blocker=blocker,
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                )
            except Exception as exc:
                self._unknown_signal_ids.add(signal_id)
                finish(
                    next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                    blocker="AUTONOMOUS_WORKER_EXCEPTION",
                    worker_status="DEGRADED",
                    signal_id=signal_id,
                    error_code="AUTONOMOUS_WORKER_EXCEPTION",
                )
                return result(
                    status="ERROR",
                    decision="AUTONOMOUS_WORKER_EXCEPTION",
                    blocker="AUTONOMOUS_WORKER_EXCEPTION",
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                    error_type=type(exc).__name__,
                )
            submission_outcome = (
                str(submission.get("execution_status") or "").strip().upper()
                if isinstance(submission, Mapping)
                else ""
            )
            if submission_outcome == "REJECTED":
                finish(
                    next_decision="WAIT_FOR_NEXT_DECISION",
                    blocker="CANARY_SUBMISSION_REJECTED",
                    signal_id=signal_id,
                )
                return result(
                    status="REJECTED",
                    decision="NOT_SUBMITTED",
                    blocker="CANARY_SUBMISSION_REJECTED",
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                    submission=dict(submission),
                )
            if submission_outcome not in self._SUBMISSION_SUCCESS_STATUSES:
                # ``submit_signal`` returns the durable ledger/signal outcome.
                # Never infer success from a transport response or a normal
                # return when that authoritative outcome is absent/unknown.
                self._unknown_signal_ids.add(signal_id)
                finish(
                    next_decision="UNKNOWN_NO_RETRY",
                    blocker="UNKNOWN_NO_RETRY",
                    signal_id=signal_id,
                    worker_status="DEGRADED",
                    error_code="CANARY_SUBMISSION_UNKNOWN",
                )
                return result(
                    status="BLOCKED",
                    decision="UNKNOWN_NO_RETRY",
                    blocker="UNKNOWN_NO_RETRY",
                    candidate_id=selected_actionable_candidate,
                    signal_id=signal_id,
                )
            finish(
                next_decision="WAIT_FOR_NEXT_DECISION",
                signal_id=signal_id,
            )
            return result(
                status="SUBMITTED",
                decision="SUBMITTED",
                candidate_id=selected_actionable_candidate,
                signal_id=signal_id,
                submission=dict(submission),
            )
        except CanaryBlocked as exc:
            blocker = str(exc) or "CANARY_BLOCKED"
            persisted_payload: dict[str, Any] = {}
            if update_scan_projection_fn is not None and scan_payload_fn is not None:
                try:
                    update_scan_projection_fn()
                    persisted_payload = scan_payload_fn()
                    persisted_payload["signal_scan_skip_reasons_json"] = (
                        signal_scan_skip_reasons_json
                    )
                except Exception:
                    persisted_payload = {}
            if service is not None:
                try:
                    if persisted_payload:
                        self._record_finish(
                            service,
                            timestamp=timestamp,
                            next_decision=blocker,
                            blocker=blocker,
                            worker_status="IDLE",
                            candidates_evaluated=candidates_evaluated,
                            signals_generated=signals_generated,
                            orders_attempted=orders_attempted,
                            error_code=None,
                            **persisted_payload,
                        )
                    else:
                        self._record_finish(
                            service,
                            timestamp=timestamp,
                            next_decision=blocker,
                            blocker=blocker,
                            worker_status="IDLE",
                            candidates_evaluated=candidates_evaluated,
                            signals_generated=signals_generated,
                            orders_attempted=orders_attempted,
                            signal_scan_checked_keys=signal_scan_checked_keys,
                            error_code=None,
                        )
                except Exception:
                    pass
            return {
                "status": "BLOCKED",
                "decision": blocker,
                "blocker": blocker,
                "error_type": type(exc).__name__,
                **persisted_payload,
                "ranking": ranking,
            }
        except BaseException as exc:
            persisted_payload: dict[str, Any] = {}
            if update_scan_projection_fn is not None and scan_payload_fn is not None:
                try:
                    update_scan_projection_fn()
                    persisted_payload = scan_payload_fn()
                    # ``scan_payload`` exposes the diagnostic as an object,
                    # while the durable state contract stores its JSON text.
                    persisted_payload["signal_scan_skip_reasons_json"] = (
                        signal_scan_skip_reasons_json
                    )
                except Exception:
                    persisted_payload = {}
            if service is not None:
                if persisted_payload:
                    self._record_finish(
                        service,
                        timestamp=timestamp,
                        next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                        blocker="AUTONOMOUS_WORKER_EXCEPTION",
                        worker_status="DEGRADED",
                        candidates_evaluated=candidates_evaluated,
                        signals_generated=signals_generated,
                        orders_attempted=orders_attempted,
                        error_code="AUTONOMOUS_WORKER_EXCEPTION",
                        **persisted_payload,
                    )
                else:
                    # A failure before cycle setup must not overwrite a
                    # previously persisted incomplete cycle.
                    self._record_finish(
                        service,
                        timestamp=timestamp,
                        next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                        worker_status="DEGRADED",
                        candidates_evaluated=candidates_evaluated,
                        signals_generated=signals_generated,
                        orders_attempted=orders_attempted,
                        signal_scan_checked_keys=signal_scan_checked_keys,
                        error_code="AUTONOMOUS_WORKER_EXCEPTION",
                    )
            if persisted_payload:
                return {
                    "status": "ERROR",
                    "decision": "AUTONOMOUS_WORKER_EXCEPTION",
                    "blocker": "AUTONOMOUS_WORKER_EXCEPTION",
                    "error_type": type(exc).__name__,
                    **persisted_payload,
                    "ranking": ranking,
                }
            return {
                "status": "ERROR",
                "decision": "AUTONOMOUS_WORKER_EXCEPTION",
                "blocker": "AUTONOMOUS_WORKER_EXCEPTION",
                "error_type": type(exc).__name__,
                "ranking": ranking,
            }
        finally:
            # ``run`` owns the lease for the life of the worker.  Keep it
            # across legacy decision windows so the next tick renews the same
            # generation; the run-level finally releases it on shutdown.
            self._decision_lock.release()

    @staticmethod
    def _rolling_member_context(
        selection: Mapping[str, Any],
        member: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Build the exact immutable lineage carried by every rolling action."""
        strategy_version_id = str(member.get("strategy_version_id") or "").strip()
        research_trial_id = str(member.get("research_trial_id") or "").strip()
        candidate_id = str(member.get("candidate_id") or "").strip()
        evidence_window_id = str(member.get("evidence_window_id") or "").strip()
        evidence_digest = str(member.get("evidence_digest") or "").strip()
        if not strategy_version_id or not research_trial_id or not candidate_id:
            raise CanaryBlocked("ROLLING_LINEAGE_INCOMPLETE")
        position_state = member.get("position_management_state")
        position_state = (
            position_state if isinstance(position_state, Mapping) else {}
        )
        member_status = str(
            member.get("status") or member.get("stage") or ""
        ).strip().upper()
        member_rejected = (
            member.get("rejected") is True or member_status == "REJECTED"
        )
        policy_hash = _canary_authorization_selection_policy_hash(selection) or ""
        selection_hash = _canary_authorization_current_selection_hash(selection)
        context: dict[str, Any] = {
            "lineage_type": "ROLLING_PORTFOLIO",
            "selection_hash": selection_hash,
            "strategy_version_id": strategy_version_id,
            "research_trial_id": research_trial_id,
            "candidate_id": candidate_id,
            "holding_period": member.get("holding_period", position_state.get("holding_period")),
            "exit_policy": member.get("exit_policy", position_state.get("exit_policy")),
            "observation_horizon": member.get(
                "observation_horizon",
                position_state.get("observation_horizon"),
            ),
            "status": member_status,
            "rejected": member_rejected,
            "selection_excluded": bool(
                member.get("selection_excluded", position_state.get("selection_excluded", False))
            ),
            "allocation": member.get("allocation"),
            "evidence_window_id": evidence_window_id,
            "evidence_digest": evidence_digest,
            "portfolio_selection_id": str(
                selection.get("portfolio_selection_id")
                or selection.get("selection_id")
                or ""
            ).strip(),
            "admission_policy_id": str(selection.get("policy_id") or "").strip(),
            "admission_policy_version": str(
                selection.get("policy_version") or ""
            ).strip(),
            "admission_policy_hash": policy_hash,
            "policy_hash": policy_hash,
            "risk_config_id": str(
                selection.get("active_risk_config_id")
                or selection.get("risk_config_id")
                or ""
            ).strip(),
            "risk_config_generation": selection.get(
                "active_risk_config_generation",
                selection.get("risk_config_generation"),
            ),
            "risk_config_hash": str(
                selection.get("active_risk_config_hash")
                or selection.get("risk_config_hash")
                or ""
            ).strip(),
        }
        for field in (
            "operational_setup_hash",
            "setup_hash",
            "setup_id",
            "setup_version",
            "operating_policy",
            "policy_id",
            "policy_version",
            "policy_hash",
            "scope_hash",
            "scope_version",
        ):
            value = member.get(field)
            if value in (None, "") and isinstance(member.get("operational_setup"), Mapping):
                value = member["operational_setup"].get(field)
            if value not in (None, ""):
                context[field] = value
        return context

    @staticmethod
    def _rolling_reduction_members(selection: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        """Return bounded member identities whose opening lots must exit now."""
        raw_members = selection.get("members", selection.get("selected_members", ()))
        raw_removed = selection.get("removed_members", ())
        candidates = (
            list(raw_members) if isinstance(raw_members, (list, tuple)) else []
        )
        if isinstance(raw_removed, (list, tuple)):
            candidates.extend(raw_removed)
        reducing: list[Mapping[str, Any]] = []
        for raw in candidates[:20]:
            if not isinstance(raw, Mapping):
                continue
            status = str(raw.get("status") or "").strip().upper()
            action = str(raw.get("action") or "").strip().upper()
            reason = str(raw.get("reason") or "").strip().upper()
            if (
                action == "REDUCE"
                or status in {"PAUSED", "REMOVED"}
                or "OVERLAP" in reason
                or "LOSER" in reason
            ):
                reducing.append(raw)
        return reducing

    @staticmethod
    def _rolling_active_members(
        selection: Mapping[str, Any],
        *,
        execution_mode: str = "EVIDENCE_SELECTED",
        authorization: Mapping[str, Any] | None = None,
    ) -> list[Mapping[str, Any]]:
        """Return bounded funded members admitted by the execution mode.

        Evidence-selected execution remains restricted to ACTIVE members.  An
        exploratory authorization can explicitly fund a currently selected
        non-ACTIVE member, but never creates allocation: the persisted member
        must already carry a positive reviewed allocation, and its exact
        strategy version or exact reviewed policy binding must authorize it.
        """
        raw_members = selection.get("members", selection.get("selected_members", ()))
        if not isinstance(raw_members, (list, tuple)):
            return []
        mode = str(execution_mode or "EVIDENCE_SELECTED").strip().upper()
        exploratory = mode in {
            "EXPLORATORY",
            "EXPLORATORY_MICRO_CANARY",
            "MICRO_CANARY",
        }
        exact_versions: set[str] | None = None
        acknowledged = False
        explicit_rejected_acknowledged = False
        policy_only = False
        if exploratory:
            if not isinstance(authorization, Mapping):
                return []
            if (
                str(authorization.get("status") or "ACTIVE").strip().upper()
                != "ACTIVE"
            ):
                return []
            raw_versions = authorization.get(
                "exact_strategy_versions",
                authorization.get(
                    "strategy_version_ids",
                    authorization.get("strategy_versions", ()),
                ),
            )
            if isinstance(raw_versions, str):
                exact_versions = {raw_versions.strip()} if raw_versions.strip() else set()
            elif isinstance(raw_versions, (list, tuple, set, frozenset)):
                exact_versions = {
                    str(value).strip()
                    for value in raw_versions
                    if str(value).strip()
                }
            else:
                exact_versions = set()
            policy_hash = str(
                authorization.get("reviewed_selection_policy_hash")
                or authorization.get("selection_policy_hash")
                or authorization.get("policy_hash")
                or ""
            ).strip()
            policy_only = not exact_versions and bool(policy_hash)
            if policy_only:
                current_selection_hash = _canary_authorization_current_selection_hash(
                    selection
                )
                authorized_selection_hash = str(
                    authorization.get("selection_hash")
                    or authorization.get("portfolio_selection_hash")
                    or authorization.get("current_selection_hash")
                    or ""
                ).strip()
                current_selection_id = str(
                    selection.get("portfolio_selection_id")
                    or selection.get("selection_id")
                    or ""
                ).strip()
                authorized_selection_id = str(
                    authorization.get("selection_id")
                    or authorization.get("portfolio_selection_id")
                    or ""
                ).strip()
                try:
                    current_policy_hash = (
                        _canary_authorization_selection_policy_hash(selection) or ""
                    )
                except CanaryBlocked:
                    return []
                if (
                    not authorized_selection_id
                    or authorized_selection_id != current_selection_id
                    or not authorized_selection_hash
                    or authorized_selection_hash != current_selection_hash
                    or policy_hash != current_policy_hash
                ):
                    return []
                exact_versions = {
                    str(member.get("strategy_version_id") or "").strip()
                    for member in raw_members
                    if isinstance(member, Mapping)
                    and str(member.get("strategy_version_id") or "").strip()
                }
            raw_ack = authorization.get(
                "adverse_evidence_ack",
                authorization.get("adverse_evidence_acknowledgment"),
            )
            acknowledged = _canary_adverse_acknowledged(raw_ack)
            explicit_rejected_acknowledged = _canary_adverse_acknowledged(
                raw_ack,
                require_required=True,
            )
        allowed_statuses = (
            {"ACTIVE", "OBSERVE", "PAPER", "REJECTED"}
            if exploratory
            else {"ACTIVE"}
        )
        active: list[Mapping[str, Any]] = []
        policy_config = selection.get("policy_config")
        policy_config = policy_config if isinstance(policy_config, Mapping) else {}
        try:
            member_limit = int(
                policy_config.get(
                    "max_members",
                    policy_config.get("max_k", selection.get("max_members", 10)),
                )
                or 10
            )
        except (TypeError, ValueError, OverflowError):
            member_limit = 10
        member_limit = max(1, min(member_limit, 64))
        for raw in raw_members[:member_limit]:
            if not isinstance(raw, Mapping):
                continue
            try:
                allocation = Decimal(str(raw.get("allocation") or "0"))
            except (InvalidOperation, TypeError, ValueError, ArithmeticError):
                continue
            if not allocation.is_finite() or allocation <= Decimal("0"):
                continue
            status = str(raw.get("status") or "").strip().upper()
            rejected = raw.get("rejected") is True or status == "REJECTED"
            if status not in allowed_statuses:
                continue
            if exploratory and isinstance(authorization, Mapping):
                strategy_version_id = str(raw.get("strategy_version_id") or "").strip()
                if exact_versions is None or strategy_version_id not in exact_versions:
                    continue
                if status in {"OBSERVE", "PAPER"} and not acknowledged:
                    continue
                if rejected and not explicit_rejected_acknowledged:
                    continue
            active.append(raw)
        return active

    def _rolling_overlap_loser_members(
        self,
        service: CanaryService,
        active_members: list[Mapping[str, Any]],
        reducing_members: list[Mapping[str, Any]],
        *,
        selection_id: str | None = None,
    ) -> list[Mapping[str, Any]]:
        """Add a bounded, paged set of persisted lots absent from the funded set.

        The current selection only contains the latest funded members.  An
        older opening can therefore disappear from that selection while its
        position lot remains open.  Match and carry the lot's immutable
        opening identity directly, rather than reconstructing it from current
        selection metadata.  The keyset cursor advances over every scanned lot
        and wraps, so a fixed oldest page cannot starve later openings.
        """
        _ensure_schema(service)
        identity_fields = ("strategy_version_id", "research_trial_id", "candidate_id")

        def identity(row: Mapping[str, Any]) -> tuple[str, str, str]:
            return tuple(str(row.get(name) or "").strip() for name in identity_fields)

        funded = {identity(member) for member in active_members}
        known = {identity(member) for member in reducing_members}
        cursor_key = str(selection_id or "").strip() or "__missing__"
        cursor: tuple[str, str] | None = self._rolling_overlap_cursor.get(cursor_key)
        cursor_store = getattr(self.store, "get_operator_config", None)
        if cursor is None and callable(cursor_store):
            try:
                stored_cursors = cursor_store("rolling_overlap_cursor", {})
            except Exception:
                stored_cursors = {}
            if isinstance(stored_cursors, Mapping):
                stored = stored_cursors.get(cursor_key)
                if isinstance(stored, Mapping):
                    opened_at = str(stored.get("opened_at") or "")
                    position_id = str(stored.get("position_id") or "")
                    if opened_at or position_id:
                        cursor = (opened_at, position_id)
                elif isinstance(stored, (list, tuple)) and len(stored) == 2:
                    opened_at = str(stored[0] or "")
                    position_id = str(stored[1] or "")
                    if opened_at or position_id:
                        cursor = (opened_at, position_id)

        base_query = (
            "SELECT strategy_version_id,research_trial_id,candidate_id,"
            "portfolio_selection_id,admission_policy_id,admission_policy_version,"
            "risk_config_id,risk_config_generation,risk_config_hash,lineage_type,"
            "position_id,status,quantity,sold_quantity,pending_exit_quantity,"
            "opened_at "
            "FROM canary_position_lots "
            "WHERE UPPER(COALESCE(lineage_type,''))='ROLLING_PORTFOLIO' "
            "AND UPPER(COALESCE(status,'')) IN "
            "('OPEN','EXIT_PENDING','MANAGEMENT_BLOCKED') "
        )
        with self.store._lock:
            if cursor is None:
                rows = self.store.connection.execute(
                    base_query
                    + "ORDER BY opened_at,position_id LIMIT ?",
                    (self._ROLLING_FORCE_EXIT_QUERY_LIMIT,),
                ).fetchall()
            else:
                rows = self.store.connection.execute(
                    base_query
                    + "AND (opened_at > ? OR "
                    "(opened_at=? AND position_id>?)) "
                    "ORDER BY opened_at,position_id LIMIT ?",
                    (
                        cursor[0],
                        cursor[0],
                        cursor[1],
                        self._ROLLING_FORCE_EXIT_QUERY_LIMIT,
                    ),
                ).fetchall()
                if len(rows) < self._ROLLING_FORCE_EXIT_QUERY_LIMIT:
                    remaining = self._ROLLING_FORCE_EXIT_QUERY_LIMIT - len(rows)
                    rows = list(rows) + list(
                        self.store.connection.execute(
                            base_query
                            + "AND (opened_at < ? OR "
                            "(opened_at=? AND position_id<=?)) "
                            "ORDER BY opened_at,position_id LIMIT ?",
                            (cursor[0], cursor[0], cursor[1], remaining),
                        ).fetchall()
                    )
        if rows:
            last = rows[-1]
            next_cursor = (
                str(last["opened_at"] or ""),
                str(last["position_id"] or ""),
            )
            self._rolling_overlap_cursor.pop(cursor_key, None)
            self._rolling_overlap_cursor[cursor_key] = next_cursor
            while len(self._rolling_overlap_cursor) > self._ROLLING_CURSOR_MAX:
                self._rolling_overlap_cursor.pop(next(iter(self._rolling_overlap_cursor)))
            cursor_setter = getattr(self.store, "set_operator_config", None)
            if callable(cursor_setter):
                try:
                    stored = (
                        cursor_store("rolling_overlap_cursor", {})
                        if callable(cursor_store)
                        else {}
                    )
                    stored = dict(stored) if isinstance(stored, Mapping) else {}
                    stored.pop(cursor_key, None)
                    stored[cursor_key] = {
                        "opened_at": next_cursor[0],
                        "position_id": next_cursor[1],
                    }
                    while len(stored) > self._ROLLING_CURSOR_MAX:
                        stored.pop(next(iter(stored)))
                    cursor_setter("rolling_overlap_cursor", stored)
                except Exception:
                    pass
        losers: list[Mapping[str, Any]] = []
        for row in rows:
            lot = dict(row)
            lot_identity = identity(lot)
            if (
                not all(lot_identity)
                or lot_identity in funded
                or lot_identity in known
            ):
                continue
            try:
                remaining = Decimal(str(lot.get("quantity") or "0")) - Decimal(
                    str(lot.get("sold_quantity") or "0")
                )
                pending = Decimal(str(lot.get("pending_exit_quantity") or "0"))
            except (InvalidOperation, TypeError, ValueError, ArithmeticError):
                continue
            if not remaining.is_finite() or not pending.is_finite() or remaining <= pending:
                continue
            lot.update(
                {
                    "action": "REDUCE",
                    "status": "REDUCE",
                    "reason": "OVERLAP_LOSER",
                }
            )
            losers.append(lot)
            known.add(lot_identity)
        return losers

    @staticmethod
    def _rolling_candidate_id(member: Mapping[str, Any]) -> str:
        candidate_id = str(member.get("candidate_id") or "").strip()
        if not candidate_id:
            raise CanaryBlocked("ROLLING_LINEAGE_INCOMPLETE")
        return candidate_id



    @staticmethod
    def _rolling_global_blocker(reason: Any) -> bool:
        text = str(reason or "").strip().upper()
        if not text:
            return False
        return any(
            token in text
            for token in (
                "ACCOUNT",
                "RISK",
                "BUDGET",
                "LIMIT",
                "CONTROL",
                "RECONCILIATION",
                "POSITION_OBLIGATION",
                "CREDENTIAL",
                "LINEAGE",
                "CANARY_KILLED",
                "CANARY_NOT_ARMED",
                "ENTRY_PAUSED",
                "DISARMED",
            )
        )

    def _rolling_selection(self) -> Mapping[str, Any] | None:
        loader = getattr(self.store, "load_current_portfolio_selection", None)
        if not callable(loader):
            return None
        result = loader()
        return result if isinstance(result, Mapping) else None

    def _remember_rolling_cursor(self, selection_id: str, cursor: int) -> None:
        """Keep only the deterministic recent rolling-selection cursor window."""
        if not selection_id:
            return
        # Reinsert existing IDs so recency is explicit and independent of
        # whether the mapping implementation preserves assignment order.
        self._rolling_cursor.pop(selection_id, None)
        self._rolling_cursor[selection_id] = int(cursor)
        while len(self._rolling_cursor) > self._ROLLING_CURSOR_MAX:
            self._rolling_cursor.pop(next(iter(self._rolling_cursor)))

    def _record_rolling_blocked(
        self,
        service: CanaryService,
        *,
        timestamp: datetime,
        decision: str,
        blocker: str,
        status: str = "DEGRADED",
        portfolio_selection_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist a fail-closed rolling decision and clear live truth."""
        try:
            with self.store._lock, self.store.connection:
                row = self.store.connection.execute(
                    "SELECT state,control_generation FROM canary_control "
                    "WHERE singleton=1"
                ).fetchone()
                state = str(row["state"] or "").upper() if row is not None else ""
                if state == "KILLED":
                    decision, blocker, status = "KILL_LATCHED", "CANARY_KILLED", "KILLED"
                elif state in {"ENTRY_PAUSED", "PAUSED"}:
                    decision, blocker, status = "ENTRY_PAUSED", "ENTRY_PAUSED", "ENTRY_PAUSED"
                elif state == "DISARMED":
                    decision, blocker, status = "DISARMED", "CANARY_NOT_ARMED", "DISARMED"
                if state == AUTONOMOUS_MICRO_LIVE:
                    try:
                        generation = int(row["control_generation"] or 0)
                    except (TypeError, ValueError, OverflowError):
                        generation = 0
                    self.store.connection.execute(
                        "UPDATE canary_control SET state='ENTRY_PAUSED',"
                        "candidate_id=NULL,updated_at=?,control_generation=? "
                        "WHERE singleton=1",
                        (
                            ensure_utc(timestamp).isoformat(),
                            max(1, generation + 1),
                        ),
                    )
            service.record_autonomous_decision(
                next_decision=decision,
                blocker=blocker,
                worker_status=status,
                timestamp=timestamp,
                candidates_evaluated=0,
                signals_generated=0,
                orders_attempted=0,
                candidates_ranked=0,
                candidates_signal_checked=0,
                actionable_candidates_found=0,
                selected_actionable_candidate=None,
                publish=True,
            )
        except Exception:
            pass
        return {
            "status": status,
            "decision": decision,
            "blocker": blocker,
            "portfolio_selection_id": portfolio_selection_id,
            "active_members": 0,
            "evaluated_members": 0,
            "ready_members": 0,
            "submissions": [],
            "paper_only": True,
            "live_execution": False,
        }
    def _rolling_tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Evaluate every active rolling member with a fair bounded cursor.

        Rolling selection, rather than the historical singleton canary winner, is
        the only execution authority on this path.  Every member is evaluated
        once per bounded tick; a member-local blocker does not starve its peers.
        """
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "decision": "DECISION_ALREADY_IN_PROGRESS"}
        timestamp = ensure_utc(now or self.clock())
        service: CanaryService | None = None
        selection: Mapping[str, Any] | None = None
        position_reconciliation: Mapping[str, Any] = {}
        position_management: Mapping[str, Any] = {}
        position_blocker: str | None = None
        global_blocker: str | None = None
        selection_blocker: str | None = None
        evaluated: list[dict[str, Any]] = []
        submissions: list[dict[str, Any]] = []
        ready: list[tuple[Mapping[str, Any], Mapping[str, Any], dict[str, Any]]] = []
        controller_lease: Mapping[str, Any] | None = None
        lease_required = callable(
            getattr(self.store, "acquire_canary_controller_lease", None)
        )
        try:
            service = CanaryService(self.store, clock=self.clock)
            # Initialize durable position tables even while disarmed.  This
            # is schema-only and must not construct a venue or submit exits.
            _ensure_schema(service)
            setattr(service, "controller_owner_id", self.controller_owner_id)
            controller_lease = self._acquire_controller_lease(timestamp)
            if isinstance(controller_lease, Mapping):
                generation = controller_lease.get("generation")
                setattr(service, "controller_lease_generation", generation)
                setattr(service, "controller_generation", generation)
            selection_error_type: str | None = None
            selection_missing = False
            try:
                selection = self._rolling_selection()
            except Exception as exc:
                selection = {}
                selection_missing = True
                selection_blocker = "ROLLING_SELECTION_UNAVAILABLE"
                selection_error_type = type(exc).__name__
            if selection is None:
                selection = {}
                selection_missing = True
                selection_blocker = "ROLLING_SELECTION_MISSING"
            if not selection_missing:
                try:
                    canonical_selection_hash = _canary_authorization_current_selection_hash(
                        selection
                    )
                    selection = dict(selection)
                    selection["selection_hash"] = canonical_selection_hash
                except CanaryBlocked as exc:
                    selection_blocker = str(exc) or "ROLLING_SELECTION_STALE"
            execution_mode = self._rolling_execution_mode(selection)
            exploratory_authorization_required = (
                execution_mode == "EXPLORATORY_MICRO_CANARY"
            )
            # In exploratory mode the pre-authorization set is only a bounded
            # candidate list.  It is narrowed to the active authorization's
            # exact strategy versions below; no member can be funded here.
            active = self._rolling_active_members(
                selection,
                execution_mode=execution_mode,
            )
            setattr(service, "execution_authorization_mode", execution_mode)
            selection_id = str(
                selection.get("portfolio_selection_id")
                or selection.get("selection_id")
                or ""
            ).strip()
            if not selection_missing:
                try:
                    service.validate_rolling_selection_fence(selection)
                except CanaryBlocked as exc:
                    selection_blocker = str(exc) or "ROLLING_SELECTION_STALE"

            control = service.authoritative_status()
            control_state = str(control.get("micro_live_canary") or "").upper()
            enabled = control_state == AUTONOMOUS_MICRO_LIVE
            control_blocker = (
                "CANARY_KILLED"
                if control_state == "KILLED"
                else "ENTRY_PAUSED"
                if control_state in {"ENTRY_PAUSED", "PAUSED"}
                else "CANARY_NOT_ARMED"
                if control_state != AUTONOMOUS_MICRO_LIVE
                else None
            )
            lease_blocker = (
                "CONTROLLER_LEASE_UNAVAILABLE"
                if lease_required and controller_lease is None
                else None
            )
            execution_authorization: Mapping[str, Any] | None = None
            authorization_blocker: str | None = None
            if enabled and (
                active
                or (exploratory_authorization_required and not selection_missing)
            ):
                authorization_loader = getattr(
                    getattr(service, "settings", None),
                    "load_active_execution_authorization",
                    None,
                )
                authorization_requirement = (
                    "EXPLORATORY_AUTHORIZATION_REQUIRED"
                    if exploratory_authorization_required
                    else "EXECUTION_AUTHORIZATION_REQUIRED"
                )
                if not callable(authorization_loader):
                    authorization_blocker = authorization_requirement
                else:
                    try:
                        execution_authorization = authorization_loader(
                            mode=execution_mode,
                            purpose=selection.get("authorization_purpose"),
                            now=timestamp,
                            scope_hash=selection.get("scope_hash")
                            or selection.get("market_scope_hash"),
                            scope_version=selection.get("scope_version")
                            or selection.get("market_scope_version"),
                            selection_id=selection_id or None,
                            selection_hash=selection.get("selection_hash"),
                        )
                    except (TypeError, ValueError, RuntimeError):
                        execution_authorization = None
                    if not isinstance(execution_authorization, Mapping):
                        authorization_blocker = authorization_requirement
                if isinstance(execution_authorization, Mapping):
                    auth_id = (
                        execution_authorization.get("authorization_id")
                        or execution_authorization.get("id")
                    )
                    auth_generation = execution_authorization.get("generation")
                    setattr(service, "execution_authorization_id", auth_id)
                    setattr(service, "execution_authorization_generation", auth_generation)
            if exploratory_authorization_required:
                active = self._rolling_active_members(
                    selection,
                    execution_mode=execution_mode,
                    authorization=execution_authorization,
                )
            if not active and selection_blocker is None and authorization_blocker is None:
                selection_blocker = "ROLLING_SELECTION_EMPTY"
            # Diff persisted openings against the authorization-filtered
            # funded set before position management.  Excluded exploratory
            # members must exit through their original lineage before any
            # newly authorized entry is considered.
            reducing = self._rolling_reduction_members(selection)
            reducing.extend(
                self._rolling_overlap_loser_members(
                    service,
                    active,
                    reducing,
                    selection_id=selection_id,
                )
            )
            if control_state == "DISARMED":
                _sync_entry_lots(service, timestamp)
            if control_state != "DISARMED" and self._position_obligations(service):
                try:
                    service.require_current_credential_binding()
                    venue_for_positions = self.venue_factory()
                    position_reconciliation = reconcile_pending(
                        service,
                        venue_for_positions,
                        allow_test_venue=self.allow_test_venue,
                    )
                    position_management = manage_positions(
                        service,
                        venue_for_positions,
                        allow_test_venue=self.allow_test_venue,
                        force_exit_members=reducing,
                    )
                    if str(position_reconciliation.get("status") or "").upper() == "DEGRADED":
                        position_blocker = "CANARY_RECONCILIATION_PROVIDER_ERROR"
                    elif str(position_management.get("status") or "").upper() == "BLOCKED":
                        position_blocker = "CANARY_POSITION_MANAGEMENT_BLOCKED"
                    elif self._unresolved_position_obligations(service):
                        position_blocker = "CANARY_POSITION_OBLIGATION_UNRESOLVED"
                except CanaryBlocked as exc:
                    position_blocker = str(exc) or "CANARY_RECONCILIATION_BLOCKED"
                    position_reconciliation = {
                        "status": "DEGRADED",
                        "blocked": 1,
                        "requests": [{"status": "UNKNOWN", "reason": position_blocker}],
                        "entries": [],
                    }
                    position_management = {
                        "status": "BLOCKED",
                        "blocked": position_blocker,
                        "submitted": 0,
                        "positions": [],
                    }
            else:
                position_reconciliation = {
                    "status": "IDLE",
                    "reconciled": 0,
                    "blocked": 0,
                    "requests": [],
                    "entries": [],
                }
                position_management = {
                    "status": "IDLE",
                    "submitted": 0,
                    "blocked": [],
                    "positions": [],
                }
            cursor = self._rolling_cursor.get(selection_id, 0)
            cursor_store = getattr(self.store, "get_operator_config", None)
            if callable(cursor_store):
                try:
                    stored_cursors = cursor_store("rolling_portfolio_cursor", {})
                except Exception:
                    stored_cursors = {}
                if isinstance(stored_cursors, Mapping):
                    try:
                        cursor = int(stored_cursors.get(selection_id, cursor) or cursor)
                    except (TypeError, ValueError):
                        cursor = self._rolling_cursor.get(selection_id, 0)
            if active:
                cursor %= len(active)
                ordered = active[cursor:] + active[:cursor]
                self._remember_rolling_cursor(
                    selection_id, (cursor + 1) % len(active)
                )
            else:
                ordered = []
                self._remember_rolling_cursor(selection_id, 0)
            cursor_setter = getattr(self.store, "set_operator_config", None)
            if callable(cursor_setter) and selection_id:
                try:
                    stored = (
                        cursor_store("rolling_portfolio_cursor", {})
                        if callable(cursor_store)
                        else {}
                    )
                    stored = dict(stored) if isinstance(stored, Mapping) else {}
                    stored.pop(selection_id, None)
                    stored[selection_id] = self._rolling_cursor.get(selection_id, 0)
                    while len(stored) > self._ROLLING_CURSOR_MAX:
                        stored.pop(next(iter(stored)))
                    cursor_setter("rolling_portfolio_cursor", stored)
                except Exception:
                    pass
            cycle_id = f"rolling-{uuid.uuid4().hex[:24]}"
            members_to_evaluate = (
                ordered
                if control_blocker is None and selection_blocker is None
                else ()
            )
            for member in members_to_evaluate:
                row: dict[str, Any] = {
                    "strategy_version_id": str(
                        member.get("strategy_version_id") or ""
                    ).strip(),
                    "candidate_id": str(member.get("candidate_id") or "").strip(),
                    "allocation": member.get("allocation"),
                    "status": member.get("status"),
                    "lineage": {},
                }
                try:
                    context = self._rolling_member_context(selection, member)
                    context["execution_authorization_mode"] = execution_mode
                    context["controller_owner_id"] = self.controller_owner_id
                    context["controller_lease_generation"] = (
                        controller_lease.get("generation")
                        if isinstance(controller_lease, Mapping)
                        else None
                    )
                    context["controller_generation"] = context["controller_lease_generation"]
                    context["execution_authorization_id"] = (
                        execution_authorization.get("authorization_id")
                        or execution_authorization.get("id")
                        if isinstance(execution_authorization, Mapping)
                        else None
                    )
                    context["execution_authorization_generation"] = (
                        execution_authorization.get("generation")
                        if isinstance(execution_authorization, Mapping)
                        else None
                    )
                    if isinstance(execution_authorization, Mapping):
                        context["execution_authorization_strategy_versions"] = (
                            execution_authorization.get(
                                "exact_strategy_versions",
                                execution_authorization.get(
                                    "strategy_version_ids",
                                    execution_authorization.get("strategy_versions", ()),
                                ),
                            )
                        )
                        context["execution_authorization_adverse_evidence_ack"] = (
                            execution_authorization.get(
                                "adverse_evidence_ack",
                                execution_authorization.get(
                                    "adverse_evidence_acknowledgment"
                                ),
                            )
                        )
                    candidate_id = self._rolling_candidate_id(member)
                    row.update(
                        {
                            "strategy_version_id": context["strategy_version_id"],
                            "candidate_id": candidate_id,
                            "lineage": dict(context),
                        }
                    )
                    evaluation = service.evaluate_signal(
                        candidate_id,
                        cycle_id=cycle_id,
                        rolling_context=context,
                    )
                except CanaryBlocked as exc:
                    reason = str(exc) or "SIGNAL_BLOCKED"
                    row.update({"status": "BLOCKED", "reason": reason})
                    evaluated.append(row)
                    if self._rolling_global_blocker(reason) and global_blocker is None:
                        global_blocker = reason
                    continue
                except Exception as exc:
                    row.update({"status": "ERROR", "reason": type(exc).__name__.upper()})
                    evaluated.append(row)
                    continue
                signal = evaluation.get("signal") if isinstance(evaluation, Mapping) else None
                row["evaluation"] = dict(evaluation) if isinstance(evaluation, Mapping) else {}
                row["signal"] = dict(signal) if isinstance(signal, Mapping) else None
                row["reason"] = (
                    str((signal or {}).get("reason_code") or "").strip().upper()
                    if isinstance(signal, Mapping)
                    else str((evaluation or {}).get("reason_code") or "").strip().upper()
                    if isinstance(evaluation, Mapping)
                    else "NO_SIGNAL"
                )
                evaluated.append(row)
                if (
                    isinstance(signal, Mapping)
                    and str(signal.get("status") or "").strip().upper() == "READY"
                    and str(signal.get("signal_id") or "").strip()
                ):
                    ready.append((member, signal, context))

            if control_blocker == "CANARY_KILLED":
                decision = "KILL_LATCHED"
                status = "KILLED"
                global_blocker = "CANARY_KILLED"
            elif control_blocker == "ENTRY_PAUSED":
                decision = "ENTRY_PAUSED"
                status = "ENTRY_PAUSED"
                global_blocker = "ENTRY_PAUSED"
            elif control_blocker == "CANARY_NOT_ARMED":
                if selection_blocker is not None:
                    decision = "WAIT_FOR_ROLLING_SELECTION"
                    status = "OBSERVING"
                else:
                    decision = "DISARMED" if control_state == "DISARMED" else "CANARY_NOT_ARMED"
                    status = "DISARMED" if control_state == "DISARMED" else "BLOCKED"
                    global_blocker = "CANARY_NOT_ARMED"
            elif selection_blocker is not None:
                decision = "WAIT_FOR_ROLLING_SELECTION"
                status = "OBSERVING"
            elif authorization_blocker is not None:
                decision = authorization_blocker
                status = "BLOCKED"
                global_blocker = authorization_blocker
            elif not active:
                decision = "WAIT_FOR_ACTIVE_ROLLING_MEMBER"
                status = "OBSERVING"
            elif position_blocker is not None:
                decision = "POSITION_RECONCILIATION_BLOCKED"
                status = "BLOCKED"
                global_blocker = position_blocker
            elif lease_blocker is not None:
                decision = lease_blocker
                status = "BLOCKED"
                global_blocker = lease_blocker
            elif global_blocker is not None:
                decision = global_blocker
                status = "BLOCKED"
            elif not ready:
                decision = "WAIT_FOR_FRESH_ROLLING_SIGNAL"
                status = "NO_SIGNAL"
            else:
                venue: Any | None = None
                credentials_ok = True
                credentials = getattr(service, "credentials", None)
                configured = getattr(credentials, "configured", None)
                if callable(configured):
                    try:
                        credentials_ok = bool(configured(allow_environment=False))
                    except TypeError:
                        credentials_ok = bool(configured())
                if not credentials_ok:
                    global_blocker = "CREDENTIALS_NOT_CONFIGURED"
                else:
                    try:
                        service.require_current_credential_binding()
                        venue = self.venue_factory()
                    except CanaryBlocked as exc:
                        global_blocker = str(exc) or "CREDENTIALS_NOT_CONFIGURED"
                if global_blocker is None:
                    for member, signal, context in ready:
                        signal_id = str(signal.get("signal_id") or "").strip()
                        candidate_id = str(context.get("candidate_id") or "").strip()
                        if not candidate_id:
                            raise CanaryBlocked("ROLLING_LINEAGE_INCOMPLETE")
                        try:
                            submitted = service.submit_signal(
                                signal_id,
                                venue=venue,
                                allow_test_venue=self.allow_test_venue,
                                rolling_context=context,
                            )
                        except CanaryBlocked as exc:
                            reason = str(exc) or "ROLLING_SUBMISSION_BLOCKED"
                            item = {
                                "candidate_id": candidate_id,
                                "strategy_version_id": context["strategy_version_id"],
                                "signal_id": signal_id,
                                "status": "BLOCKED",
                                "reason": reason,
                                "lineage": dict(context),
                            }
                            submissions.append(item)
                            if self._rolling_global_blocker(reason):
                                global_blocker = reason
                                break
                            continue
                        outcome = (
                            str(submitted.get("execution_status") or "").upper()
                            if isinstance(submitted, Mapping)
                            else ""
                        )
                        item = {
                            "candidate_id": candidate_id,
                            "strategy_version_id": context["strategy_version_id"],
                            "signal_id": signal_id,
                            "status": outcome or "UNKNOWN",
                            "submission": dict(submitted) if isinstance(submitted, Mapping) else {},
                            "lineage": dict(context),
                        }
                        submissions.append(item)
                        if outcome not in self._SUBMISSION_SUCCESS_STATUSES and self._rolling_global_blocker(outcome):
                            global_blocker = outcome
                            break
                status = "SUBMITTED" if any(
                    item.get("status") in self._SUBMISSION_SUCCESS_STATUSES
                    for item in submissions
                ) else "BLOCKED"
                decision = global_blocker or (
                    "SUBMITTED" if status == "SUBMITTED" else "ROLLING_SUBMISSION_COMPLETE"
                )
            payload = {
                "status": status,
                "decision": decision,
                "blocker": global_blocker,
                "portfolio_selection_id": selection_id,
                "execution_authorization_mode": execution_mode,
                "execution_authorization_id": (
                    execution_authorization.get("authorization_id")
                    or execution_authorization.get("id")
                    if isinstance(execution_authorization, Mapping)
                    else None
                ),
                "execution_authorization_generation": (
                    execution_authorization.get("generation")
                    if isinstance(execution_authorization, Mapping)
                    else None
                ),
                "controller_owner_id": self.controller_owner_id,
                "controller_generation": (
                    controller_lease.get("generation")
                    if isinstance(controller_lease, Mapping)
                    else None
                ),
                "active_members": len(active),
                "reducing_members": len(reducing),
                "evaluated_members": len(evaluated),
                "ready_members": len(ready),
                "evaluated": evaluated[: self._SCAN_CAP],
                "submissions": submissions[: self._SCAN_CAP],
                "position_reconciliation": dict(position_reconciliation),
                "position_management": dict(position_management),
                "position_blocker": position_blocker,
                "rolling_cursor": self._rolling_cursor.get(selection_id, 0),
                "lineage": {
                    "portfolio_selection_id": selection_id,
                    "admission_policy_id": selection.get("policy_id"),
                    "admission_policy_version": selection.get("policy_version"),
                    "admission_policy_hash": selection.get(
                        "policy_hash",
                        selection.get(
                            "config_hash",
                            selection.get("policy_config", {}).get("config_hash")
                            if isinstance(selection.get("policy_config"), Mapping)
                            else None,
                        ),
                    ),
                    "risk_config_id": selection.get("active_risk_config_id", selection.get("risk_config_id")),
                    "risk_config_generation": selection.get("active_risk_config_generation", selection.get("risk_config_generation")),
                    "risk_config_hash": selection.get("active_risk_config_hash", selection.get("risk_config_hash")),
                },
                "paper_only": True,
                "live_execution": False,
            }
            live_submitted = bool(
                enabled
                and any(
                    item.get("status") in self._SUBMISSION_SUCCESS_STATUSES
                    for item in submissions
                )
            )
            payload["paper_only"] = not live_submitted
            payload["live_execution"] = live_submitted
            payload["live_canary"] = live_submitted
            payload["execution_mode"] = "live-canary" if live_submitted else "paper"
            payload["operating_state"] = (
                "observing"
                if selection_blocker is not None or not active
                else "exploratory_micro_canary"
                if exploratory_authorization_required
                else "evidence_selected"
            )
            payload["next_work"] = (
                "refresh_rolling_evidence"
                if selection_blocker is not None or not active
                else "evaluate_exploratory_strategies"
                if exploratory_authorization_required
                else "evaluate_selected_strategies"
            )
            payload["no_entry_reason"] = (
                {
                    "category": "SELECTION",
                    "reason": selection_blocker or "NO_ACTIVE_ROLLING_MEMBER",
                    "resolver": "refresh_rolling_evidence",
                    "next_scheduled_action": "refresh_rolling_evidence",
                }
                if selection_blocker is not None or not active
                else None
            )
            if global_blocker is not None:
                payload["blocker_projection"] = {
                    "category": (
                        "AUTHORIZATION"
                        if "AUTHORIZATION" in global_blocker
                        else "CONTROLLER"
                        if "LEASE" in global_blocker
                        else "POSITION"
                        if "POSITION" in global_blocker or "RECONCILIATION" in global_blocker
                        else "CONTROL"
                    ),
                    "reason": global_blocker,
                    "resolver": (
                        "activate_execution_authorization"
                        if "AUTHORIZATION" in global_blocker
                        else "renew_canary_controller_lease"
                        if "LEASE" in global_blocker
                        else "reconcile_canary_positions"
                        if "POSITION" in global_blocker or "RECONCILIATION" in global_blocker
                        else "review_canary_control"
                    ),
                    "next_scheduled_action": payload.get("next_work")
                    or "evaluate_selected_strategies",
                }
            if selection_error_type is not None:
                payload["error_type"] = selection_error_type
            try:
                service.record_autonomous_decision(
                    next_decision=decision,
                    selected_actionable_candidate=(
                        str(ready[0][2].get("candidate_id") or "")
                        if ready
                        else None
                    ),
                    worker_status=(
                        "KILLED"
                        if decision == "KILL_LATCHED"
                        else "DEGRADED" if global_blocker else "IDLE"
                    ),
                    timestamp=timestamp,
                    candidates_evaluated=len(evaluated),
                    signals_generated=sum(
                        1 for row in evaluated if isinstance(row.get("signal"), Mapping)
                    ),
                    orders_attempted=len(submissions),
                    candidates_ranked=len(active),
                    candidates_signal_checked=len(evaluated),
                    actionable_candidates_found=len(ready),
                    publish=False,
                )
            except Exception:
                pass
            return payload
        finally:
            # The lease spans decision windows; run() releases it when the
            # worker receives a normal stop signal.
            self._decision_lock.release()

    def tick_rolling(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Evaluate the persisted rolling selection explicitly."""
        return self._rolling_tick(now=now)

    def tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Use rolling authority only when a persisted selection is present."""
        loader = getattr(self.store, "load_current_portfolio_selection", None)
        if callable(loader):
            # The real storage API is a bound method and must fail closed into
            # rolling mode even when its persisted selection is absent or
            # malformed.  A policy-only callable is an authorization context,
            # not a selection; retain the legacy ranker path for that
            # compatibility context and avoid treating it as an empty rolling
            # portfolio.
            bound_to_store = getattr(loader, "__self__", None) is self.store
            if bound_to_store:
                return self.tick_rolling(now=now)
            try:
                selection = loader()
            except Exception:
                return self.tick_rolling(now=now)
            if not isinstance(selection, Mapping):
                return self.tick_rolling(now=now)
            if any(
                name in selection
                for name in (
                    "portfolio_selection_id",
                    "selection_id",
                    "members",
                    "selected_members",
                )
            ):
                return self.tick_rolling(now=now)
            if not set(selection).issubset(
                {"selection_policy_hash", "reviewed_selection_policy_hash"}
            ):
                return self.tick_rolling(now=now)
        return self._legacy_tick(now=now)

    def run(self, stop_event: threading.Event) -> None:
        """Run immediately, then wait for the next bounded decision window."""
        try:
            while not stop_event.is_set():
                try:
                    self.tick()
                except BaseException:
                    # Keep this loop alive even if a caller replaces tick with
                    # an unsafe implementation; the node supervisor persists
                    # the degraded worker state on its next boundary.
                    pass
                stop_event.wait(self.interval_seconds)
        finally:
            self._release_controller_lease(ensure_utc(self.clock()), reason="worker_stop")


__all__ = ["AutonomousCanaryWorker"]
