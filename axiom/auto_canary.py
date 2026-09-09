"""Bounded autonomous canary scheduling, isolated from collection and research."""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
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
)
from .domain import ensure_utc, utc_now
from .ranker import CandidateCanaryRanker
from .lifecycle import _canonical_scope_gate_error
from .storage import AxiomStore

class AutonomousCanaryWorker:
    """Run one serialized rank/signal/submit decision at a bounded cadence."""

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
        self._unknown_signal_ids: set[str] = set()
        self._last_scan_skip_reasons: dict[str, int] = {
            reason: 0 for reason in self._SCAN_SKIP_REASONS
        }
    _SCAN_CAP = 10
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
        if isinstance(evaluation, Mapping):
            reason = str(evaluation.get("reason_code") or "").strip().upper()
            if reason in cls._SIGNAL_REASON_CODES:
                return reason
        if isinstance(signal, Mapping) and str(signal.get("status") or "").upper() == "READY":
            return "READY_SIGNAL"
        return "NO_STRATEGY_SIGNAL"

    @staticmethod
    def _evaluation_blocker(evaluation: Any) -> str | None:
        if not isinstance(evaluation, Mapping):
            return None
        evidence = evaluation.get("evidence")
        if not isinstance(evidence, Mapping):
            return None
        evaluation_reason = str(evaluation.get("reason_code") or "").strip().upper()
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

    def tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Rank once, then inspect one bounded diversity-aware signal window."""
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "decision": "DECISION_ALREADY_IN_PROGRESS"}
        timestamp = ensure_utc(now or self.clock())
        service: CanaryService | None = None
        ranking: Mapping[str, Any] = {}
        candidates_evaluated = 0
        signals_generated = 0
        orders_attempted = 0
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
            self._record_start(service, timestamp)
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
            if isinstance(ranking, Mapping):
                try:
                    candidates_evaluated = max(
                        0,
                        int(
                            ranking.get(
                                "candidates_evaluated",
                                ranking.get("eligible_count", 0),
                            )
                        ),
                    )
                except (TypeError, ValueError):
                    candidates_evaluated = 0
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
            except (TypeError, ValueError):
                candidates_ranked = len(
                    ranking.get("rankings")
                    if isinstance(ranking.get("rankings"), list)
                    else []
                )
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
                signal_scan_coverage_percentage = (
                    100.0 * signal_scan_checked_this_cycle / total if total else 100.0
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
                    "COMPLETE_NO_SIGNAL"
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
                return {**extra, **scan_payload(), "ranking": ranking}

            control = service.authoritative_status()
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
            if not ordered:
                blocker = "NO_ELIGIBLE_RANKABLE_CANDIDATE"
                update_scan_projection(force_complete=True)
                finish(
                    next_decision="WAIT_FOR_RANKABLE_CANDIDATE",
                    blocker=blocker,
                )
                return result(
                    status="BLOCKED",
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
                reason = self._evaluation_reason(evaluation, signal)
                evaluation_blocker = evaluation_blocker or self._evaluation_blocker(
                    evaluation
                )
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
                blocker = (
                    "UNKNOWN_NO_RETRY"
                    if unknown_seen
                    else evaluation_blocker or "NO_ACTIONABLE_SIGNAL"
                )
                decision = (
                    "UNKNOWN_NO_RETRY"
                    if unknown_seen
                    else blocker
                )
                update_scan_projection()
                finish(
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
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
                    status="BLOCKED" if unknown_seen or evaluation_blocker else "NO_SIGNAL",
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
            try:
                venue = self.venue_factory()
            except Exception as exc:
                # Venue construction has not transmitted anything.  Keep the
                # READY signal retryable and record a worker failure without
                # counting an order attempt or poisoning the signal as UNKNOWN.
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
                        blocker="AUTONOMOUS_WORKER_EXCEPTION",
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
            self._decision_lock.release()

    def run(self, stop_event: threading.Event) -> None:
        """Run immediately, then wait for the next bounded decision window."""
        while not stop_event.is_set():
            try:
                self.tick()
            except BaseException:
                # Keep this loop alive even if a caller replaces tick with an
                # unsafe implementation; the node supervisor persists the
                # degraded worker state on its next boundary.
                pass
            stop_event.wait(self.interval_seconds)


__all__ = ["AutonomousCanaryWorker"]
