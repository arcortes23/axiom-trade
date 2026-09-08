"""Bounded autonomous canary scheduling, isolated from collection and research."""
from __future__ import annotations

from datetime import datetime, timedelta
import math
import threading
from typing import Any, Callable, Mapping

from .canary import (
    AUTONOMOUS_MICRO_LIVE,
    CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS,
    CanaryBlocked,
    CanaryService,
    PolymarketClobV2Venue,
)
from .domain import ensure_utc, utc_now
from .ranker import CandidateCanaryRanker
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
    _SCAN_CAP = 10
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
        if not ranking_run_id:
            return []
        try:
            rows = ranker.rankings(limit=10_000)
        except Exception:
            return []
        valid: list[tuple[Mapping[str, Any], int]] = []
        representative_clusters: set[str] = set()
        for row in rows:
            try:
                accepted = ranker.validate_persisted_ranking(
                    row,
                    ranking_run_id=str(ranking_run_id),
                    now=timestamp,
                )
            except Exception:
                accepted = False
            if not accepted:
                continue
            # Rank zero is reserved for the persisted diversity follower
            # representation.  A representative (or a row with a forged
            # reason marker) must never become actionable.
            try:
                persisted_rank = int(row.get("rank"))
            except (TypeError, ValueError, OverflowError):
                continue
            if persisted_rank < 0:
                continue
            try:
                representative_marker = int(row.get("cluster_representative"))
            except (TypeError, ValueError, OverflowError):
                continue
            if persisted_rank == 0:
                if not (
                    str(row.get("reason") or "").strip()
                    == "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                    and representative_marker == 0
                ):
                    continue
            elif representative_marker == 1:
                representative_clusters.add(str(row.get("cluster_key") or ""))
            valid.append((row, persisted_rank))
        # A follower is meaningful only when this same fenced run persisted
        # its cluster representative.  In particular, an orphaned or forged
        # rank-zero follower must not turn a no-rankable run into a candidate.
        return [
            row
            for row, persisted_rank in valid
            if persisted_rank > 0
            or str(row.get("cluster_key") or "") in representative_clusters
        ]

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
                "next_signal_scan_start_rank,next_signal_scan_end_rank "
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
        signal_scan_cursor: int = 0,
        signal_scan_ranking_run_id: str | None = None,
        next_signal_scan_start_rank: int | None = None,
        next_signal_scan_end_rank: int | None = None,
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
                candidates_signal_checked=max(0, int(candidates_signal_checked)),
                candidates_no_signal=max(0, int(candidates_no_signal)),
                actionable_candidates_found=max(0, int(actionable_candidates_found)),
                selected_actionable_candidate=selected_actionable_candidate,
                selected_actionable_rank=selected_actionable_rank,
                selected_actionable_score=selected_actionable_score,
                signal_scan_cursor=max(0, int(signal_scan_cursor)),
                signal_scan_ranking_run_id=signal_scan_ranking_run_id,
                next_signal_scan_start_rank=next_signal_scan_start_rank,
                next_signal_scan_end_rank=next_signal_scan_end_rank,
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
        signal_scan_cursor = 0
        signal_scan_ranking_run_id: str | None = None
        next_signal_scan_start_rank: int | None = None
        next_signal_scan_end_rank: int | None = None
        try:
            service = CanaryService(self.store, clock=self.clock)
            self._record_start(service, timestamp)
            ranker = CandidateCanaryRanker(
                self.store,
                service=service,
                clock=self.clock,
            )
            ranking = ranker.evaluate_and_select(timestamp)
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
            candidates_evaluated = max(candidates_evaluated, candidates_ranked)
            # A ranking run with no rankable candidates cannot authorize a
            # signal scan.  The persisted ranking table may still contain
            # structural rows (for example, rank-zero followers), but those
            # rows are not actionable without a rankable representative in
            # this run.
            if candidates_ranked == 0:
                ordered = []
            previous = self._scan_state()
            try:
                previous_cursor = max(0, int(previous.get("signal_scan_cursor") or 0))
            except (TypeError, ValueError):
                previous_cursor = 0
            ranking_changed = (
                signal_scan_ranking_run_id is not None
                and str(previous.get("signal_scan_ranking_run_id") or "")
                != signal_scan_ranking_run_id
            )
            signal_scan_cursor = 0 if ranking_changed else previous_cursor
            if not ordered:
                signal_scan_cursor = 0
                scan_start = scan_end = next_signal_scan_start_rank = next_signal_scan_end_rank = 0
            else:
                if signal_scan_cursor >= len(ordered):
                    signal_scan_cursor = 0
                scan_start = signal_scan_cursor
                scan_end = min(scan_start + self._SCAN_CAP, len(ordered))
                next_signal_scan_start_rank = scan_start
                next_signal_scan_end_rank = scan_end

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
                }

            def finish(
                *,
                next_decision: str,
                blocker: str | None = None,
                signal_id: str | None = None,
                worker_status: str = "IDLE",
                error_code: str | None = None,
            ) -> None:
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
                    **scan_payload(),
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
            if candidates_ranked == 0:
                blocker = "NO_ELIGIBLE_RANKABLE_CANDIDATE"
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
            for row in ordered[scan_start:scan_end]:
                candidate_id = str(row.get("candidate_id") or "").strip()
                candidates_signal_checked += 1
                signal = service.generate_signal(candidate_id)
                if isinstance(signal, Mapping):
                    signals_generated += 1
                status = str(signal.get("status") or "").upper() if isinstance(signal, Mapping) else "NONE"
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
                    status == "READY"
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
                signal_scan_cursor = 0
                next_signal_scan_start_rank = 0
                next_signal_scan_end_rank = min(self._SCAN_CAP, len(ordered))
            else:
                signal_id = unknown_signal_id
                blocker = "UNKNOWN_NO_RETRY" if unknown_seen else "NO_ACTIONABLE_SIGNAL"
                if scan_end >= len(ordered):
                    signal_scan_cursor = 0
                else:
                    signal_scan_cursor = scan_end
                next_signal_scan_start_rank = signal_scan_cursor
                next_signal_scan_end_rank = min(
                    signal_scan_cursor + self._SCAN_CAP,
                    len(ordered),
                )
                finish(
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
                    blocker=blocker,
                    signal_id=signal_id,
                )
                return result(
                    status="NO_SIGNAL" if not unknown_seen else "BLOCKED",
                    decision="NO_ACTIONABLE_SIGNAL" if not unknown_seen else "UNKNOWN_NO_RETRY",
                    blocker=blocker,
                    candidate_id=(
                        str(ordered[scan_start].get("candidate_id") or "")
                        if ordered[scan_start:scan_end]
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
            if service is not None:
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                    blocker="AUTONOMOUS_WORKER_EXCEPTION",
                    worker_status="DEGRADED",
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                    orders_attempted=orders_attempted,
                    candidates_ranked=candidates_ranked,
                    candidates_signal_checked=candidates_signal_checked,
                    candidates_no_signal=candidates_no_signal,
                    actionable_candidates_found=actionable_candidates_found,
                    selected_actionable_candidate=selected_actionable_candidate,
                    selected_actionable_rank=selected_actionable_rank,
                    selected_actionable_score=selected_actionable_score,
                    signal_scan_cursor=signal_scan_cursor,
                    signal_scan_ranking_run_id=signal_scan_ranking_run_id,
                    next_signal_scan_start_rank=next_signal_scan_start_rank,
                    next_signal_scan_end_rank=next_signal_scan_end_rank,
                    error_code="AUTONOMOUS_WORKER_EXCEPTION",
                )
            return {
                "status": "ERROR",
                "decision": "AUTONOMOUS_WORKER_EXCEPTION",
                "blocker": "AUTONOMOUS_WORKER_EXCEPTION",
                "error_type": type(exc).__name__,
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
