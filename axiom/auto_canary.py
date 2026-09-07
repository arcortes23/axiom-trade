"""Bounded autonomous canary scheduling, isolated from collection and research."""
from __future__ import annotations

from datetime import datetime, timedelta
import threading
from typing import Any, Callable, Mapping

from .canary import (
    AUTONOMOUS_MICRO_LIVE,
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
    ) -> None:
        self.store = store
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.clock = clock
        self.venue_factory = venue_factory or (lambda: PolymarketClobV2Venue(allow_environment=False))
        self._decision_lock = threading.Lock()
        self._consecutive_failures = 0
        self._next_retry_at: datetime | None = None
        self._unknown_signal_ids: set[str] = set()

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
            )
        except Exception:
            return

    def tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Evaluate all candidates once; never retry an existing UNKNOWN signal."""
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "decision": "DECISION_ALREADY_IN_PROGRESS"}
        timestamp = ensure_utc(now or self.clock())
        service: CanaryService | None = None
        candidates_evaluated = 0
        signals_generated = 0
        orders_attempted = 0
        try:
            # Construction belongs inside the isolated tick boundary.  A
            # transient schema/credential initialization error must not kill
            # the autonomous thread or strand the node lock.
            service = CanaryService(self.store, clock=self.clock)
            self._record_start(service, timestamp)
            ranking = CandidateCanaryRanker(
                self.store,
                service=service,
                clock=self.clock,
            ).evaluate_and_select(timestamp)
            try:
                candidates_evaluated = max(
                    0,
                    int(
                        ranking.get(
                            "candidates_evaluated",
                            ranking.get("eligible_count", len(ranking.get("rankings", ()))),
                        )
                    ),
                )
            except (TypeError, ValueError):
                candidates_evaluated = len(ranking.get("rankings", ())) if isinstance(ranking, Mapping) else 0
            control = service.authoritative_status()
            if control.get("micro_live_canary") == "KILLED":
                result = {
                    "status": "BLOCKED",
                    "decision": "KILL_LATCHED",
                    "blocker": "CANARY_KILLED",
                    "ranking": ranking,
                }
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="KILL_LATCHED",
                    blocker="CANARY_KILLED",
                    worker_status="KILLED",
                    candidates_evaluated=candidates_evaluated,
                )
                return result
            if control.get("micro_live_canary") != AUTONOMOUS_MICRO_LIVE:
                result = {
                    "status": "DISABLED",
                    "decision": "AUTONOMOUS_CANARY_DISABLED",
                    "blocker": "AUTONOMOUS_CANARY_DISABLED",
                    "ranking": ranking,
                }
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="ENABLE AUTO CANARY",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                    candidates_evaluated=candidates_evaluated,
                )
                return result
            winner = ranking.get("selected_candidate")
            if not winner:
                result = {
                    "status": "BLOCKED",
                    "decision": "NO_ELIGIBLE_CANDIDATE",
                    "blocker": "NO_ELIGIBLE_RANKABLE_CANDIDATE",
                    "ranking": ranking,
                }
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WAIT_FOR_ELIGIBLE_CANDIDATE",
                    blocker="NO_ELIGIBLE_RANKABLE_CANDIDATE",
                    candidates_evaluated=candidates_evaluated,
                )
                return result
            try:
                signal = service.generate_signal(str(winner))
                signals_generated = 1
            except CanaryBlocked as exc:
                blocker = str(exc)
                result = {
                    "status": "BLOCKED",
                    "decision": "NO_ACTIONABLE_SIGNAL",
                    "blocker": blocker,
                    "candidate_id": str(winner),
                    "ranking": ranking,
                }
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
                    blocker=blocker,
                    candidates_evaluated=candidates_evaluated,
                )
                return result
            if not isinstance(signal, Mapping) or str(signal.get("status") or "").upper() != "READY":
                signal_status = str(signal.get("status") if isinstance(signal, Mapping) else "NONE").upper()
                blocker = "UNKNOWN_NO_RETRY" if signal_status == "UNKNOWN" else "NO_ACTIONABLE_SIGNAL"
                signal_id = str(signal.get("signal_id")) if isinstance(signal, Mapping) else None
                if signal_status == "UNKNOWN" and signal_id:
                    self._unknown_signal_ids.add(signal_id)
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
                    blocker=blocker,
                    signal_id=signal_id,
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                )
                return {
                    "status": "NO_SIGNAL",
                    "decision": "NO_ACTIONABLE_SIGNAL",
                    "blocker": blocker,
                    "candidate_id": str(winner),
                    "signal": dict(signal) if isinstance(signal, Mapping) else None,
                    "ranking": ranking,
                }
            signal_id = str(signal["signal_id"])
            if signal_id in self._unknown_signal_ids:
                blocker = "UNKNOWN_NO_RETRY"
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="UNKNOWN_NO_RETRY",
                    blocker=blocker,
                    signal_id=signal_id,
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                )
                return {
                    "status": "BLOCKED",
                    "decision": blocker,
                    "blocker": blocker,
                    "candidate_id": str(winner),
                    "signal_id": signal_id,
                    "ranking": ranking,
                }
            if not service.credentials.configured(allow_environment=False):
                blocker = "CREDENTIALS_NOT_CONFIGURED"
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WAIT_FOR_CREDENTIALS",
                    blocker=blocker,
                    signal_id=signal_id,
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                )
                return {
                    "status": "BLOCKED",
                    "decision": "CREDENTIALS_NOT_CONFIGURED",
                    "blocker": blocker,
                    "candidate_id": str(winner),
                    "signal_id": signal_id,
                    "ranking": ranking,
                }
            venue = self.venue_factory()
            orders_attempted = 1
            try:
                submission = service.submit_signal(signal_id, venue=venue)
            except CanaryBlocked as exc:
                blocker = str(exc)
                if blocker == "CANARY_SUBMISSION_UNKNOWN":
                    self._unknown_signal_ids.add(signal_id)
                decision = "UNKNOWN_NO_RETRY" if blocker == "CANARY_SUBMISSION_UNKNOWN" else blocker
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision=(
                        "UNKNOWN_NO_RETRY"
                        if decision == "UNKNOWN_NO_RETRY"
                        else "WAIT_FOR_NEXT_DECISION"
                    ),
                    blocker=decision,
                    signal_id=signal_id,
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                    orders_attempted=orders_attempted,
                )
                return {
                    "status": "BLOCKED",
                    "decision": decision,
                    "blocker": blocker,
                    "candidate_id": str(winner),
                    "signal_id": signal_id,
                    "ranking": ranking,
                }
            except Exception as exc:
                # submit_signal persists UNKNOWN before surfacing transport
                # failures.  Conservatively fence this signal locally too.
                self._unknown_signal_ids.add(signal_id)
                self._record_finish(
                    service,
                    timestamp=timestamp,
                    next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                    blocker="AUTONOMOUS_WORKER_EXCEPTION",
                    signal_id=signal_id,
                    worker_status="DEGRADED",
                    candidates_evaluated=candidates_evaluated,
                    signals_generated=signals_generated,
                    orders_attempted=orders_attempted,
                    error_code="AUTONOMOUS_WORKER_EXCEPTION",
                )
                return {
                    "status": "ERROR",
                    "decision": "AUTONOMOUS_WORKER_EXCEPTION",
                    "blocker": "AUTONOMOUS_WORKER_EXCEPTION",
                    "candidate_id": str(winner),
                    "signal_id": signal_id,
                    "error_type": type(exc).__name__,
                    "ranking": ranking,
                }
            self._record_finish(
                service,
                timestamp=timestamp,
                next_decision="WAIT_FOR_NEXT_DECISION",
                signal_id=signal_id,
                candidates_evaluated=candidates_evaluated,
                signals_generated=signals_generated,
                orders_attempted=orders_attempted,
            )
            return {
                "status": "SUBMITTED",
                "decision": "SUBMITTED",
                "candidate_id": str(winner),
                "signal_id": signal_id,
                "submission": dict(submission),
                "ranking": ranking,
            }
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
