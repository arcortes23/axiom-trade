"""Bounded autonomous canary scheduling, isolated from collection and research."""
from __future__ import annotations

from datetime import datetime
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

    def tick(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Evaluate all candidates once; never retry an existing UNKNOWN signal."""
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "decision": "DECISION_ALREADY_IN_PROGRESS"}
        timestamp = ensure_utc(now or self.clock())
        service = CanaryService(self.store, clock=self.clock)
        try:
            service.record_autonomous_decision(
                next_decision="EVALUATING_CANDIDATES",
                worker_status="RUNNING",
                timestamp=timestamp,
            )
            ranking = CandidateCanaryRanker(
                self.store,
                service=service,
                clock=self.clock,
            ).evaluate_and_select(timestamp)
            control = service.status()
            if control.get("micro_live_canary") == "KILLED":
                result = {
                    "status": "BLOCKED",
                    "decision": "KILL_LATCHED",
                    "blocker": "CANARY_KILLED",
                    "ranking": ranking,
                }
                service.record_autonomous_decision(
                    next_decision="KILL_LATCHED",
                    blocker="CANARY_KILLED",
                    worker_status="KILLED",
                    timestamp=timestamp,
                )
                return result
            if control.get("micro_live_canary") != AUTONOMOUS_MICRO_LIVE:
                result = {
                    "status": "DISABLED",
                    "decision": "AUTONOMOUS_CANARY_DISABLED",
                    "blocker": "AUTONOMOUS_CANARY_DISABLED",
                    "ranking": ranking,
                }
                service.record_autonomous_decision(
                    next_decision="ENABLE AUTO CANARY",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                    worker_status="IDLE",
                    timestamp=timestamp,
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
                service.record_autonomous_decision(
                    next_decision="WAIT_FOR_ELIGIBLE_CANDIDATE",
                    blocker="NO_ELIGIBLE_RANKABLE_CANDIDATE",
                    worker_status="IDLE",
                    timestamp=timestamp,
                )
                return result
            try:
                signal = service.generate_signal(str(winner))
            except CanaryBlocked as exc:
                result = {
                    "status": "BLOCKED",
                    "decision": "NO_ACTIONABLE_SIGNAL",
                    "blocker": str(exc),
                    "candidate_id": str(winner),
                    "ranking": ranking,
                }
                service.record_autonomous_decision(
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
                    blocker=str(exc),
                    worker_status="IDLE",
                    timestamp=timestamp,
                )
                return result
            if not isinstance(signal, Mapping) or str(signal.get("status") or "").upper() != "READY":
                signal_status = str(signal.get("status") if isinstance(signal, Mapping) else "NONE").upper()
                blocker = "UNKNOWN_NO_RETRY" if signal_status == "UNKNOWN" else "NO_ACTIONABLE_SIGNAL"
                service.record_autonomous_decision(
                    next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
                    blocker=blocker,
                    signal_id=str(signal.get("signal_id")) if isinstance(signal, Mapping) else None,
                    worker_status="IDLE",
                    timestamp=timestamp,
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
            if not service.credentials.configured(allow_environment=False):
                blocker = "CREDENTIALS_NOT_CONFIGURED"
                service.record_autonomous_decision(
                    next_decision="WAIT_FOR_CREDENTIALS",
                    blocker=blocker,
                    signal_id=signal_id,
                    worker_status="IDLE",
                    timestamp=timestamp,
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
            try:
                submission = service.submit_signal(signal_id, venue=venue)
            except CanaryBlocked as exc:
                blocker = str(exc)
                decision = "UNKNOWN_NO_RETRY" if blocker == "CANARY_SUBMISSION_UNKNOWN" else blocker
                service.record_autonomous_decision(
                    next_decision="UNKNOWN_NO_RETRY" if decision == "UNKNOWN_NO_RETRY" else "WAIT_FOR_NEXT_DECISION",
                    blocker=decision,
                    signal_id=signal_id,
                    worker_status="IDLE",
                    timestamp=timestamp,
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
                service.record_autonomous_decision(
                    next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                    blocker="AUTONOMOUS_WORKER_EXCEPTION",
                    signal_id=signal_id,
                    worker_status="ERROR",
                    timestamp=timestamp,
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
            service.record_autonomous_decision(
                next_decision="WAIT_FOR_NEXT_DECISION",
                signal_id=signal_id,
                worker_status="IDLE",
                timestamp=timestamp,
            )
            return {
                "status": "SUBMITTED",
                "decision": "SUBMITTED",
                "candidate_id": str(winner),
                "signal_id": signal_id,
                "submission": dict(submission),
                "ranking": ranking,
            }
        except Exception as exc:
            service.record_autonomous_decision(
                next_decision="WORKER_ERROR_REVIEW_REQUIRED",
                blocker="AUTONOMOUS_WORKER_EXCEPTION",
                worker_status="ERROR",
                timestamp=timestamp,
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
            self.tick()
            stop_event.wait(self.interval_seconds)


__all__ = ["AutonomousCanaryWorker"]
