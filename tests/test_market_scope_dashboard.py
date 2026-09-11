from __future__ import annotations

import unittest

from datetime import datetime, timezone
import tempfile

from axiom.market_scope import MarketScopeResolution
from axiom.storage import AxiomStore
from axiom.dashboard import DashboardData, _dashboard_html


class _CountingCandidateResults(list):
    def __init__(self, values):
        super().__init__(values)
        self.visited = 0

    def __iter__(self):
        for value in super().__iter__():
            self.visited += 1
            yield value


class _PersistedScopeStore:
    def __init__(self) -> None:
        self.calls = 0
        self.payload = {
            "available": True,
            "as_of": "2026-09-09T12:00:00+00:00",
            "resolution_count": 1,
            "stage_counts": {
                "historically_qualified": 1,
                "valid_frozen_scope": 1,
                "matching_current_markets": 2,
                "fresh_complete_inputs": 1,
                "strategy_evaluated": 1,
                "ready_signal": 0,
                "execution_feasible": 0,
                "submitted": 0,
                "filled": 0,
            },
            "stages": {
                "historically_qualified": {
                    "count": 1,
                    "blocker_counts": {},
                    "timestamps": {"latest": "2026-09-09T11:59:00+00:00", "earliest": "2026-09-09T11:59:00+00:00"},
                },
                "valid_frozen_scope": {
                    "count": 1,
                    "blocker_counts": {},
                    "timestamps": {"latest": "2026-09-09T11:59:01+00:00", "earliest": "2026-09-09T11:59:01+00:00"},
                },
                "matching_current_markets": {
                    "count": 2,
                    "blocker_counts": {"MARKET_NOT_FOUND": 3},
                    "timestamps": {"latest": "2026-09-09T11:59:02+00:00", "earliest": "2026-09-09T11:59:02+00:00"},
                },
                "fresh_complete_inputs": {
                    "count": 1,
                    "blocker_counts": {"STALE_INPUT": 1, "INCOMPLETE_INPUT": 2},
                    "timestamps": {"latest": "2026-09-09T11:59:03+00:00", "earliest": "2026-09-09T11:59:03+00:00"},
                },
                "strategy_evaluated": {
                    "count": 1,
                    "blocker_counts": {},
                    "timestamps": {"latest": "2026-09-09T11:59:04+00:00", "earliest": "2026-09-09T11:59:04+00:00"},
                },
                "ready_signal": {"count": 0, "blocker_counts": {"NO_SIGNAL": 1}, "timestamps": {"latest": None, "earliest": None}},
                "execution_feasible": {"count": 0, "blocker_counts": {"RISK_BLOCKED": 1}, "timestamps": {"latest": None, "earliest": None}},
                "submitted": {"count": 0, "blocker_counts": {"NOT_READY": 1}, "timestamps": {"latest": None, "earliest": None}},
                "filled": {"count": 0, "blocker_counts": {"NOT_SUBMITTED": 1}, "timestamps": {"latest": None, "earliest": None}},
            },
            "blocker_counts": {"MARKET_NOT_FOUND": 3, "STALE_INPUT": 1, "INCOMPLETE_INPUT": 2, "NO_SIGNAL": 1},
            "timestamps": {
                "as_of": "2026-09-09T12:00:00+00:00",
                "latest": "2026-09-09T11:59:04+00:00",
                "earliest": "2026-09-09T11:59:00+00:00",
            },
        }

    def market_scope_resolution_funnel(self, *, limit: int = 1000):
        self.calls += 1
        if limit > 1000:
            raise AssertionError("dashboard exceeded bounded funnel limit")
        return self.payload

    def candidate_forward_requirements(self, **_kwargs):
        raise AssertionError("dashboard must not recompute candidate authority")

    def polymarket_required_health(self, **_kwargs):
        raise AssertionError("dashboard must not scan current markets")


class _ResearchProgressStore:
    def __init__(self, *, with_candidate: bool = True) -> None:
        self.with_candidate = with_candidate
        candidate_id = "candidate-progress"
        self.queue_item = {
            "item_id": "queue-progress",
            "item_type": "hypothesis",
            "status": "REJECTED",
            "created_at": "2026-09-11T00:00:00+00:00",
            "updated_at": "2026-09-11T01:00:00+00:00",
            "payload": {
                "dataset_id": "Polymarket-historical",
                "dataset_version": "sha256:dataset-v1",
            },
            "result": {
                "accepted": False,
                "blocker": "NO_SUPPORTED_EDGE",
                "reason_code": "INSUFFICIENT_DATA",
                "dataset_selector": {
                    "dataset_id": "Polymarket-historical",
                    "dataset_version": "sha256:dataset-v1",
                },
                "candidate_results": [
                    {
                        "candidate_id": candidate_id,
                        "stage": None,
                        "reason_code": "INSUFFICIENT_DATA",
                    }
                ],
            },
        }
        self.lifecycle = {
            "candidate_id": candidate_id,
            "stage": "SCHEMA_VALIDATED",
            "updated_at": "2026-09-11T01:00:00+00:00",
            "payload": {
                "dataset_id": "Polymarket-historical",
                "dataset_version": "sha256:dataset-v1",
                "experiment_plan": {"min_samples": 30, "min_trades": 20},
                "minimum_sample_check": {
                    "count": 830,
                    "min_observations": 30,
                    "trades": 6,
                    "min_trades": 20,
                    "passed": False,
                },
            },
        }

    def market_scope_resolution_funnel(self, *, limit: int = 1000):
        return {"available": False, "stage_counts": {}, "stages": {}, "blocker_counts": {}}

    def dashboard_overview_summary(self, *, activity_limit: int = 8):
        return {
            "counts": {},
            "catalog": {},
            "candidate_stages": {"SCHEMA_VALIDATED": 1} if self.with_candidate else {},
            "queue_statuses": {"REJECTED": 1} if self.with_candidate else {},
            "bootstrap_statuses": {},
            "workers": [],
            "latest_queue_item": self.queue_item if self.with_candidate else None,
            "latest_activity": [],
            "logical_rows": {},
        }

    def list_research_items(self, *, limit: int = 50):
        return [self.queue_item] if self.with_candidate else []

    def load_candidate_lifecycle(self, candidate_id=None, *, limit=50):
        if not self.with_candidate:
            return []
        return [self.lifecycle] if candidate_id is None else self.lifecycle if candidate_id == self.lifecycle["candidate_id"] else None

    def load_forward_tests(self, *, limit: int = 100):
        return [
            {
                "experiment_id": "forward-candidate-progress",
                "config": {"candidate_id": "candidate-progress"},
            }
        ] if self.with_candidate else []

    def paper_history_counts(self, experiment_id: str):
        return {"observations": 4, "fills": 0} if experiment_id == "forward-candidate-progress" else {"observations": 0, "fills": 0}

    def get_scheduler_state(self, name: str):
        return {
            "status": "ACTIVE",
            "schedule": "after_each_collection",
            "last_run_at": "2026-09-11T01:00:00+00:00",
            "next_run_at": "2026-09-11T02:00:00+00:00",
        }

    def list_worker_states(self, *, limit: int = 32):
        return [{"worker_name": "research-queue", "status": "idle", "payload": {}}]

    def get_collector_state(self, name: str):
        return {
            "last_cycle_ended_at": "2026-09-11T00:59:00+00:00",
            "next_scheduled_collection_at": "2026-09-11T01:05:00+00:00",
        }

    def list_collection_cycles(self, *, collector_name: str = "polymarket", limit: int = 4):
        return []


class _LifecycleHistoryStore(_ResearchProgressStore):
    def load_candidate_lifecycle(self, candidate_id=None, *, limit=50):
        if candidate_id is not None:
            return None
        older = dict(self.lifecycle)
        older["stage"] = "IDEA"
        older["updated_at"] = "2026-09-11T00:00:00+00:00"
        newer = dict(self.lifecycle)
        newer["stage"] = "PAPER_FORWARD"
        newer["updated_at"] = "2026-09-11T01:00:00+00:00"
        return [older, newer]


class _MixedResearchProgressStore(_ResearchProgressStore):
    """Persist a newer legacy row beside an older authenticated starter."""

    def __init__(self) -> None:
        super().__init__()
        marker = {
            "schema": "axiom-generated-queue-v1",
            "generated": True,
            "kind": "predeclared_starting_set",
            "proposal_identity": "sha256:current-starter-identity",
            "dataset_id": "Polymarket-historical",
            "dataset_version": "history-v2",
            "attestation_hash": "sha256:current-attestation",
        }
        current_payload = {
            "candidate_id": "candidate-current",
            "dataset_id": "Polymarket-historical",
            "dataset_version": "history-v2",
            "experiment_plan": {"min_samples": 30, "min_trades": 20},
            "provenance": {"internal": marker},
        }
        self.current_queue_item = {
            "item_id": "queue-current",
            "item_type": "hypothesis",
            "status": "REJECTED",
            "created_at": "2026-09-11T16:00:00+00:00",
            "updated_at": "2026-09-11T16:40:00+00:00",
            "payload": current_payload,
            "result": {
                "dataset_id": "Polymarket-historical",
                "dataset_version": "history-v2",
                # This aggregate value belongs to neither candidate and must
                # not replace the selected candidate's plan gate.
                "required_trades": 1,
                "candidate_results": [
                    {
                        "candidate_id": "candidate-current",
                        "stage": "SCHEMA_VALIDATED",
                        "reason_code": "CURRENT_BLOCKER",
                    }
                ],
            },
        }
        self.queue_item["updated_at"] = "2026-09-11T16:55:00+00:00"
        self.queue_item["payload"] = {
            "dataset_id": "prediction:4199932",
            "dataset_version": "legacy-v1",
        }
        self.queue_item["result"]["dataset_selector"] = {
            "dataset_id": "prediction:4199932",
            "dataset_version": "legacy-v1",
        }
        self.queue_item["result"]["required_trades"] = 1
        self.queue_item["result"]["candidate_results"][0]["candidate_id"] = "candidate-legacy"
        self.legacy_lifecycle = dict(self.lifecycle)
        self.legacy_lifecycle["candidate_id"] = "candidate-legacy"
        self.legacy_lifecycle["payload"] = {
            "dataset_id": "prediction:4199932",
            "dataset_version": "legacy-v1",
            "experiment_plan": {"min_samples": 1, "min_trades": 1},
        }
        self.current_lifecycle = {
            "candidate_id": "candidate-current",
            "stage": "SCHEMA_VALIDATED",
            "updated_at": "2026-09-11T16:45:00+00:00",
            "payload": {
                **current_payload,
                "blocker": "CURRENT_BLOCKER",
            },
        }

    def list_research_items(self, *, limit: int = 50):
        return [self.queue_item, self.current_queue_item][:limit]

    def load_candidate_lifecycle(self, candidate_id=None, *, limit=50):
        rows = [self.legacy_lifecycle, self.current_lifecycle]
        if candidate_id is not None:
            return next((row for row in rows if candidate_id == row["candidate_id"]), None)
        return rows[:limit]

    def list_worker_states(self, *, limit: int = 32):
        return [
            {
                "worker_name": "research-queue",
                "status": "idle",
                "heartbeat_at": "2026-09-11T16:47:05+00:00",
                "payload": {
                    "last_cycle": {
                        "status": "idle",
                        "claimed": 0,
                        "completed_at": "2026-09-11T16:47:05+00:00",
                    }
                },
            }
        ][:limit]






class MarketScopeDashboardTests(unittest.TestCase):
    def test_overview_reads_only_persisted_bounded_funnel(self) -> None:
        store = _PersistedScopeStore()
        snapshot = DashboardData(store=store).overview_summary()
        funnel = snapshot["market_scope_funnel"]
        self.assertEqual(store.calls, 1)
        self.assertEqual(funnel["stage_counts"]["matching_current_markets"], 2)
        self.assertEqual(funnel["stages"]["fresh_complete_inputs"]["blocker_counts"], {"STALE_INPUT": 1, "INCOMPLETE_INPUT": 2})
        self.assertEqual(funnel["timestamps"]["latest"], "2026-09-09T11:59:04+00:00")
        self.assertEqual(snapshot["forward_evidence"]["grade_scope"], "persisted_market_scope_resolution")

    def test_research_progress_joins_compact_queue_stage_and_reports_persisted_validation(self) -> None:
        snapshot = DashboardData(store=_ResearchProgressStore()).overview_summary()
        progress = snapshot["research_progress"]
        self.assertEqual(progress["candidate_count"], 1)
        self.assertEqual(progress["candidate_stage"], "SCHEMA_VALIDATED")
        self.assertEqual(progress["dataset_id"], "Polymarket-historical")
        self.assertEqual(progress["dataset_version"], "sha256:dataset-v1")
        self.assertEqual(progress["samples_available"], 830)
        self.assertEqual(progress["samples_required"], 30)
        self.assertEqual(progress["trades_available"], 6)
        self.assertEqual(progress["trades_required"], 20)
        self.assertEqual(progress["forward_observations"], 4)
        self.assertEqual(progress["blocker"], "INSUFFICIENT_DATA")
        self.assertEqual(progress["job_status"], "ACTIVE")
        self.assertEqual(progress["next_run_at"], "2026-09-11T02:00:00+00:00")

    def test_research_progress_prefers_current_generated_candidate_and_worker_tick(self) -> None:
        progress = DashboardData(store=_MixedResearchProgressStore()).overview_summary()[
            "research_progress"
        ]

        self.assertEqual(progress["dataset_id"], "Polymarket-historical")
        self.assertEqual(progress["dataset_version"], "history-v2")
        self.assertEqual(progress["candidate_stage"], "SCHEMA_VALIDATED")
        self.assertEqual(progress["blocker"], "CURRENT_BLOCKER")
        self.assertIsNone(progress["samples_available"])
        self.assertEqual(progress["samples_required"], 30)
        self.assertIsNone(progress["trades_available"])
        self.assertEqual(progress["trades_required"], 20)
        self.assertEqual(progress["job_status"], "ACTIVE")
        self.assertEqual(progress["last_completion_at"], "2026-09-11T16:47:05+00:00")


    def test_research_progress_bounds_oversized_nested_candidate_results(self) -> None:
        store = _ResearchProgressStore()
        candidate_results = _CountingCandidateResults(
            [
                {"candidate_id": "candidate-progress", "stage": None, "reason_code": "INSUFFICIENT_DATA"},
                *[
                    {
                        "candidate_id": f"candidate-extra-{index:03d}",
                        "stage": "SCHEMA_VALIDATED",
                    }
                    for index in range(200)
                ],
            ]
        )
        store.queue_item["result"]["candidate_results"] = candidate_results
        store.queue_item["status"] = "PENDING"

        progress = DashboardData(store=store).overview_summary()["research_progress"]

        self.assertEqual(candidate_results.visited, 50)
        self.assertEqual(progress["candidate_count"], 50)
        self.assertEqual(progress["candidate_stage"], "SCHEMA_VALIDATED")
        self.assertEqual(progress["samples_available"], 830)
        self.assertEqual(progress["trades_available"], 6)
        self.assertEqual(progress["forward_observations"], 4)

    def test_research_progress_fallback_uses_latest_lifecycle_record(self) -> None:
        store = _LifecycleHistoryStore()
        store.queue_item["result"]["candidate_results"] = []

        progress = DashboardData(store=store).overview_summary()["research_progress"]

        self.assertEqual(progress["candidate_count"], 1)
        self.assertEqual(progress["candidate_stage"], "PAPER_FORWARD")
        self.assertEqual(progress["samples_available"], 830)

    def test_research_progress_does_not_invent_candidate_or_blocker_when_queue_is_empty(self) -> None:
        progress = DashboardData(store=_ResearchProgressStore(with_candidate=False)).overview_summary()[
            "research_progress"
        ]
        self.assertEqual(progress["candidate_count"], 0)
        self.assertIsNone(progress["candidate_stage"])
        self.assertIsNone(progress["blocker"])
        self.assertEqual(progress["forward_observations"], 0)
    def test_research_progress_uses_exact_worker_lookup_beyond_bounded_page(self) -> None:
        store = AxiomStore(":memory:")
        self.addCleanup(store.close)
        heartbeat = datetime(2026, 9, 11, 16, 47, 5, tzinfo=timezone.utc)
        payload = {"last_cycle": {"status": "idle", "claimed": 0}}
        for index in range(33):
            store.save_worker_state(f"research-{index:03d}", "IDLE", {})
        store.save_worker_state("research-queue", "IDLE", payload, heartbeat_at=heartbeat)
        store.set_scheduler_state(
            "hermes-control",
            {"status": "ACTIVE", "last_run_at": "2026-09-01T00:00:00+00:00"},
        )

        worker = store.get_worker_state("research-queue")
        self.assertIsNotNone(worker)
        assert worker is not None
        self.assertEqual(
            set(worker),
            {"worker_name", "status", "payload", "started_at", "heartbeat_at", "updated_at"},
        )
        self.assertEqual(worker["worker_name"], "research-queue")
        self.assertEqual(worker["status"], "IDLE")
        self.assertEqual(worker["payload"], payload)
        self.assertIsNone(worker["started_at"])
        self.assertEqual(worker["heartbeat_at"], heartbeat)
        self.assertIsInstance(worker["updated_at"], datetime)

        progress = DashboardData(store=store).overview_summary()["research_progress"]
        self.assertEqual(progress["job_status"], "ACTIVE")
        self.assertEqual(progress["last_completion_at"], str(heartbeat))

    def test_real_store_resolution_is_exposed_without_market_catalog_scan(self) -> None:
        store = AxiomStore(":memory:")
        self.addCleanup(store.close)
        resolution = MarketScopeResolution(
            candidate_id="candidate-1",
            scope_hash="sha256:scope",
            scope_version="1",
            resolved_at=datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc),
            status="MATCHED",
            reason="MATCHED",
            policy={"schema_version": "1", "mode": "EXACT_MARKETS", "market_ids": ["market-1"]},
            provenance={"source": "test"},
        )
        store.save_market_scope_resolution(resolution)
        funnel = DashboardData(store=store).market_scope_funnel_data()
        self.assertEqual(funnel["total"], 1)
        self.assertEqual(funnel["resolution_status_counts"], {"MATCHED": 1})
        self.assertEqual(funnel["resolution_items"][0]["candidate_id"], "candidate-1")
        self.assertEqual(funnel["timestamps"]["latest"], "2026-09-09T12:00:00+00:00")
    def test_resolution_statuses_and_reasons_remain_distinct_from_order_readiness(self) -> None:
        store = _PersistedScopeStore()
        store.payload = {
            "total": 2,
            "status_counts": {"MATCHED": 1, "INVALID_POLICY": 1},
            "reason_counts": {"MATCHED": 1, "MALFORMED_POLICY": 1},
            "stages": [{"status": "INVALID_POLICY", "count": 1}, {"status": "MATCHED", "count": 1}],
            "blockers": [{"reason": "MALFORMED_POLICY", "count": 1}],
            "latest_resolved_at": "2026-09-09T12:00:00+00:00",
            "items": [{"candidate_id": "c-1", "status": "MATCHED", "reason": "MATCHED", "resolved_at": "2026-09-09T12:00:00+00:00"}],
        }
        snapshot = DashboardData(store=store).overview_summary()
        funnel = snapshot["market_scope_funnel"]
        self.assertEqual(funnel["resolution_status_counts"], {"MATCHED": 1, "INVALID_POLICY": 1})
        self.assertEqual(funnel["resolution_reason_counts"], {"MATCHED": 1, "MALFORMED_POLICY": 1})
        self.assertEqual(funnel["blocker_counts"], {"MALFORMED_POLICY": 1})
        self.assertEqual(funnel["timestamps"]["latest"], "2026-09-09T12:00:00+00:00")
        self.assertEqual(funnel["stage_counts"]["ready_signal"], 0)
        self.assertEqual(
            funnel["stages"]["ready_signal"]["blocker_counts"],
            {"NO_PERSISTED_READY_SIGNAL": 0},
        )

    def test_rendered_surface_names_all_handoff_stages_and_separates_readiness(self) -> None:
        html = _dashboard_html()
        for label in (
            "historically_qualified",
            "valid_frozen_scope",
            "matching_current_markets",
            "fresh_complete_inputs",
            "strategy_evaluated",
            "ready_signal",
            "execution_feasible",
            "submitted",
            "filled",
        ):
            self.assertIn(label, html)
        self.assertIn("Qualification is historical evidence only", html)
        self.assertIn("blocker_counts", html)
        self.assertIn("market-scope-funnel", html)
        self.assertIn("AUTOMATIC RESEARCH", html)
        self.assertIn("Samples available / required", html)
        self.assertIn("Trades available / required", html)
        self.assertIn("Forward observations", html)
        self.assertIn("Last completion", html)
        self.assertIn("Next run", html)

    def test_settings_review_surface_hides_machine_fences_and_duplicate_inputs(self) -> None:
        html = _dashboard_html()
        self.assertIn("Submissions/day", html)
        self.assertIn("Maximum all-in buy", html)
        self.assertIn("Gross daily buy budget", html)
        self.assertIn("Open exposure", html)
        self.assertIn("Review changes", html)
        self.assertIn("Confirm activation", html)
        self.assertIn("Review active limits and enable", html)
        self.assertNotIn("Draft config ID", html)
        self.assertNotIn("Reviewed ACTIVE config ID", html)
        self.assertNotIn("risk-config-id", html)
        self.assertNotIn("risk-active-config-id", html)
        self.assertNotIn("max_aggregate_open_cost_usd\",\"Open exposure", html)
        self.assertNotIn("Advanced drawdown", html)
        self.assertIn("NO_ELIGIBLE_CANDIDATES", html)
        self.assertIn("CONNECTIVITY_CHECK_STALE", html)
        self.assertIn("function renderCanary(data)", html)
        self.assertIn("connectivityAgeMs>=0", html)
        self.assertIn("values.max_aggregate_open_cost_usd=input.value", html)
        self.assertIn('data-risk-optional="clearable"', html)
        self.assertIn('input.dataset.riskOptional==="clearable"', html)
        self.assertIn('input.value===""?null:input.value', html)
        self.assertIn('displayedStatus=c.status==="READY"&&!fresh?"STALE":c.status', html)
        self.assertIn('throw new Error("CUSTOM_ORDER_SUBMISSIONS_REQUIRED")', html)
        self.assertIn('["max_submitted_orders_per_day","Submissions/day","number"]', html)

    def test_rendered_autonomous_scan_does_not_promote_historical_winner(self) -> None:
        html = _dashboard_html()
        start = html.index("function renderCanaryAutonomousState(data)")
        end = html.index("const _renderCanaryResearchAndAction", start)
        renderer = html[start:end]
        self.assertIn(
            'const currentSelectionBinding=selectionValid&&selectionStatus==="CURRENT"',
            renderer,
        )
        self.assertIn(
            "const researchCandidate=currentSelectionBinding?boundResearchCandidate:null",
            renderer,
        )
        self.assertIn(
            "Number(eligibleCount)===0&&Number(rankableCount)===0;",
            renderer,
        )
        self.assertIn("const coverage=noEligible?0:", renderer)
        self.assertIn('scanStatus==="IN_PROGRESS"?"IN_PROGRESS"', renderer)
        self.assertIn("NO_ELIGIBLE_CANDIDATES", renderer)
        self.assertIn("Selected winner · Historical selected ID", html)


if __name__ == "__main__":
    unittest.main()
