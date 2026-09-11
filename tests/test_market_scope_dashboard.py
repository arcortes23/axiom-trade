from __future__ import annotations

import unittest

from datetime import datetime, timezone
import tempfile

from axiom.market_scope import MarketScopeResolution
from axiom.storage import AxiomStore
from axiom.dashboard import DashboardData, _dashboard_html


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
