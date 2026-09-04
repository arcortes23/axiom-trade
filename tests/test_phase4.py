from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
from axiom.dashboard import DashboardData, _dashboard_html
from axiom.director import research_summary, validate_hermes_proposal
from axiom.experiment_plan import ExperimentPlan, ExperimentPlanError
from axiom.forward import ForwardTestRegistry
from axiom.lifecycle import CandidateStage, PromotionCriteria
from axiom.paper import LiveExecutionDisabled, PaperTradingConfig
from axiom.paper_engine import run_forward_paper
from axiom.research import ResearchReport, write_report
from axiom.research_bus import DurableResearchBus, ResearchQueueStatus
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


def prediction_rows(*, version: str = "v1", model_probability: float = 0.8) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(10):
        opened = T0 + timedelta(hours=index * 2)
        common: dict[str, object] = {
            "market_id": f"market-{index}",
            "question": "Will the public event resolve YES?",
            "yes_bid": 0.49,
            "yes_ask": 0.51,
            "yes_mid": 0.50,
            "no_bid": 0.49,
            "no_ask": 0.51,
            "no_mid": 0.50,
            "model_probability": model_probability,
            "liquidity": 100.0,
            "expiry": (opened + timedelta(days=1)).isoformat(),
            "resolution_criteria": "publicly observable result",
            "dataset_version": version,
        }
        rows.append({**common, "timestamp": opened.isoformat(), "settlement": "open"})
        rows.append(
            {
                **common,
                "timestamp": (opened + timedelta(hours=1)).isoformat(),
                "settlement": "resolved_yes",
            }
        )
    return rows


def experiment_plan(*, dataset_id: str = "dataset", dataset_version: str = "v1", max_variants: int = 1) -> dict[str, object]:
    return {
        "market_type": "prediction",
        "template": "probability_mispricing",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "parameters": {"threshold": [0.03, 0.05][:max_variants]},
        "min_samples": 1,
        "min_trades": 0,
        "max_variants": max_variants,
        "paper_only": True,
    }


def proposal(
    proposal_id: str,
    *,
    dataset_id: str = "dataset",
    dataset_version: str = "v1",
    model_probability: float | None = None,
    max_variants: int = 1,
) -> dict[str, object]:
    plan = experiment_plan(dataset_id=dataset_id, dataset_version=dataset_version, max_variants=max_variants)
    if model_probability is not None:
        plan["model_document"] = {"probability": model_probability}
    return {
        "proposal_id": proposal_id,
        "statement": "A deterministic public-data probability edge is testable.",
        "source": "immutable test dataset",
        "tests": ["chronological train-validation backtest", "bounded robustness checks"],
        "dataset_version": dataset_version,
        "time_split": "train-validation-holdout",
        "paper_only": True,
        "experiment_plan": plan,
    }


def relaxed_criteria(*, forward_trades: int = 0) -> PromotionCriteria:
    return PromotionCriteria(
        min_independent_samples=0,
        min_trades=forward_trades,
        max_drawdown=1.0,
        min_expectancy=-1.0,
        min_confidence_lower_bound=-1.0,
        min_stability=0.0,
        min_calibration=0.0,
        min_liquidity=0.0,
        min_forward_duration_seconds=0.0,
        min_regimes=0,
    )


def processor(
    store: AxiomStore,
    bus: DurableResearchBus,
    *,
    max_items: int = 1,
    lease_seconds: float = 300.0,
    max_children: int = 0,
    max_generation: int = 2,
    daily_limit: int = 250,
    criteria: PromotionCriteria | None = None,
) -> AutonomousResearchProcessor:
    return AutonomousResearchProcessor(
        store,
        bus=bus,
        config=AutonomousResearchConfig(
            max_items_per_cycle=max_items,
            lease_seconds=lease_seconds,
            max_plan_variants=8,
            max_children_per_parent=max_children,
            max_generation_depth=max_generation,
            max_experiments_per_day=daily_limit,
            promotion_criteria=criteria or relaxed_criteria(),
        ),
        clock=lambda: T0,
    )


class Phase4AutonomousLoopTests(unittest.TestCase):
    def test_plan_numeric_ranges_are_bounded_and_deterministic(self) -> None:
        ranged = ExperimentPlan.from_mapping(
            {
                **experiment_plan(),
                "hypothesis_id": "hypothesis-range",
                "max_variants": 3,
                "parameters": {"threshold": {"min": 0.03, "max": 0.05, "step": 0.01}},
            }
        )
        variants = ranged.variants()
        self.assertEqual([item["threshold"] for item in variants], [0.03, 0.04, 0.05])
        self.assertEqual(ranged.plan_hash, ExperimentPlan.from_mapping(ranged.as_dict()).plan_hash)
        with self.assertRaisesRegex(ExperimentPlanError, "UNBOUNDED_PARAMETER_RANGE"):
            ExperimentPlan.from_mapping(
                {
                    **experiment_plan(),
                    "hypothesis_id": "hypothesis-unbounded",
                    "parameters": {"threshold": {"min": 0.0, "max": 100.0, "step": 0.01}},
                }
            )

    def test_declarative_plan_runs_historical_lifecycle_and_registers_forward(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", prediction_rows())
            bus = DurableResearchBus(store)
            item = bus.submit_hypothesis(proposal("hypothesis-complete"), available_at=T0, dedupe_key="hypothesis-complete")
            cycle = processor(store, bus).process_pending(now=T0)

            self.assertEqual(cycle.completed, 1)
            queued = bus.get(item.item_id)
            self.assertIsNotNone(queued)
            assert queued is not None
            self.assertEqual(queued.status, ResearchQueueStatus.COMPLETED)
            self.assertTrue(queued.result["accepted"])
            self.assertFalse(queued.result["paper_only"] is False)

            plans = store.list_experiment_plans()
            self.assertEqual(len(plans), 1)
            self.assertEqual(plans[0]["status"], "COMPLETED")
            self.assertEqual(plans[0]["plan"]["experiment_family"], "probability_mispricing")

            candidates = store.load_candidate_lifecycle(limit=None)
            summary = research_summary(store, now=T0, limit=10)
            self.assertIn("budget", summary["autonomous"])
            self.assertEqual(summary["autonomous"]["lifecycle_funnel"], {"PAPER_FORWARD": 1})
            self.assertEqual(summary["autonomous"]["accounting"]["dataset_reuse"], {"v1": 1})
            self.assertEqual(summary["autonomous"]["accounting"]["daily_reservations"], 1)
            dashboard = DashboardData(store=store).snapshot("autonomous-research")
            self.assertEqual(dashboard["lifecycle_funnel"], {"PAPER_FORWARD": 1})
            self.assertIsNotNone(dashboard["budgets"])
            self.assertEqual(len(candidates), 1)
            candidate = candidates[0]
            self.assertEqual(candidate["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertFalse(candidate["payload"]["holdout_used"])
            self.assertEqual(candidate["payload"]["registration_timestamp"], T0.isoformat())
            self.assertEqual(len(store.load_forward_tests()), 1)

            phases = [event["to_status"] for event in store.list_research_queue_events(item.item_id)]
            self.assertEqual(
                phases,
                [
                    "TESTING",
                    "CLAIM",
                    "VALIDATE",
                    "ACCEPT",
                    "BOUNDED_EXPERIMENT",
                    "TEST",
                    "RESULT",
                    "LIFECYCLE",
                    "COMPLETE",
                    "COMPLETED",
                ],
            )
            budget = store.load_experiment_budget("autonomous")
            self.assertEqual(budget["budget"]["used_by_family"], {"probability_mispricing": 1})
            forward_config = store.load_forward_tests()[0]["config"]
            self.assertFalse(forward_config["live_execution"])
            self.assertNotIn("broker", json.dumps(forward_config).lower())

    def test_forward_paper_evidence_reaches_human_review_gate_without_live_route(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", prediction_rows())
            bus = DurableResearchBus(store)
            bus.submit_hypothesis(proposal("hypothesis-forward"), available_at=T0, dedupe_key="hypothesis-forward")
            active = processor(store, bus, criteria=relaxed_criteria(forward_trades=1))
            active.process_pending(now=T0)
            record = store.load_candidate_lifecycle(limit=None)[0]
            payload = record["payload"]
            spec = ForwardTestRegistry(store).get(payload["forward_test_id"])
            self.assertIsNotNone(spec)
            assert spec is not None
            strategy = payload["strategy"]
            future = T0 + timedelta(hours=1)
            observations = [
                {
                    "market_id": "forward-market",
                    "timestamp": future.isoformat(),
                    "yes_bid": 0.49,
                    "yes_ask": 0.51,
                    "yes_mid": 0.50,
                    "no_ask": 0.51,
                    "no_mid": 0.50,
                    "model_probability": 0.80,
                    "liquidity": 100.0,
                    "expiry": (future + timedelta(days=1)).isoformat(),
                    "settlement": "open",
                },
                {
                    "market_id": "forward-market",
                    "timestamp": (future + timedelta(minutes=1)).isoformat(),
                    "yes_bid": 0.49,
                    "yes_ask": 0.51,
                    "yes_mid": 0.50,
                    "no_bid": 0.49,
                    "no_ask": 0.51,
                    "no_mid": 0.50,
                    "liquidity": 100.0,
                    "expiry": (future + timedelta(days=1)).isoformat(),
                    "settlement": "resolved_yes",
                },
            ]
            paper_cycle = run_forward_paper(
                spec,
                store=store,
                strategy=strategy,
                model={"field": "model_probability"},
                observations=observations,
                now=future + timedelta(minutes=1),
            )
            self.assertGreaterEqual(paper_cycle.fills_inserted, 1)
            result = active.reevaluate_forward_candidates(now=future + timedelta(days=1))
            self.assertEqual(result[0]["stage"], CandidateStage.PAPER_PROMOTABLE.value)
            updated = store.load_candidate_lifecycle(record["candidate_id"])
            self.assertEqual(updated["stage"], CandidateStage.PAPER_PROMOTABLE.value)
            self.assertFalse(updated["payload"]["holdout_used"])
            with self.assertRaises(LiveExecutionDisabled):
                PaperTradingConfig(live=True)
        with self.assertRaisesRegex(ExperimentPlanError, "UNSAFE_PLAN_FIELD"):
            ExperimentPlan.from_proposal(
                {
                    "proposal_id": "unsafe-top-level",
                    "statement": "ignored",
                    "source": "public source",
                    "dataset_version": "v1",
                    "paper_only": True,
                    "code": "return 1",
                }
            )
        validation = validate_hermes_proposal(
            {
                "proposal_id": "unsafe-top-level-director",
                "statement": "ignored",
                "source": "public source",
                "tests": ["bounded test"],
                "dataset_version": "v1",
                "time_split": "train-validation-holdout",
                "paper_only": True,
                "code": "return 1",
            }
        )
        self.assertFalse(validation.accepted)
        self.assertIn("UNSAFE_PLAN_FIELD", validation.reasons)

    def test_restart_after_crash_retries_transaction_without_duplicates(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", prediction_rows())
            bus = DurableResearchBus(store)
            item = bus.submit_hypothesis(proposal("hypothesis-restart"), available_at=T0, dedupe_key="hypothesis-restart")

            class CrashAfterWork(AutonomousResearchProcessor):
                crashed = False

                def _process_item(self, queued, now):  # type: ignore[no-untyped-def]
                    result = super()._process_item(queued, now)
                    if not self.crashed:
                        self.crashed = True
                        raise KeyboardInterrupt("simulated process restart")
                    return result

            crashing = CrashAfterWork(
                store,
                bus=bus,
                config=processor(store, bus, lease_seconds=1).config,
                clock=lambda: T0,
            )
            with self.assertRaises(KeyboardInterrupt):
                crashing.process_pending(now=T0)
            claimed = bus.get(item.item_id)
            self.assertIsNotNone(claimed)
            assert claimed is not None
            self.assertEqual(claimed.status, ResearchQueueStatus.TESTING)
            self.assertEqual(store.list_experiment_plans(), [])
            self.assertEqual(store.load_candidate_lifecycle(limit=None), [])

            retry = processor(store, bus, lease_seconds=1)
            cycle = retry.process_pending(now=T0 + timedelta(seconds=2))
            self.assertEqual(cycle.released, 1)
            self.assertEqual(cycle.completed, 1)
            self.assertEqual(len(store.list_experiment_plans()), 1)
            self.assertEqual(len(store.load_candidate_lifecycle(limit=None)), 1)
            self.assertEqual(len(store.list_strategies()), 1)

    def test_losing_validation_is_terminal_rejection_and_safety_fields_are_explicit(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", prediction_rows(model_probability=0.2))
            bus = DurableResearchBus(store)
            losing = bus.submit_hypothesis(proposal("hypothesis-losing"), available_at=T0, dedupe_key="hypothesis-losing")
            cycle = processor(store, bus, max_children=0).process_pending(now=T0)
            self.assertEqual(cycle.rejected, 1)
            rejected = bus.get(losing.item_id)
            self.assertIsNotNone(rejected)
            assert rejected is not None
            self.assertEqual(rejected.status, ResearchQueueStatus.REJECTED)
            self.assertEqual(rejected.result["status"], "unsupported_by_validation")
            lifecycle = store.load_candidate_lifecycle(limit=None)[0]
            self.assertEqual(lifecycle["stage"], CandidateStage.REJECTED.value)
            self.assertEqual(lifecycle["payload"]["rejection_reason"], "negative_validation_expectancy")

            raw = store.enqueue_research_item(
                "hypothesis",
                {**proposal("malicious"), "live_execution": False},
                available_at=T0,
                dedupe_key="malicious",
            )
            safety_cycle = processor(store, bus, max_children=0).process_pending(now=T0)
            self.assertEqual(safety_cycle.rejected, 1)
            safety_result = bus.get(raw["item_id"])
            self.assertIsNotNone(safety_result)
            assert safety_result is not None
            self.assertEqual(safety_result.result["reason_code"], "LIVE_EXECUTION_FORBIDDEN")
            rejection_summary = research_summary(store, now=T0)
            self.assertGreaterEqual(rejection_summary["autonomous"]["rejection_reasons"]["LIVE_EXECUTION_FORBIDDEN"], 1)

            with self.assertRaisesRegex(ExperimentPlanError, "LIVE_EXECUTION_FORBIDDEN"):
                ExperimentPlan.from_mapping({**experiment_plan(), "hypothesis_id": "unsafe", "live_execution": False})
            with self.assertRaisesRegex(ExperimentPlanError, "UNSAFE_PLAN_FIELD"):
                ExperimentPlan.from_mapping({**experiment_plan(), "hypothesis_id": "unsafe", "callback": "not executable"})

    def test_mutation_limits_daily_budget_and_queue_types_are_deterministic(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", prediction_rows())
            bus = DurableResearchBus(store)
            root = bus.submit_hypothesis(
                proposal("hypothesis-mutations", max_variants=2),
                available_at=T0,
                dedupe_key="hypothesis-mutations",
            )
            first = processor(store, bus, max_children=2, max_generation=1, daily_limit=3).process_pending(now=T0)
            self.assertEqual(first.completed, 1)
            pending_candidates = store.list_research_items(status="PENDING", limit=20)
            self.assertEqual(len(pending_candidates), 1)
            self.assertTrue(all(item["item_type"] == "candidate" for item in pending_candidates))
            self.assertTrue(all(item["payload"]["generation"] == 1 for item in pending_candidates))
            self.assertTrue(all(item["payload"]["holdout_used"] is False for item in pending_candidates))
            budget = store.load_experiment_budget("autonomous")["budget"]
            self.assertEqual(budget["used_total"], 3)
            self.assertLessEqual(len(pending_candidates), 2)

            second = processor(store, bus, max_children=1, max_generation=2, daily_limit=4).process_pending(now=T0)
            self.assertEqual(second.completed, 1)
            nested_candidates = store.list_research_items(status="PENDING", limit=20)
            self.assertEqual(len(nested_candidates), 1)
            self.assertEqual(nested_candidates[0]["payload"]["generation"], 2)
            self.assertEqual(store.load_experiment_budget("autonomous")["budget"]["used_total"], 4)
            malformed = dict(nested_candidates[0]["payload"])
            malformed["candidate_id"] = "candidate-malformed-generation"
            malformed["generation"] = "2"
            malformed_item = bus.submit_candidate(
                malformed,
                available_at=T0,
                dedupe_key="candidate-malformed-generation",
            )
            self.assertEqual(malformed_item.status, ResearchQueueStatus.PENDING)
            self.assertEqual(store.load_experiment_budget("autonomous")["budget"]["used_total"], 4)
            report = bus.submit_report({"report_id": "report-1", "summary": "persisted"}, available_at=T0, dedupe_key="report-1")
            review = bus.submit_review_request({"hypothesis_id": "hypothesis-mutations"}, available_at=T0, dedupe_key="review-1")
            raw_unknown = store.enqueue_research_item("unsupported", {}, available_at=T0, dedupe_key="unsupported")
            cycle = processor(store, bus, max_items=8, max_children=0, daily_limit=3).process_pending(now=T0)
            self.assertGreaterEqual(cycle.completed, 2)
            self.assertEqual(bus.get(report.item_id).status, ResearchQueueStatus.COMPLETED)
            self.assertEqual(bus.get(review.item_id).status, ResearchQueueStatus.COMPLETED)
            self.assertEqual(bus.get(raw_unknown["item_id"]).status, ResearchQueueStatus.REJECTED)
            self.assertEqual(bus.get(raw_unknown["item_id"]).result["reason_code"], "UNSUPPORTED_ITEM_TYPE")
            self.assertEqual(bus.get(malformed_item.item_id).status, ResearchQueueStatus.REJECTED)
            self.assertEqual(bus.get(malformed_item.item_id).result["reason_code"], "INVALID_CANDIDATE")
            self.assertEqual(len(store.list_research_items(status="PENDING", limit=20)), 0)
            self.assertEqual(len(store.load_candidate_lifecycle(limit=None)), 4)
            self.assertEqual(root.item_id.startswith("queue-"), True)

    def test_summary_dashboard_and_report_outputs_are_machine_and_human_readable(self) -> None:
        with AxiomStore(":memory:") as store:
            summary = research_summary(store, now=T0, limit=10)
            self.assertFalse(summary["live_execution"])
            self.assertIn("hermes", summary)
            self.assertIn("autonomous", summary)
            self.assertIn("accounting", summary["autonomous"])
            self.assertIsNotNone(DashboardData(store=store).snapshot("autonomous-research"))
            self.assertIn("autonomous-research", _dashboard_html())
            json.dumps(summary)

        report = ResearchReport(T0, {"bars": 0}, {"markets": 0}, ("public data only",))
        with tempfile.TemporaryDirectory() as directory:
            json_path = Path(directory) / "report.json"
            markdown_path = Path(directory) / "report.md"
            write_report(report, str(json_path))
            write_report(report, str(markdown_path))
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))["crypto"]["bars"], 0)
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertTrue(markdown.startswith("# Axiom Research Report"))
            self.assertIn("## Limitations", markdown)


if __name__ == "__main__":
    unittest.main()
