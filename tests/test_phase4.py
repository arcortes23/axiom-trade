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
from axiom.research_bus import DurableResearchBus, ResearchBusPermissionError, ResearchQueueStatus
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


def relaxed_criteria(
    *,
    forward_trades: int = 0,
    min_resolved_bets_for_performance_rejection: int = 5,
) -> PromotionCriteria:
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
        min_resolved_bets_for_performance_rejection=min_resolved_bets_for_performance_rejection,
    )


class _BuyEverySnapshot:
    def __init__(self, quantity: float = 1.0) -> None:
        self.quantity = quantity

    def signal(self, context: object) -> dict[str, object]:
        return {"side": "buy_yes", "quantity": self.quantity}

    def to_dict(self) -> dict[str, object]:
        return {"id": "buy-every-snapshot"}


class _NoSignal:
    def signal(self, context: object) -> None:
        return None

    def to_dict(self) -> dict[str, object]:
        return {"id": "no-signal"}


def _forward_evidence(store: AxiomStore, spec: object, *, now: datetime) -> dict[str, object]:
    active = processor(store, DurableResearchBus(store), criteria=relaxed_criteria())
    return active._forward_evidence({"payload": {"forward_test_id": spec.experiment_id}}, now)




def _observation(
    market_id: str,
    timestamp: datetime,
    *,
    settlement: str = "open",
    model_probability: float = 0.8,
    **extra: object,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "timestamp": timestamp.isoformat(),
        "yes_bid": 0.49,
        "yes_ask": 0.51,
        "yes_mid": 0.50,
        "no_bid": 0.49,
        "no_ask": 0.51,
        "no_mid": 0.50,
        "model_probability": model_probability,
        "liquidity": 100.0,
        "expiry": (timestamp + timedelta(days=1)).isoformat(),
        "settlement": settlement,
        **extra,
    }
def _register_forward_candidate(
    store: AxiomStore,
    bus: DurableResearchBus,
    *,
    proposal_id: str,
    criteria: PromotionCriteria,
) -> tuple[AutonomousResearchProcessor, dict[str, object], object]:
    store.save_dataset("dataset", "v1", prediction_rows())
    item = bus.submit_hypothesis(proposal(proposal_id), available_at=T0, dedupe_key=proposal_id)
    active = processor(store, bus, criteria=criteria)
    cycle = active.process_pending(now=T0)
    if cycle.completed != 1:
        raise AssertionError(cycle)
    record = store.load_candidate_lifecycle(limit=None)[0]
    spec = ForwardTestRegistry(store).get(record["payload"]["forward_test_id"])
    if spec is None:
        raise AssertionError(f"missing forward spec for {item.item_id}")
    return active, record, spec


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
                {
                    "market_id": "forward-market-2",
                    "timestamp": (future + timedelta(minutes=2)).isoformat(),
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
                    "market_id": "forward-market-2",
                    "timestamp": (future + timedelta(minutes=3)).isoformat(),
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
                now=future + timedelta(minutes=3),
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
    def test_ordering_fields_are_safe_but_order_and_secret_fields_remain_forbidden(self) -> None:
        safe_proposal = proposal("ordering-regression")
        safe_proposal["experiment_plan"]["methodology"] = {
            "ordering": "chronological",
            "chronological_ordering": True,
            "ordering_policy": "oldest_first",
        }
        plan = ExperimentPlan.from_proposal(safe_proposal)
        self.assertEqual(plan.methodology["ordering"], "chronological")
        self.assertEqual(plan.methodology["ordering_policy"], "oldest_first")
        with AxiomStore(":memory:") as store:
            item = DurableResearchBus(store).submit_hypothesis(safe_proposal)
            self.assertEqual(item.payload["experiment_plan"]["methodology"]["ordering"], "chronological")
            for forbidden_key in (
                "order",
                "order_id",
                "orders",
                "place_order",
                "submit_order",
                "execute_order",
                "live_execution",
                "execution",
                "api_key",
                "private_key",
                "credential",
                "wallet",
                "account",
                "withdraw",
                "authorization",
                "bearer",
                "secret",
                "password",
            ):
                with self.subTest(validator="research_bus", forbidden_key=forbidden_key):
                    unsafe = proposal(f"bus-{forbidden_key}")
                    unsafe["experiment_plan"]["methodology"] = {forbidden_key: "unsafe"}
                    with self.assertRaises(ResearchBusPermissionError):
                        DurableResearchBus(store).submit_hypothesis(unsafe)
                with self.subTest(validator="experiment_plan", forbidden_key=forbidden_key):
                    unsafe = proposal(f"plan-{forbidden_key}")
                    unsafe["experiment_plan"]["methodology"] = {forbidden_key: "unsafe"}
                    with self.assertRaises(ExperimentPlanError):
                        ExperimentPlan.from_proposal(unsafe)
        unsafe_holdout = proposal("plan-locked-holdout")
        unsafe_holdout["experiment_plan"]["methodology"] = {"locked_holdout": "unsafe"}
        with self.assertRaises(ExperimentPlanError):
            ExperimentPlan.from_proposal(unsafe_holdout)

    def test_one_losing_forward_bet_does_not_trigger_performance_rejection(self) -> None:
        with AxiomStore(":memory:") as store:
            bus = DurableResearchBus(store)
            criteria = relaxed_criteria(
                forward_trades=1,
                min_resolved_bets_for_performance_rejection=2,
            )
            active, record, spec = _register_forward_candidate(
                store,
                bus,
                proposal_id="one-losing-forward-bet",
                criteria=criteria,
            )
            start = T0 + timedelta(hours=1)
            observations = [
                _observation("losing-market", start),
                _observation("losing-market", start + timedelta(minutes=1), settlement="resolved_no"),
            ]
            run_forward_paper(
                spec,
                store=store,
                strategy=record["payload"]["strategy"],
                model={"field": "model_probability"},
                observations=observations,
                now=start + timedelta(minutes=1),
            )
            result = active.reevaluate_forward_candidates(now=start + timedelta(days=1))[0]
            self.assertEqual(result["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertEqual(result["forward_evidence"]["forward_independent_resolved_bets"], 1)
            self.assertLess(result["forward_evidence"]["forward_expectancy"], 0.0)
            self.assertNotIn("forward_negative_expectancy", result["promotion_reasons"])

    def test_multiple_fills_same_market_count_one_independent_resolved_bet(self) -> None:
        with AxiomStore(":memory:") as store:
            strategy = _BuyEverySnapshot()
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"field": "model_probability"},
                start_timestamp=T0,
                allowed_markets=("same-market",),
            )
            start = T0 + timedelta(hours=1)
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=_BuyEverySnapshot(),
                model={"field": "model_probability"},
                observations=[
                    _observation("same-market", start),
                    _observation("same-market", start + timedelta(minutes=1)),
                    _observation("same-market", start + timedelta(minutes=2), settlement="resolved_yes"),
                ],
                now=start + timedelta(minutes=2),
            )
            evidence = _forward_evidence(store, spec, now=start + timedelta(days=1))
            ledger = store.list_paper_bet_ledger(spec.experiment_id)
            self.assertEqual(cycle.fills_inserted, 2)
            self.assertEqual(evidence["fills"], 2)
            self.assertEqual(evidence["successful_order_attempts"], 2)
            self.assertEqual(evidence["independent_markets_traded"], 1)
            self.assertEqual(evidence["forward_independent_resolved_bets"], 1)
            self.assertEqual(evidence["resolved_positions"], 1)
            self.assertEqual(len(ledger), 1)
            self.assertEqual(ledger[0]["payload"]["fills"], 2)

    def test_unresolved_fills_are_excluded_from_expectancy(self) -> None:
        with AxiomStore(":memory:") as store:
            strategy = _BuyEverySnapshot()
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"field": "model_probability"},
                start_timestamp=T0,
                allowed_markets=("unresolved-market",),
            )
            start = T0 + timedelta(hours=1)
            run_forward_paper(
                spec,
                store=store,
                strategy=_BuyEverySnapshot(),
                model={"field": "model_probability"},
                observations=[_observation("unresolved-market", start)],
                now=start,
            )
            evidence = _forward_evidence(store, spec, now=start + timedelta(days=1))
            self.assertEqual(evidence["fills"], 1)
            self.assertEqual(evidence["markets_traded"], 1)
            self.assertEqual(evidence["markets_resolved"], 0)
            self.assertEqual(evidence["forward_independent_resolved_bets"], 0)
            self.assertEqual(evidence["unresolved_fills"], 1)
            self.assertIsNone(evidence["forward_expectancy"])
            self.assertIsNone(evidence["forward_net_pnl"])

    def test_no_signal_snapshot_is_not_counted_as_no_fill(self) -> None:
        with AxiomStore(":memory:") as store:
            strategy = _NoSignal()
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"field": "model_probability"},
                start_timestamp=T0,
                allowed_markets=("no-signal-market",),
            )
            start = T0 + timedelta(hours=1)
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=_NoSignal(),
                model={"field": "model_probability"},
                observations=[_observation("no-signal-market", start)],
                now=start,
            )
            evidence = _forward_evidence(store, spec, now=start + timedelta(days=1))
            events = store.list_paper_execution_events(spec.experiment_id)
            self.assertEqual(cycle.fills_inserted, 0)
            self.assertEqual(events[0]["status"], "NO_SIGNAL")
            self.assertEqual(evidence["observations_without_signal"], 1)
            self.assertEqual(evidence["forward_order_attempts"], 0)
            self.assertEqual(evidence["no_fill_orders"], 0)

    def test_partial_fill_accounting_uses_requested_and_filled_quantity(self) -> None:
        with AxiomStore(":memory:") as store:
            strategy = _BuyEverySnapshot(quantity=2.0)
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"field": "model_probability"},
                start_timestamp=T0,
                allowed_markets=("partial-market",),
            )
            start = T0 + timedelta(hours=1)
            observation = _observation(
                "partial-market",
                start,
                yes_order_book={
                    "timestamp": start.isoformat(),
                    "bids": [{"price": 0.49, "size": 10.0}],
                    "asks": [{"price": 0.51, "size": 0.25}],
                },
            )
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=_BuyEverySnapshot(quantity=2.0),
                model={"field": "model_probability"},
                observations=[observation],
                now=start,
            )
            evidence = _forward_evidence(store, spec, now=start + timedelta(days=1))
            event = store.list_paper_execution_events(spec.experiment_id)[0]
            self.assertEqual(cycle.fills_inserted, 1)
            self.assertEqual(event["status"], "PARTIAL_FILL")
            self.assertEqual(evidence["partial_fills"], 1)
            self.assertEqual(evidence["successful_order_attempts"], 1)
            self.assertGreater(evidence["requested_quantity"], evidence["filled_quantity"])
            self.assertLess(evidence["fill_ratio"], 1.0)
            self.assertGreater(evidence["depth_consumed"], 0.0)

    def test_forward_promotion_does_not_borrow_validation_metrics(self) -> None:
        with AxiomStore(":memory:") as store:
            bus = DurableResearchBus(store)
            active, record, spec = _register_forward_candidate(
                store,
                bus,
                proposal_id="validation-not-forward",
                criteria=relaxed_criteria(),
            )
            result = active.reevaluate_forward_candidates(now=T0 + timedelta(days=1))[0]
            evidence = result["forward_evidence"]
            self.assertEqual(result["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertIsNone(evidence["forward_confidence_lower_bound"])
            self.assertIsNone(evidence["forward_stability"])
            self.assertIsNone(evidence["forward_calibration"])
            self.assertIn("forward_confidence_lower_bound", result["promotion_reasons"])
            self.assertIn("forward_stability_floor", result["promotion_reasons"])
            self.assertIn("forward_calibration_floor", result["promotion_reasons"])
            persisted = store.load_candidate_lifecycle(record["candidate_id"])
            self.assertIsNone(persisted["payload"]["forward_confidence_lower_bound"])
            self.assertIsNotNone(persisted["payload"]["validation_confidence_lower_bound"])
            self.assertEqual(persisted["payload"]["forward_test_id"], spec.experiment_id)

    def test_expectancy_uses_resolved_independent_markets_and_costs(self) -> None:
        with AxiomStore(":memory:") as store:
            strategy = _BuyEverySnapshot()
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"field": "model_probability"},
                start_timestamp=T0,
                allowed_markets=("market-one", "market-two"),
            )
            start = T0 + timedelta(hours=1)
            observations = [
                _observation("market-one", start),
                _observation("market-one", start + timedelta(minutes=1)),
                _observation("market-one", start + timedelta(minutes=2), settlement="resolved_yes"),
                _observation("market-two", start + timedelta(minutes=3)),
                _observation("market-two", start + timedelta(minutes=4), settlement="resolved_no"),
            ]
            run_forward_paper(
                spec,
                store=store,
                strategy=_BuyEverySnapshot(),
                model={"field": "model_probability"},
                config=PaperTradingConfig(fee_rate=0.01, slippage_bps=100.0),
                observations=observations,
                now=start + timedelta(minutes=4),
            )
            evidence = _forward_evidence(store, spec, now=start + timedelta(days=1))
            ledger = store.list_paper_bet_ledger(spec.experiment_id)
            ledger_payloads = [row["payload"] for row in ledger]
            expected = sum(float(row["net_pnl"]) for row in ledger_payloads) / len(ledger_payloads)
            self.assertEqual(evidence["markets_traded"], 2)
            self.assertEqual(evidence["forward_independent_resolved_bets"], 2)
            self.assertEqual(evidence["fills"], 3)
            self.assertEqual(evidence["forward_expectancy"], expected)
            self.assertEqual(
                evidence["forward_net_pnl"],
                sum(float(row["net_pnl"]) for row in ledger_payloads),
            )
            self.assertGreater(evidence["forward_fees"], 0.0)
            self.assertGreater(evidence["forward_slippage"], 0.0)

    def test_severe_drawdown_is_hard_rejection_without_resolved_bets(self) -> None:
        with AxiomStore(":memory:") as store:
            bus = DurableResearchBus(store)
            criteria = PromotionCriteria(
                min_independent_samples=0,
                min_trades=0,
                max_drawdown=0.20,
                min_expectancy=-1.0,
                min_confidence_lower_bound=-1.0,
                min_stability=0.0,
                min_calibration=0.0,
                min_liquidity=0.0,
                min_forward_duration_seconds=0.0,
                min_regimes=0,
                min_resolved_bets_for_performance_rejection=5,
                min_order_attempts_for_execution_rejection=5,
            )
            active, record, spec = _register_forward_candidate(
                store,
                bus,
                proposal_id="hard-drawdown",
                criteria=criteria,
            )
            store.save_paper_state(
                spec.experiment_id,
                {"portfolio": {"equity": 1000.0}, "paper_only": True},
                timestamp=T0 + timedelta(hours=1),
                expected_version=-1,
            )
            result = active.reevaluate_forward_candidates(now=T0 + timedelta(hours=1))[0]
            self.assertEqual(result["stage"], CandidateStage.REJECTED.value)
            self.assertEqual(result["forward_evidence"]["forward_independent_resolved_bets"], 0)
            persisted = store.load_candidate_lifecycle(record["candidate_id"])
            self.assertEqual(persisted["payload"]["rejection_reason"], "forward_drawdown_limit")

    def test_locked_holdout_is_not_consumed_by_autonomous_evaluation(self) -> None:
        marker = "LOCKED_HOLDOUT_SENTINEL"
        rows = prediction_rows()
        for row in rows:
            if row["market_id"] == "market-9":
                row["question"] = marker
        with AxiomStore(":memory:") as store:
            store.save_dataset("dataset", "v1", rows)
            bus = DurableResearchBus(store)

            class RecordingProcessor(AutonomousResearchProcessor):
                seen_splits: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

                def _evaluate_datasets(self, plan, strategy, train, validation):  # type: ignore[no-untyped-def]
                    self.seen_splits.append(
                        (
                            tuple(str(row.get("question", "")) for row in train),
                            tuple(str(row.get("question", "")) for row in validation),
                        )
                    )
                    return super()._evaluate_datasets(plan, strategy, train, validation)

            active = RecordingProcessor(
                store,
                bus=bus,
                config=processor(store, bus).config,
                clock=lambda: T0,
            )
            plan = ExperimentPlan.from_proposal(proposal("holdout-isolation"))
            _, split = active._load_split(plan)
            self.assertTrue(any(marker in str(row.get("question")) for row in split.holdout))
            self.assertFalse(any(marker in str(row.get("question")) for row in split.train))
            self.assertFalse(any(marker in str(row.get("question")) for row in split.validation))
            item = bus.submit_hypothesis(
                proposal("holdout-isolation"),
                available_at=T0,
                dedupe_key="holdout-isolation",
            )
            cycle = active.process_pending(now=T0)
            self.assertEqual(cycle.completed, 1)
            self.assertTrue(active.seen_splits)
            for train_questions, validation_questions in active.seen_splits:
                self.assertNotIn(marker, train_questions)
                self.assertNotIn(marker, validation_questions)
            queued = bus.get(item.item_id)
            self.assertIsNotNone(queued)
            self.assertNotIn(marker, json.dumps(queued.result))
            self.assertNotIn(marker, json.dumps(store.load_candidate_lifecycle(limit=None), default=str))


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
