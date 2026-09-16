from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from axiom.rolling_portfolio import (
    MAX_REASON_LENGTH,
    RollingEvidence,
    RollingAdmissionPolicy,
    default_rolling_admission_policy,
    evaluate_rolling_selection,
)
from axiom.backtest.prediction import (
    PRICE_PROXY_RESEARCH,
    RECORDED_BOOK_REPLAY,
    PredictionMarketBacktester,
    run_prediction_research_mode,
)
from axiom.domain import Fill, MarketType, Side
from axiom.metrics import calculate_prediction_metrics
from axiom.experiment_plan import normalize_market_scope
from axiom.storage import AxiomStore
from axiom.autonomous import (
    AutonomousResearchProcessor,
    _MAX_ROLLING_REPLAY_PAYLOAD_BYTES,
    _rolling_cursor_index,
    _rolling_cursor_record,
    _rolling_hash,
    _rolling_prerequisite_fingerprint,
    _rolling_inject_source_binding,
    _rolling_rule_scope_market_ids,
    _rolling_work_items,
)


UTC = timezone.utc
NOW = datetime(2026, 1, 31, 12, tzinfo=UTC)


def _store(tmp_path) -> AxiomStore:
    return AxiomStore(str(tmp_path / "rolling.sqlite3"))


def _strategy(strategy_version_id: str = "sv-alpha", **overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "strategy_version_id": strategy_version_id,
        "strategy_id": strategy_version_id.split("-", 1)[-1],
        "version": "1",
        "strategy_hash": f"sha256:{strategy_version_id}",
        "config_hash": f"config:{strategy_version_id}",
        "candidate_id": f"candidate-{strategy_version_id}",
        "created_at": NOW.isoformat(),
        "payload": {"family": "mean-reversion", "parameters": {"lookback": 7}},
    }
    record.update(overrides)
    return record


def _trial(
    strategy_version_id: str = "sv-alpha",
    research_trial_id: str | None = None,
    **overrides: object,
) -> dict[str, object]:
    record: dict[str, object] = {
        "research_trial_id": research_trial_id or f"trial-{strategy_version_id}",
        "strategy_version_id": strategy_version_id,
        "candidate_id": f"candidate-{strategy_version_id}",
        "trial_kind": "ROLLING_RESEARCH",
        "status": "COMPLETED",
        "started_at": (NOW - timedelta(days=8)).isoformat(),
        "completed_at": (NOW - timedelta(days=1)).isoformat(),
        "terminal": True,
        "result": {"paper_only": True, "outcomes": 12},
    }
    record.update(overrides)
    return record


def _policy(policy_id: str = "rolling-default", **overrides: object) -> RollingAdmissionPolicy:
    values: dict[str, object] = {
        "policy_id": policy_id,
        "version": "1",
        "config_hash": f"sha256:{policy_id}",
        "requested_window_days": (7,),
        "review_interval_days": 1,
        "max_members": 3,
        "min_actual_coverage_seconds": 6 * 24 * 60 * 60,
        "min_completed_outcomes": 5,
        "min_reliability": "0.70",
        "min_score": "0.00",
        "replacement_margin": "0.10",
        "cooldown_seconds": 24 * 60 * 60,
        "global_budget": "10.00",
        "experimental_allocation_enabled": False,
        "weights": {"return": "1", "reliability": "1", "drawdown": "1"},
    }
    values.update(overrides)
    return RollingAdmissionPolicy.from_mapping(values)

def _canonicalize_evidence(record: dict[str, object]) -> dict[str, object]:
    canonical = dict(record)
    canonical.pop("evidence_digest", None)
    canonical.pop("digest", None)
    if isinstance(canonical.get("execution_feasibility"), bool):
        canonical["execution_feasibility"] = str(canonical["execution_feasibility"])
    canonical["evidence_digest"] = RollingEvidence.from_mapping(canonical).evidence_digest
    return canonical




def _evidence(
    strategy_version_id: str = "sv-alpha",
    evidence_window_id: str = "window-alpha-7",
    *,
    days: int = 7,
    actual_days: int | None = None,
    score: str = "0.80",
    overlap_key: str | None = None,
    hard_failure: bool = False,
    **overrides: object,
) -> dict[str, object]:
    actual_days = days if actual_days is None else actual_days
    through = NOW
    available_from = through - timedelta(days=actual_days)
    record: dict[str, object] = {
        "strategy_version_id": strategy_version_id,
        "evidence_window_id": evidence_window_id,
        "candidate_id": f"candidate-{strategy_version_id}",
        "research_trial_id": f"trial-{strategy_version_id}",
        "available_from": available_from.isoformat(),
        "available_through": through.isoformat(),
        "requested_days": days,
        "actual_coverage_seconds": actual_days * 86400,
        "source_class": "HISTORICAL",
        "observation_completeness": str(
            min(Decimal("1"), Decimal(actual_days) / Decimal(days))
        ),
        "paper_sizing": "10.00",
        "fee_assumption": "0.0025",
        "slippage_assumption": "0.0050",
        "allocated_capital_net_return": score,
        "realized_pnl": "8.00",
        "unrealized_pnl": "1.00",
        "fees": "0.25",
        "costs": "0.50",
        "drawdown": "0.03",
        "completed_outcomes": 12,
        "reliability": "0.90",
        "execution_feasibility": True,
        "evidence_digest": "",
        "overlap_key": overlap_key or f"overlap:{strategy_version_id}",
        "hard_failure": hard_failure,
        "failure_reason": "BROKEN_EXECUTION_FEASIBILITY" if hard_failure else None,
    }
    record.update(overrides)
    return _canonicalize_evidence(record)


def _v2_evidence(
    strategy_version_id: str = "sv-v2",
    evidence_window_id: str = "window-v2-7",
    *,
    no_trade: bool = False,
    **overrides: object,
) -> dict[str, object]:
    accounting: dict[str, object] = {
        "accounting_available": True,
        "initial_cash": "100.00",
        "cash": "100.00",
        "equity": "100.00",
        "realized_pnl": "0.00" if no_trade else "8.00",
        "unrealized_pnl": "0.00",
        "net_pnl": "0.00" if no_trade else "8.00",
        "fees": "0.00" if no_trade else "0.25",
        "costs": "0.00" if no_trade else "0.50",
        "open_positions": [],
        "opening_fills": 0,
        "closing_fills": 0,
        "partial_closing_fills": 0,
        "completed_round_trips": 0 if no_trade else 12,
    }
    record = _evidence(
        strategy_version_id,
        evidence_window_id,
        score="0.80" if no_trade else "0.90",
        completed_outcomes=0 if no_trade else 12,
        realized_pnl="0.00" if no_trade else "8.00",
        unrealized_pnl="0.00",
        fees="0.00" if no_trade else "0.25",
        costs="0.00" if no_trade else "0.50",
        source_digest="sha256:source-v2",
        accounting_digest="sha256:accounting-v2",
        accounting_available=True,
        accounting_complete=True,
        accounting_partial=False,
        evaluation_run_id="run-v2",
        evaluation_version="rolling-evaluation:v2",
        evaluator_invoked=True,
        evaluator_completed=True,
        evaluated_observations=12,
        signal_count=0 if no_trade else 12,
        diagnostic_summary_count=0,
        portfolio_accounting=accounting,
        evaluation={
            "evaluation_run_id": "run-v2",
            "evaluation_version": "rolling-evaluation:v2",
            "evaluator_invoked": True,
            "evaluator_completed": True,
        },
    )
    record.update(overrides)
    return _canonicalize_evidence(record)


def _selection(
    selection_id: str,
    *,
    policy_id: str = "rolling-default",
    policy_version: str = "1",
    selected_at: datetime = NOW,
    review_due_at: datetime | None = None,
    risk_config_id: str = "risk-1",
    risk_generation: int = 1,
    risk_config_hash: str = "sha256:risk-1",
    k: int = 1,
    global_budget: str = "10.00",
) -> dict[str, object]:
    return {
        "portfolio_selection_id": selection_id,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "risk_config_id": risk_config_id,
        "risk_generation": risk_generation,
        "risk_config_hash": risk_config_hash,
        "selected_at": selected_at.isoformat(),
        "review_due_at": (review_due_at or selected_at + timedelta(days=1)).isoformat(),
        "k": k,
        "global_budget": global_budget,
    }


def _member(
    strategy_version_id: str,
    *,
    selection_id: str,
    status: str = "ACTIVE",
    allocation: str = "10.00",
    score: str = "0.80",
    reason: str = "eligible",
    evidence_window_id: str | None = None,
    overlap_key: str | None = None,
    candidate_id: str | None = None,
    research_trial_id: str | None = None,
    evidence_digest: str | None = None,
    position_management_state: dict[str, object] | None = None,
) -> dict[str, object]:
    evidence_id = evidence_window_id or f"window-{strategy_version_id}-7"
    member_candidate = (
        f"candidate-{strategy_version_id}" if candidate_id is None else candidate_id
    )
    member_trial = (
        f"trial-{strategy_version_id}"
        if research_trial_id is None
        else research_trial_id
    )
    member_overlap = overlap_key or f"overlap:{strategy_version_id}"
    member_digest = evidence_digest
    if member_digest is None:
        member_digest = str(
            _evidence(
                strategy_version_id,
                evidence_id,
                candidate_id=member_candidate,
                research_trial_id=member_trial,
                overlap_key=member_overlap,
            )["evidence_digest"]
        )
    return {
        "portfolio_selection_id": selection_id,
        "strategy_version_id": strategy_version_id,
        "candidate_id": member_candidate,
        "research_trial_id": member_trial,
        "allocation": allocation,
        "status": status,
        "score": score,
        "reason": reason,
        "evidence_window_id": evidence_id,
        "evidence_digest": member_digest,
        "overlap_key": member_overlap,
        "position_management_state": position_management_state or {},
    }




def _decision_status(decision: object) -> str:
    return str(getattr(decision, "status", None) or decision.as_dict()["status"])


def _decision_members(decision: object) -> list[object]:
    members = getattr(decision, "members", None)
    if members is None:
        members = decision.as_dict().get("members", ())
    return list(members or ())


def _member_value(member: object, key: str) -> object:
    if isinstance(member, dict):
        return member[key]
    return getattr(member, key)



def _prediction_strategy(
    strategy_id: str = "prediction-integration",
    *,
    threshold: float = 0.05,
) -> dict[str, object]:
    return {
        "version": 1,
        "strategy_id": strategy_id,
        "market_type": "prediction",
        "family": "probability_mispricing",
        "parameters": {"threshold": threshold},
        "operations": [],
        "probability_model": "fixture-model-v1",
        "resolution_aware": True,
        "resolution_inputs": ["settlement"],
    }


def _raw_prediction_row(
    index: int,
    yes_price: float,
    *,
    market_id: str = "market-integration",
    model_probability: float = 0.80,
    settlement: str = "open",
    book: bool = False,
    bid_depth: float = 1000.0,
    ask_depth: float = 1000.0,
) -> dict[str, object]:
    timestamp = NOW + timedelta(days=index)
    row: dict[str, object] = {
        "market_id": market_id,
        "timestamp": timestamp,
        "model_probability": model_probability,
        "yes_mid": yes_price,
        "yes_ask": yes_price,
        "yes_bid": yes_price,
        "no_mid": 1.0 - yes_price,
        "no_ask": 1.0 - yes_price,
        "no_bid": 1.0 - yes_price,
        "liquidity": max(bid_depth, ask_depth),
        "settlement": settlement,
    }
    if book:
        row.update(
            {
                "source_type": "HISTORICAL",
                "source_timestamp": timestamp,
                "source_snapshot_id": f"source-{market_id}-{index}",
                "order_book": {
                    "timestamp": timestamp.isoformat(),
                    "bids": [[yes_price, bid_depth]],
                    "asks": [[yes_price, ask_depth]],
                    "token_id": f"yes-{market_id}",
                },
                "no_order_book": {
                    "timestamp": timestamp.isoformat(),
                    "bids": [[1.0 - yes_price, bid_depth]],
                    "asks": [[1.0 - yes_price, ask_depth]],
                    "token_id": f"no-{market_id}",
                },
            }
        )
    return row

def _prediction_fill(
    index: int,
    side: Side,
    quantity: float,
    price: float,
    *,
    market_id: str,
    symbol: str = "shared-symbol",
    partial: bool = False,
) -> Fill:
    return Fill(
        timestamp=NOW + timedelta(days=index),
        market_type=MarketType.PREDICTION,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        fees=0.0,
        slippage=0.0,
        strategy_id="metrics-regression",
        order_id=f"order-{index}",
        market_id=market_id,
        metadata={"outcome": "yes", "partial_fill": partial},
    )

class TestRollingPortfolio(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.tmp_path = Path(self._temporary_directory.name)

    def test_cursor_round_robin_wrap_persists_next_tuple(self) -> None:
        items = _rolling_work_items((_strategy("sv-beta"), _strategy("sv-alpha")))
        self.assertEqual(len(items), 16)
        cursor = _rolling_cursor_record(None, items[-1], items[0], NOW)

        self.assertEqual(_rolling_cursor_index(cursor, items), 0)
        self.assertEqual(cursor["next_strategy_version_id"], items[0][0]["strategy_version_id"])
        self.assertEqual(cursor["next_requested_days"], items[0][1])
        self.assertEqual(cursor["next_source_class"], items[0][2])
        self.assertEqual(next(iter(cursor["attempts"].values())), 1)

    def test_blocker_skip_is_scoped_to_immutable_prerequisite_fingerprint(self) -> None:
        record = {
            "work_key": "work-a",
            "strategy_version_id": "sv-alpha",
            "research_trial_id": "trial-alpha",
            "candidate_id": "candidate-sv-alpha",
            "requested_days": 7,
            "source_class": "HISTORICAL",
            "prerequisite_fingerprint": "fp-a",
            "blocker": "SOURCE_LOAD_FAILED",
            "last_attempted_at": NOW.isoformat(),
        }
        with _store(self.tmp_path) as store:
            self.assertTrue(store.save_rolling_evidence_blocker(record))
            self.assertIsNotNone(
                store.load_rolling_evidence_blocker(
                    work_key="work-a",
                    prerequisite_fingerprint="fp-a",
                )
            )
            self.assertIsNone(
                store.load_rolling_evidence_blocker(
                    work_key="work-a",
                    prerequisite_fingerprint="fp-b",
                )
            )
            self.assertTrue(
                store.save_rolling_evidence_blocker(
                    {**record, "prerequisite_fingerprint": "fp-b"}
                )
            )
            latest = store.load_rolling_evidence_blocker(work_key="work-a")
            self.assertEqual(latest["prerequisite_fingerprint"], "fp-b")

    def test_public_backtest_unresolved_event_entry_then_exit_reports_gain_and_loss(self) -> None:
        strategy = _prediction_strategy("unresolved-entry-exit")
        for exit_price, expected_pnl in ((0.60, 25.0), (0.20, -25.0)):
            rows = [
                _raw_prediction_row(0, 0.40),
                _raw_prediction_row(1, 0.40),
                _raw_prediction_row(2, exit_price),
            ]
            with self.subTest(exit_price=exit_price):
                result = run_prediction_research_mode(
                    rows,
                    strategy,
                    mode=PRICE_PROXY_RESEARCH,
                    initial_cash=100.0,
                    fee_bps=0.0,
                    slippage_bps=0.0,
                    allocation=0.50,
                    holding_period=1,
                    exit_policy={"type": "fixed_holding_period", "holding_period": 1},
                )
                accounting = result.metrics["portfolio_accounting"]
                evaluation = result.metrics["evaluation"]
                self.assertEqual(
                    [fill.metadata["execution_kind"] for fill in result.fills],
                    ["entry", "exit"],
                )
                self.assertIsNone(result.fills[1].expected_probability)
                self.assertEqual(evaluation["evaluated_observations"], 3)
                self.assertEqual(evaluation["signal_count"], 3)
                self.assertTrue(accounting["accounting_available"])
                self.assertEqual(accounting["initial_cash"], 100.0)
                self.assertAlmostEqual(accounting["cash"], 100.0 + expected_pnl)
                self.assertAlmostEqual(accounting["equity"], 100.0 + expected_pnl)
                self.assertAlmostEqual(accounting["realized_pnl"], expected_pnl)
                self.assertEqual(accounting["unrealized_pnl"], 0.0)
                self.assertAlmostEqual(accounting["net_pnl"], expected_pnl)
                self.assertEqual(accounting["open_positions"], [])
                self.assertEqual(accounting["opening_fills"], 1)
                self.assertEqual(accounting["closing_fills"], 1)
                self.assertEqual(accounting["partial_closing_fills"], 0)
                self.assertEqual(accounting["completed_round_trips"], 1)

    def test_public_legacy_same_outcome_strengthening_rebalances_inventory(self) -> None:
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.25,
        ).run(
            [
                _raw_prediction_row(0, 0.40),
                _raw_prediction_row(1, 0.20),
            ],
            _prediction_strategy("legacy-strengthening"),
        )
        self.assertEqual(
            [fill.metadata["execution_kind"] for fill in result.fills],
            ["entry", "entry"],
        )
        self.assertAlmostEqual(result.fills[0].quantity, 62.5)
        self.assertAlmostEqual(result.fills[1].quantity, 31.25)


    def test_public_recorded_replay_partial_exit_keeps_remaining_inventory(self) -> None:
        strategy = _prediction_strategy("partial-exit")
        rows = [
            _raw_prediction_row(0, 0.40, book=True, ask_depth=1000.0),
            _raw_prediction_row(
                1,
                0.60,
                book=True,
                model_probability=0.50,
                bid_depth=25.0,
                ask_depth=1000.0,
            ),
            _raw_prediction_row(
                2,
                0.60,
                book=True,
                model_probability=0.50,
                bid_depth=0.0,
                ask_depth=1000.0,
            ),
            _raw_prediction_row(
                3,
                0.60,
                book=True,
                model_probability=0.50,
                bid_depth=10.0,
                ask_depth=1000.0,
            ),
        ]
        result = run_prediction_research_mode(
            rows,
            strategy,
            mode=RECORDED_BOOK_REPLAY,
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        accounting = result.metrics["portfolio_accounting"]
        evaluation = result.metrics["evaluation"]
        self.assertEqual(
            [fill.metadata["execution_kind"] for fill in result.fills],
            ["entry", "exit", "exit"],
        )
        self.assertTrue(all(fill.expected_probability is None for fill in result.fills[1:]))
        self.assertEqual(evaluation["evaluated_observations"], 4)
        self.assertEqual(evaluation["signal_count"], 4)
        self.assertTrue(accounting["accounting_available"])
        self.assertEqual(accounting["closing_fills"], 0)
        self.assertEqual(accounting["partial_closing_fills"], 2)
        self.assertEqual(accounting["completed_round_trips"], 0)
        self.assertEqual(accounting["open_positions"], ["market-integration"])
        self.assertGreater(accounting["unrealized_pnl"], 0.0)
        self.assertEqual(accounting["net_pnl"], accounting["realized_pnl"] + accounting["unrealized_pnl"])

    def test_recorded_replay_exit_metadata_records_depth_walk_vwap(self) -> None:
        rows = [
            _raw_prediction_row(0, 0.40, book=True, ask_depth=1000.0),
            _raw_prediction_row(
                1,
                0.60,
                book=True,
                model_probability=0.50,
                bid_depth=1000.0,
                ask_depth=1000.0,
            ),
        ]
        exit_book = rows[1]["order_book"]
        assert isinstance(exit_book, dict)
        exit_book["bids"] = [[0.60, 25.0], [0.50, 25.0]]
        result = run_prediction_research_mode(
            rows,
            _prediction_strategy("depth-walk-vwap"),
            mode=RECORDED_BOOK_REPLAY,
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        exit_fill = result.fills[1]
        self.assertEqual(exit_fill.metadata["execution_kind"], "exit")
        self.assertAlmostEqual(exit_fill.metadata["raw_execution_price"], 0.55)
        self.assertAlmostEqual(exit_fill.metadata["reference_price"], 0.55)
        self.assertAlmostEqual(exit_fill.price, 0.55)

    def test_recorded_replay_curve_evidence_keeps_pending_exit_outcome_on_signal_flip(self) -> None:
        rows = [
            _raw_prediction_row(0, 0.40, book=True, ask_depth=1000.0),
            _raw_prediction_row(
                1,
                0.60,
                book=True,
                model_probability=0.20,
                bid_depth=1000.0,
                ask_depth=1000.0,
            ),
        ]
        yes_exit_book = rows[1]["order_book"]
        no_signal_book = rows[1]["no_order_book"]
        assert isinstance(yes_exit_book, dict)
        assert isinstance(no_signal_book, dict)
        yes_exit_book["timestamp"] = NOW.isoformat()
        yes_exit_book["token_id"] = "yes-exit-book"
        no_signal_book["token_id"] = "no-signal-book"
        result = run_prediction_research_mode(
            rows,
            _prediction_strategy("flip-evidence"),
            mode=RECORDED_BOOK_REPLAY,
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        self.assertEqual(
            [fill.metadata["execution_kind"] for fill in result.fills],
            ["entry", "exit"],
        )
        evidence = result.equity_curve[1]["execution_evidence"]
        self.assertEqual(evidence["outcome"], "yes")
        self.assertEqual(evidence["book_token_id"], "yes-exit-book")
        self.assertEqual(evidence["book_timestamp"], NOW.isoformat())

    def test_prediction_lot_matching_uses_market_id_before_shared_symbol(self) -> None:
        fills = (
            _prediction_fill(0, Side.BUY, 1.0, 0.40, market_id="market-a"),
            _prediction_fill(1, Side.BUY, 1.0, 0.50, market_id=" market-b "),
            _prediction_fill(2, Side.SELL, 1.0, 0.60, market_id="market-b"),
        )
        metrics = calculate_prediction_metrics([100.0], fills=fills, initial_equity=100.0)
        accounting = metrics["portfolio_accounting"]
        self.assertAlmostEqual(accounting["realized_pnl"], 0.10)
        self.assertEqual(accounting["completed_round_trips"], 1)
        self.assertEqual(accounting["open_positions"], ["market-a"])

    def test_prediction_capital_at_risk_tracks_peak_outstanding_cost_basis(self) -> None:
        fills = (
            _prediction_fill(0, Side.BUY, 10.0, 1.00, market_id="market-risk"),
            _prediction_fill(1, Side.SELL, 10.0, 1.20, market_id="market-risk"),
            _prediction_fill(2, Side.BUY, 5.0, 3.00, market_id="market-risk"),
        )
        metrics = calculate_prediction_metrics([100.0], fills=fills, initial_equity=100.0)
        accounting = metrics["portfolio_accounting"]
        self.assertAlmostEqual(accounting["allocated_capital"], 25.0)
        self.assertAlmostEqual(accounting["capital_at_risk"], 15.0)
        self.assertEqual(accounting["closing_fills"], 1)
        self.assertEqual(accounting["partial_closing_fills"], 0)

    def test_prediction_partial_closing_fill_is_counted_once(self) -> None:
        fills = (
            _prediction_fill(0, Side.BUY, 2.0, 0.40, market_id="market-partial"),
            _prediction_fill(
                1,
                Side.SELL,
                1.0,
                0.50,
                market_id="market-partial",
                partial=True,
            ),
        )
        metrics = calculate_prediction_metrics([100.0], fills=fills, initial_equity=100.0)
        accounting = metrics["portfolio_accounting"]
        self.assertEqual(accounting["closing_fills"], 0)
        self.assertEqual(accounting["partial_closing_fills"], 1)

    def test_public_backtest_completed_no_signal_has_zero_available_activity(self) -> None:
        strategy = _prediction_strategy("no-signal")
        rows = [
            _raw_prediction_row(index, 0.50, model_probability=0.50)
            for index in range(3)
        ]
        result = run_prediction_research_mode(
            rows,
            strategy,
            mode=PRICE_PROXY_RESEARCH,
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
        )
        accounting = result.metrics["portfolio_accounting"]
        evaluation = result.metrics["evaluation"]
        self.assertTrue(accounting["accounting_available"])
        self.assertEqual(accounting["initial_cash"], 100.0)
        self.assertEqual(accounting["cash"], 100.0)
        self.assertEqual(accounting["equity"], 100.0)
        self.assertEqual(accounting["realized_pnl"], 0.0)
        self.assertEqual(accounting["unrealized_pnl"], 0.0)
        self.assertEqual(accounting["net_pnl"], 0.0)
        self.assertEqual(accounting["opening_fills"], 0)
        self.assertEqual(accounting["closing_fills"], 0)
        self.assertEqual(accounting["partial_closing_fills"], 0)
        self.assertEqual(accounting["completed_round_trips"], 0)
        self.assertEqual(evaluation["evaluated_observations"], 3)
        self.assertEqual(evaluation["signal_count"], 0)


    def test_public_backtest_evaluator_failure_is_unavailable_without_synthetic_rows(self) -> None:
        rows = [
            _raw_prediction_row(0, 0.40),
            _raw_prediction_row(1, 0.40),
        ]
        with patch(
            "axiom.backtest.prediction.evaluate_signal_evaluation",
            side_effect=RuntimeError("fixture evaluator exploded"),
        ):
            result = run_prediction_research_mode(
                rows,
                _prediction_strategy("evaluator-failure"),
                mode=PRICE_PROXY_RESEARCH,
                initial_cash=100.0,
                fee_bps=0.0,
                slippage_bps=0.0,
                allocation=0.50,
            )
        accounting = result.metrics["portfolio_accounting"]
        evaluation = result.metrics["evaluation"]
        self.assertFalse(accounting["accounting_available"])
        self.assertIsNone(accounting["realized_pnl"])
        self.assertIsNone(accounting["unrealized_pnl"])
        self.assertIsNone(accounting["net_pnl"])
        self.assertTrue(evaluation["evaluator_invoked"])
        self.assertFalse(evaluation["evaluator_completed"])
        self.assertEqual(evaluation["evaluated_observations"], 0)
        self.assertEqual(evaluation["signal_count"], 0)
        self.assertIsNone(evaluation["evaluator_name"])
        self.assertEqual(evaluation["evaluator_error"], "fixture evaluator exploded")

    def test_public_backtest_rejects_malformed_timestamped_replay_input(self) -> None:
        malformed = _raw_prediction_row(0, 0.40, book=True)
        malformed["timestamp"] = "not-a-timestamp"
        with self.assertRaisesRegex(ValueError, "missing timestamp"):
            run_prediction_research_mode(
                [malformed],
                _prediction_strategy("malformed-replay"),
                mode=RECORDED_BOOK_REPLAY,
            )
    def test_unsupported_snapshot_loader_fails_closed(self) -> None:
        class UnsupportedStore:
            def load_polymarket_snapshots(self) -> list[dict[str, object]]:
                return []

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = UnsupportedStore()
        record = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "sv-alpha",
            "research_trial_id": "trial-alpha",
            "candidate_id": "candidate-sv-alpha",
        }
        with self.assertRaisesRegex(ValueError, "SOURCE_LOADER_UNSUPPORTED"):
            processor._rolling_source_rows(record, "LIVE", NOW)




    def test_evidence_provenance_digest_roundtrip_and_failure_reason_validation(self) -> None:
        base = _evidence(
            "sv-provenance",
            "window-provenance",
            source_digest="sha256:source-a",
            accounting_digest="sha256:accounting-a",
            accounting_available=True,
            accounting_complete=True,
            accounting_partial=False,
            requested_rows=4,
            available_rows=4,
            evaluated_rows=4,
        )
        first = RollingEvidence.from_mapping(base)
        second = RollingEvidence.from_mapping(
            {**base, "source_digest": "sha256:source-b"}
        )
        self.assertNotEqual(first.evidence_digest, second.evidence_digest)

        restored = RollingEvidence.from_mapping(
            json.loads(json.dumps(first.as_dict()))
        )
        self.assertEqual(restored.evidence_digest, first.evidence_digest)
        self.assertEqual(restored.source_digest, "sha256:source-a")
        self.assertEqual(restored.accounting_digest, "sha256:accounting-a")
        self.assertTrue(restored.accounting_available)
        self.assertTrue(restored.accounting_complete)
        self.assertFalse(restored.accounting_partial)
        self.assertEqual(
            (restored.requested_rows, restored.available_rows, restored.evaluated_rows),
            (4, 4, 4),
        )

        legacy_payload = {
            key: value
            for key, value in base.items()
            if key
            not in {
                "source_digest",
                "accounting_digest",
                "accounting_available",
                "accounting_complete",
                "accounting_partial",
                "requested_rows",
                "available_rows",
                "evaluated_rows",
            }
        }
        legacy = RollingEvidence.from_mapping(legacy_payload)
        self.assertIsNone(legacy.source_digest)
        self.assertIsNone(legacy.accounting_available)

        normalized = RollingEvidence.from_mapping(
            {**base, "failure_reason": "  malformed accounting  "}
        )
        self.assertEqual(normalized.failure_reason, "malformed accounting")
        with self.assertRaises(ValueError):
            RollingEvidence.from_mapping(
                {**base, "failure_reason": "x" * (MAX_REASON_LENGTH + 1)}
            )

    def test_v2_missing_accounting_state_or_metrics_cannot_be_admitted(self) -> None:
        policy = _policy(
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        unknown = _v2_evidence("sv-v2-unknown", "window-v2-unknown")
        for field_name in (
            "accounting_available",
            "accounting_complete",
            "accounting_partial",
            "evaluator_completed",
        ):
            unknown.pop(field_name, None)
        unknown["portfolio_accounting"] = {
            key: value
            for key, value in unknown["portfolio_accounting"].items()
            if key != "accounting_available"
        }
        unknown["evaluation"] = {
            key: value
            for key, value in unknown["evaluation"].items()
            if key != "evaluator_completed"
        }
        unknown = _canonicalize_evidence(unknown)
        restored_unknown = RollingEvidence.from_mapping(unknown)
        self.assertIsNone(restored_unknown.realized_pnl)
        self.assertIn(
            "accounting_unavailable",
            restored_unknown.minimum_evidence_failures(policy),
        )
        unknown_decision = evaluate_rolling_selection(policy, [unknown], None, NOW)
        unknown_member = _decision_members(unknown_decision)[0]
        self.assertNotEqual(_member_value(unknown_member, "status"), "ACTIVE")
        self.assertEqual(
            Decimal(str(_member_value(unknown_member, "allocation"))),
            Decimal("0"),
        )

        missing_metric = _v2_evidence("sv-v2-metric", "window-v2-metric")
        missing_metric["portfolio_accounting"] = {
            **missing_metric["portfolio_accounting"],
            "realized_pnl": None,
        }
        missing_metric = _canonicalize_evidence(missing_metric)
        restored_missing = RollingEvidence.from_mapping(missing_metric)
        self.assertIsNone(restored_missing.realized_pnl)
        self.assertIn(
            "accounting_fields",
            restored_missing.minimum_evidence_failures(policy),
        )
        missing_decision = evaluate_rolling_selection(policy, [missing_metric], None, NOW)
        missing_member = _decision_members(missing_decision)[0]
        self.assertNotEqual(_member_value(missing_member, "status"), "ACTIVE")

        unavailable = _v2_evidence("sv-v2-storage", "window-v2-storage")
        unavailable.update(
            {
                "accounting_available": False,
                "accounting_complete": False,
                "accounting_partial": True,
                "evaluator_completed": False,
                "portfolio_accounting": {
                    **unavailable["portfolio_accounting"],
                    "accounting_available": False,
                    "realized_pnl": None,
                    "unrealized_pnl": None,
                    "net_pnl": None,
                    "fees": None,
                    "costs": None,
                },
                "evaluation": {
                    **unavailable["evaluation"],
                    "evaluator_completed": False,
                },
            }
        )
        unavailable = _canonicalize_evidence(unavailable)
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-v2-storage"))
            store.save_strategy_evidence_window(unavailable)
            loaded = store.list_strategy_evidence_windows("sv-v2-storage")[0]
            self.assertIsNone(loaded["realized_pnl"])
            self.assertIsNone(loaded["unrealized_pnl"])
            self.assertIsNone(loaded["net_pnl"])
            self.assertIsNone(loaded["fees"])
            self.assertIsNone(loaded["costs"])

    def test_v2_accounting_content_is_part_of_evidence_digest(self) -> None:
        original = _v2_evidence("sv-v2-digest", "window-v2-digest")
        altered = {
            **original,
            "evidence_digest": original["evidence_digest"],
            "portfolio_accounting": {
                **original["portfolio_accounting"],
                "net_pnl": "999.00",
            },
        }
        self.assertNotEqual(
            RollingEvidence.from_mapping(altered).evidence_digest,
            original["evidence_digest"],
        )

        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-v2-digest"))
            with self.assertRaisesRegex(ValueError, "evidence_digest does not match canonical evidence"):
                store.save_strategy_evidence_window(altered)
        altered_status = {
            **original,
            "accounting_complete": False,
        }
        self.assertNotEqual(
            RollingEvidence.from_mapping(altered_status).evidence_digest,
            original["evidence_digest"],
        )
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-v2-digest-status"))
            with self.assertRaisesRegex(ValueError, "evidence_digest does not match canonical evidence"):
                store.save_strategy_evidence_window(altered_status)
        conflicting_status = {
            **original,
            "portfolio_accounting": {
                **original["portfolio_accounting"],
                "accounting_available": False,
            },
        }
        with self.assertRaisesRegex(ValueError, "conflicts between rolling evidence projections"):
            RollingEvidence.from_mapping(conflicting_status)

    def test_direct_constructor_rejects_mixed_null_immutable_projections(self) -> None:
        common = {
            "strategy_version_id": "sv-direct-constructor",
            "evidence_window_id": "window-direct-constructor",
            "source_class": "HISTORICAL",
            "available_from": NOW - timedelta(days=7),
            "available_through": NOW,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
        }
        with self.assertRaisesRegex(ValueError, "evaluation_kind conflicts"):
            RollingEvidence(
                **common,
                evaluation_kind="CANONICAL_SIMULATION",
                evaluation={"evaluation_kind": None},
            )
        with self.assertRaisesRegex(ValueError, "supersedes_evidence_id conflicts"):
            RollingEvidence(
                **common,
                supersedes_evidence_id="window-direct-predecessor",
                evaluation={"supersedes_evidence_id": None},
            )
        for field_name, scalar_value, nested_value in (
            ("evaluation_run_id", "run-direct-scalar", None),
            ("evaluation_version", "version-direct-scalar", None),
            ("evaluation_run_id", "run-direct-scalar", "run-direct-nested"),
            ("evaluation_version", "version-direct-scalar", "version-direct-nested"),
        ):
            with self.subTest(field=field_name, nested=nested_value):
                with self.assertRaisesRegex(ValueError, f"{field_name} conflicts"):
                    RollingEvidence(
                        **common,
                        **{field_name: scalar_value},
                        evaluation={field_name: nested_value},
                    )

        for field_name, nested_value in (
            ("evaluation_run_id", "run-direct-derived"),
            ("evaluation_version", "version-direct-derived"),
        ):
            with self.subTest(field=field_name, source="evaluation"):
                evidence = RollingEvidence(
                    **common,
                    evaluation={field_name: nested_value},
                )
                self.assertEqual(getattr(evidence, field_name), nested_value)
            with self.subTest(field=field_name, source="metrics.evaluation"):
                evidence = RollingEvidence(
                    **common,
                    metrics={"evaluation": {field_name: nested_value}},
                )
                self.assertEqual(getattr(evidence, field_name), nested_value)
    def test_direct_constructor_reconciles_metrics_status_projections(self) -> None:
        accounting = {
            "accounting_available": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "initial_cash": "100.00",
            "cash": "100.00",
            "equity": "100.00",
            "realized_pnl": "0.00",
            "unrealized_pnl": "0.00",
            "net_pnl": "0.00",
            "fees": "0.00",
            "costs": "0.00",
            "open_positions": [],
            "opening_fills": 0,
            "closing_fills": 0,
            "partial_closing_fills": 0,
            "completed_round_trips": 0,
        }
        common = {
            "strategy_version_id": "sv-direct-metrics-status",
            "evidence_window_id": "window-direct-metrics-status",
            "source_class": "HISTORICAL",
            "available_from": NOW - timedelta(days=7),
            "available_through": NOW,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "evaluation_run_id": "run-direct-metrics-status",
            "evaluation_version": "rolling-evaluation:v2",
        }
        evidence = RollingEvidence(
            **common,
            metrics={
                "evaluation": {
                    "evaluator_invoked": True,
                    "evaluator_completed": True,
                },
                "portfolio_accounting": accounting,
            },
        )
        self.assertEqual(
            (
                evidence.evaluator_invoked,
                evidence.evaluator_completed,
                evidence.accounting_available,
                evidence.accounting_complete,
                evidence.accounting_partial,
            ),
            (True, True, True, True, False),
        )
        self.assertIsNotNone(evidence.portfolio_accounting)
        restored = RollingEvidence.from_mapping(evidence.as_dict())
        self.assertEqual(
            (
                restored.evaluator_invoked,
                restored.evaluator_completed,
                restored.accounting_available,
                restored.accounting_complete,
                restored.accounting_partial,
            ),
            (
                evidence.evaluator_invoked,
                evidence.evaluator_completed,
                evidence.accounting_available,
                evidence.accounting_complete,
                evidence.accounting_partial,
            ),
        )

        with self.assertRaisesRegex(ValueError, "evaluator_invoked conflicts"):
            RollingEvidence(
                **common,
                evaluator_invoked=True,
                metrics={
                    "evaluation": {"evaluator_invoked": False},
                    "portfolio_accounting": accounting,
                },
            )
        with self.assertRaisesRegex(ValueError, "accounting_available conflicts"):
            RollingEvidence(
                **common,
                accounting_available=True,
                metrics={
                    "evaluation": {"evaluator_invoked": True},
                    "portfolio_accounting": {
                        **accounting,
                        "accounting_available": False,
                    },
                },
            )
        with self.assertRaisesRegex(ValueError, "evaluator_completed conflicts"):
            RollingEvidence(
                **common,
                evaluator_completed=True,
                metrics={
                    "evaluation": {"evaluator_completed": None},
                    "portfolio_accounting": accounting,
                },
            )
        with self.assertRaisesRegex(ValueError, "accounting_complete conflicts"):
            RollingEvidence(
                **common,
                accounting_complete=True,
                metrics={
                    "evaluation": {"evaluator_invoked": True},
                    "portfolio_accounting": {
                        **accounting,
                        "accounting_complete": None,
                    },
                },
            )

    def test_from_mapping_rejects_mixed_null_and_conflicting_evaluator_identity(self) -> None:
        for field_name, scalar_value, nested_value in (
            ("evaluation_run_id", "run-mapping-scalar", None),
            ("evaluation_version", "version-mapping-scalar", None),
            ("evaluation_run_id", "run-mapping-scalar", "run-mapping-nested"),
            ("evaluation_version", "version-mapping-scalar", "version-mapping-nested"),
        ):
            record = _v2_evidence(
                "sv-mapping-identity",
                f"window-mapping-{field_name}-{nested_value or 'null'}",
            )
            record[field_name] = scalar_value
            record["evaluation"] = {
                **record["evaluation"],
                field_name: nested_value,
            }
            with self.subTest(field=field_name, nested=nested_value):
                with self.assertRaisesRegex(ValueError, f"{field_name} conflicts"):
                    RollingEvidence.from_mapping(record)


    def test_link_only_v2_provenance_is_digest_bound(self) -> None:
        legacy = _evidence(
            "sv-link-only",
            "window-link-only",
            available_through=NOW.isoformat(),
        )
        legacy_evidence = RollingEvidence.from_mapping(legacy)
        linked = dict(legacy)
        linked["supersedes_evidence_id"] = "window-link-only-predecessor"
        linked_evidence = RollingEvidence.from_mapping(linked)
        self.assertEqual(linked_evidence.evaluation_kind, "CANONICAL_SIMULATION")
        self.assertNotEqual(linked_evidence.evidence_digest, legacy_evidence.evidence_digest)

        relinked = dict(linked)
        relinked["supersedes_evidence_id"] = "window-link-only-other-predecessor"
        self.assertNotEqual(
            RollingEvidence.from_mapping(relinked).evidence_digest,
            linked_evidence.evidence_digest,
        )

    def test_storage_rejects_conflicting_projections_and_numeric_aliases(self) -> None:
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-storage-projections"))

            evaluation_conflict = _v2_evidence(
                "sv-storage-projections",
                "window-evaluation-conflict",
            )
            evaluation_conflict["metrics"] = {
                "evaluation": {
                    **evaluation_conflict["evaluation"],
                    "evaluation_run_id": "run-foreign",
                }
            }
            evaluation_conflict["evidence_digest"] = ""
            with self.assertRaisesRegex(ValueError, "evaluation projections conflict"):
                store.save_strategy_evidence_window(evaluation_conflict)

            accounting_conflict = _v2_evidence(
                "sv-storage-projections",
                "window-accounting-conflict",
            )
            accounting_conflict["metrics"] = {
                "portfolio_accounting": {
                    **accounting_conflict["portfolio_accounting"],
                    "net_pnl": "999.00",
                }
            }
            accounting_conflict["evidence_digest"] = ""
            with self.assertRaisesRegex(ValueError, "portfolio_accounting projections conflict"):
                store.save_strategy_evidence_window(accounting_conflict)

            alias_conflict = _v2_evidence(
                "sv-storage-projections",
                "window-alias-conflict",
            )
            alias_conflict["portfolio_accounting"] = {
                **alias_conflict["portfolio_accounting"],
                "realized_pnl_usd": "999.00",
            }
            alias_conflict["evidence_digest"] = ""
            with self.assertRaisesRegex(ValueError, "realized_pnl conflicts"):
                store.save_strategy_evidence_window(alias_conflict)

    def test_storage_explicit_nested_null_does_not_resurrect_scalar_accounting(self) -> None:
        record = _v2_evidence(
            "sv-storage-null",
            "window-storage-null",
        )
        record["portfolio_accounting"] = {
            **record["portfolio_accounting"],
            "realized_pnl": None,
        }
        record["evidence_digest"] = ""
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-storage-null"))
            store.save_strategy_evidence_window(record)
            loaded = store.list_strategy_evidence_windows("sv-storage-null")[0]
            self.assertIsNone(loaded["realized_pnl"])
            self.assertIsNone(loaded["portfolio_accounting"]["realized_pnl"])

    def test_storage_rejects_malformed_v2_activity(self) -> None:
        malformed_values: tuple[tuple[str, object], ...] = (
            ("open_positions", "not-a-collection"),
            ("open_positions", list(range(33))),
            ("opening_fills", 1.5),
            ("closing_fills", -1),
            ("partial_closing_fills", True),
            ("completed_round_trips", "NaN"),
        )
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-storage-activity"))
            for index, (field_name, value) in enumerate(malformed_values):
                record = _v2_evidence(
                    "sv-storage-activity",
                    f"window-storage-activity-{index}",
                )
                record["portfolio_accounting"] = {
                    **record["portfolio_accounting"],
                    field_name: value,
                }
                record["evidence_digest"] = ""
                with self.subTest(field=field_name, value=value):
                    with self.assertRaises(ValueError):
                        store.save_strategy_evidence_window(record)

    def test_storage_validates_supersedes_evidence_lineage(self) -> None:
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-storage-lineage"))
            predecessor = _v2_evidence(
                "sv-storage-lineage",
                "window-storage-lineage-old",
            )
            predecessor["evidence_digest"] = ""
            store.save_strategy_evidence_window(predecessor)

            missing = _v2_evidence(
                "sv-storage-lineage",
                "window-storage-lineage-missing",
                supersedes_evidence_id="window-storage-lineage-missing-predecessor",
            )
            missing["evidence_digest"] = ""
            with self.assertRaisesRegex(ValueError, "predecessor does not exist"):
                store.save_strategy_evidence_window(missing)

            foreign = _v2_evidence(
                "sv-storage-lineage",
                "window-storage-lineage-foreign",
                candidate_id="candidate-foreign",
                supersedes_evidence_id="window-storage-lineage-old",
            )
            foreign["evidence_digest"] = ""
            with self.assertRaisesRegex(ValueError, "predecessor identity mismatch"):
                store.save_strategy_evidence_window(foreign)

            valid = _v2_evidence(
                "sv-storage-lineage",
                "window-storage-lineage-new",
                supersedes_evidence_id="window-storage-lineage-old",
            )
            valid["evidence_digest"] = ""
            store.save_strategy_evidence_window(valid)
            self.assertEqual(
                len(store.list_strategy_evidence_windows("sv-storage-lineage")),
                2,
            )


    def test_v2_completed_no_trade_explicit_zero_accounting_remains_measured(self) -> None:
        record = _v2_evidence("sv-v2-zero", "window-v2-zero", no_trade=True)
        restored = RollingEvidence.from_mapping(record)
        self.assertEqual(restored.realized_pnl, Decimal("0.00"))
        self.assertEqual(restored.unrealized_pnl, Decimal("0.00"))
        self.assertEqual(restored.fees, Decimal("0.00"))
        self.assertEqual(restored.costs, Decimal("0.00"))
        policy = _policy(
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        decision = evaluate_rolling_selection(policy, [record], None, NOW)
        member = _decision_members(decision)[0]
        self.assertEqual(_member_value(member, "status"), "ACTIVE")
        self.assertGreater(
            Decimal(str(_member_value(member, "allocation"))),
            Decimal("0"),
        )

    def test_direct_selection_rejects_malformed_v2_accounting(self) -> None:
        policy = _policy(
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        malformed_values: tuple[tuple[str, object], ...] = (
            ("net_pnl", "NaN"),
            ("cash", float("inf")),
            ("open_positions", "not-a-collection"),
            ("open_positions", list(range(33))),
            ("opening_fills", 1.5),
            ("closing_fills", -1),
            ("partial_closing_fills", True),
            ("completed_round_trips", "NaN"),
        )
        for index, (field_name, value) in enumerate(malformed_values):
            record = _v2_evidence(
                "sv-direct-v2-malformed",
                f"window-direct-v2-malformed-{index}",
            )
            record["portfolio_accounting"] = {
                **record["portfolio_accounting"],
                field_name: value,
            }
            with self.subTest(field=field_name, value=value):
                with self.assertRaises(ValueError):
                    evaluate_rolling_selection(policy, [record], None, NOW)

    def test_direct_selection_rejects_mixed_null_status_projections(self) -> None:
        policy = _policy(
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        record = _v2_evidence(
            "sv-direct-v2-status",
            "window-direct-v2-status",
        )
        record["evaluator_completed"] = None
        with self.assertRaisesRegex(ValueError, "evaluator_completed conflicts"):
            evaluate_rolling_selection(policy, [record], None, NOW)

    def test_direct_parsing_rejects_mixed_null_supersedes_projection(self) -> None:
        record = _v2_evidence(
            "sv-direct-v2-supersedes",
            "window-direct-v2-supersedes",
        )
        record["supersedes_evidence_id"] = "window-direct-v2-predecessor"
        record["evaluation"] = {
            **record["evaluation"],
            "supersedes_evidence_id": None,
        }
        with self.assertRaisesRegex(ValueError, "supersedes_evidence_id conflicts"):
            RollingEvidence.from_mapping(record)

    def test_direct_parsing_rejects_mixed_null_evaluation_kind_projection(self) -> None:
        record = _v2_evidence(
            "sv-direct-v2-evaluation-kind",
            "window-direct-v2-evaluation-kind",
        )
        record["evaluation_kind"] = "CANONICAL_SIMULATION"
        record["evaluation"] = {
            **record["evaluation"],
            "evaluation_kind": None,
        }
        with self.assertRaisesRegex(ValueError, "evaluation_kind conflicts"):
            RollingEvidence.from_mapping(record)


    def test_actual_ledger_requires_explicit_false_evaluator_status(self) -> None:
        record = _v2_evidence(
            "sv-direct-v2-ledger",
            "window-direct-v2-ledger",
        )
        record.update(
            {
                "evaluation_kind": "ACTUAL_LEDGER",
                "evaluator_invoked": False,
                "evaluator_completed": False,
                "evaluation": {
                    **record["evaluation"],
                    "evaluator_invoked": False,
                    "evaluator_completed": False,
                },
            }
        )
        record = _canonicalize_evidence(record)
        restored = RollingEvidence.from_mapping(record)
        self.assertEqual(restored.evaluation_kind, "ACTUAL_LEDGER")
        self.assertFalse(restored.evaluator_invoked)
        self.assertFalse(restored.evaluator_completed)
        policy = _policy(
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        decision = evaluate_rolling_selection(policy, [record], None, NOW)
        self.assertEqual(_member_value(_decision_members(decision)[0], "status"), "ACTIVE")

    def test_set_like_activity_hashes_deterministically(self) -> None:
        list_record = _v2_evidence(
            "sv-direct-v2-set",
            "window-direct-v2-set",
        )
        list_record["portfolio_accounting"] = {
            **list_record["portfolio_accounting"],
            "open_positions": ["market-a", "market-b"],
        }
        list_record = _canonicalize_evidence(list_record)
        set_record = {
            **list_record,
            "portfolio_accounting": {
                **list_record["portfolio_accounting"],
                "open_positions": {"market-b", "market-a"},
            },
        }
        frozenset_record = {
            **list_record,
            "portfolio_accounting": {
                **list_record["portfolio_accounting"],
                "open_positions": frozenset({"market-a", "market-b"}),
            },
        }
        self.assertEqual(
            RollingEvidence.from_mapping(set_record).evidence_digest,
            RollingEvidence.from_mapping(frozenset_record).evidence_digest,
        )


    def test_accounting_quality_flags_block_selection_even_when_numeric_thresholds_pass(self) -> None:
        policy = _policy(
            requested_window_days=(7,),
            min_actual_coverage_seconds=0,
            min_completed_outcomes=0,
            min_reliability="0",
            experimental_allocation_enabled=True,
            global_budget="100",
        )
        for field, value in (
            ("accounting_available", False),
            ("accounting_complete", False),
            ("accounting_partial", True),
        ):
            with self.subTest(field=field):
                decision = evaluate_rolling_selection(
                    policy,
                    [
                        _evidence(
                            f"sv-{field}",
                            f"window-{field}",
                            source_digest="sha256:source",
                            accounting_digest="sha256:accounting",
                            **{field: value},
                        )
                    ],
                    None,
                    NOW,
                )
                member = _decision_members(decision)[0]
                self.assertNotEqual(_member_value(member, "status"), "ACTIVE")
                self.assertEqual(
                    Decimal(str(_member_value(member, "allocation"))),
                    Decimal("0"),
                )
                expected_reason = {
                    "accounting_available": "accounting_unavailable",
                    "accounting_complete": "accounting_incomplete",
                    "accounting_partial": "accounting_partial",
                }[field]
                self.assertIn(expected_reason, _member_value(member, "reason"))

        initial = evaluate_rolling_selection(
            policy,
            [_evidence("sv-incumbent", "window-incumbent")],
            None,
            NOW,
        )
        reviewed = evaluate_rolling_selection(
            policy,
            [
                _evidence(
                    "sv-incumbent",
                    "window-incumbent-partial",
                    source_digest="sha256:source",
                    accounting_digest="sha256:accounting",
                    accounting_partial=True,
                )
            ],
            initial,
            NOW + timedelta(hours=1),
        )
        incumbent = _decision_members(reviewed)[0]
        self.assertNotEqual(_member_value(incumbent, "status"), "ACTIVE")
        self.assertEqual(
            Decimal(str(_member_value(incumbent, "allocation"))),
            Decimal("0"),
        )


    def test_paper_loader_marks_exact_unmaterialized_intent_pending_then_matches_materialized_spec(self) -> None:
        strategy_hash = "sha256:strategy-alpha"
        record = {
            "strategy_hash": strategy_hash,
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        intent_config = {
            "observation_intent": True,
            "market_authority_required": False,
            "candidate_id": "candidate-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "source_strategy_hash": strategy_hash,
            "rolling_strategy_hash": strategy_hash,
        }
        pending = {
            "experiment_id": "observation-intent-alpha",
            "strategy_hash": strategy_hash,
            "model_hash": "sha256:model",
            "config": intent_config,
            "allowed_markets": [],
            "risk_limits": {},
            "quality": "PAPER_FORWARD",
        }
        materialized = {
            **pending,
            "experiment_id": "forward-alpha",
            "config": {
                **intent_config,
                "market_authority_required": True,
            },
            "allowed_markets": ["m1"],
        }

        class PaperStore:
            def __init__(self) -> None:
                self.specs = [pending]
                self.ledger_calls: list[str] = []

            def load_forward_tests(self, *, limit: int = 1000):
                return list(self.specs)

            def list_paper_bet_ledger(self, experiment_id: str, *, limit: int = 1000):
                self.ledger_calls.append(experiment_id)
                return []

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        store = PaperStore()
        processor.store = store
        with self.assertRaisesRegex(ValueError, "PAPER_MARKET_AUTHORITY_PENDING"):
            processor._rolling_source_rows(record, "PAPER", NOW)
        self.assertEqual(store.ledger_calls, [])

        store.specs = [materialized]
        self.assertEqual(processor._rolling_source_rows(record, "PAPER", NOW), [])
        self.assertEqual(store.ledger_calls, ["forward-alpha"])

    def test_paper_loader_rejects_foreign_identity_in_nested_accounting_ledger(self) -> None:
        strategy_hash = "sha256:strategy-alpha"
        record = {
            "strategy_hash": strategy_hash,
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        class PaperStore:
            def load_forward_tests(self, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": "forward-alpha",
                        "strategy_hash": strategy_hash,
                        "config": {
                            "candidate_id": "candidate-alpha",
                            "research_trial_id": "research-trial-alpha",
                            "strategy_version_id": "strategy-version-alpha",
                        },
                    }
                ]

            def list_paper_bet_ledger(self, experiment_id: str, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": experiment_id,
                        "strategy_id": strategy_hash,
                        "market_id": "m1",
                        "accounting": {
                            "allocated_capital": "10",
                            "net_return": "1",
                            "realized_pnl": "1",
                            "unrealized_pnl": "0",
                            "fees": "0",
                            "costs": "0",
                            "drawdown": "0",
                            "completed_outcomes": 1,
                            "reliability": "1",
                            "ledger": {
                                "resolved_bet": {
                                    "strategy_id": "sha256:foreign",
                                }
                            },
                        },
                        "timestamp": NOW.isoformat(),
                        "available_from": (NOW - timedelta(days=7)).isoformat(),
                        "available_through": NOW.isoformat(),
                    }
                ]

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = PaperStore()
        rows = processor._rolling_source_rows(record, "PAPER", NOW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["_rolling_accounting_rejection"],
            "PAPER_STRATEGY_BINDING_CONFLICT",
        )

    def test_source_loader_binding_injection_and_conflict_rejection(self) -> None:
        expected = {
            "dataset_id": "dataset-alpha",
            "dataset_version": "v1",
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        accounting = {
            "allocated_capital": "10",
            "net_return": "1",
            "realized_pnl": "1",
            "unrealized_pnl": "0",
            "fees": "0",
            "costs": "0",
            "drawdown": "0",
            "completed_outcomes": 1,
            "reliability": "1",
        }
        row = {
            "source_type": "HISTORICAL",
            "timestamp": NOW.isoformat(),
            "available_from": (NOW - timedelta(days=7)).isoformat(),
            "available_through": NOW.isoformat(),
            "accounting": accounting,
        }
        injected = _rolling_inject_source_binding(row, expected)
        self.assertEqual(injected["candidate_id"], expected["candidate_id"])
        self.assertNotIn("_rolling_accounting_rejection", injected)
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        projected = processor._rolling_accounting_projection(injected, expected)
        self.assertIsNotNone(projected)

        conflict = _rolling_inject_source_binding(
            {**row, "candidate_id": "candidate-foreign"},
            expected,
        )
        self.assertEqual(conflict["_rolling_accounting_rejection"], "SOURCE_BINDING_CONFLICT")

    def test_historical_loader_does_not_fabricate_strategy_lineage(self) -> None:
        record = {
            "dataset_id": "dataset-alpha",
            "dataset_version": "v1",
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        source_row = {
            "source_type": "HISTORICAL",
            "timestamp": NOW.isoformat(),
            "available_from": (NOW - timedelta(days=7)).isoformat(),
            "available_through": NOW.isoformat(),
            "accounting": {
                "allocated_capital": "10",
                "net_return": "1",
                "realized_pnl": "1",
                "unrealized_pnl": "0",
                "fees": "0",
                "costs": "0",
                "drawdown": "0",
                "completed_outcomes": 1,
                "reliability": "1",
            },
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = object()
        processor._load_rolling_historical_dataset = lambda _dataset, _version: [source_row]
        rows = processor._rolling_source_rows(record, "HISTORICAL", NOW)
        self.assertEqual(len(rows), 1)
        self.assertNotIn("candidate_id", rows[0])
        self.assertNotIn("research_trial_id", rows[0])
        self.assertFalse(rows[0]["_rolling_lineage_proven"])
        self.assertNotIn("_rolling_accounting", rows[0])
    def test_historical_preflight_checks_length_before_sqlite_json(self) -> None:
        with AxiomStore(":memory:") as store:
            store.connection.execute(
                "INSERT INTO datasets(dataset_id,version,payload_json,metadata_json,quality,created_at) "
                "VALUES (?,?,?,?,?,?)",
                ("oversized", "v1", "[]", "{}", "HIGH", NOW.isoformat()),
            )
            store.connection.commit()
            json_calls: list[object] = []

            def forbidden_json(value: object) -> int:
                json_calls.append(value)
                raise AssertionError("JSON1 must not inspect an oversized payload")

            store.connection.create_function("json_valid", 1, forbidden_json)
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            with patch(
                "axiom.autonomous._MAX_ROLLING_DATASET_PAYLOAD_BYTES",
                1,
            ), self.assertRaisesRegex(
                ValueError, "HISTORICAL_DATASET_PAYLOAD_TOO_LARGE"
            ):
                processor._load_rolling_historical_dataset("oversized", "v1")
            self.assertEqual(json_calls, [])
    def test_replay_ranks_catalogs_before_loading_only_winner_and_accepts_62mb_payload(self) -> None:
        dataset_id = "Polymarket-recorded-book-replay"
        stamp = NOW - timedelta(days=31)

        def encoded(value: object) -> str:
            return json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                default=lambda item: item.isoformat()
                if isinstance(item, datetime)
                else str(item),
            )

        with AxiomStore(":memory:") as store:
            def publish(market_id: str, filler_size: int) -> str:
                snapshot_id = f"snapshot-{market_id}"
                publisher_payload = {
                    "settlement": "CLOSED",
                    "source_type": "FORWARD_COLLECTED",
                }
                store.save_polymarket_snapshot(
                    snapshot_id,
                    market_id,
                    stamp,
                    stamp,
                    publisher_payload,
                    source_type="FORWARD_COLLECTED",
                )
                source_record_hash = _rolling_hash(
                    {
                        "snapshot_id": snapshot_id,
                        "market_id": market_id,
                        "source_timestamp": stamp,
                        "observed_at": stamp,
                        "payload": publisher_payload,
                    }
                )
                row = {
                    "market_id": market_id,
                    "timestamp": stamp,
                    "source_timestamp": stamp,
                    "observed_at": stamp,
                    "source_type": "FORWARD_COLLECTED",
                    "source_snapshot_id": snapshot_id,
                    "source_record_hash": source_record_hash,
                    "research_mode": "RECORDED_BOOK_REPLAY",
                    "settlement": "CLOSED",
                    "publisher_padding": "x" * filler_size,
                }
                manifest = [
                    {
                        "snapshot_id": snapshot_id,
                        "source_record_hash": source_record_hash,
                        "market_id": market_id,
                        "source_timestamp": stamp,
                    }
                ]
                version = _rolling_hash(
                    {
                        "dataset_id": dataset_id,
                        "research_mode": "RECORDED_BOOK_REPLAY",
                        "cutoff": NOW,
                        "manifest": manifest,
                        "rows": [row],
                    }
                )
                store.connection.execute(
                    "INSERT INTO datasets(dataset_id,version,payload_json,metadata_json,quality,created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        dataset_id,
                        version,
                        encoded([row]),
                        "{}",
                        "ORDER_BOOK_SIMULATED",
                        NOW.isoformat(),
                    ),
                )
                store.connection.execute(
                    "INSERT INTO dataset_catalog("
                    "dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
                    "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
                    "quality,source_type,snapshot_id,created_at,updated_at,metadata_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        dataset_id,
                        version,
                        "publisher-fixture",
                        "POLYMARKET",
                        "prediction",
                        "1d",
                        stamp.isoformat(),
                        stamp.isoformat(),
                        1,
                        1.0,
                        "[]",
                        "ORDER_BOOK_SIMULATED",
                        "FORWARD_COLLECTED",
                        f"manifest:{version}",
                        NOW.isoformat(),
                        NOW.isoformat(),
                        encoded(
                            {
                                "research_mode": "RECORDED_BOOK_REPLAY",
                                "exact_cutoff": NOW,
                                "snapshot_manifest": manifest,
                            }
                        ),
                    ),
                )
                store.connection.commit()
                return version

            winner = publish("winner-market", 61_000_000)
            loser = publish("loser-market", 32)
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            calls: list[str] = []
            load_projection = store.load_dataset_payload_projection

            def counted(
                dataset: str,
                version: str,
                *,
                max_payload_bytes: int,
            ) -> object:
                calls.append(version)
                return load_projection(
                    dataset,
                    version,
                    max_payload_bytes=max_payload_bytes,
                )

            with patch.object(store, "load_dataset_payload_projection", counted):
                rows, catalog = processor._rolling_replay_catalog_rows(
                    {"market_scope": {"market_ids": ["winner-market"]}},
                    NOW,
                )
            self.assertEqual(calls, [winner])
            self.assertEqual(catalog["dataset_version"], winner)
            self.assertEqual(len(rows), 1)
            self.assertEqual(len(rows[0]["publisher_padding"]), 61_000_000)
            self.assertNotEqual(winner, loser)

    def test_replay_payload_over_cap_is_rejected_before_json_decode(self) -> None:
        def encoded(value: object) -> str:
            return json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                default=lambda item: item.isoformat()
                if isinstance(item, datetime)
                else str(item),
            )

        dataset_id = "Polymarket-recorded-book-replay"
        version = "sha256:" + ("0" * 64)
        metadata = {
            "research_mode": "RECORDED_BOOK_REPLAY",
            "exact_cutoff": NOW,
            "snapshot_manifest": [
                {
                    "snapshot_id": "never-loaded",
                    "source_record_hash": "sha256:" + ("1" * 64),
                    "market_id": "market-over-cap",
                    "source_timestamp": NOW - timedelta(days=31),
                }
            ],
        }
        with AxiomStore(":memory:") as store:
            store.connection.execute(
                "INSERT INTO datasets(dataset_id,version,payload_json,metadata_json,quality,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    dataset_id,
                    version,
                    encoded_payload := json.dumps(
                        "x" * (_MAX_ROLLING_REPLAY_PAYLOAD_BYTES + 1)
                    ),
                    "{}",
                    "ORDER_BOOK_SIMULATED",
                    NOW.isoformat(),
                ),
            )
            store.connection.execute(
                "INSERT INTO dataset_catalog("
                "dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
                "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
                "quality,source_type,snapshot_id,created_at,updated_at,metadata_json"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    dataset_id,
                    version,
                    "publisher-fixture",
                    "POLYMARKET",
                    "prediction",
                    "1d",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    1,
                    1.0,
                    "[]",
                    "ORDER_BOOK_SIMULATED",
                    "FORWARD_COLLECTED",
                    f"manifest:{version}",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    encoded(metadata),
                ),
            )
            store.connection.commit()
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            with patch("axiom.storage._load", side_effect=AssertionError("decoded")) as load:
                with self.assertRaisesRegex(
                    ValueError, "REPLAY_DATASET_UNAVAILABLE"
                ):
                    processor._rolling_replay_catalog_rows({}, NOW)
            load.assert_not_called()
            self.assertGreater(len(encoded_payload.encode("utf-8")), _MAX_ROLLING_REPLAY_PAYLOAD_BYTES)
    def test_replay_scope_without_overlap_is_ineligible_before_payload_load(self) -> None:
        dataset_id = "Polymarket-recorded-book-replay"
        version = "sha256:" + ("2" * 64)
        metadata = json.dumps(
            {
                "research_mode": "RECORDED_BOOK_REPLAY",
                "exact_cutoff": NOW,
                "snapshot_manifest": [
                    {
                        "snapshot_id": "foreign-snapshot",
                        "source_record_hash": "sha256:" + ("3" * 64),
                        "market_id": "foreign-market",
                        "source_timestamp": NOW - timedelta(days=31),
                    }
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: item.isoformat()
            if isinstance(item, datetime)
            else str(item),
        )
        with AxiomStore(":memory:") as store:
            store.connection.execute(
                "INSERT INTO datasets(dataset_id,version,payload_json,metadata_json,quality,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    dataset_id,
                    version,
                    "[]",
                    "{}",
                    "ORDER_BOOK_SIMULATED",
                    NOW.isoformat(),
                ),
            )
            store.connection.execute(
                "INSERT INTO dataset_catalog("
                "dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
                "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
                "quality,source_type,snapshot_id,created_at,updated_at,metadata_json"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    dataset_id,
                    version,
                    "publisher-fixture",
                    "POLYMARKET",
                    "prediction",
                    "1d",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    1,
                    1.0,
                    "[]",
                    "ORDER_BOOK_SIMULATED",
                    "FORWARD_COLLECTED",
                    f"manifest:{version}",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    metadata,
                ),
            )
            store.connection.commit()
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            calls: list[str] = []

            def forbidden_payload_load(*args: object, **kwargs: object) -> object:
                calls.append(str(args[1] if len(args) > 1 else kwargs.get("version")))
                return []

            with patch.object(
                store,
                "load_dataset_payload_projection",
                forbidden_payload_load,
            ):
                with self.assertRaisesRegex(
                    ValueError, "REPLAY_DATASET_UNAVAILABLE"
                ):
                    processor._rolling_replay_catalog_rows(
                        {"market_scope": {"market_ids": ["requested-market"]}},
                        NOW,
                    )
            self.assertEqual(calls, [])
    def test_rule_scope_partial_persisted_resolution_blocks_replay(self) -> None:
        record = {
            "candidate_id": "rule-candidate",
            "market_scope_hash": "scope-hash",
            "market_scope_version": "scope-v1",
            "market_scope": {
                "mode": "RULE_BASED_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "filters": {},
                "regime_restrictions": {},
            },
        }

        class PartialResolutionStore:
            def load_market_scope_resolution(self, *_args: object, **_kwargs: object) -> object:
                return {
                    "candidate_id": "rule-candidate",
                    "scope_hash": "scope-hash",
                    "scope_version": "scope-v1",
                    "status": "PARTIAL",
                    "matched_markets": [{"market_id": "scope-present"}],
                }

        with self.assertRaisesRegex(
            ValueError, "RULE_BASED_MARKET_SCOPE_RESOLUTION_INCOMPLETE"
        ):
            _rolling_rule_scope_market_ids(
                PartialResolutionStore(),
                record,
                record,
                (),
                NOW,
            )

    def test_rule_scope_missing_resolution_blocks_replay(self) -> None:
        record = {
            "candidate_id": "rule-candidate-missing",
            "market_scope_hash": "scope-hash-missing",
            "market_scope_version": "scope-v1",
            "market_scope": {
                "mode": "RULE_BASED_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "filters": {},
                "regime_restrictions": {},
            },
        }

        class MissingResolutionStore:
            def load_market_scope_resolution(self, *_args: object, **_kwargs: object) -> object:
                return None

        with self.assertRaisesRegex(
            ValueError, "RULE_BASED_MARKET_SCOPE_RESOLUTION_INCOMPLETE"
        ):
            _rolling_rule_scope_market_ids(
                MissingResolutionStore(),
                record,
                record,
                (),
                NOW,
            )

    def test_replay_partial_scope_is_ineligible_before_payload_load(self) -> None:
        dataset_id = "Polymarket-recorded-book-replay"
        version = "sha256:" + ("4" * 64)
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                dataset_id,
                version,
                provider="publisher-fixture",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="1d",
                start_timestamp=NOW - timedelta(days=31),
                end_timestamp=NOW - timedelta(days=31),
                row_count=1,
                completeness=1.0,
                missing_ranges=(),
                quality="ORDER_BOOK_SIMULATED",
                source_type="FORWARD_COLLECTED",
                snapshot_id=f"manifest:{version}",
                metadata={
                    "research_mode": "RECORDED_BOOK_REPLAY",
                    "exact_cutoff": NOW,
                    "snapshot_manifest": [
                        {
                            "snapshot_id": "partial-snapshot",
                            "source_record_hash": "sha256:" + ("5" * 64),
                            "market_id": "scope-present",
                            "source_timestamp": NOW - timedelta(days=31),
                        }
                    ],
                },
                created_at=NOW,
                updated_at=NOW,
            )
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            calls: list[str] = []

            def forbidden_payload_load(*args: object, **kwargs: object) -> object:
                calls.append(str(args[1] if len(args) > 1 else kwargs.get("version")))
                return []

            with patch.object(
                store,
                "load_dataset_payload_projection",
                forbidden_payload_load,
            ):
                with self.assertRaisesRegex(
                    ValueError, "REPLAY_DATASET_UNAVAILABLE"
                ):
                    processor._rolling_replay_catalog_rows(
                        {
                            "market_scope": {
                                "market_ids": ["scope-present", "scope-missing"]
                            }
                        },
                        NOW,
                    )
            self.assertEqual(calls, [])




    def test_paper_loader_requires_exact_registry_lineage_for_resolved_bet(self) -> None:
        strategy_hash = "sha256:strategy-alpha"
        record = {
            "strategy_hash": strategy_hash,
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        resolved = {
            "experiment_id": "forward-alpha",
            "resolution": "resolved_yes",
            "closed": True,
            "capital_at_risk": "10",
            "net_pnl": "1",
            "realized_pnl": "1",
            "unrealized_pnl": "0",
            "fees": "0",
            "slippage": "0",
            "drawdown": "0",
            "completed_outcomes": 1,
            "reliability": "1",
        }

        class PaperStore:
            def load_forward_tests(self, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": "forward-alpha",
                        "strategy_hash": strategy_hash,
                        "model_hash": "sha256:model",
                        "config": {
                            "candidate_id": "candidate-alpha",
                            "research_trial_id": "research-trial-alpha",
                            "strategy_version_id": "strategy-version-alpha",
                        },
                        "start_timestamp": NOW.isoformat(),
                        "bankroll": 10_000,
                        "allowed_markets": ["m1"],
                        "risk_limits": {},
                        "quality": "PAPER_FORWARD",
                    }
                ]

            def list_paper_bet_ledger(self, experiment_id: str, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": experiment_id,
                        "strategy_id": strategy_hash,
                        "market_id": "m1",
                        "payload": resolved,
                        "timestamp": NOW.isoformat(),
                        "available_from": (NOW - timedelta(days=7)).isoformat(),
                        "available_through": NOW.isoformat(),
                    }
                ]

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = PaperStore()
        rows = processor._rolling_source_rows(record, "PAPER", NOW)
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0]["_rolling_accounting"], dict)

    def test_paper_loader_rejects_nested_foreign_strategy_identity(self) -> None:
        strategy_hash = "sha256:strategy-alpha"
        record = {
            "strategy_hash": strategy_hash,
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        class PaperStore:
            def load_forward_tests(self, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": "forward-alpha",
                        "strategy_hash": strategy_hash,
                        "model_hash": "sha256:model",
                        "config": {
                            "candidate_id": "candidate-alpha",
                            "research_trial_id": "research-trial-alpha",
                            "strategy_version_id": "strategy-version-alpha",
                        },
                        "start_timestamp": NOW.isoformat(),
                        "bankroll": 10_000,
                        "allowed_markets": ["m1"],
                        "risk_limits": {},
                        "quality": "PAPER_FORWARD",
                    }
                ]

            def list_paper_bet_ledger(self, experiment_id: str, *, limit: int = 1000):
                return [
                    {
                        "experiment_id": experiment_id,
                        "strategy_id": strategy_hash,
                        "market_id": "m1",
                        "payload": {
                            "resolved_bet": {
                                "experiment_id": experiment_id,
                                "strategy_id": "sha256:foreign",
                                "resolution": "resolved_yes",
                                "closed": True,
                                "capital_at_risk": "10",
                                "net_pnl": "1",
                                "realized_pnl": "1",
                                "unrealized_pnl": "0",
                                "fees": "0",
                                "slippage": "0",
                                "drawdown": "0",
                                "completed_outcomes": 1,
                                "reliability": "1",
                            }
                        },
                        "timestamp": NOW.isoformat(),
                        "available_from": (NOW - timedelta(days=7)).isoformat(),
                        "available_through": NOW.isoformat(),
                    }
                ]

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = PaperStore()
        rows = processor._rolling_source_rows(record, "PAPER", NOW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["_rolling_accounting_rejection"],
            "PAPER_STRATEGY_BINDING_CONFLICT",
        )

    def test_accounting_rejects_nonfinite_and_price_proxy_rows(self) -> None:
        expected = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        base = {
            "timestamp": NOW.isoformat(),
            "available_from": (NOW - timedelta(days=7)).isoformat(),
            "available_through": NOW.isoformat(),
            "accounting": {
                "allocated_capital": "10",
                "net_return": "NaN",
                "realized_pnl": "1",
                "unrealized_pnl": "0",
                "fees": "0",
                "costs": "0",
                "drawdown": "0",
                "completed_outcomes": 1,
                "reliability": "1",
            },
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        malformed = _rolling_inject_source_binding(base, expected)
        self.assertIsNone(processor._rolling_accounting_projection(malformed, expected))
        self.assertEqual(malformed["_rolling_accounting_rejection"], "ACCOUNTING_METRIC_NONFINITE")
        proxy = _rolling_inject_source_binding(
            {"source_type": "PRICE_PROXY", "timestamp": NOW.isoformat()},
            expected,
        )
        self.assertIsNone(processor._rolling_accounting_projection(proxy, expected))
        self.assertEqual(proxy["_rolling_accounting_rejection"], "PRICE_PROXY_ACCOUNTING_UNAVAILABLE")
    def test_accounting_rejects_negative_nonnegative_metrics_as_row_blockers(self) -> None:
        expected = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
        }
        accounting = {
            "allocated_capital": "10",
            "net_return": "-1",
            "realized_pnl": "-1",
            "unrealized_pnl": "0",
            "fees": "0",
            "costs": "0",
            "drawdown": "0",
            "completed_outcomes": 1,
            "reliability": "1",
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        for metric in ("allocated_capital", "fees", "costs"):
            with self.subTest(metric=metric):
                row = _rolling_inject_source_binding(
                    {
                        "timestamp": NOW.isoformat(),
                        "available_from": (NOW - timedelta(days=7)).isoformat(),
                        "available_through": NOW.isoformat(),
                        "accounting": {**accounting, metric: "-1"},
                    },
                    expected,
                )
                self.assertIsNone(
                    processor._rolling_accounting_projection(row, expected)
                )
                self.assertEqual(
                    row["_rolling_accounting_rejection"],
                    "ACCOUNTING_METRIC_NEGATIVE_IMPOSSIBLE",
                )
                self.assertEqual(row["_rolling_accounting_metric"], metric)

    def test_paper_resolved_bet_projection_requires_exact_experiment_binding(self) -> None:
        expected = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
            "experiment_id": "forward-alpha",
        }
        row = {
            **expected,
            "strategy_id": expected["strategy_hash"],
            "_paper_experiment_id": expected["experiment_id"],
            "timestamp": NOW.isoformat(),
            "available_from": (NOW - timedelta(days=7)).isoformat(),
            "available_through": NOW.isoformat(),
            "resolved_bet": {
                "experiment_id": expected["experiment_id"],
                "resolution": "resolved_yes",
                "closed": True,
                "capital_at_risk": "10",
                "net_pnl": "1",
                "realized_pnl": "1",
                "unrealized_pnl": "0",
                "fees": "0",
                "slippage": "0",
                "drawdown": "0",
                "completed_outcomes": 1,
                "reliability": "1",
            },
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        self.assertIsNotNone(processor._rolling_accounting_projection(row, expected))
        row["resolved_bet"] = {**row["resolved_bet"], "experiment_id": "forward-foreign"}
        self.assertIsNone(processor._rolling_accounting_projection(row, expected))
        self.assertEqual(row["_rolling_accounting_rejection"], "PAPER_RESOLVED_BET_BINDING_INVALID")

    def test_paper_coverage_uses_source_open_timestamp_not_delayed_ledger_created_at(self) -> None:
        expected = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
            "experiment_id": "forward-alpha",
        }
        opened = NOW - timedelta(days=7)
        resolved = {
            "experiment_id": expected["experiment_id"],
            "resolution": "resolved_yes",
            "closed": True,
            "capital_at_risk": "10",
            "net_pnl": "1",
            "fees": "0",
            "slippage": "0",
            "observation_open_timestamp": opened.isoformat(),
            "resolved_at": NOW.isoformat(),
        }
        row = {
            **expected,
            "_paper_experiment_id": expected["experiment_id"],
            "created_at": (NOW + timedelta(days=3)).isoformat(),
            "resolved_bet": resolved,
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        projected = processor._rolling_accounting_projection(row, expected)
        self.assertIsNotNone(projected)
        assert projected is not None
        self.assertEqual(projected["_available_from"], opened)
        self.assertEqual(projected["_available_through"], NOW)

    def test_paper_coverage_rejects_missing_or_inverted_source_open_timestamp(self) -> None:
        expected = {
            "strategy_hash": "sha256:strategy-alpha",
            "strategy_version_id": "strategy-version-alpha",
            "research_trial_id": "research-trial-alpha",
            "candidate_id": "candidate-alpha",
            "experiment_id": "forward-alpha",
        }
        resolved = {
            "experiment_id": expected["experiment_id"],
            "resolution": "resolved_yes",
            "closed": True,
            "capital_at_risk": "10",
            "net_pnl": "1",
            "fees": "0",
            "slippage": "0",
            "resolved_at": NOW.isoformat(),
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        missing = {
            **expected,
            "_paper_experiment_id": expected["experiment_id"],
            "created_at": (NOW + timedelta(days=3)).isoformat(),
            "resolved_bet": resolved,
        }
        self.assertIsNone(processor._rolling_accounting_projection(missing, expected))
        self.assertEqual(missing["_rolling_accounting_rejection"], "ACCOUNTING_COVERAGE_MISSING")
        inverted = {
            **missing,
            "resolved_bet": {
                **resolved,
                "observation_open_timestamp": (NOW + timedelta(minutes=1)).isoformat(),
            },
        }
        self.assertIsNone(processor._rolling_accounting_projection(inverted, expected))
        self.assertEqual(inverted["_rolling_accounting_rejection"], "ACCOUNTING_COVERAGE_INVALID")

    def test_sql_unavailable_legacy_discovery_calls_selector_adapters_without_scan_as_id(self) -> None:
        candidate_id = "candidate-alpha"
        strategy = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
            "strategy_id": candidate_id,
        }
        scope = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": ["market-alpha"],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        ).as_dict()

        class SqlUnavailable:
            def __init__(self) -> None:
                self.calls: list[tuple[str, object]] = []
                self.connection = self

            def execute(self, *_args: object, **_kwargs: object) -> object:
                raise sqlite3.OperationalError("SQL unavailable")

            def list_strategies(self, *, limit: int) -> list[dict[str, object]]:
                self.calls.append(("strategy", limit))
                return [
                    {
                        "strategy_id": candidate_id,
                        "version": "1",
                        "strategy": strategy,
                    }
                ][:limit]

            def load_candidate_lifecycle(self, *, limit: int) -> list[dict[str, object]]:
                self.calls.append(("candidate", limit))
                return [
                    {
                        "candidate_id": candidate_id,
                        "stage": "FROZEN",
                        "payload": {
                            "candidate_id": candidate_id,
                            "strategy_document": strategy,
                            "market_scope": scope,
                            "research_trial_id": "trial-alpha",
                        },
                    }
                ][:limit]

            def save_rolling_enrollment(self, _record: object) -> None:
                return None

        adapter = SqlUnavailable()
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = adapter
        documents = processor._rolling_strategy_documents()
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["candidate_id"], candidate_id)
        self.assertEqual(adapter.calls, [("strategy", 2048), ("candidate", 2048)])
    def test_production_shaped_enrollment_is_bounded_versioned_and_idempotent(self) -> None:
        strategy_template = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
        }
        scope = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": ["market-production"],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        ).as_dict()
        bulk = [
            {"candidate_id": f"unrelated-{index}", "snapshot_id": "x" * 128}
            for index in range(1000)
        ]

        def seed(
            store: AxiomStore,
            candidate_id: str,
            *,
            stage: str = "FROZEN",
            payload_extra: dict[str, object] | None = None,
        ) -> None:
            strategy = {**strategy_template, "strategy_id": candidate_id}
            store.save_strategy(candidate_id, strategy, version="1")
            payload: dict[str, object] = {
                "candidate_id": candidate_id,
                "strategy_document": strategy,
                "market_scope": scope,
                "dataset_attestation": {
                    "dataset_id": "production-shaped",
                    "dataset_version": "v1",
                    "constituent_bindings": bulk,
                },
            }
            payload.update(payload_extra or {})
            store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=NOW)
            store.save_candidate_lifecycle(
                candidate_id,
                stage,
                payload,
                from_stage="IDEA",
                timestamp=NOW,
            )

        with _store(self.tmp_path) as store:
            seed(store, "candidate-valid")
            seed(
                store,
                "candidate-conflict",
                payload_extra={"identity": {"candidate_id": "candidate-foreign"}},
            )
            seed(
                store,
                "candidate-ambiguous",
                payload_extra={"identity": {"candidate_id": ["candidate-ambiguous"]}},
            )
            seed(
                store,
                "candidate-campaign",
                payload_extra={
                    "experiment_plan": {
                        "campaign_id": "campaign-production",
                        "campaign_trial_id": "trial-production",
                        "campaign_configuration_id": "config-production",
                        "campaign_protocol": {
                            "schema_version": "campaign-v1",
                            "protocol_hash": "hash-production",
                        },
                    },
                    "economic_outcome": {"net_return": "0.12", "completed": 3},
                    "rejection_reason": "legacy campaign rejection",
                    "provenance": {
                        "source": "legacy-campaign",
                        "attestation_id": "attestation-production",
                    },
                }
            )
            seed(
                store,
                "candidate-paper-missing-scope",
                stage="PAPER_FORWARD",
                payload_extra={"market_scope": None},
            )
            store.save_rolling_enrollment(
                {
                    "enrollment_id": "legacy-excluded-candidate-valid",
                    "candidate_id": "candidate-valid",
                    "status": "EXCLUDED",
                    "reason": "CANDIDATE_IDENTITY_MISMATCH",
                    "validation_version": "rolling-enrollment-v1",
                    "provenance": {
                        "candidate_id": "candidate-valid",
                        "legacy": True,
                    },
                }
            )
            processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
            processor.store = store
            documents = processor._rolling_strategy_documents()
            self.assertEqual(
                {item["candidate_id"] for item in documents},
                {"candidate-valid"},
            )
            self.assertEqual(
                {
                    (item["candidate_id"], item["reason"])
                    for item in store.list_rolling_enrollments()
                    if item["validation_version"] == "rolling-enrollment-v2"
                },
                {
                    ("candidate-ambiguous", "CANDIDATE_IDENTITY_MISMATCH"),
                    ("candidate-campaign", "CAMPAIGN_BOUND"),
                    ("candidate-conflict", "CANDIDATE_IDENTITY_MISMATCH"),
                    ("candidate-paper-missing-scope", "SCOPE_INVALID"),
                },
            )
            missing_scope = next(
                item
                for item in store.list_rolling_enrollments()
                if item["candidate_id"] == "candidate-paper-missing-scope"
            )
            self.assertEqual(
                missing_scope["provenance"]["next_work"],
                "PERSIST_EXPLICIT_MARKET_SCOPE_AND_SCOPE_IDENTITY",
            )
            campaign = next(
                item
                for item in store.list_rolling_enrollments()
                if item["candidate_id"] == "candidate-campaign"
            )
            campaign_provenance = campaign["provenance"]
            self.assertEqual(campaign_provenance["campaign_id"], "campaign-production")
            self.assertEqual(campaign_provenance["campaign_trial_id"], "trial-production")
            self.assertEqual(
                campaign_provenance["campaign_configuration_id"],
                "config-production",
            )
            self.assertEqual(
                campaign_provenance["campaign_protocol"],
                {
                    "schema_version": "campaign-v1",
                    "protocol_hash": "hash-production",
                },
            )
            self.assertEqual(
                campaign_provenance["original_economic_outcome"],
                {"economic_outcome": {"net_return": "0.12", "completed": 3}},
            )
            self.assertEqual(
                campaign_provenance["original_rejection"],
                {"rejection_reason": "legacy campaign rejection"},
            )
            self.assertEqual(
                campaign_provenance["original_provenance"],
                {
                    "source": "legacy-campaign",
                    "attestation_id": "attestation-production",
                },
            )
            persisted = processor._rolling_persist_strategy_lineage(documents, NOW)
            self.assertEqual(len(persisted), 1)
            accepted = store.list_rolling_enrollments(
                candidate_id="candidate-valid",
                status="ACCEPTED",
            )
            self.assertEqual(len(accepted), 1)
            self.assertEqual(accepted[0]["validation_version"], "rolling-enrollment-v2")
            self.assertEqual(
                accepted[0]["predecessor_enrollment_id"],
                "legacy-excluded-candidate-valid",
            )
            self.assertEqual(
                store.load_rolling_enrollment("legacy-excluded-candidate-valid")["status"],
                "EXCLUDED",
            )
            before = len(store.list_rolling_enrollments())
            rerun_documents = processor._rolling_strategy_documents()
            processor._rolling_persist_strategy_lineage(rerun_documents, NOW)
            self.assertEqual(len(store.list_rolling_enrollments()), before)
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM strategy_versions WHERE strategy_version_id LIKE 'strategy-version-%'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM research_trials WHERE research_trial_id LIKE 'research-trial-%'"
                ).fetchone()[0],
                1,
            )
    def test_rolling_enrollment_requires_nonblank_version_and_valid_predecessor(self) -> None:
        with _store(self.tmp_path) as store:
            base = {
                "enrollment_id": "excluded-base",
                "candidate_id": "candidate-base",
                "status": "EXCLUDED",
                "reason": "LEGACY_REJECTION",
                "created_at": NOW.isoformat(),
            }
            with self.assertRaises(ValueError):
                store.save_rolling_enrollment({**base, "validation_version": " "})
            with self.assertRaises(ValueError):
                store.save_rolling_enrollment({**base, "attempt_version": "\t"})
            store.save_rolling_enrollment(base)
            self.assertEqual(
                store.load_rolling_enrollment("excluded-base")["validation_version"],
                "rolling-enrollment-v1",
            )

            with self.assertRaises(ValueError):
                store.save_rolling_enrollment(
                    {
                        **base,
                        "enrollment_id": "dangling-successor",
                        "validation_version": "rolling-enrollment-v2",
                        "predecessor_enrollment_id": "missing",
                    }
                )
            with self.assertRaises(ValueError):
                store.save_rolling_enrollment(
                    {
                        **base,
                        "enrollment_id": "mismatched-successor",
                        "candidate_id": "candidate-other",
                        "validation_version": "rolling-enrollment-v2",
                        "predecessor_enrollment_id": "excluded-base",
                    }
                )
            store.save_rolling_enrollment(
                {
                    **base,
                    "enrollment_id": "accepted-base",
                    "candidate_id": "candidate-base",
                    "status": "ACCEPTED",
                    "strategy_version_id": "strategy-version-accepted-base",
                    "research_trial_id": "research-trial-accepted-base",
                }
            )
            with self.assertRaises(ValueError):
                store.save_rolling_enrollment(
                    {
                        **base,
                        "enrollment_id": "accepted-successor",
                        "validation_version": "rolling-enrollment-v2",
                        "predecessor_enrollment_id": "accepted-base",
                    }
                )
            with self.assertRaises(ValueError):
                store.save_rolling_enrollment(
                    {
                        **base,
                        "enrollment_id": "self-successor",
                        "validation_version": "rolling-enrollment-v2",
                        "predecessor_enrollment_id": "self-successor",
                    }
                )
            self.assertEqual(len(store.list_rolling_enrollments()), 2)

    def test_rolling_predecessor_lookup_is_latest_and_bounded(self) -> None:
        with _store(self.tmp_path) as store:
            for index in range(300):
                store.save_rolling_enrollment(
                    {
                        "enrollment_id": f"excluded-{index:03d}",
                        "candidate_id": "candidate-latest",
                        "status": "EXCLUDED",
                        "reason": "LEGACY_REJECTION",
                        "created_at": (NOW + timedelta(seconds=index)).isoformat(),
                    }
                )
            newest = store.list_rolling_enrollments(
                candidate_id="candidate-latest",
                status="EXCLUDED",
                limit=1,
            )
            self.assertEqual([item["enrollment_id"] for item in newest], ["excluded-299"])

    def test_strategy_trial_policy_and_evidence_rows_are_immutable(self) -> None:
        self.assertEqual(default_rolling_admission_policy().requested_window_days, (7, 30))
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy())
            store.save_research_trial(_trial())
            policy = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "config_hash": policy.config_hash,
                    "policy": policy,
                    "created_at": NOW.isoformat(),
                }
            )
            store.save_strategy_evidence_window(_evidence())
    
            with self.assertRaises(ValueError):
                store.save_strategy_version(_strategy(payload={"changed": True}))
            with self.assertRaises(ValueError):
                store.save_research_trial(_trial(result={"changed": True}))
            with self.assertRaises(ValueError):
                store.save_admission_policy(
                    {
                        "policy_id": policy.policy_id,
                        "version": policy.version,
                        "config_hash": policy.config_hash,
                        "policy": {"changed": True},
                        "created_at": NOW.isoformat(),
                    }
                )
            with self.assertRaises(ValueError):
                store.save_strategy_evidence_window(_evidence(realized_pnl="999.00"))
    
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM strategy_versions').fetchone()[0], 1)
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM research_trials').fetchone()[0], 1)
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM admission_policies').fetchone()[0], 1)
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM strategy_evidence_windows').fetchone()[0], 1)
            loaded = store.load_admission_policy(policy.policy_id, policy.version)
            self.assertEqual(loaded['config_hash'], policy.config_hash)

    def test_v1_availability_from_payload_survives_migration_and_resave(self) -> None:
        legacy = _evidence(
            "sv-v1-migrated",
            "window-v1-migrated",
            source_digest="sha256:legacy-source",
            accounting_digest="sha256:legacy-accounting",
            accounting_available=True,
        )
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-v1-migrated"))
            store.save_strategy_evidence_window(legacy)
            store.connection.execute(
                "UPDATE strategy_evidence_windows "
                "SET accounting_available=NULL "
                "WHERE evidence_window_id=?",
                ("window-v1-migrated",),
            )
            store.connection.commit()

            loaded = store.list_strategy_evidence_windows("sv-v1-migrated")[0]
            self.assertTrue(loaded["accounting_available"])
            self.assertEqual(loaded["evidence_digest"], legacy["evidence_digest"])
            self.assertEqual(loaded["realized_pnl"], "8.00")

            store.save_strategy_evidence_window(loaded)
            restored = store.list_strategy_evidence_windows("sv-v1-migrated")[0]
            self.assertTrue(restored["accounting_available"])
            self.assertEqual(restored["evidence_digest"], legacy["evidence_digest"])
            self.assertEqual(restored["realized_pnl"], "8.00")

    
    
    def test_evidence_windows_preserve_requested_7_30_days_and_actual_coverage(self) -> None:
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy())
            store.save_strategy_evidence_window(_evidence(evidence_window_id="window-7", days=7, actual_days=7))
            store.save_strategy_evidence_window(
                _evidence(
                    evidence_window_id="window-30",
                    days=30,
                    actual_days=17,
                    available_from=(NOW - timedelta(days=17)).isoformat(),
                )
            )
    
            rows = store.list_strategy_evidence_windows("sv-alpha")
            by_id = {row["evidence_window_id"]: row for row in rows}
            self.assertEqual(by_id['window-7']['requested_days'], 7)
            self.assertEqual(by_id['window-7']['actual_coverage_seconds'], 7 * 24 * 60 * 60)
            self.assertEqual(by_id['window-30']['requested_days'], 30)
            self.assertEqual(by_id['window-30']['actual_coverage_seconds'], 17 * 24 * 60 * 60)
            self.assertNotEqual(by_id['window-30']['available_from'], by_id['window-30']['available_through'])
    
    
    def test_selection_is_deterministic_and_bounded_by_k(self) -> None:
        policy = _policy(max_members=2, experimental_allocation_enabled=True)
        evidence = [
            _evidence("sv-zeta", "w-zeta", score="0.90"),
            _evidence("sv-alpha", "w-alpha", score="0.90"),
            _evidence("sv-beta", "w-beta", score="0.80"),
            _evidence("sv-gamma", "w-gamma", score="0.70"),
        ]
    
        first = evaluate_rolling_selection(policy, evidence, None, NOW)
        second = evaluate_rolling_selection(policy, list(reversed(evidence)), None, NOW)
    
        first_ids = [str(_member_value(member, "strategy_version_id")) for member in _decision_members(first)]
        second_ids = [str(_member_value(member, "strategy_version_id")) for member in _decision_members(second)]
        self.assertLessEqual(len(first_ids), 2)
        self.assertEqual(first_ids, ['sv-alpha', 'sv-zeta'])
        self.assertEqual(second_ids, first_ids)
        self.assertIn(_decision_status(first), {'ACTIVE', 'PAPER'})
    
    
    def test_low_evidence_is_observation_or_paper_without_funding(self) -> None:
        policy = _policy(experimental_allocation_enabled=False, min_actual_coverage_seconds=7 * 24 * 60 * 60)
        low = _evidence("sv-low", "w-low", actual_days=2, score="0.95")
    
        decision = evaluate_rolling_selection(policy, [low], None, NOW)
        members = _decision_members(decision)
    
        self.assertTrue(members)
        self.assertIn(_decision_status(decision), {'OBSERVE', 'PAPER', 'PAUSE'})
        member = members[0]
        self.assertIn(str(_member_value(member, 'status')), {'OBSERVE', 'PAPER'})
        self.assertEqual(Decimal(str(_member_value(member, 'allocation'))), Decimal('0'))
        self.assertTrue('coverage' in str(_member_value(member, 'reason')).lower() or 'evidence' in str(_member_value(member, 'reason')).lower())
    
    
    def test_overlapping_strategies_are_not_separately_funded(self) -> None:
        policy = _policy(max_members=3, experimental_allocation_enabled=True)
        evidence = [
            _evidence("sv-alpha", "w-alpha", score="0.95", overlap_key="same-exposure"),
            _evidence("sv-beta", "w-beta", score="0.90", overlap_key="same-exposure"),
            _evidence("sv-gamma", "w-gamma", score="0.85", overlap_key="independent-exposure"),
        ]
    
        decision = evaluate_rolling_selection(policy, evidence, None, NOW)
        funded = [
            member
            for member in _decision_members(decision)
            if str(_member_value(member, "status")) == "ACTIVE"
            and Decimal(str(_member_value(member, "allocation"))) > 0
        ]
        overlap_keys = [str(_member_value(member, "overlap_key")) for member in funded]
        self.assertEqual(len(overlap_keys), len(set(overlap_keys)))
        self.assertIn('same-exposure', overlap_keys)
        self.assertLessEqual({str(_member_value(member, 'strategy_version_id')) for member in funded}, {'sv-alpha', 'sv-gamma'})
    
    
    def test_shared_allocations_are_finite_nonnegative_and_within_budget(self) -> None:
        policy = _policy(max_members=3, experimental_allocation_enabled=True)
        evidence = [_evidence("sv-a", "w-a", score="0.9"), _evidence("sv-b", "w-b", score="0.8")]
        decision = evaluate_rolling_selection(policy, evidence, None, NOW)
        allocations = [Decimal(str(_member_value(member, "allocation"))) for member in _decision_members(decision)]
    
        self.assertTrue(all((value >= 0 and value.is_finite() for value in allocations)))
    
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-a"))
            store.save_strategy_version(_strategy("sv-b"))
            store.save_research_trial(_trial("sv-a"))
            store.save_research_trial(_trial("sv-b"))
            store.save_strategy_evidence_window(_evidence("sv-a", "w-a"))
            store.save_strategy_evidence_window(_evidence("sv-b", "w-b"))
            policy_for_storage = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy_for_storage.policy_id,
                    "version": policy_for_storage.version,
                    "config_hash": policy_for_storage.config_hash,
                    "policy": policy_for_storage,
                    "created_at": NOW.isoformat(),
                }
            )
            selection = _selection("selection-allocation", k=2)
            with self.assertRaises(ValueError):
                store.commit_portfolio_selection(
                    selection,
                    [
                        _member("sv-a", selection_id="selection-allocation", allocation="-1"),
                        _member("sv-b", selection_id="selection-allocation", allocation="11"),
                    ],
                )
            with self.assertRaises(ValueError):
                store.commit_portfolio_selection(
                    selection,
                    [_member("sv-a", selection_id="selection-allocation", allocation="NaN")],
                )
            with self.assertRaises(ValueError):
                store.commit_portfolio_selection(
                    selection,
                    [
                        _member("sv-a", selection_id="selection-allocation", allocation="6"),
                        _member("sv-b", selection_id="selection-allocation", allocation="5"),
                    ],
                )
    
    
    def test_ordinary_single_loss_does_not_override_hysteresis(self) -> None:
        policy = _policy(
            max_members=1,
            experimental_allocation_enabled=True,
            replacement_margin="0.20",
            cooldown_seconds=7 * 24 * 60 * 60,
        )
        current = {
            **_selection("current-selection", k=1),
            "strategy_version_id": "sv-incumbent",
            "score": "0.80",
            "status": "ACTIVE",
            "members": [_member("sv-incumbent", selection_id="current-selection", score="0.80", position_management_state={"open_lot": "lot-1"})],
        }
        evidence = [
            _evidence("sv-incumbent", "w-incumbent", score="0.55"),
            _evidence("sv-challenger", "w-challenger", score="0.60"),
        ]
    
        decision = evaluate_rolling_selection(policy, evidence, current, NOW + timedelta(hours=1))
        ids = [str(_member_value(member, "strategy_version_id")) for member in _decision_members(decision)]
        self.assertEqual(ids, ['sv-incumbent'])
        incumbent = _decision_members(decision)[0]
        self.assertEqual(_member_value(incumbent, 'strategy_version_id'), 'sv-incumbent')
        self.assertIn(str(_member_value(incumbent, 'status')), {'ACTIVE', 'RETAINED'})
        self.assertEqual(Decimal(str(_member_value(incumbent, 'allocation'))), Decimal('10.00'))
        self.assertEqual(_member_value(incumbent, 'position_management_state')['open_lot'], 'lot-1')
        self.assertIn(_decision_status(decision), {'ACTIVE', 'PAPER'})
        self.assertNotIn('loss', ' '.join((str(reason) for reason in getattr(decision, 'reasons', ()))).lower())
    
    
    def test_pause_replace_and_re_admit_require_new_evidence(self) -> None:
        policy = _policy(max_members=1, experimental_allocation_enabled=True, replacement_margin="0.05")
        current = {
            **_selection("current-selection", k=1),
            "strategy_version_id": "sv-incumbent",
            "score": "0.80",
            "status": "ACTIVE",
            "members": [_member("sv-incumbent", selection_id="current-selection", score="0.80")],
        }
    
        paused = evaluate_rolling_selection(
            policy,
            [_evidence("sv-incumbent", "w-failed", hard_failure=True)],
            current,
            NOW,
        )
        self.assertIn(_decision_status(paused), {'PAUSE', 'OBSERVE'})
        self.assertIn(str(_member_value(_decision_members(paused)[0], 'status')), {'PAUSED', 'OBSERVE'})
    
        replacement = evaluate_rolling_selection(
            policy,
            [_evidence("sv-replacement", "w-replacement", score="0.95")],
            current,
            NOW + timedelta(days=2),
        )
        self.assertEqual([str(_member_value(member, 'strategy_version_id')) for member in _decision_members(replacement)], ['sv-replacement'])
    
        stale_re_admit = evaluate_rolling_selection(
            policy,
            [_evidence("sv-incumbent", "w-failed", hard_failure=True)],
            None,
            NOW + timedelta(days=3),
        )
        self.assertFalse(any((str(_member_value(member, 'status')) == 'ACTIVE' for member in _decision_members(stale_re_admit))))
    
        re_admitted = evaluate_rolling_selection(
            policy,
            [_evidence("sv-incumbent", "w-new", score="0.90", hard_failure=False)],
            None,
            NOW + timedelta(days=4),
        )
        self.assertEqual([str(_member_value(member, 'strategy_version_id')) for member in _decision_members(re_admitted)], ['sv-incumbent'])
        self.assertEqual(str(_member_value(_decision_members(re_admitted)[0], 'evidence_window_id')), 'w-new')
    
    
    def test_portfolio_selection_is_append_only_and_current_survives_restart(self) -> None:
        path = self.tmp_path / "restart.sqlite3"
        with AxiomStore(str(path)) as first:
            first.save_strategy_version(_strategy("sv-alpha"))
            policy_for_storage = _policy()
            first.save_admission_policy(
                {
                    "policy_id": policy_for_storage.policy_id,
                    "version": policy_for_storage.version,
                    "config_hash": policy_for_storage.config_hash,
                    "policy": policy_for_storage,
                    "created_at": NOW.isoformat(),
                }
            )
            first.save_strategy_version(_strategy("sv-beta"))
            first.save_research_trial(_trial("sv-beta"))
            first.save_research_trial(_trial("sv-alpha"))
            first.save_strategy_evidence_window(_evidence("sv-alpha", "window-alpha-7"))
            first.save_strategy_evidence_window(_evidence("sv-beta", "window-beta-7"))
            first.commit_portfolio_selection(
                _selection("selection-1", selected_at=NOW),
                [_member("sv-alpha", selection_id="selection-1", evidence_window_id="window-alpha-7")],
            )
            with self.assertRaises(ValueError):
                first.commit_portfolio_selection(
                    _selection("selection-1", selected_at=NOW),
                    [_member("sv-beta", selection_id="selection-1", evidence_window_id="window-beta-7")],
                )
            self.assertEqual(first.load_current_portfolio_selection()['portfolio_selection_id'], 'selection-1')

        with AxiomStore(str(path)) as restarted:
            restarted.commit_portfolio_selection(
                _selection("selection-2", selected_at=NOW + timedelta(days=1), k=0),
                [_member("sv-beta", selection_id="selection-2", status="PAPER", allocation="0", evidence_window_id="window-beta-7")],
            )
            current = restarted.load_current_portfolio_selection()
            history = restarted.list_portfolio_selections(limit=10)
            self.assertEqual(current['portfolio_selection_id'], 'selection-2')
            self.assertEqual({row['portfolio_selection_id'] for row in history}, {'selection-1', 'selection-2'})
            self.assertEqual(len(history), 2)

    def test_foreign_or_newer_trial_cannot_replace_selected_trial(self) -> None:
        policy = _policy(max_members=1, experimental_allocation_enabled=True, cooldown_seconds=0)
        selection_id = "selection-trial-lineage"
        incumbent = _member(
            "sv-alpha",
            selection_id=selection_id,
            research_trial_id="trial-alpha-old",
            evidence_window_id="window-alpha",
        )
        current = {
            **_selection(selection_id, k=1),
            "members": [incumbent],
        }
        newer_evidence = _evidence(
            "sv-alpha",
            "window-alpha-new",
            research_trial_id="trial-alpha-new",
        )
        decision = evaluate_rolling_selection(policy, [newer_evidence], current, NOW + timedelta(days=1))
        selected = _decision_members(decision)[0]
        self.assertEqual(_member_value(selected, "research_trial_id"), "trial-alpha-old")

        with _store(self.tmp_path) as store:
            store.save_admission_policy(
                {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "config_hash": policy.config_hash,
                    "policy": policy,
                    "created_at": NOW.isoformat(),
                }
            )
            store.save_strategy_version(_strategy("sv-alpha"))
            store.save_strategy_version(_strategy("sv-foreign"))
            store.save_research_trial(_trial("sv-alpha", "trial-alpha-old"))
            store.save_research_trial(_trial("sv-alpha", "trial-alpha-new"))
            store.save_research_trial(_trial("sv-alpha", "trial-foreign"))
            store.save_strategy_evidence_window(
                _evidence(
                    "sv-alpha",
                    "window-alpha",
                    research_trial_id="trial-alpha-old",
                    candidate_id="candidate-sv-alpha",
                )
            )
            store.save_strategy_evidence_window(
                _evidence(
                    "sv-alpha",
                    "window-alpha-new",
                    research_trial_id="trial-alpha-new",
                    candidate_id="candidate-sv-alpha",
                )
            )
            store.commit_portfolio_selection(_selection(selection_id, k=1), [incumbent])

            with self.assertRaisesRegex(ValueError, "identity conflict"):
                store.commit_portfolio_selection(
                    _selection(selection_id, k=1),
                    [
                        _member(
                            "sv-alpha",
                            selection_id=selection_id,
                            research_trial_id="trial-alpha-new",
                            evidence_window_id="window-alpha-new",
                        )
                    ],
                )
            with self.assertRaisesRegex(ValueError, "evidence window lineage mismatch"):
                store.commit_portfolio_selection(
                    _selection(selection_id, k=1),
                    [
                        _member(
                            "sv-alpha",
                            selection_id=selection_id,
                            research_trial_id="trial-foreign",
                            evidence_window_id="window-alpha",
                        )
                    ],
                )
            loaded = store.load_current_portfolio_selection()
            assert loaded is not None
            self.assertEqual(loaded["members"][0]["research_trial_id"], "trial-alpha-old")
    
    
    def test_review_state_round_trips_without_mutating_selection_history(self) -> None:
        with _store(self.tmp_path) as store:
            policy_for_storage = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy_for_storage.policy_id,
                    "version": policy_for_storage.version,
                    "config_hash": policy_for_storage.config_hash,
                    "policy": policy_for_storage,
                    "created_at": NOW.isoformat(),
                }
            )
            store.save_strategy_version(_strategy("sv-alpha"))
            store.save_strategy_evidence_window(_evidence("sv-alpha", "window-alpha-7"))
            store.save_research_trial(_trial("sv-alpha"))
            store.commit_portfolio_selection(
                _selection("selection-review"),
                [_member("sv-alpha", selection_id="selection-review", evidence_window_id="window-alpha-7")],
            )
            store.save_portfolio_review_state(
                {
                    "status": "PAUSED",
                    "reason": "awaiting new evidence",
                    "paused_at": NOW.isoformat(),
                    "review_due_at": (NOW + timedelta(days=1)).isoformat(),
                }
            )
            state = store.load_portfolio_review_state()
            self.assertEqual(state['status'], 'PAUSED')
            self.assertEqual(state['reason'], 'awaiting new evidence')
            self.assertEqual(store.list_portfolio_selections(limit=10)[0]['portfolio_selection_id'], 'selection-review')
    
    
    def test_hard_failure_isolated_to_affected_strategy(self) -> None:
        policy = _policy(max_members=2, experimental_allocation_enabled=True)
        current = {
            **_selection(
                "selection-current",
                k=2,
                selected_at=NOW - timedelta(days=2),
                review_due_at=NOW - timedelta(days=1),
            ),
            "members": [
                _member(
                    "sv-failed",
                    selection_id="selection-current",
                    allocation="5.00",
                    score="0.70",
                ),
                _member(
                    "sv-healthy",
                    selection_id="selection-current",
                    allocation="5.00",
                    score="0.60",
                ),
            ],
        }

        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence("sv-failed", "w-failed", hard_failure=True),
                _evidence("sv-healthy", "w-healthy", score="0.90"),
            ],
            current,
            NOW,
        )
        members = {
            str(_member_value(member, "strategy_version_id")): member
            for member in _decision_members(decision)
        }

        self.assertEqual(set(members), {"sv-failed", "sv-healthy"})
        self.assertEqual(str(_member_value(members["sv-failed"], "status")), "PAUSED")
        self.assertEqual(Decimal(str(_member_value(members["sv-failed"], "allocation"))), Decimal("0"))
        self.assertEqual(str(_member_value(members["sv-healthy"], "status")), "ACTIVE")
        self.assertGreater(Decimal(str(_member_value(members["sv-healthy"], "allocation"))), Decimal("0"))

    def test_overlap_losing_incumbent_is_defunded(self) -> None:
        policy = _policy(
            max_members=1,
            experimental_allocation_enabled=True,
            replacement_margin="0.10",
            cooldown_seconds=0,
        )
        current = {
            **_selection(
                "selection-current",
                k=1,
                selected_at=NOW - timedelta(days=2),
                review_due_at=NOW - timedelta(days=1),
            ),
            "members": [
                _member(
                    "sv-incumbent",
                    selection_id="selection-current",
                    allocation="10.00",
                    score="0.43",
                    overlap_key="shared-exposure",
                    position_management_state={"position_id": "position-overlap"},
                )
            ],
        }
        evidence = [
            _evidence(
                "sv-incumbent",
                "w-incumbent",
                overlap_key="shared-exposure",
                paper_sizing="1.00",
                allocated_capital_net_return="0.95",
            ),
            _evidence(
                "sv-challenger",
                "w-challenger",
                overlap_key="shared-exposure",
                paper_sizing="1.00",
                allocated_capital_net_return="1.00",
            ),
        ]

        decision = evaluate_rolling_selection(policy, evidence, current, NOW)
        members = _decision_members(decision)
        removed_members = list(getattr(decision, "removed_members", ()) or ())
        incumbent_rows = [
            member
            for member in (*members, *removed_members)
            if str(_member_value(member, "strategy_version_id")) == "sv-incumbent"
        ]
        self.assertEqual(len(incumbent_rows), 1)
        incumbent = incumbent_rows[0]

        self.assertIn(str(_member_value(incumbent, "status")), {"OBSERVE", "REMOVED"})
        self.assertEqual(Decimal(str(_member_value(incumbent, "allocation"))), Decimal("0"))
        state = _member_value(incumbent, "position_management_state")
        self.assertEqual(state["position_id"], "position-overlap")
        funded = [
            member
            for member in members
            if Decimal(str(_member_value(member, "allocation"))) > Decimal("0")
        ]
        self.assertEqual(
            len(
                {
                    str(_member_value(member, "overlap_key"))
                    for member in funded
                }
            ),
            len(funded),
        )

    def test_zero_allocation_observations_with_duplicate_overlap_persist(self) -> None:
        with _store(self.tmp_path) as store:
            policy = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "config_hash": policy.config_hash,
                    "policy": policy,
                    "created_at": NOW.isoformat(),
                }
            )
            for strategy_id, window_id in (
                ("sv-observe-a", "w-observe-a"),
                ("sv-observe-b", "w-observe-b"),
            ):
                store.save_strategy_version(_strategy(strategy_id))
                store.save_research_trial(_trial(strategy_id))
                store.save_strategy_evidence_window(_evidence(strategy_id, window_id))

            committed = store.commit_portfolio_selection(
                _selection("selection-observations", k=0),
                [
                    _member(
                        "sv-observe-a",
                        selection_id="selection-observations",
                        status="OBSERVE",
                        allocation="0",
                        evidence_window_id="w-observe-a",
                        overlap_key="shared-observation",
                    ),
                    _member(
                        "sv-observe-b",
                        selection_id="selection-observations",
                        status="OBSERVE",
                        allocation="0",
                        evidence_window_id="w-observe-b",
                        overlap_key="shared-observation",
                    ),
                ],
            )
            self.assertEqual(
                [member["overlap_key"] for member in committed["members"]],
                ["shared-observation", "shared-observation"],
            )
            loaded = store.load_current_portfolio_selection()
            assert loaded is not None
            self.assertEqual(len(loaded["members"]), 2)
            self.assertEqual(
                {member["strategy_version_id"] for member in loaded["members"]},
                {"sv-observe-a", "sv-observe-b"},
            )
            self.assertTrue(all(member["status"] == "OBSERVE" for member in loaded["members"]))
            self.assertTrue(all(Decimal(str(member["allocation"])) == Decimal("0") for member in loaded["members"]))

    def test_actual_coverage_cannot_exceed_timestamp_boundaries(self) -> None:
        inflated = _evidence(
            "sv-coverage",
            "w-coverage-inflated",
            actual_days=7,
        )
        inflated["actual_coverage_seconds"] = 7 * 86400 + 1
        with self.assertRaises(ValueError):
            RollingEvidence.from_mapping(inflated)

        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-coverage"))
            with self.assertRaises(ValueError):
                store.save_strategy_evidence_window(inflated)

    def test_positive_allocation_requires_persisted_same_strategy_evidence(self) -> None:
        with _store(self.tmp_path) as store:
            policy = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "config_hash": policy.config_hash,
                    "policy": policy,
                    "created_at": NOW.isoformat(),
                }
            )
            store.save_strategy_version(_strategy("sv-funded"))
            store.save_research_trial(_trial("sv-funded"))
            without_evidence = _member(
                "sv-funded",
                selection_id="selection-no-evidence",
                status="ACTIVE",
                allocation="1.00",
            )
            without_evidence["evidence_window_id"] = None
            with self.assertRaises(ValueError):
                store.commit_portfolio_selection(
                    _selection("selection-no-evidence"),
                    [without_evidence],
                )

            store.save_strategy_version(_strategy("sv-other"))
            store.save_strategy_evidence_window(_evidence("sv-other", "w-other"))
            with self.assertRaises(ValueError):
                store.commit_portfolio_selection(
                    _selection("selection-wrong-evidence"),
                    [
                        _member(
                            "sv-funded",
                            selection_id="selection-wrong-evidence",
                            status="ACTIVE",
                            allocation="1.00",
                            evidence_window_id="w-other",
                        )
                    ],
                )

    def test_position_management_state_survives_incumbent_rebuild(self) -> None:
        position_state = {
            "position_id": "position-1",
            "open_quantity": Decimal("2.5"),
            "risk": {"stop_fraction": Decimal("0.25")},
        }
        policy = _policy()
        current = {
            **_selection(
                "selection-current",
                selected_at=NOW - timedelta(days=2),
                review_due_at=NOW - timedelta(days=1),
            ),
            "members": [
                _member(
                    "sv-incumbent",
                    selection_id="selection-current",
                    allocation="3.00",
                    score="0.80",
                    position_management_state=position_state,
                )
            ],
        }
        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence(
                    "sv-incumbent",
                    "w-low",
                    paper_sizing="1.00",
                    allocated_capital_net_return="0.00",
                )
            ],
            current,
            NOW,
        )
        member = _decision_members(decision)[0]
        state = _member_value(member, "position_management_state")

        self.assertEqual(str(_member_value(member, "strategy_version_id")), "sv-incumbent")
        self.assertIn(str(_member_value(member, "status")), {"ACTIVE", "RETAINED"})
        self.assertEqual(Decimal(str(_member_value(member, "allocation"))), Decimal("3.00"))
        self.assertEqual(state["position_id"], "position-1")
        self.assertEqual(state["open_quantity"], Decimal("2.5"))
        self.assertEqual(state["risk"]["stop_fraction"], Decimal("0.25"))
        self.assertIn(_decision_status(decision), {'ACTIVE', 'PAPER'})

    def test_nested_assumptions_round_trip_into_canonical_evidence(self) -> None:
        record = _evidence("sv-assumptions", "w-assumptions")
        for scalar in ("paper_sizing", "fee_assumption", "slippage_assumption"):
            record.pop(scalar)
        record.update(
            {
                "paper_sizing_assumptions": {
                    "paper_sizing": "10.00",
                    "sizing_mode": "fixed",
                    "nested": {"desk": "research"},
                },
                "paper_fee_assumptions": {
                    "fee_assumption": "0.0025",
                    "fee_mode": "maker",
                },
                "paper_slippage_assumptions": {
                    "slippage_assumption": "0.0050",
                    "slippage_mode": "conservative",
                },
            }
        )
        record = _canonicalize_evidence(record)

        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-assumptions"))
            store.save_strategy_evidence_window(record)
            loaded = store.list_strategy_evidence_windows("sv-assumptions")[0]
            self.assertEqual(
                loaded["paper_sizing_assumptions"],
                record["paper_sizing_assumptions"],
            )
            self.assertEqual(
                loaded["paper_fee_assumptions"],
                record["paper_fee_assumptions"],
            )
            self.assertEqual(
                loaded["paper_slippage_assumptions"],
                record["paper_slippage_assumptions"],
            )

        canonical = RollingEvidence.from_mapping(loaded)
        self.assertEqual(canonical.paper_sizing, Decimal("10.00"))
        self.assertEqual(canonical.fee_assumption, Decimal("0.0025"))
        self.assertEqual(canonical.slippage_assumption, Decimal("0.0050"))
        self.assertEqual(canonical.allocated_capital, Decimal("10.00"))

    def test_slippage_cost_does_not_double_across_round_trip(self) -> None:
        original = RollingEvidence.from_mapping(
            _evidence("sv-slippage", "w-slippage", slippage_costs="0.20")
        )
        self.assertEqual(original.costs, Decimal("0.70"))
        payload = json.loads(json.dumps(original.as_dict()))
        restored = RollingEvidence.from_mapping(payload)

        self.assertEqual(restored.costs, original.costs)
        self.assertEqual(restored.execution_cost, original.execution_cost)
        self.assertEqual(restored.execution_cost_rate, original.execution_cost_rate)

    def test_selection_as_dict_is_recursively_json_serializable(self) -> None:
        @dataclass(frozen=True)
        class PositionSnapshot:
            quantity: Decimal
            opened_at: datetime

        snapshot = PositionSnapshot(Decimal("2.5"), NOW - timedelta(hours=2))
        current = {
            **_selection(
                "selection-json",
                selected_at=NOW - timedelta(days=2),
                review_due_at=NOW - timedelta(days=1),
            ),
            "members": [
                _member(
                    "sv-json",
                    selection_id="selection-json",
                    allocation="1.00",
                    position_management_state={"snapshot": snapshot},
                )
            ],
        }
        decision = evaluate_rolling_selection(_policy(), [], current, NOW)
        encoded = json.dumps(decision.as_dict(), sort_keys=True)
        decoded = json.loads(encoded)

        self.assertEqual(decoded["global_budget"], "10.00")
        self.assertEqual(
            decoded["members"][0]["position_management_state"]["snapshot"],
            {
                "opened_at": (NOW - timedelta(hours=2)).isoformat(),
                "quantity": "2.5",
            },
        )

    def test_empty_observation_selection_persists(self) -> None:
        with _store(self.tmp_path) as store:
            policy = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "config_hash": policy.config_hash,
                    "policy": policy,
                    "created_at": NOW.isoformat(),
                }
            )
            committed = store.commit_portfolio_selection(
                _selection("selection-empty", k=0),
                [],
            )
            self.assertEqual(committed["portfolio_selection_id"], "selection-empty")
            self.assertEqual(committed["k"], 0)
            self.assertEqual(committed["members"], [])
            current = store.load_current_portfolio_selection()
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(current["portfolio_selection_id"], "selection-empty")
            self.assertEqual(current["k"], 0)
            self.assertEqual(current["members"], [])

    def test_drawdown_is_dimensionless_and_usd_alias_is_rejected(self) -> None:
        with _store(self.tmp_path) as store:
            store.save_strategy_version(_strategy("sv-drawdown"))
            usd = _evidence("sv-drawdown", "w-drawdown-usd")
            usd.pop("drawdown")
            usd["drawdown_usd"] = "0.25"
            usd = _canonicalize_evidence(usd)
            with self.assertRaises(ValueError):
                store.save_strategy_evidence_window(usd)

            oversized = _evidence(
                "sv-drawdown",
                "w-drawdown-oversized",
            )
            oversized["drawdown"] = "1.01"
            with self.assertRaises(ValueError):
                store.save_strategy_evidence_window(oversized)

    def test_k_migration_backfills_exact_member_counts(self) -> None:
        legacy_path = self.tmp_path / "legacy-k.sqlite3"
        connection = sqlite3.connect(str(legacy_path))
        connection.executescript(
            """
            CREATE TABLE portfolio_selections (
                portfolio_selection_id TEXT PRIMARY KEY,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                risk_config_id TEXT NOT NULL,
                risk_config_generation INTEGER NOT NULL,
                risk_config_hash TEXT NOT NULL,
                global_budget TEXT NOT NULL,
                selected_at TEXT NOT NULL,
                review_due_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                committed_at TEXT NOT NULL
            );
            CREATE TABLE portfolio_selection_members (
                portfolio_selection_id TEXT NOT NULL,
                strategy_version_id TEXT NOT NULL,
                allocation TEXT NOT NULL,
                status TEXT NOT NULL,
                score TEXT NOT NULL,
                reason TEXT NOT NULL,
                evidence_window_id TEXT,
                overlap_key TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                PRIMARY KEY (portfolio_selection_id, strategy_version_id)
            );
            """
        )
        legacy_rows = (
            ("legacy-empty", 0, NOW),
            ("legacy-one", 1, NOW + timedelta(hours=1)),
            ("legacy-two", 2, NOW + timedelta(hours=2)),
        )
        for selection_id, member_count, selected_at in legacy_rows:
            connection.execute(
                """
                INSERT INTO portfolio_selections(
                    portfolio_selection_id, policy_id, policy_version,
                    risk_config_id, risk_config_generation, risk_config_hash,
                    global_budget, selected_at, review_due_at, payload_json, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
                """,
                (
                    selection_id,
                    "legacy-policy",
                    "1",
                    "risk-legacy",
                    1,
                    "sha256:risk-legacy",
                    "10.00",
                    selected_at.isoformat(),
                    (selected_at + timedelta(days=1)).isoformat(),
                    selected_at.isoformat(),
                ),
            )
            for index in range(member_count):
                connection.execute(
                    """
                    INSERT INTO portfolio_selection_members(
                        portfolio_selection_id, strategy_version_id, allocation,
                        status, score, reason, evidence_window_id, overlap_key,
                        payload_json, created_at
                    ) VALUES (?, ?, '0', 'OBSERVE', '0', 'legacy', NULL, '', '{}', ?)
                    """,
                    (selection_id, f"legacy-strategy-{index}", selected_at.isoformat()),
                )
        connection.commit()
        connection.close()

        with AxiomStore(str(legacy_path)) as store:
            rows = store.connection.execute(
                """
                SELECT portfolio_selection_id, k
                FROM portfolio_selections
                ORDER BY portfolio_selection_id
                """
            ).fetchall()
            self.assertEqual(
                {row["portfolio_selection_id"]: int(row["k"]) for row in rows},
                {
                    "legacy-empty": 0,
                    "legacy-one": 1,
                    "legacy-two": 2,
                },
            )
            loaded = {
                row["portfolio_selection_id"]: len(row["members"])
                for row in store.list_portfolio_selections(limit=10)
            }
            self.assertEqual(loaded, {"legacy-empty": 0, "legacy-one": 1, "legacy-two": 2})

    def test_retained_allocation_does_not_exceed_global_budget(self) -> None:
        policy = _policy(
            max_members=2,
            experimental_allocation_enabled=True,
            cooldown_seconds=0,
        )
        current = {
            **_selection(
                "selection-current",
                k=1,
                selected_at=NOW - timedelta(days=2),
                review_due_at=NOW - timedelta(days=1),
            ),
            "members": [
                _member(
                    "sv-incumbent",
                    selection_id="selection-current",
                    allocation="5.00",
                    score="0.80",
                    position_management_state={"position_id": "position-incumbent"},
                )
            ],
        }
        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence(
                    "sv-incumbent",
                    "w-incumbent-low",
                    paper_sizing="1.00",
                    allocated_capital_net_return="0.00",
                ),
                _evidence("sv-new", "w-new", score="0.90"),
            ],
            current,
            NOW,
        )
        members = {
            str(_member_value(member, "strategy_version_id")): member
            for member in _decision_members(decision)
        }
        allocations = {
            strategy_id: Decimal(str(_member_value(member, "allocation")))
            for strategy_id, member in members.items()
        }

        self.assertEqual(set(members), {"sv-incumbent", "sv-new"})
        self.assertIn(str(_member_value(members["sv-incumbent"], "status")), {"ACTIVE", "RETAINED"})
        self.assertEqual(str(_member_value(members["sv-incumbent"], "strategy_version_id")), "sv-incumbent")
        self.assertLessEqual(allocations["sv-incumbent"], Decimal("5.00"))
        self.assertEqual(_member_value(members["sv-incumbent"], "position_management_state")["position_id"], "position-incumbent")
        self.assertGreater(allocations["sv-new"], Decimal("0"))
        self.assertLessEqual(sum(allocations.values(), Decimal("0")), Decimal("10.00"))
        self.assertIn(_decision_status(decision), {'ACTIVE', 'PAPER'})

    def test_daily_reviews_preserve_cooldown_anchor_until_day_seven_replacement(self) -> None:
        policy = _policy(
            max_members=1,
            experimental_allocation_enabled=True,
            replacement_margin="0",
            cooldown_seconds=7 * 86400,
        )
        change_at = NOW
        current = {
            **_selection(
                "selection-current",
                k=1,
                selected_at=change_at,
                review_due_at=change_at,
            ),
            "last_membership_change_at": change_at.isoformat(),
            "members": [
                _member(
                    "sv-incumbent",
                    selection_id="selection-current",
                    allocation="5.00",
                    score="0.20",
                )
            ],
        }
        evidence = [
            _evidence(
                "sv-incumbent",
                "w-incumbent-low",
                paper_sizing="1.00",
                allocated_capital_net_return="0.00",
            ),
            _evidence(
                "sv-challenger",
                "w-challenger",
                paper_sizing="1.00",
                allocated_capital_net_return="1.00",
            ),
        ]

        def cooldown_anchor(value: object) -> datetime:
            raw = getattr(value, "last_membership_change_at", None)
            if raw is None and isinstance(value, dict):
                raw = value.get("last_membership_change_at")
            if raw is None:
                raw = value.as_dict()["last_membership_change_at"]
            if isinstance(raw, datetime):
                return raw.astimezone(UTC)
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)

        reviewed: object = current
        for day in range(1, 7):
            reviewed = evaluate_rolling_selection(
                policy,
                evidence,
                reviewed,
                change_at + timedelta(days=day),
            )
            members = _decision_members(reviewed)
            self.assertEqual(
                [str(_member_value(member, "strategy_version_id")) for member in members],
                ["sv-incumbent"],
            )
            self.assertLessEqual(
                Decimal(str(_member_value(members[0], "allocation"))),
                Decimal("5.00"),
            )
            self.assertEqual(cooldown_anchor(reviewed), change_at)

        replaced = evaluate_rolling_selection(
            policy,
            evidence,
            reviewed,
            change_at + timedelta(days=7),
        )
        self.assertEqual(
            [
                str(_member_value(member, "strategy_version_id"))
                for member in _decision_members(replaced)
            ],
            ["sv-challenger"],
        )
        self.assertEqual(
            str(_member_value(_decision_members(replaced)[0], "status")),
            "ACTIVE",
        )
        self.assertGreater(
            Decimal(str(_member_value(_decision_members(replaced)[0], "allocation"))),
            Decimal("0"),
        )
        self.assertEqual(cooldown_anchor(replaced), change_at + timedelta(days=7))

    def test_newer_healthy_window_supersedes_older_hard_failure(self) -> None:
        policy = _policy(max_members=1, experimental_allocation_enabled=True)
        older_failure = _evidence(
            "sv-recovering",
            "w-recovering-failed",
            hard_failure=True,
        )
        newer_healthy = _evidence(
            "sv-recovering",
            "w-recovering-healthy",
            available_from=(NOW - timedelta(days=7) + timedelta(hours=1)).isoformat(),
            available_through=(NOW + timedelta(hours=1)).isoformat(),
            hard_failure=False,
        )

        decision = evaluate_rolling_selection(
            policy,
            [older_failure, newer_healthy],
            None,
            NOW + timedelta(hours=2),
        )
        members = _decision_members(decision)

        self.assertEqual(len(members), 1)
        self.assertEqual(
            str(_member_value(members[0], "strategy_version_id")),
            "sv-recovering",
        )
        self.assertEqual(
            str(_member_value(members[0], "evidence_window_id")),
            "w-recovering-healthy",
        )
        self.assertEqual(str(_member_value(members[0], "status")), "ACTIVE")
        self.assertGreater(
            Decimal(str(_member_value(members[0], "allocation"))),
            Decimal("0"),
        )

    def test_correction_chain_selects_active_latest_record(self) -> None:
        policy = _policy(
            max_members=1,
            experimental_allocation_enabled=True,
            min_actual_coverage_seconds=6 * 24 * 60 * 60,
            min_completed_outcomes=0,
            min_reliability="0",
        )
        predecessor = _v2_evidence(
            "sv-correction-chain",
            "window-correction-base",
            available_through=NOW.isoformat(),
            actual_coverage_seconds=6 * 24 * 60 * 60,
            hard_failure=True,
        )
        correction = _v2_evidence(
            "sv-correction-chain",
            "window-correction-middle",
            available_through=(NOW - timedelta(hours=1)).isoformat(),
            actual_coverage_seconds=6 * 24 * 60 * 60,
            hard_failure=True,
            evaluation_run_id="run-correction-middle",
            supersedes_evidence_id="window-correction-base",
            evaluation={
                "evaluation_run_id": "run-correction-middle",
                "evaluation_version": "rolling-evaluation:v2",
                "evaluator_invoked": True,
                "evaluator_completed": True,
            },
        )
        latest = _v2_evidence(
            "sv-correction-chain",
            "window-correction-latest",
            available_through=(NOW - timedelta(hours=2)).isoformat(),
            actual_coverage_seconds=6 * 24 * 60 * 60,
            evaluation_run_id="run-correction-latest",
            supersedes_evidence_id="window-correction-middle",
            evaluation={
                "evaluation_run_id": "run-correction-latest",
                "evaluation_version": "rolling-evaluation:v2",
                "evaluator_invoked": True,
                "evaluator_completed": True,
            },
        )

        decision = evaluate_rolling_selection(
            policy,
            [predecessor, correction, latest],
            None,
            NOW,
        )
        members = _decision_members(decision)

        self.assertEqual(len(members), 1)
        self.assertEqual(
            str(_member_value(members[0], "evidence_window_id")),
            "window-correction-latest",
        )
        self.assertEqual(str(_member_value(members[0], "status")), "ACTIVE")

    def test_foreign_correction_cannot_suppress_predecessor(self) -> None:
        policy = _policy(
            max_members=1,
            experimental_allocation_enabled=True,
        )
        predecessor = _v2_evidence(
            "sv-foreign-correction",
            "window-foreign-correction-base",
            available_through=NOW.isoformat(),
        )
        foreign = _v2_evidence(
            "sv-foreign-correction",
            "window-foreign-correction-foreign",
            candidate_id="candidate-foreign",
            available_through=NOW.isoformat(),
            evaluation_run_id="run-foreign-correction",
            supersedes_evidence_id="window-foreign-correction-base",
            evaluation={
                "evaluation_run_id": "run-foreign-correction",
                "evaluation_version": "rolling-evaluation:v2",
                "evaluator_invoked": True,
                "evaluator_completed": True,
            },
        )

        decision = evaluate_rolling_selection(
            policy,
            [predecessor, foreign],
            None,
            NOW,
        )
        members = _decision_members(decision)
        self.assertEqual(len(members), 1)
        self.assertEqual(
            str(_member_value(members[0], "evidence_window_id")),
            "window-foreign-correction-base",
        )

    def test_unsupported_score_declarations_reject_and_default_provenance_matches(self) -> None:
        with self.assertRaises(ValueError):
            _policy(score_formula="unsupported-score-formula")
        with self.assertRaises(ValueError):
            _policy(formula_version="unsupported-score-version")

        policy = default_rolling_admission_policy()
        evidence = RollingEvidence.from_mapping(_evidence("sv-formula", "w-formula"))
        decision = evaluate_rolling_selection(policy, [evidence], None, NOW)
        member = _decision_members(decision)[0]
        policy_payload = policy.as_dict()
        decision_payload = decision.as_dict()

        self.assertEqual(policy_payload["score_formula"], policy.score_formula)
        self.assertEqual(policy_payload["formula_version"], policy.formula_version)
        self.assertEqual(decision_payload["score_formula"], policy_payload["score_formula"])
        self.assertEqual(decision_payload["formula_version"], policy_payload["formula_version"])
        self.assertEqual(
            Decimal(str(_member_value(member, "score"))),
            evidence.score(policy),
        )

    def test_terminal_finite_campaign_remains_independent_of_rolling_rows(self) -> None:
        with _store(self.tmp_path) as store:
            policy_for_storage = _policy()
            store.save_admission_policy(
                {
                    "policy_id": policy_for_storage.policy_id,
                    "version": policy_for_storage.version,
                    "config_hash": policy_for_storage.config_hash,
                    "policy": policy_for_storage,
                    "created_at": NOW.isoformat(),
                }
            )
            store.save_experiment(
                "finite-campaign-1",
                {"status": "COMPLETED", "terminal": True, "result": {"return": "0.12"}},
                strategy_id="finite-strategy",
            )
            before = store.load_experiment("finite-campaign-1")
            store.save_strategy_version(_strategy("sv-rolling"))
            store.save_research_trial(_trial("sv-rolling", "trial-rolling"))
            store.save_strategy_evidence_window(
                _evidence(
                    "sv-rolling",
                    "w-rolling",
                    candidate_id="candidate-sv-rolling",
                    research_trial_id="trial-rolling",
                )
            )
            store.commit_portfolio_selection(
                _selection("selection-rolling"),
                [
                    _member(
                        "sv-rolling",
                        selection_id="selection-rolling",
                        candidate_id="candidate-sv-rolling",
                        research_trial_id="trial-rolling",
                        evidence_window_id="w-rolling",
                    )
                ],
            )
            after = store.load_experiment("finite-campaign-1")

            self.assertEqual(after, before)
            self.assertEqual(after["status"], "COMPLETED")
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM experiments WHERE experiment_id=?",
                    ("finite-campaign-1",),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                store.load_current_portfolio_selection()["portfolio_selection_id"],
                "selection-rolling",
            )
    def _legacy_model_lineage(
        self,
        store: AxiomStore,
        *,
        model: Mapping[str, object] | None = None,
        model_hash: str | None = None,
        plan_id: str = "plan-legacy-model",
        extra_plan_ids: Sequence[str] = (),
        strategy_version_id: str = "sv-legacy-model",
        candidate_id: str = "candidate-legacy-model",
        trial_id: str = "trial-legacy-model",
        hypothesis_id: str = "hypothesis-legacy-model",
    ) -> dict[str, object]:
        resolved_model = dict(model or {})
        resolved_hash = model_hash or _rolling_hash(resolved_model)
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "legacy-fixture",
            "resolution_aware": True,
            "resolution_inputs": ["settlement"],
        }
        provenance: dict[str, object] = {
            "candidate_id": candidate_id,
            "research_trial_id": trial_id,
            "plan_id": plan_id,
            "hypothesis_id": hypothesis_id,
            "model_hash": resolved_hash,
        }
        for index, alternate in enumerate(extra_plan_ids):
            provenance[f"alternate_{index}"] = {"plan_id": alternate}
        strategy = {
            "strategy_version_id": strategy_version_id,
            "strategy_id": "legacy-model",
            "version": "1",
            "strategy_hash": _rolling_hash(strategy_document),
            "config_hash": "config:legacy-model",
            "candidate_id": candidate_id,
            "hypothesis_id": hypothesis_id,
            "research_trial_id": trial_id,
            "plan_id": plan_id,
            "model_hash": resolved_hash,
            "strategy_document": strategy_document,
            "model_document": None,
            "provenance": provenance,
        }
        store.save_strategy_version(strategy)
        store.save_research_trial(
            {
                "research_trial_id": trial_id,
                "hypothesis_id": hypothesis_id,
                "strategy_version_id": strategy_version_id,
                "candidate_id": candidate_id,
                "plan_id": plan_id,
                "model_hash": resolved_hash,
            }
        )
        store.save_candidate_lifecycle(
            candidate_id,
            "IDEA",
            {"candidate_id": candidate_id},
            timestamp=NOW,
        )
        store.save_candidate_lifecycle(
            candidate_id,
            "FROZEN",
            {
                "candidate_id": candidate_id,
                "strategy_version_id": strategy_version_id,
                "research_trial_id": trial_id,
                "plan_id": plan_id,
                "hypothesis_id": hypothesis_id,
                "model_hash": resolved_hash,
            },
            from_stage="IDEA",
            timestamp=NOW,
        )
        store.save_experiment_plan(
            plan_id,
            resolved_model,
            hypothesis_id=hypothesis_id,
            timestamp=NOW,
        )
        return strategy

    def test_legacy_experiment_plan_model_resolves_under_exact_hash_proof(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            before = store.load_strategy_version(strategy["strategy_version_id"])
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            model, resolution = processor._resolve_rolling_model(strategy)
            after = store.load_strategy_version(strategy["strategy_version_id"])

            self.assertEqual(model, {"probability": 0.5})
            self.assertEqual(resolution["source_type"], "experiment_plan")
            self.assertEqual(resolution["plan_id"], "plan-legacy-model")
            self.assertEqual(resolution["model_hash"], _rolling_hash(model))
            self.assertEqual(after, before)

    def test_plan_prefixed_source_trial_alias_resolves_as_plan_provenance(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
                strategy_version_id="sv-plan-source-trial",
                candidate_id="candidate-plan-source-trial",
                trial_id="research-trial-plan-source-trial",
                plan_id="plan-plan-source-trial",
            )
            plan_id = "plan-plan-source-trial"
            strategy_payload = dict(strategy)
            strategy_payload.pop("plan_id", None)
            strategy_payload["source_trial_id"] = plan_id
            strategy_payload["provenance"] = {
                **strategy_payload["provenance"],
                "plan_id": None,
                "source_trial_id": plan_id,
            }
            store.connection.execute(
                "UPDATE strategy_versions SET payload_json=? WHERE strategy_version_id=?",
                (
                    json.dumps(strategy_payload, default=str),
                    "sv-plan-source-trial",
                ),
            )
            trial = store.load_research_trial("research-trial-plan-source-trial")
            self.assertIsNotNone(trial)
            assert trial is not None
            trial_payload = dict(trial)
            trial_payload.pop("plan_id", None)
            trial_payload["source_trial_id"] = plan_id
            store.connection.execute(
                "UPDATE research_trials SET payload_json=? WHERE research_trial_id=?",
                (
                    json.dumps(trial_payload, default=str),
                    "research-trial-plan-source-trial",
                ),
            )
            candidate = store.load_candidate_lifecycle("candidate-plan-source-trial")
            self.assertIsNotNone(candidate)
            assert candidate is not None
            candidate_payload = dict(candidate["payload"])
            candidate_payload.pop("plan_id", None)
            candidate_payload["source_trial_id"] = plan_id
            store.connection.execute(
                "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id=? AND stage=?",
                (
                    json.dumps(candidate_payload, default=str),
                    "candidate-plan-source-trial",
                    "FROZEN",
                ),
            )

            strategy_payload["research_trial_id"] = "research-trial-plan-source-trial"
            strategy_payload["source_trial_id"] = plan_id
            model, resolution = AutonomousResearchProcessor(
                store,
                clock=lambda: NOW,
            )._resolve_rolling_model(strategy_payload)
            self.assertEqual(model, {"probability": 0.5})
            self.assertEqual(resolution["plan_id"], plan_id)

            conflicting = dict(strategy_payload)
            conflicting["source_trial_id"] = "research-trial-other"
            with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_AMBIGUOUS"):
                AutonomousResearchProcessor(
                    store,
                    clock=lambda: NOW,
                )._resolve_rolling_model(conflicting)

    def test_legacy_experiment_plan_model_hash_mismatch_fails_closed(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.6},
                model_hash=_rolling_hash({"probability": 0.5}),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_MISMATCH"):
                processor._resolve_rolling_model(strategy)

    def test_legacy_model_plan_absent_or_ambiguous_fails_closed(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            store.connection.execute(
                "DELETE FROM experiment_plans WHERE plan_id=?",
                ("plan-legacy-model",),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            with self.assertRaisesRegex(ValueError, "MODEL_PLAN_MISSING"):
                processor._resolve_rolling_model(strategy)

            ambiguous = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
                plan_id="plan-legacy-a",
                extra_plan_ids=("plan-legacy-b",),
                strategy_version_id="sv-ambiguous-model",
                candidate_id="candidate-ambiguous-model",
                trial_id="trial-ambiguous-model",
            )
            with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_AMBIGUOUS"):
                processor._resolve_rolling_model(ambiguous)

    def test_embedded_current_model_remains_preferred_without_plan_lookup(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = _strategy(
                "sv-embedded-model",
                candidate_id="candidate-embedded-model",
                research_trial_id="trial-embedded-model",
                model_document={"probability": 0.4},
                model_hash=_rolling_hash({"probability": 0.4}),
                plan_id="missing-plan-is-irrelevant",
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            model, resolution = processor._resolve_rolling_model(strategy)

            self.assertEqual(model, {"probability": 0.4})
            self.assertEqual(resolution["source_type"], "embedded")
            self.assertIsNone(resolution["plan_id"])

    def test_canonical_verified_model_overrides_all_row_probability_hints(self) -> None:
        with _store(self.tmp_path) as store:
            model = {"probability": 0.5}
            strategy = _strategy(
                "sv-row-model-conflict",
                strategy_document={
                    "version": 1,
                    "market_type": "prediction",
                    "family": "probability_mispricing",
                    "parameters": {"threshold": 0.05},
                    "operations": [],
                    "probability_model": "embedded-fixture",
                    "resolution_aware": True,
                    "resolution_inputs": ["settlement"],
                },
                model_document=model,
                model_hash=_rolling_hash(model),
            )
            evaluation = AutonomousResearchProcessor(
                store,
                clock=lambda: NOW,
            )._rolling_canonical_evaluation(
                strategy,
                [
                    {
                        "snapshot": {
                            "yes_mid": 0.4,
                            "model_probability": 0.1,
                            "probability": 0.1,
                            "predicted_probability": 0.1,
                            "p": 0.1,
                            "settlement": "YES",
                        },
                        "timestamp": NOW.isoformat(),
                        "market_id": "market-row-model-conflict",
                        "yes_mid": 0.4,
                        "model_probability": 0.1,
                        "probability": 0.1,
                        "predicted_probability": 0.1,
                        "p": 0.1,
                        "settlement": "YES",
                    }
                ],
                "HISTORICAL",
            )
            self.assertIsNotNone(evaluation)
            assert evaluation is not None
            self.assertTrue(evaluation["evaluator_completed"])
            self.assertEqual(evaluation["signal_count"], 1)
            self.assertEqual(
                evaluation["evaluation"]["model_resolution"]["model_hash"],
                _rolling_hash(model),
            )


    def test_verified_model_field_preserves_named_row_alias_only(self) -> None:
        with _store(self.tmp_path) as store:
            model = {"field": "model_probability"}
            strategy = _strategy(
                "sv-row-model-field",
                strategy_document={
                    "version": 1,
                    "market_type": "prediction",
                    "family": "probability_mispricing",
                    "parameters": {"threshold": 0.05},
                    "operations": [],
                    "probability_model": "field-fixture",
                    "resolution_aware": True,
                    "resolution_inputs": ["settlement"],
                },
                model_document=model,
                model_hash=_rolling_hash(model),
            )
            evaluation = AutonomousResearchProcessor(
                store,
                clock=lambda: NOW,
            )._rolling_canonical_evaluation(
                strategy,
                [
                    {
                        "snapshot": {
                            "yes_mid": 0.4,
                            "model_probability": 0.5,
                            "probability": 0.1,
                            "predicted_probability": 0.1,
                            "p": 0.1,
                        },
                        "timestamp": NOW.isoformat(),
                        "market_id": "market-row-model-field",
                        "yes_mid": 0.4,
                        "model_probability": 0.1,
                        "probability": 0.1,
                        "predicted_probability": 0.1,
                        "p": 0.1,
                        "settlement": "YES",
                    }
                ],
                "HISTORICAL",
            )
            self.assertIsNotNone(evaluation)
            assert evaluation is not None
            self.assertTrue(evaluation["evaluator_completed"])
            self.assertEqual(evaluation["signal_count"], 1)
    def test_row_model_probability_cannot_bypass_missing_immutable_model(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = _strategy(
                "sv-row-model-probability",
                strategy_document={
                    "version": 1,
                    "market_type": "prediction",
                    "family": "probability_mispricing",
                    "parameters": {"threshold": 0.05},
                    "operations": [],
                },
                model_document=None,
                provenance={
                    "candidate_id": "candidate-sv-row-model-probability",
                    "research_trial_id": "trial-row-model-probability",
                },
            )
            rows = [
                {
                    "timestamp": NOW.isoformat(),
                    "market_id": "market-row-model-probability",
                    "yes_mid": 0.4,
                    "model_probability": 0.9,
                    "settlement": "YES",
                }
            ]
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            evaluation = processor._rolling_canonical_evaluation(
                strategy,
                rows,
                "HISTORICAL",
            )

            assert evaluation is not None
            self.assertFalse(evaluation["evaluator_invoked"])
            self.assertFalse(evaluation["evaluator_completed"])
            self.assertEqual(
                evaluation["evaluator_prerequisite"],
                "MODEL_INPUT_MISSING",
            )

    def test_plan_hash_must_be_lowercase_canonical_sha256(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            plan = store.load_experiment_plan("plan-legacy-model")
            assert plan is not None
            store.connection.execute(
                "UPDATE experiment_plans SET plan_hash=? WHERE plan_id=?",
                (str(plan["plan_hash"]).upper(), "plan-legacy-model"),
            )
            with self.assertRaisesRegex(ValueError, "MODEL_PLAN_HASH_MISMATCH"):
                AutonomousResearchProcessor(
                    store,
                    clock=lambda: NOW,
                )._resolve_rolling_model(strategy)

    def test_plan_hypothesis_must_match_frozen_lineage(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            store.connection.execute(
                "UPDATE research_trials SET payload_json=? WHERE research_trial_id=?",
                (
                    json.dumps(
                        {
                            "research_trial_id": "trial-legacy-model",
                            "hypothesis_id": "hypothesis-other",
                        }
                    ),
                    "trial-legacy-model",
                ),
            )
            with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_MISMATCH"):
                AutonomousResearchProcessor(
                    store,
                    clock=lambda: NOW,
                )._resolve_rolling_model(strategy)

    def test_invalid_embedded_model_does_not_fall_back_to_legacy_plan(self) -> None:
        with _store(self.tmp_path) as store:
            model = {"probability": 2.0}
            strategy = _strategy(
                "sv-invalid-embedded-model",
                candidate_id="candidate-invalid-embedded-model",
                research_trial_id="trial-invalid-embedded-model",
                plan_id="legacy-plan-that-must-not-be-used",
                model_document=model,
                model_hash=_rolling_hash(model),
            )
            with self.assertRaisesRegex(ValueError, "MODEL_INPUT_INVALID"):
                AutonomousResearchProcessor(
                    store,
                    clock=lambda: NOW,
                )._resolve_rolling_model(strategy)

    def test_nested_model_resolution_is_persisted_in_manifest_and_evidence(self) -> None:
        with _store(self.tmp_path) as store:
            model = {"probability": 0.5}
            strategy = _strategy(
                "sv-nested-model-resolution",
                candidate_id="candidate-nested-model-resolution",
                research_trial_id="trial-nested-model-resolution",
                model_document=model,
                model_hash=_rolling_hash(model),
                strategy_document={
                    "version": 1,
                    "market_type": "prediction",
                    "family": "probability_mispricing",
                    "parameters": {"threshold": 0.05},
                    "operations": [],
                },
            )
            resolution = {
                "source_type": "embedded",
                "plan_id": None,
                "model_hash": _rolling_hash(model),
            }
            evidence = AutonomousResearchProcessor(
                store,
                clock=lambda: NOW,
            )._rolling_evidence_record(
                strategy,
                [
                    {
                        "timestamp": NOW.isoformat(),
                        "market_id": "market-nested-model-resolution",
                        "yes_mid": 0.4,
                    }
                ],
                "HISTORICAL",
                7,
                NOW,
                evaluation={
                    "evaluation_kind": "CANONICAL_SIMULATION",
                    "evaluator_invoked": False,
                    "evaluator_completed": False,
                    "evaluation": {
                        "evaluation_kind": "CANONICAL_SIMULATION",
                        "evaluator_invoked": False,
                        "evaluator_completed": False,
                        "model_resolution": resolution,
                    },
                    "portfolio_accounting": {
                        "accounting_available": False,
                        "open_positions": [],
                    },
                },
            )

            self.assertIsNotNone(evidence)
            assert evidence is not None
            self.assertEqual(evidence["input_manifest"]["model_resolution"], resolution)
            self.assertEqual(evidence["evaluation"]["model_resolution"], resolution)

            tampered = dict(evidence)
            tampered_manifest = dict(tampered["input_manifest"])
            tampered_resolution = dict(resolution)
            tampered_resolution["model_hash"] = _rolling_hash({"probability": 0.4})
            tampered_manifest["model_resolution"] = tampered_resolution
            tampered["input_manifest"] = tampered_manifest
            tampered_evaluation = dict(tampered["evaluation"])
            tampered_evaluation["model_resolution"] = tampered_resolution
            tampered["evaluation"] = tampered_evaluation
            tampered_metrics = dict(tampered["metrics"])
            tampered_metrics_evaluation = dict(tampered_metrics["evaluation"])
            tampered_metrics_evaluation["model_resolution"] = tampered_resolution
            tampered_metrics["evaluation"] = tampered_metrics_evaluation
            tampered["metrics"] = tampered_metrics
            with self.assertRaisesRegex(ValueError, "evidence_digest"):
                store.save_strategy_evidence_window(tampered)

    def test_direct_nested_model_resolution_binds_digest_without_shape_promotion(self) -> None:
        model_hash = _rolling_hash({"probability": 0.5})
        resolution = {
            "source_type": "experiment_plan",
            "plan_id": "plan-direct-nested",
            "model_hash": model_hash,
        }
        evidence = RollingEvidence(
            strategy_version_id="sv-direct-nested",
            evidence_window_id="window-direct-nested",
            source_class="HISTORICAL",
            evaluation={"model_resolution": resolution},
        )
        self.assertEqual(evidence.model_resolution, resolution)
        serialized = evidence.as_dict()
        self.assertNotIn("model_resolution", serialized)
        self.assertEqual(serialized["evaluation"]["model_resolution"], resolution)
        self.assertEqual(
            RollingEvidence.from_mapping(serialized).evidence_digest,
            evidence.evidence_digest,
        )
        changed = RollingEvidence(
            strategy_version_id="sv-direct-nested",
            evidence_window_id="window-direct-nested",
            source_class="HISTORICAL",
            evaluation={
                "model_resolution": {
                    **resolution,
                    "model_hash": _rolling_hash({"probability": 0.6}),
                }
            },
        )

        self.assertNotEqual(changed.evidence_digest, evidence.evidence_digest)

    def test_discovery_does_not_write_plan_id_as_source_trial_provenance(self) -> None:
        candidate_id = "candidate-source-trial-plan-only"
        plan_id = "plan-source-trial-plan-only"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "probability_model": "fixed-fixture",
            "operations": [],
            "resolution_aware": True,
            "resolution_inputs": ["settlement"],
        }
        scope = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": ["market-source-trial-plan-only"],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        ).as_dict()
        strategy_payload = {
            "candidate_id": candidate_id,
            "strategy_document": strategy_document,
            "source_trial_id": plan_id,
        }
        lifecycle_payload = {
            "candidate_id": candidate_id,
            "source_trial_id": plan_id,
            "market_scope": scope,
        }
        enrollments: list[Mapping[str, object]] = []

        class DiscoveryStore:
            def list_strategies(self, *, limit: int) -> list[dict[str, object]]:
                return [
                    {
                        "strategy_id": candidate_id,
                        "version": "1",
                        "strategy": strategy_payload,
                    }
                ][:limit]

            def load_candidate_lifecycle(self, *, limit: int) -> list[dict[str, object]]:
                return [
                    {
                        "candidate_id": candidate_id,
                        "stage": "FROZEN",
                        "payload": lifecycle_payload,
                    }
                ][:limit]

            def save_rolling_enrollment(self, record: Mapping[str, object]) -> None:
                enrollments.append(record)

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = DiscoveryStore()
        documents = processor._rolling_strategy_documents()
        self.assertEqual(len(documents), 1, enrollments)
        self.assertEqual(documents[0]["plan_id"], plan_id)

        self.assertIsNone(documents[0]["provenance"]["source_trial_id"])

    def test_future_lineage_writer_roundtrips_source_plan_id_without_trial_alias(self) -> None:
        plan_id = "plan-writer-source-plan"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed-fixture",
            "resolution_aware": True,
            "resolution_inputs": ["settlement"],
        }

        class CaptureStore:
            def __init__(self) -> None:
                self.strategy: Mapping[str, object] | None = None
                self.trial: Mapping[str, object] | None = None

            def save_strategy_version(self, record: Mapping[str, object]) -> None:
                self.strategy = record

            def save_research_trial(self, record: Mapping[str, object]) -> None:
                self.trial = record

            def save_rolling_enrollment(self, _record: Mapping[str, object]) -> None:
                return None

        store = CaptureStore()
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = store
        persisted = processor._rolling_persist_strategy_lineage(
            [
                {
                    "strategy_version_id": "sv-writer-source-plan",
                    "strategy_hash": _rolling_hash(strategy_document),
                    "candidate_id": "candidate-writer-source-plan",
                    "strategy_document": strategy_document,
                    "source_plan_id": plan_id,
                    "provenance": {
                        "source_plan_id": plan_id,
                        "source_trial_id": plan_id,
                    },
                }
            ],
            NOW,
        )
        self.assertEqual(persisted[0]["source_plan_id"], plan_id)
        self.assertIsNotNone(store.strategy)
        self.assertIsNotNone(store.trial)
        assert store.strategy is not None
        assert store.trial is not None
        self.assertEqual(store.strategy["source_plan_id"], plan_id)
        self.assertEqual(store.strategy["provenance"]["source_plan_id"], plan_id)
        self.assertNotIn("source_trial_id", store.strategy["provenance"])
        self.assertEqual(store.trial["payload"]["provenance"]["source_plan_id"], plan_id)
        self.assertNotIn("source_trial_id", store.trial["payload"]["provenance"])

    def test_manifest_only_model_resolution_roundtrips_as_explicit_top_level(self) -> None:
        resolution = {
            "source_type": "experiment_plan",
            "plan_id": "plan-manifest-only",
            "model_hash": _rolling_hash({"probability": 0.5}),
        }
        record = _evidence(
            "sv-manifest-only",
            "window-manifest-only",
            input_manifest={"model_resolution": resolution},
        )
        evidence = RollingEvidence.from_mapping(record)
        serialized = evidence.as_dict()
        self.assertEqual(serialized["model_resolution"], resolution)
        restored = RollingEvidence.from_mapping(serialized)
        self.assertEqual(restored.model_resolution, resolution)
        self.assertEqual(restored.evidence_digest, evidence.evidence_digest)

    def test_discovery_rejects_conflicting_lineage_declarations_before_merge(self) -> None:
        candidate_id = "candidate-conflicting-lineage"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
        }
        strategy_payload = {
            "candidate_id": candidate_id,
            "strategy_document": strategy_document,
            "plan_id": "plan-conflicting-lineage",
            "plan_hash": "sha256:strategy-plan",
        }
        lifecycle_payload = {
            "candidate_id": candidate_id,
            "plan_id": "plan-conflicting-lineage",
            "plan_hash": "sha256:lifecycle-plan",
        }
        enrollments: list[Mapping[str, object]] = []

        class DiscoveryStore:
            def list_strategies(self, *, limit: int) -> list[dict[str, object]]:
                return [
                    {
                        "strategy_id": candidate_id,
                        "version": "1",
                        "strategy": strategy_payload,
                    }
                ][:limit]

            def load_candidate_lifecycle(self, *, limit: int) -> list[dict[str, object]]:
                return [
                    {
                        "candidate_id": candidate_id,
                        "stage": "FROZEN",
                        "payload": lifecycle_payload,
                    }
                ][:limit]

            def save_rolling_enrollment(self, record: Mapping[str, object]) -> None:
                enrollments.append(record)

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = DiscoveryStore()
        self.assertEqual(processor._rolling_strategy_documents(), ())
        self.assertEqual(len(enrollments), 1)
        self.assertEqual(enrollments[0]["reason"], "MODEL_LINEAGE_AMBIGUOUS")


    def test_database_lineage_failure_is_explicit_and_fail_closed(self) -> None:
        class BrokenLineageStore:
            def load_strategy_version(self, _identifier):
                raise sqlite3.OperationalError("database is unavailable")

            def load_research_trial(self, _identifier):
                raise sqlite3.OperationalError("database is unavailable")

            def load_candidate_lifecycle(self, _identifier):
                raise sqlite3.OperationalError("database is unavailable")

        strategy = _strategy(
            "sv-database-lineage-failure",
            candidate_id="candidate-database-lineage-failure",
            research_trial_id="trial-database-lineage-failure",
            strategy_document={"family": "probability_mispricing"},
            model_document=None,
            plan_id="plan-database-lineage-failure",
        )
        with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_LOAD_FAILED"):
            AutonomousResearchProcessor(
                BrokenLineageStore(),
                clock=lambda: NOW,
            )._resolve_rolling_model(strategy)

    def test_prerequisite_fingerprint_tracks_plan_hash_and_hypothesis(self) -> None:
        strategy = _strategy(
            "sv-fingerprint-lineage",
            plan_id="plan-fingerprint",
            plan_hash="sha256:plan-a",
            hypothesis_id="hypothesis-a",
            provenance={
                "plan_id": "plan-fingerprint",
                "plan_hash": "sha256:plan-a",
                "hypothesis_id": "hypothesis-a",
            },
        )
        changed_plan = dict(strategy)
        changed_plan["plan_hash"] = "sha256:plan-b"
        changed_plan["provenance"] = {
            **strategy["provenance"],
            "plan_hash": "sha256:plan-b",
        }
        changed_hypothesis = dict(strategy)
        changed_hypothesis["hypothesis_id"] = "hypothesis-b"
        changed_hypothesis["provenance"] = {
            **strategy["provenance"],
            "hypothesis_id": "hypothesis-b",
        }

        baseline = _rolling_prerequisite_fingerprint(strategy, "REPLAY")
        self.assertNotEqual(
            baseline,
            _rolling_prerequisite_fingerprint(changed_plan, "REPLAY"),
        )
        self.assertNotEqual(
            baseline,
            _rolling_prerequisite_fingerprint(changed_hypothesis, "REPLAY"),
        )

    def test_loaded_plan_hash_alias_must_match_frozen_lineage(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            candidate = store.load_candidate_lifecycle("candidate-legacy-model")
            self.assertIsNotNone(candidate)
            assert candidate is not None
            payload = dict(candidate["payload"])
            payload["experiment_plan_hash"] = "sha256:wrong-plan"
            store.connection.execute(
                "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id=? AND stage=?",
                (json.dumps(payload), "candidate-legacy-model", "FROZEN"),
            )
            with self.assertRaisesRegex(ValueError, "MODEL_LINEAGE_MISMATCH"):
                AutonomousResearchProcessor(
                    store,
                    clock=lambda: NOW,
                )._resolve_rolling_model(strategy)

    def test_prerequisite_fingerprint_tracks_loaded_plan_presence_and_hash(self) -> None:
        with _store(self.tmp_path) as store:
            strategy = self._legacy_model_lineage(
                store,
                model={"probability": 0.5},
            )
            present = _rolling_prerequisite_fingerprint(
                strategy,
                "REPLAY",
                store=store,
            )
            store.connection.execute(
                "DELETE FROM experiment_plans WHERE plan_id=?",
                ("plan-legacy-model",),
            )
            absent = _rolling_prerequisite_fingerprint(
                strategy,
                "REPLAY",
                store=store,
            )
            self.assertNotEqual(present, absent)
