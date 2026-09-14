from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from axiom.rolling_portfolio import (
    MAX_REASON_LENGTH,
    RollingEvidence,
    RollingAdmissionPolicy,
    default_rolling_admission_policy,
    evaluate_rolling_selection,
)
from axiom.experiment_plan import normalize_market_scope
from axiom.storage import AxiomStore
from axiom.autonomous import (
    AutonomousResearchProcessor,
    _rolling_cursor_index,
    _rolling_cursor_record,
    _rolling_inject_source_binding,
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

    def test_fixed_holding_exit_uses_canonical_pnl_for_wins_and_losses(self) -> None:
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        rows = [
            {
                "snapshot_id": "snapshot-accounting",
                "timestamp": NOW.isoformat(),
                "order_book": {"bids": [], "asks": []},
                "_rolling_lineage_proven": True,
            }
        ]
        strategy = {"strategy_document": {"family": "fixture"}}
        for final_equity, expected_pnl in (("102.00", "2.00"), ("98.00", "-2.00")):
            result = SimpleNamespace(
                fills=(
                    SimpleNamespace(
                        market_id="market-fixed-holding",
                        symbol="market-fixed-holding",
                        quantity=Decimal("10"),
                        price=Decimal("0.40"),
                        fees=Decimal("0.10"),
                        slippage=Decimal("0.02"),
                        metadata={"execution_kind": "entry"},
                    ),
                    SimpleNamespace(
                        market_id="market-fixed-holding",
                        symbol="market-fixed-holding",
                        quantity=Decimal("10"),
                        price=Decimal("0.60"),
                        fees=Decimal("0.10"),
                        slippage=Decimal("0.02"),
                        metadata={"execution_kind": "exit"},
                    ),
                ),
                outcomes={"market-fixed-holding": "RESOLVED_YES"},
                metrics={
                    "initial_equity": Decimal("100.00"),
                    "final_equity": Decimal(final_equity),
                    "portfolio": {
                        "realized_pnl": Decimal(expected_pnl),
                        "unrealized_pnl": Decimal("0"),
                    },
                },
                equity_curve=(),
                unresolved=(),
                research_quality=None,
            )
            with self.subTest(final_equity=final_equity), patch(
                "axiom.autonomous.load_strategy", return_value=object()
            ), patch(
                "axiom.autonomous.run_prediction_research_mode", return_value=result
            ):
                evaluation = processor._rolling_canonical_evaluation(
                    strategy, rows, "HISTORICAL"
                )
            self.assertIsNotNone(evaluation)
            assert evaluation is not None
            self.assertEqual(evaluation["realized_pnl"], Decimal(expected_pnl))
            self.assertEqual(evaluation["unrealized_pnl"], Decimal("0"))
            self.assertEqual(evaluation["fills"], 2)
            self.assertEqual(evaluation["capital_at_risk"], Decimal("4.00"))

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



    def test_partial_accounting_reports_available_rows_but_cannot_admit(self) -> None:
        strategy = _strategy("sv-partial", research_trial_id="trial-partial")
        accounting = {
            "_available_from": NOW - timedelta(days=7),
            "_available_through": NOW,
            "allocated_capital": "10",
            "allocated_capital_net_return": "1",
            "realized_pnl": "1",
            "unrealized_pnl": "0",
            "fees": "0",
            "costs": "0",
            "drawdown": "0",
            "completed_outcomes": 1,
            "reliability": "1",
        }
        rows = [
            {"timestamp": NOW.isoformat(), "_rolling_accounting": accounting},
            {
                "timestamp": NOW.isoformat(),
                "_rolling_accounting_rejection": "SOURCE_BINDING_UNPROVEN",
            },
        ]
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        record = processor._rolling_evidence_record(
            strategy,
            rows,
            "HISTORICAL",
            7,
            NOW,
            evaluation={
                "available_from": (NOW - timedelta(days=7)).isoformat(),
                "available_through": NOW.isoformat(),
                "accounting_available": False,
                "accounting_unavailable_reason": "SOURCE_BINDING_UNPROVEN",
            },
        )
        self.assertEqual(record["requested_rows"], 2)
        self.assertEqual(record["available_rows"], 1)
        self.assertEqual(record["evaluated_rows"], 1)
        self.assertTrue(record["source_digest"])
        self.assertTrue(record["accounting_digest"])
        self.assertFalse(record["accounting_available"])
        self.assertFalse(record["accounting_complete"])
        self.assertTrue(record["accounting_partial"])
        self.assertFalse(record["admitted"])
        self.assertEqual(record["admission_reasons"], ["ACCOUNTING_UNAVAILABLE"])

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

    def test_producer_persists_canonical_provenance_and_row_counts(self) -> None:
        strategy = _strategy("sv-producer", research_trial_id="trial-producer")
        accounting = {
            "_available_from": NOW - timedelta(days=7),
            "_available_through": NOW,
            "allocated_capital": "10",
            "allocated_capital_net_return": "1",
            "realized_pnl": "1",
            "unrealized_pnl": "0",
            "fees": "0",
            "costs": "0",
            "drawdown": "0",
            "completed_outcomes": 1,
            "reliability": "1",
        }
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        record = processor._rolling_evidence_record(
            strategy,
            [
                {"timestamp": NOW.isoformat(), "_rolling_accounting": accounting},
                {"timestamp": NOW.isoformat(), "_rolling_accounting": accounting},
            ],
            "PAPER",
            7,
            NOW,
            evaluation={
                "available_from": (NOW - timedelta(days=7)).isoformat(),
                "available_through": NOW.isoformat(),
                "source_digest": "sha256:canonical-source",
                "accounting_digest": "sha256:canonical-accounting",
                "accounting_available": True,
                "accounting_complete": True,
                "accounting_partial": False,
                "requested_rows": 2,
                "available_rows": 2,
                "evaluated_rows": 2,
            },
        )
        assert record is not None
        self.assertEqual(record["source_digest"], "sha256:canonical-source")
        self.assertEqual(record["accounting_digest"], "sha256:canonical-accounting")
        self.assertTrue(record["accounting_available"])
        self.assertTrue(record["accounting_complete"])
        self.assertFalse(record["accounting_partial"])
        self.assertEqual(
            (record["requested_rows"], record["available_rows"], record["evaluated_rows"]),
            (2, 2, 2),
        )

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
