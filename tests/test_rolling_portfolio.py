from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from axiom.rolling_portfolio import (
    RollingEvidence,
    RollingAdmissionPolicy,
    default_rolling_admission_policy,
    evaluate_rolling_selection,
)
from axiom.storage import AxiomStore


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
        "created_at": NOW.isoformat(),
        "payload": {"family": "mean-reversion", "parameters": {"lookback": 7}},
    }
    record.update(overrides)
    return record


def _trial(
    strategy_version_id: str = "sv-alpha",
    research_trial_id: str = "trial-alpha-1",
    **overrides: object,
) -> dict[str, object]:
    record: dict[str, object] = {
        "research_trial_id": research_trial_id,
        "strategy_version_id": strategy_version_id,
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
        "available_from": available_from.isoformat(),
        "available_through": through.isoformat(),
        "requested_days": days,
        "actual_coverage_seconds": actual_days * 86400,
        "source_class": "HISTORICAL",
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
        "evidence_digest": f"sha256:{evidence_window_id}",
        "overlap_key": overlap_key or f"overlap:{strategy_version_id}",
        "hard_failure": hard_failure,
        "failure_reason": "BROKEN_EXECUTION_FEASIBILITY" if hard_failure else None,
    }
    record.update(overrides)
    return record


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
    position_management_state: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "portfolio_selection_id": selection_id,
        "strategy_version_id": strategy_version_id,
        "allocation": allocation,
        "status": status,
        "score": score,
        "reason": reason,
        "evidence_window_id": evidence_window_id or f"window-{strategy_version_id}-7",
        "overlap_key": overlap_key or f"overlap:{strategy_version_id}",
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
            "members": [_member("sv-incumbent", selection_id="current-selection", score="0.80")],
        }
        evidence = [
            _evidence("sv-incumbent", "w-incumbent", score="0.55"),
            _evidence("sv-challenger", "w-challenger", score="0.60"),
        ]
    
        decision = evaluate_rolling_selection(policy, evidence, current, NOW + timedelta(hours=1))
        ids = [str(_member_value(member, "strategy_version_id")) for member in _decision_members(decision)]
        self.assertEqual(ids, ['sv-incumbent'])
        self.assertIn(str(_member_value(_decision_members(decision)[0], 'status')), {'ACTIVE', 'RETAINED'})
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
                _selection("selection-2", selected_at=NOW + timedelta(days=1)),
                [_member("sv-beta", selection_id="selection-2", status="PAPER", allocation="0", evidence_window_id="window-beta-7")],
            )
            current = restarted.load_current_portfolio_selection()
            history = restarted.list_portfolio_selections(limit=10)
            self.assertEqual(current['portfolio_selection_id'], 'selection-2')
            self.assertEqual({row['portfolio_selection_id'] for row in history}, {'selection-1', 'selection-2'})
            self.assertEqual(len(history), 2)
    
    
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
                store.save_strategy_evidence_window(_evidence(strategy_id, window_id))

            committed = store.commit_portfolio_selection(
                _selection("selection-observations", k=2),
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
            actual_coverage_seconds=7 * 86400 + 1,
        )
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

        self.assertIn(str(_member_value(member, "status")), {"ACTIVE", "RETAINED"})
        self.assertEqual(Decimal(str(_member_value(member, "allocation"))), Decimal("3.00"))
        self.assertEqual(state["position_id"], "position-1")
        self.assertEqual(state["open_quantity"], Decimal("2.5"))
        self.assertEqual(state["risk"]["stop_fraction"], Decimal("0.25"))

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
            with self.assertRaises(ValueError):
                store.save_strategy_evidence_window(usd)

            oversized = _evidence(
                "sv-drawdown",
                "w-drawdown-oversized",
                drawdown="1.01",
            )
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
            store.save_strategy_evidence_window(_evidence("sv-rolling", "w-rolling"))
            store.commit_portfolio_selection(
                _selection("selection-rolling"),
                [_member("sv-rolling", selection_id="selection-rolling", evidence_window_id="w-rolling")],
            )
            after = store.load_experiment("finite-campaign-1")
    
            self.assertEqual(after, before)
            self.assertEqual(after['status'], 'COMPLETED')
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM experiments WHERE experiment_id=?', ('finite-campaign-1',)).fetchone()[0], 1)
            self.assertEqual(store.load_current_portfolio_selection()['portfolio_selection_id'], 'selection-rolling')
