from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import sqlite3
import threading
import time
import unittest
from axiom.dashboard import DashboardData
from axiom.storage import AxiomStore
from unittest.mock import patch


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class DashboardScaleFixtureTests(unittest.TestCase):
    """Exercise bounded overview reads against catalog and activity scale."""

    def test_overview_aggregates_large_fixture_during_concurrent_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-scale.sqlite3"
            store = AxiomStore(str(database_path))
            writer = AxiomStore(str(database_path))
            try:
                self._seed_catalog(store, 2_000)
                self._seed_activity(store, 50_000)

                started = time.perf_counter()
                summary = store.dashboard_overview_summary(activity_limit=8)
                elapsed = time.perf_counter() - started

                self.assertEqual(summary["counts"]["dataset_catalog"], 2_000)
                self.assertEqual(summary["counts"]["collection_cycles"], 50_000)
                self.assertGreaterEqual(summary["logical_rows"]["catalog"], 2_000_000)
                self.assertLessEqual(len(summary["latest_activity"]), 8)
                self.assertLess(elapsed, 1.0)

                writer_started = threading.Event()
                writer_errors: list[BaseException] = []

                def append_activity() -> None:
                    try:
                        writer_started.set()
                        for index in range(200):
                            timestamp = T0 + timedelta(days=2, seconds=index)
                            writer.save_collection_cycle(
                                f"writer-cycle-{index:04d}",
                                "polymarket",
                                {"markets_seen": 100, "markets_successful": 100, "markets_failed": 0},
                                started_at=timestamp,
                                ended_at=timestamp,
                            )
                    except BaseException as exc:  # pragma: no cover - assertion below reports it
                        writer_errors.append(exc)

                thread = threading.Thread(target=append_activity, name="dashboard-scale-writer")
                thread.start()
                self.assertTrue(writer_started.wait(timeout=2.0))
                concurrent_started = time.perf_counter()
                concurrent_summary = store.dashboard_overview_summary(activity_limit=8)
                concurrent_elapsed = time.perf_counter() - concurrent_started
                thread.join(timeout=10.0)

                self.assertFalse(thread.is_alive())
                self.assertEqual(writer_errors, [])
                self.assertGreaterEqual(concurrent_summary["counts"]["collection_cycles"], 50_000)
                self.assertLessEqual(len(concurrent_summary["latest_activity"]), 8)
                self.assertLess(concurrent_elapsed, 1.0)
            finally:
                writer.close()
                store.close()

    def test_overview_forward_evidence_stays_bounded_at_catalog_scale(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-forward-scale.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                self._seed_catalog(store, 2_000)
                dashboard = DashboardData(store=store)

                started = time.perf_counter()
                overview = dashboard.overview_summary()
                elapsed = time.perf_counter() - started

                self.assertIsInstance(overview, dict)
                evidence = overview["forward_evidence"]
                self.assertLessEqual(len(evidence["candidate_bound_markets"]), 100)
                self.assertLessEqual(len(evidence["scheduled"]), 100)
                self.assertLessEqual(len(evidence["fresh"]), 100)
                self.assertLessEqual(len(evidence["stale"]), 100)
                self.assertLessEqual(len(evidence["missing"]), 100)
                self.assertIn(evidence["grade"], {"A", "B", "C", "D", "F", "UNKNOWN"})
                self.assertIsInstance(evidence["reason_code"], str)
                self.assertLess(elapsed, 1.0)
            finally:
                store.close()
    def test_campaign_projection_does_not_deserialize_unrelated_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-campaign-scale.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                store.set_operator_job(
                    "polymarket-research-campaign:bounded",
                    "RUNNING",
                    {
                        "campaign_id": "bounded",
                        "budget_limit": 4,
                        "budget_used": 1,
                    },
                    timestamp=T0,
                )
                # The old dashboard path listed every operator job and
                # deserialized each payload before filtering to campaigns.
                store.set_operator_job(
                    "unrelated-large-job",
                    "DONE",
                    {"unrelated": True, "large": "x" * 60_000},
                    timestamp=T0 + timedelta(seconds=1),
                )
                dashboard = DashboardData(store=store)

                def load_campaign_only(encoded: str) -> object:
                    value = json.loads(encoded)
                    if isinstance(value, dict) and value.get("unrelated"):
                        raise AssertionError("unrelated operator payload was deserialized")
                    return value

                with patch("axiom.storage._load", side_effect=load_campaign_only):
                    progress = dashboard._campaign_progress_projection()

                self.assertEqual(progress["campaign_id"], "bounded")
                self.assertEqual(progress["budget_used"], 1)
                self.assertEqual(progress["budget_remaining"], 3)
            finally:
                store.close()


    def test_overview_skips_deserializing_oversized_worker_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-oversized-worker.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                store.save_worker_state(
                    "polymarket-historical-refresh",
                    "RUNNING",
                    {"unrelated": "x" * 1_100_000},
                    heartbeat_at=T0,
                )
                dashboard = DashboardData(store=store)

                def reject_oversized_load(encoded: str) -> object:
                    if len(encoded) > 1_000_000:
                        raise AssertionError("oversized worker payload was deserialized")
                    return json.loads(encoded)

                with patch("axiom.storage._load", side_effect=reject_oversized_load):
                    overview = dashboard.overview_summary()

                self.assertTrue(overview["available"])
                workers = overview["research_progress"].get("worker_status", {})
                self.assertIsInstance(workers, dict)
            finally:
                store.close()

    def test_rolling_projection_aggregates_nested_fill_counts_before_legacy_aliases(self) -> None:
        row = {
            "strategy_version_id": "scale-strategy",
            "research_trial_id": "scale-trial",
            "candidate_id": "scale-candidate",
            "evidence_window_id": "scale-window",
            "evidence_digest": "scale-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "opening_fills": 99,
            "closing_fills": 99,
            "partial_closing_fills": 99,
            "openings": 99,
            "closings": 99,
            "partial_closings": 99,
            "fills": 99,
            "portfolio_accounting": {
                "accounting_available": True,
                "opening_fills": [{"fill_id": "opening"}],
                "closing_fills": [{"fill_id": "closing"}],
                "partial_closing_fills": [],
                "completed_round_trips": 1,
            },
        }
        dashboard = self._rolling_projection([row])

        measured = dashboard["evidence"]["measured"]
        self.assertEqual(measured["opening_fills"], 1)
        self.assertEqual(measured["closing_fills"], 1)
        self.assertEqual(measured["partial_closing_fills"], 0)
        self.assertEqual(measured["fills"], 2)
        self.assertEqual(measured["exits"], 1)
        self.assertEqual(dashboard["evidence"]["accounting"]["fills"], 2)
        self.assertEqual(dashboard["evidence"]["accounting"]["exits"], 1)

    def test_rolling_projection_keeps_nested_accounting_rows_legacy_without_v2_provenance(
        self,
    ) -> None:
        row = {
            "strategy_version_id": "legacy-accounting-strategy",
            "research_trial_id": "legacy-accounting-trial",
            "candidate_id": "legacy-accounting-candidate",
            "evidence_window_id": "legacy-accounting-window",
            "evidence_digest": "legacy-accounting-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "portfolio_accounting": {
                "accounting_available": True,
                "cash": "100.00",
                "equity": "100.00",
                "opening_fills": 0,
                "closing_fills": 0,
                "partial_closing_fills": 0,
                "completed_round_trips": 0,
            },
        }
        dashboard = self._rolling_projection([row], complete_selection=True)
        projected = dashboard["active_rows"][0]

        self.assertFalse(projected["evaluation"]["evaluation_v2"])
        self.assertIsNone(projected["evaluation_kind"])
        self.assertNotIn("evaluator_invoked_required", projected["blockers"])
        self.assertNotIn("evaluator_completed_required", projected["blockers"])
        self.assertTrue(projected["executable"])

    def test_rolling_projection_blocks_conflicting_duplicate_accounting_projections(self) -> None:
        row = {
            "strategy_version_id": "conflict-strategy",
            "research_trial_id": "conflict-trial",
            "candidate_id": "conflict-candidate",
            "evidence_window_id": "conflict-window",
            "evidence_digest": "conflict-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "portfolio_accounting": {
                "accounting_available": True,
                "cash": "100.00",
            },
            "metrics": {
                "portfolio_accounting": {
                    "accounting_available": False,
                    "cash": "100.00",
                },
            },
            "payload": {
                "portfolio_accounting": {
                    "accounting_available": True,
                    "cash": None,
                },
            },
        }
        dashboard = self._rolling_projection([row])
        projected = dashboard["active_rows"][0]

        self.assertFalse(projected["executable"])
        self.assertFalse(projected["accounting_available"])
        self.assertEqual(projected["accounting_status"], "UNAVAILABLE")
        self.assertEqual(
            projected["portfolio_accounting"]["accounting_projection_mismatch"],
            True,
        )
        self.assertEqual(projected["evidence_status"], "MISMATCH")
        self.assertIsNone(projected["portfolio_accounting"]["cash"])

    def test_rolling_projection_accepts_identical_duplicate_accounting_projections(self) -> None:
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
            "allocated_capital": "100.00",
            "capital_at_risk": "10.00",
            "open_positions": {f"position-{index:03d}" for index in range(96)},
            "opening_fills": 0,
            "closing_fills": 0,
            "partial_closing_fills": 0,
            "completed_round_trips": 0,
        }
        row = {
            "strategy_version_id": "identical-strategy",
            "research_trial_id": "identical-trial",
            "candidate_id": "identical-candidate",
            "evidence_window_id": "identical-window",
            "evidence_digest": "identical-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "evaluation_kind": "CANONICAL_SIMULATION",
            "evaluator_invoked": True,
            "evaluator_completed": True,
            "portfolio_accounting": accounting,
            "metrics": {"portfolio_accounting": dict(accounting)},
            "payload": {"portfolio_accounting": dict(accounting)},
        }
        dashboard = self._rolling_projection([row])
        projected = dashboard["active_rows"][0]

        self.assertTrue(projected["accounting_available"])
        self.assertEqual(projected["accounting_status"], "AVAILABLE")
        self.assertNotIn("accounting_projection_mismatch", projected["blockers"])
        self.assertLessEqual(len(projected["portfolio_accounting"]["open_positions"]), 32)

    def test_rolling_projection_blocks_incomplete_canonical_simulation(self) -> None:
        row = {
            "strategy_version_id": "simulation-strategy",
            "research_trial_id": "simulation-trial",
            "candidate_id": "simulation-candidate",
            "evidence_window_id": "simulation-window",
            "evidence_digest": "simulation-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "evaluation_kind": "CANONICAL_SIMULATION",
            "evaluator_invoked": True,
            "evaluator_completed": False,
            "portfolio_accounting": {
                "accounting_available": True,
                "initial_cash": "100.00",
                "cash": "100.00",
                "equity": "100.00",
                "opening_fills": 0,
                "closing_fills": 0,
                "partial_closing_fills": 0,
                "completed_round_trips": 0,
            },
        }
        dashboard = self._rolling_projection([row])
        projected = dashboard["active_rows"][0]

        self.assertEqual(projected["evaluation_kind"], "CANONICAL_SIMULATION")
        self.assertFalse(projected["evaluation"]["evaluator_completed"])
        self.assertFalse(projected["executable"])
        self.assertEqual(projected["evidence_status"], "MISMATCH")

    def test_rolling_projection_blocks_supersedes_only_without_v2_evaluator_provenance(
        self,
    ) -> None:
        row = {
            "strategy_version_id": "supersedes-strategy",
            "research_trial_id": "supersedes-trial",
            "candidate_id": "supersedes-candidate",
            "evidence_window_id": "supersedes-window",
            "evidence_digest": "supersedes-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "supersedes_evidence_id": "supersedes-predecessor",
            "portfolio_accounting": {
                "accounting_available": True,
                "initial_cash": "100.00",
                "cash": "100.00",
                "equity": "100.00",
                "opening_fills": 0,
                "closing_fills": 0,
                "partial_closing_fills": 0,
                "completed_round_trips": 0,
            },
        }
        dashboard = self._rolling_projection([row], complete_selection=True)
        projected = dashboard["active_rows"][0]

        self.assertTrue(projected["evaluation"]["evaluation_v2"])
        self.assertEqual(projected["evaluation_kind"], "CANONICAL_SIMULATION")
        self.assertIn("evaluator_invoked_required", projected["blockers"])
        self.assertIn("evaluator_completed_required", projected["blockers"])
        self.assertFalse(projected["executable"])
        self.assertEqual(projected["evidence_status"], "MISMATCH")

    def test_rolling_projection_actual_ledger_and_unavailable_accounting_are_publicly_distinct(
        self,
    ) -> None:
        row = {
            "strategy_version_id": "ledger-strategy",
            "research_trial_id": "ledger-trial",
            "candidate_id": "ledger-candidate",
            "evidence_window_id": "ledger-window",
            "evidence_digest": "ledger-digest",
            "source_class": "PAPER",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": False,
            "accounting_partial": True,
            "evaluation_kind": "ACTUAL_LEDGER",
            "portfolio_accounting": {
                "accounting_available": False,
                "initial_cash": "100.00",
                "cash": "90.00",
                "equity": "95.00",
                "realized_pnl": "5.00",
                "unrealized_pnl": "1.00",
                "net_pnl": "6.00",
                "fees": "0.25",
                "costs": "0.50",
                "allocated_capital": "100.00",
                "capital_at_risk": "50.00",
                "open_positions": [{"position_id": str(index)} for index in range(96)],
                "opening_fills": 0,
                "closing_fills": 0,
                "partial_closing_fills": 0,
                "completed_round_trips": 0,
            },
        }
        dashboard = self._rolling_projection([row])
        projected = dashboard["active_rows"][0]
        monetary = dashboard["evidence"]["monetary"]

        self.assertEqual(projected["evaluation_kind"], "ACTUAL_LEDGER")
        self.assertNotIn("evaluator_invoked_required", projected["blockers"])
        self.assertNotIn("evaluator_completed_required", projected["blockers"])
        for field_name in (
            "initial_cash",
            "cash",
            "equity",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
            "allocated_capital",
            "capital_at_risk",
        ):
            self.assertIsNone(projected["portfolio_accounting"][field_name])
            self.assertIsNone(monetary[field_name])
        self.assertLessEqual(len(dashboard["active_rows"]), 10)
        self.assertLessEqual(len(dashboard["evidence"]["attribution"]), 64)
        self.assertLessEqual(
            len(projected["portfolio_accounting"]["open_positions"]),
            32,
        )


    def test_rolling_projection_bounds_evaluator_diagnostics_at_evidence_scale(self) -> None:
        rows = []
        for index in range(96):
            rows.append(
                {
                    "strategy_version_id": "scale-strategy",
                    "research_trial_id": "scale-trial",
                    "candidate_id": "scale-candidate",
                    "evidence_window_id": f"scale-window-{index:03d}",
                    "evidence_digest": "scale-digest",
                    "source_class": "FORWARD_COLLECTED",
                    "available_from": (T0 + timedelta(seconds=index)).isoformat(),
                    "available_through": (
                        T0 + timedelta(days=7, seconds=index)
                    ).isoformat(),
                    "requested_days": 7,
                    "actual_coverage_seconds": 7 * 24 * 60 * 60,
                    "admitted": True,
                    "accounting_complete": True,
                    "accounting_partial": False,
                    "portfolio_accounting": {
                        "accounting_available": True,
                        "opening_fills": 1,
                        "closing_fills": 1,
                        "partial_closing_fills": 0,
                        "completed_round_trips": 1,
                    },
                    "evaluation": {
                        "evaluator_error": "error-" + ("x" * 8_000),
                        "evaluator_prerequisite": "prerequisite-" + ("y" * 8_000),
                    },
                }
            )
        dashboard = self._rolling_projection(rows)
        evidence = dashboard["evidence"]
        evaluation = evidence["evaluation"]

        for diagnostics in (evaluation["errors"], evaluation["prerequisites"]):
            self.assertLessEqual(len(diagnostics), 64)
            self.assertTrue(diagnostics)
            self.assertTrue(all(len(value) <= 512 for value in diagnostics))
        self.assertLessEqual(len(evidence["reasons"]), 64)
        self.assertTrue(all(len(value) <= 512 for value in evidence["reasons"]))
        for row in dashboard["active_rows"]:
            row_evaluation = row["evaluation"]
            self.assertIsInstance(row_evaluation, dict)
            for field_name in ("evaluator_error", "evaluator_prerequisite"):
                value = row_evaluation.get(field_name)
                if value is not None:
                    self.assertLessEqual(len(value), 512)
        for row in dashboard["active_rows"]:
            for field_name in ("evaluation_error", "evaluation_prerequisite"):
                value = row.get(field_name)
                if value is not None:
                    self.assertLessEqual(len(value), 512)

    def test_exact_sql_fallback_hydrates_json_evidence_like_normal_path(self) -> None:
        evaluation = {
            "evaluation_kind": "CANONICAL_SIMULATION",
            "evaluation_run_id": "run-exact",
            "evaluation_version": "v2",
            "evaluator_invoked": True,
            "evaluator_completed": True,
        }
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
            "allocated_capital": "100.00",
            "capital_at_risk": "10.00",
            "open_positions": [],
            "opening_fills": 0,
            "closing_fills": 0,
            "partial_closing_fills": 0,
            "completed_round_trips": 0,
        }
        payload = {
            "evidence_window_id": "exact-window",
            "strategy_version_id": "exact-strategy",
            "research_trial_id": "exact-trial",
            "candidate_id": "exact-candidate",
            "evidence_digest": "exact-digest",
            "source_class": "FORWARD_COLLECTED",
            "available_from": T0.isoformat(),
            "available_through": (T0 + timedelta(days=7)).isoformat(),
            "requested_days": 7,
            "actual_coverage_seconds": 7 * 24 * 60 * 60,
            "admitted": True,
            "accounting_complete": True,
            "accounting_partial": False,
            "evaluation": evaluation,
            "portfolio_accounting": accounting,
        }
        raw_row = {
            **{
                key: payload[key]
                for key in (
                    "evidence_window_id",
                    "strategy_version_id",
                    "research_trial_id",
                    "candidate_id",
                    "source_class",
                    "evidence_digest",
                )
            },
            "payload_json": json.dumps(payload, sort_keys=True),
            "evaluation_json": json.dumps(evaluation, sort_keys=True),
            "portfolio_accounting_json": json.dumps(accounting, sort_keys=True),
            "accounting_available": 1,
            "evaluation_run_id": "run-exact",
            "evaluation_version": "v2",
            "evaluator_invoked": 1,
            "evaluator_completed": 1,
        }
        normal_row = {
            **payload,
            "evaluation": dict(evaluation),
            "portfolio_accounting": dict(accounting),
            "accounting_available": True,
            "evaluation_run_id": "run-exact",
            "evaluation_version": "v2",
            "evaluator_invoked": True,
            "evaluator_completed": True,
        }

        class ExactFallbackStore:
            def __init__(
                self,
                rows: list[dict[str, object]],
                sql_row: dict[str, object] | None,
            ) -> None:
                self._rows = rows
                self.connection = sqlite3.connect(":memory:")
                self.connection.row_factory = sqlite3.Row
                self.connection.execute(
                    "CREATE TABLE strategy_evidence_windows ("
                    "evidence_window_id TEXT, strategy_version_id TEXT, "
                    "research_trial_id TEXT, candidate_id TEXT, source_class TEXT, "
                    "evidence_digest TEXT, payload_json TEXT, evaluation_json TEXT, "
                    "portfolio_accounting_json TEXT, accounting_available INTEGER, "
                    "evaluation_run_id TEXT, evaluation_version TEXT, "
                    "evaluator_invoked INTEGER, evaluator_completed INTEGER)"
                )
                if sql_row is not None:
                    columns = tuple(sql_row)
                    self.connection.execute(
                        "INSERT INTO strategy_evidence_windows("
                        + ",".join(columns)
                        + ") VALUES("
                        + ",".join("?" for _ in columns)
                        + ")",
                        tuple(sql_row[column] for column in columns),
                    )
                    self.connection.commit()

            def load_current_portfolio_selection(self) -> dict[str, object]:
                return {
                    "k": 1,
                    "policy_id": "rolling-policy",
                    "policy_version": "v1",
                    "policy_hash": "rolling-policy-hash",
                    "active_risk_config_id": "risk-config",
                    "active_risk_config_generation": 1,
                    "active_risk_config_hash": "risk-config-hash",
                    "members": [
                        {
                            "status": "ACTIVE",
                            "allocation": "1",
                            "strategy_version_id": "exact-strategy",
                            "research_trial_id": "exact-trial",
                            "candidate_id": "exact-candidate",
                            "evidence_window_id": "exact-window",
                            "evidence_digest": "exact-digest",
                            "source_class": "FORWARD_COLLECTED",
                        }
                    ],
                }

            def load_portfolio_review_state(self) -> dict[str, object]:
                return {}

            def list_worker_states(self, *, limit: int) -> list[dict[str, object]]:
                return []

            def list_strategy_evidence_windows(
                self, *args: object, **kwargs: object
            ) -> list[dict[str, object]]:
                return self._rows

            def load_rolling_evidence_cursor(self) -> dict[str, object]:
                return {}

            def list_rolling_evidence_blockers(
                self, *, limit: int
            ) -> list[dict[str, object]]:
                return []

            def list_portfolio_selections(
                self, *, limit: int
            ) -> list[dict[str, object]]:
                return []

            def get_operator_config(
                self, *args: object, **kwargs: object
            ) -> dict[str, object]:
                if args and args[0] == "rolling_admission_policy_active":
                    return {
                        "policy_id": "rolling-policy",
                        "version": "v1",
                        "config_hash": "rolling-policy-hash",
                        "risk": {
                            "risk_config_id": "risk-config",
                            "risk_config_generation": 1,
                            "risk_config_hash": "risk-config-hash",
                        },
                    }
                return {}
            def load_admission_policy(
                self, *args: object, **kwargs: object
            ) -> dict[str, object]:
                return {
                    "policy_id": "rolling-policy",
                    "version": "v1",
                    "config_hash": "rolling-policy-hash",
                }


            def canary_risk_accounting(self, *, now: datetime) -> dict[str, object]:
                return {}

            def close(self) -> None:
                self.connection.close()

        def project(
            rows: list[dict[str, object]],
            sql_row: dict[str, object] | None,
        ) -> dict[str, object]:
            store = ExactFallbackStore(rows, sql_row)
            try:
                with patch.object(
                    DashboardData,
                    "risk_settings_data",
                    return_value={
                        "config_id": "risk-config",
                        "generation": 1,
                        "config_hash": "risk-config-hash",
                    },
                ):
                    return DashboardData(store=store, clock=lambda: T0).rolling_portfolio_data()
            finally:
                store.close()

        normal = project([normal_row], None)
        exact = project([], raw_row)
        normal_active = normal["active_rows"][0]
        exact_active = exact["active_rows"][0]

        self.assertEqual(normal_active["evidence_status"], "AVAILABLE")
        self.assertEqual(exact_active["evidence_status"], "AVAILABLE")
        self.assertTrue(exact_active["executable"])
        self.assertEqual(exact_active["evaluation"], normal_active["evaluation"])
        self.assertEqual(
            exact_active["portfolio_accounting"],
            normal_active["portfolio_accounting"],
        )
        self.assertEqual(exact_active["evidence_digest"], "exact-digest")

    @staticmethod
    def _rolling_projection(
        rows: list[dict[str, object]],
        *,
        complete_selection: bool = False,
    ) -> dict[str, object]:
        first = rows[0]
        member = {
            "status": "ACTIVE",
            "allocation": "1",
            "strategy_version_id": first["strategy_version_id"],
            "research_trial_id": first["research_trial_id"],
            "candidate_id": first["candidate_id"],
            "evidence_window_id": first["evidence_window_id"],
            "evidence_digest": first["evidence_digest"],
            "source_class": first["source_class"],
        }

        class RollingStore:
            def load_current_portfolio_selection(self) -> dict[str, object]:
                selection: dict[str, object] = {"k": 1, "members": [member]}
                if complete_selection:
                    selection.update(
                        {
                            "policy_id": "rolling-policy",
                            "policy_version": "v1",
                            "policy_hash": "rolling-policy-hash",
                            "active_risk_config_id": "risk-config",
                            "active_risk_config_generation": 1,
                            "active_risk_config_hash": "risk-config-hash",
                        }
                    )
                return selection

            def load_portfolio_review_state(self) -> dict[str, object]:
                return {}

            def list_worker_states(self, *, limit: int) -> list[dict[str, object]]:
                return []

            def list_strategy_evidence_windows(
                self, *, limit: int
            ) -> list[dict[str, object]]:
                return rows

            def get_strategy_evidence_window(
                self, *args: object, **kwargs: object
            ) -> dict[str, object]:
                return rows[0]

            def load_rolling_evidence_cursor(self) -> dict[str, object]:
                return {}

            def list_rolling_evidence_blockers(
                self, *, limit: int
            ) -> list[dict[str, object]]:
                return []

            def list_portfolio_selections(
                self, *, limit: int
            ) -> list[dict[str, object]]:
                return []

            def get_operator_config(
                self, *args: object, **kwargs: object
            ) -> dict[str, object]:
                if complete_selection and args and args[0] == "rolling_admission_policy_active":
                    return {
                        "policy_id": "rolling-policy",
                        "version": "v1",
                        "config_hash": "rolling-policy-hash",
                        "risk": {
                            "risk_config_id": "risk-config",
                            "risk_config_generation": 1,
                            "risk_config_hash": "risk-config-hash",
                        },
                    }
                return {}

            def load_admission_policy(
                self, *args: object, **kwargs: object
            ) -> dict[str, object]:
                if complete_selection:
                    return {
                        "policy_id": "rolling-policy",
                        "version": "v1",
                        "config_hash": "rolling-policy-hash",
                    }
                return {}

            def canary_risk_accounting(self, *, now: datetime) -> dict[str, object]:
                return {}

        with patch.object(
            DashboardData,
            "risk_settings_data",
            return_value=(
                {
                    "config_id": "risk-config",
                    "generation": 1,
                    "config_hash": "risk-config-hash",
                }
                if complete_selection
                else {}
            ),
        ):
            return DashboardData(
                store=RollingStore(),
                clock=lambda: T0,
            ).rolling_portfolio_data()

    @staticmethod
    def _seed_catalog(store: AxiomStore, count: int) -> None:
        rows = []
        for index in range(count):
            timestamp = T0 + timedelta(seconds=index)
            rows.append(
                (
                    f"scale-dataset-{index:04d}",
                    "scale-v1",
                    "fixture",
                    f"instrument-{index:04d}",
                    "CRYPTO_SPOT" if index % 2 else "PREDICTION",
                    "1h",
                    timestamp.isoformat(),
                    (timestamp + timedelta(hours=1)).isoformat(),
                    1_500 + index,
                    0.99,
                    "[]",
                    "HIGH",
                    "HISTORICAL" if index % 2 else "FORWARD_COLLECTED",
                    f"scale-snapshot-{index:04d}",
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                    json.dumps({"fixture": "dashboard-scale"}),
                )
            )
        with store.transaction():
            store.connection.executemany(
                "INSERT INTO dataset_catalog(" 
                "dataset_id,dataset_version,provider,instrument,market_type,timeframe," 
                "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json," 
                "quality,source_type,snapshot_id,created_at,updated_at,metadata_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )

    @staticmethod
    def _seed_activity(store: AxiomStore, count: int) -> None:
        rows = []
        for index in range(count):
            timestamp = T0 + timedelta(seconds=index)
            payload = json.dumps(
                {
                    "markets_seen": 100,
                    "markets_attempted": 100,
                    "markets_successful": 100,
                    "markets_failed": 0,
                }
            )
            rows.append(
                (
                    f"scale-cycle-{index:05d}",
                    "polymarket",
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                    payload,
                    timestamp.isoformat(),
                )
            )
        with store.transaction():
            store.connection.executemany(
                "INSERT INTO collection_cycles(cycle_id,collector_name,started_at,ended_at,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )


if __name__ == "__main__":
    unittest.main()
