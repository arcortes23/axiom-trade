from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import patch

from axiom.storage import AxiomStore

UTC = timezone.utc
THROUGH = datetime(2026, 1, 31, 12, tzinfo=UTC)


def _strategy(strategy_version_id: str = "storage-v2") -> dict[str, object]:
    return {
        "strategy_version_id": strategy_version_id,
        "strategy_id": strategy_version_id,
        "version": "1",
        "strategy_hash": f"sha256:{strategy_version_id}",
        "config_hash": f"config:{strategy_version_id}",
        "created_at": THROUGH.isoformat(),
    }


def _evidence(
    *,
    strategy_version_id: str = "storage-v2",
    evidence_window_id: str = "window-storage-v2",
    evaluation_kind: str | None = "CANONICAL_SIMULATION",
    evaluator_invoked: bool = True,
    evaluator_completed: bool = True,
    root_evaluation_kind: object = ..., 
) -> dict[str, object]:
    accounting = {
        "accounting_available": True,
        "accounting_complete": True,
        "accounting_partial": False,
        "initial_cash": "100.00",
        "cash": "97.25",
        "equity": "101.50",
        "realized_pnl": "2.00",
        "unrealized_pnl": "1.50",
        "net_pnl": "3.50",
        "fees": "0.25",
        "costs": "0.50",
        "open_positions": ["market-a"],
        "opening_fills": 2,
        "closing_fills": 1,
        "partial_closing_fills": 0,
        "completed_round_trips": 1,
    }
    evaluation = {
        "evaluation_run_id": f"run-{evidence_window_id}",
        "evaluation_version": "rolling-evaluation:v2",
        "evaluation_kind": evaluation_kind,
        "evaluator_invoked": evaluator_invoked,
        "evaluator_completed": evaluator_completed,
    }
    record: dict[str, object] = {
        "strategy_version_id": strategy_version_id,
        "evidence_window_id": evidence_window_id,
        "available_from": (THROUGH - timedelta(days=7)).isoformat(),
        "available_through": THROUGH.isoformat(),
        "requested_days": 7,
        "actual_coverage_seconds": 7 * 24 * 60 * 60,
        "observation_completeness": "1",
        "source_class": "HISTORICAL",
        "paper_sizing_assumptions": {"paper_sizing": "10.00"},
        "paper_fee_assumptions": {"fee_assumption": "0.01"},
        "paper_slippage_assumptions": {"slippage_assumption": "0.01"},
        "allocated_capital_net_return": "3.50",
        "drawdown": "0.03",
        "completed_outcomes": 1,
        "reliability": "0.90",
        "portfolio_accounting": accounting,
        "evaluation": evaluation,
    }
    if root_evaluation_kind is not ...:
        record["evaluation_kind"] = root_evaluation_kind
    return record


class StorageEvidenceV2Tests(unittest.TestCase):
    def _save(self, store: AxiomStore, record: dict[str, object]) -> None:
        store.save_strategy_version(_strategy(str(record["strategy_version_id"])))
        store.save_strategy_evidence_window(record)
    def test_dataset_payload_projection_checks_bound_before_decode(self) -> None:
        with AxiomStore(":memory:") as store:
            payload = json.dumps([{"value": "bounded"}], separators=(",", ":"))
            store.connection.execute(
                "INSERT INTO datasets(dataset_id,version,payload_json,metadata_json,quality,created_at) "
                "VALUES (?,?,?,?,?,?)",
                ("projection", "v1", payload, "{}", "HIGH", THROUGH.isoformat()),
            )
            store.connection.commit()
            with patch("axiom.storage._load", side_effect=AssertionError("decoded")) as load:
                with self.assertRaisesRegex(ValueError, "requested byte bound"):
                    store.load_dataset_payload_projection(
                        "projection",
                        "v1",
                        max_payload_bytes=len(payload.encode("utf-8")) - 1,
                    )
            load.assert_not_called()
            self.assertEqual(
                store.load_dataset_payload_projection(
                    "projection",
                    "v1",
                    max_payload_bytes=len(payload.encode("utf-8")),
                ),
                [{"value": "bounded"}],
            )


    def test_hydrated_v2_evidence_can_be_resaved_idempotently(self) -> None:
        with AxiomStore(":memory:") as store:
            self._save(store, _evidence())

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            identity = {
                field_name: loaded[field_name]
                for field_name in (
                    "evidence_window_id",
                    "strategy_version_id",
                    "research_trial_id",
                    "candidate_id",
                )
            }
            digest = loaded["evidence_digest"]

            store.save_strategy_evidence_window(loaded)

            restored = store.list_strategy_evidence_windows("storage-v2")[0]
            self.assertEqual(
                {field_name: restored[field_name] for field_name in identity},
                identity,
            )
            self.assertEqual(restored["evidence_digest"], digest)

    def test_actual_ledger_false_false_round_trips_complete_accounting(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(
                evaluation_kind="ACTUAL_LEDGER",
                evaluator_invoked=False,
                evaluator_completed=False,
            )
            self._save(store, record)

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            self.assertEqual(loaded["evaluation_kind"], "ACTUAL_LEDGER")
            self.assertFalse(loaded["evaluator_invoked"])
            self.assertFalse(loaded["evaluator_completed"])
            self.assertTrue(loaded["accounting_available"])
            self.assertTrue(loaded["accounting_complete"])
            self.assertFalse(loaded["accounting_partial"])
            accounting = loaded["portfolio_accounting"]
            self.assertIsInstance(accounting, dict)
            self.assertEqual(accounting["cash"], "97.25")
            self.assertEqual(accounting["open_positions"], ["market-a"])
            self.assertEqual(accounting["completed_round_trips"], 1)

    def test_actual_ledger_true_evaluator_flags_are_rejected(self) -> None:
        for field_name in ("evaluator_invoked", "evaluator_completed"):
            with self.subTest(field_name=field_name), AxiomStore(":memory:") as store:
                record = _evidence(
                    evaluation_kind="ACTUAL_LEDGER",
                    evaluator_invoked=False,
                    evaluator_completed=False,
                )
                record["evaluation"] = {
                    **record["evaluation"],
                    field_name: True,
                }
                self._save_strategy_only(store, record)
                with self.assertRaisesRegex(ValueError, "ACTUAL_LEDGER evaluator flags"):
                    store.save_strategy_evidence_window(record)

    def test_simulation_incomplete_evaluator_fails_closed(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(
                evaluator_invoked=True,
                evaluator_completed=False,
            )
            self._save(store, record)

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            self.assertEqual(loaded["evaluation_kind"], "CANONICAL_SIMULATION")
            self.assertFalse(loaded["accounting_available"])
            self.assertFalse(loaded["accounting_complete"])
            self.assertTrue(loaded["accounting_partial"])
            self.assertIsNone(loaded["realized_pnl"])
            accounting = loaded["portfolio_accounting"]
            self.assertIsInstance(accounting, dict)
            self.assertIsNone(accounting["cash"])


    def test_unavailable_v2_accounting_is_canonical_before_persistence_and_resave(
        self,
    ) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(evaluator_completed=False)
            record["metrics"] = {
                "portfolio_accounting": dict(record["portfolio_accounting"]),
            }
            self._save(store, record)

            row = store.connection.execute(
                "SELECT portfolio_accounting_json,evidence_digest "
                "FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (record["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            persisted_accounting = json.loads(row["portfolio_accounting_json"])
            for field_name in (
                "initial_cash",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "net_pnl",
                "fees",
                "costs",
            ):
                self.assertIsNone(persisted_accounting[field_name])
            self.assertNotIn("97.25", row["portfolio_accounting_json"])
            self.assertEqual(persisted_accounting["opening_fills"], 2)
            self.assertEqual(persisted_accounting["closing_fills"], 1)
            self.assertEqual(persisted_accounting["completed_round_trips"], 1)
            self.assertEqual(persisted_accounting["open_positions"], ["market-a"])

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            accounting = loaded["portfolio_accounting"]
            self.assertIsInstance(accounting, dict)
            self.assertIsNone(accounting["cash"])
            self.assertEqual(accounting["opening_fills"], 2)
            self.assertEqual(accounting["open_positions"], ["market-a"])

            store.save_strategy_evidence_window(loaded)
            restored_row = store.connection.execute(
                "SELECT portfolio_accounting_json,evidence_digest "
                "FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (record["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(restored_row)
            assert restored_row is not None
            self.assertEqual(
                restored_row["portfolio_accounting_json"],
                row["portfolio_accounting_json"],
            )
            self.assertEqual(restored_row["evidence_digest"], row["evidence_digest"])

            mutated = dict(loaded)
            mutated["portfolio_accounting"] = {
                **accounting,
                "cash": "999.00",
            }
            with self.assertRaisesRegex(ValueError, "conflict"):
                store.save_strategy_evidence_window(mutated)

    def test_root_null_nested_status_values_are_rejected(self) -> None:
        status_projections = (
            ("accounting_available", "portfolio_accounting"),
            ("accounting_complete", "portfolio_accounting"),
            ("accounting_partial", "portfolio_accounting"),
            ("evaluator_invoked", "evaluation"),
            ("evaluator_completed", "evaluation"),
        )
        for field_name, nested_name in status_projections:
            with self.subTest(field_name=field_name), AxiomStore(":memory:") as store:
                record = _evidence(
                    evidence_window_id=f"window-status-conflict-{field_name}",
                )
                record[field_name] = None
                nested = dict(record[nested_name])
                nested[field_name] = True
                record[nested_name] = nested
                self._save_strategy_only(store, record)

                with self.assertRaisesRegex(ValueError, f"{field_name} conflicts"):
                    store.save_strategy_evidence_window(record)

    def test_supersedes_only_evidence_is_v2_and_canonicalizes_unavailable_accounting(
        self,
    ) -> None:
        with AxiomStore(":memory:") as store:
            predecessor = _evidence(
                strategy_version_id="storage-v2-link-only",
                evidence_window_id="window-link-only-predecessor",
            )
            predecessor.pop("evaluation")
            self._save(store, predecessor)
            predecessor_row = store.connection.execute(
                "SELECT evidence_digest FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (predecessor["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(predecessor_row)
            assert predecessor_row is not None

            linked = _evidence(
                strategy_version_id="storage-v2-link-only",
                evidence_window_id="window-link-only-correction",
            )
            linked.pop("evaluation")
            linked["supersedes_evidence_id"] = predecessor["evidence_window_id"]
            self._save(store, linked)

            linked_row = store.connection.execute(
                "SELECT payload_json,portfolio_accounting_json,evidence_digest "
                "FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (linked["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(linked_row)
            assert linked_row is not None
            self.assertNotEqual(
                linked_row["evidence_digest"],
                predecessor_row["evidence_digest"],
            )
            persisted_payload = json.loads(linked_row["payload_json"])
            persisted_accounting = json.loads(linked_row["portfolio_accounting_json"])
            for field_name in (
                "allocated_capital_net_return",
                "initial_cash",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "net_pnl",
                "fees",
                "costs",
            ):
                self.assertIsNone(persisted_payload[field_name])
                if field_name != "allocated_capital_net_return":
                    self.assertIsNone(persisted_accounting[field_name])
            self.assertEqual(
                persisted_payload["supersedes_evidence_id"],
                predecessor["evidence_window_id"],
            )

            loaded = store.list_strategy_evidence_windows("storage-v2-link-only")
            self.assertEqual(len(loaded), 2)
            correction = next(
                item
                for item in loaded
                if item["evidence_window_id"] == linked["evidence_window_id"]
            )
            self.assertFalse(correction["evaluation_legacy"])
            self.assertEqual(correction["evaluation_kind"], "CANONICAL_SIMULATION")
            self.assertEqual(
                correction["supersedes_evidence_id"],
                predecessor["evidence_window_id"],
            )
            self.assertIsNone(correction["allocated_capital_net_return"])
            self.assertIsNone(correction["portfolio_accounting"]["cash"])

            altered_link = dict(linked)
            altered_link["supersedes_evidence_id"] = "window-link-only-other"
            altered_link["evidence_digest"] = linked_row["evidence_digest"]
            with self.assertRaisesRegex(ValueError, "does not match canonical evidence"):
                store.save_strategy_evidence_window(altered_link)

            store.save_strategy_evidence_window(correction)
            restored_row = store.connection.execute(
                "SELECT payload_json,portfolio_accounting_json,evidence_digest "
                "FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (linked["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(restored_row)
            assert restored_row is not None
            self.assertEqual(restored_row["payload_json"], linked_row["payload_json"])
            self.assertEqual(
                restored_row["portfolio_accounting_json"],
                linked_row["portfolio_accounting_json"],
            )
            self.assertEqual(restored_row["evidence_digest"], linked_row["evidence_digest"])

    def test_root_supersedes_id_cannot_override_nested_null(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence()
            record["supersedes_evidence_id"] = "predecessor"
            record["evaluation"] = {
                **record["evaluation"],
                "supersedes_evidence_id": None,
            }
            self._save_strategy_only(store, record)

            with self.assertRaisesRegex(ValueError, "supersedes_evidence_id conflicts"):
                store.save_strategy_evidence_window(record)

    def test_hydration_rejects_persisted_root_nested_supersedes_null_mismatch(
        self,
    ) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence()
            self._save(store, record)

            row = store.connection.execute(
                "SELECT payload_json,evaluation_json "
                "FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (record["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            payload = json.loads(row["payload_json"])
            evaluation = json.loads(row["evaluation_json"])
            payload["supersedes_evidence_id"] = "predecessor"
            evaluation["supersedes_evidence_id"] = None
            store.connection.execute(
                "UPDATE strategy_evidence_windows "
                "SET supersedes_evidence_id=?,payload_json=?,evaluation_json=? "
                "WHERE evidence_window_id=?",
                (
                    "predecessor",
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    json.dumps(evaluation, sort_keys=True, separators=(",", ":")),
                    record["evidence_window_id"],
                ),
            )
            store.connection.commit()

            with self.assertRaisesRegex(ValueError, "supersedes_evidence_id conflicts"):
                store.list_strategy_evidence_windows("storage-v2")


    def test_null_root_kind_and_nested_kind_conflict(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(
                evaluation_kind="ACTUAL_LEDGER",
                evaluator_invoked=False,
                evaluator_completed=False,
                root_evaluation_kind=None,
            )
            self._save_strategy_only(store, record)
            with self.assertRaisesRegex(ValueError, "evaluation_kind conflicts"):
                store.save_strategy_evidence_window(record)

    def test_hydration_rejects_sql_evaluator_status_conflicts(self) -> None:
        for field_name in ("evaluator_invoked", "evaluator_completed"):
            with self.subTest(field_name=field_name), AxiomStore(":memory:") as store:
                record = _evidence(
                    evidence_window_id=f"window-sql-evaluator-conflict-{field_name}",
                )
                self._save(store, record)
                store.connection.execute(
                    f"UPDATE strategy_evidence_windows SET {field_name}=? "
                    "WHERE evidence_window_id=?",
                    (0, record["evidence_window_id"]),
                )
                store.connection.commit()

                with self.assertRaisesRegex(ValueError, f"{field_name} conflicts"):
                    store.list_strategy_evidence_windows("storage-v2")

    def test_hydration_rejects_sql_accounting_status_conflict(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(
                evidence_window_id="window-sql-accounting-conflict",
            )
            self._save(store, record)
            store.connection.execute(
                "UPDATE strategy_evidence_windows SET accounting_available=0 "
                "WHERE evidence_window_id=?",
                (record["evidence_window_id"],),
            )
            store.connection.commit()

            with self.assertRaisesRegex(ValueError, "accounting_available conflicts"):
                store.list_strategy_evidence_windows("storage-v2")

    def test_hydration_rejects_sql_evaluation_identity_conflicts(self) -> None:
        for field_name in ("evaluation_run_id", "evaluation_version"):
            with self.subTest(field_name=field_name), AxiomStore(":memory:") as store:
                record = _evidence(
                    evidence_window_id=f"window-sql-identity-conflict-{field_name}",
                )
                self._save(store, record)
                store.connection.execute(
                    f"UPDATE strategy_evidence_windows SET {field_name}=? "
                    "WHERE evidence_window_id=?",
                    (f"other-{field_name}", record["evidence_window_id"]),
                )
                store.connection.commit()

                with self.assertRaisesRegex(ValueError, f"{field_name} conflicts"):
                    store.list_strategy_evidence_windows("storage-v2")

    def test_hydration_accepts_compatible_json_projections(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(evidence_window_id="window-compatible-projections")
            self._save(store, record)
            row = store.connection.execute(
                "SELECT payload_json FROM strategy_evidence_windows "
                "WHERE evidence_window_id=?",
                (record["evidence_window_id"],),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            payload = json.loads(row["payload_json"])
            payload["metrics"] = {
                "evaluation": dict(payload["evaluation"]),
                "portfolio_accounting": dict(payload["portfolio_accounting"]),
            }
            store.connection.execute(
                "UPDATE strategy_evidence_windows SET payload_json=? "
                "WHERE evidence_window_id=?",
                (
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    record["evidence_window_id"],
                ),
            )
            store.connection.commit()

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            self.assertEqual(loaded["evaluator_invoked"], True)
            self.assertEqual(loaded["evaluator_completed"], True)
            self.assertEqual(loaded["accounting_available"], True)
            self.assertEqual(loaded["evaluation_run_id"], record["evaluation"]["evaluation_run_id"])
            self.assertEqual(
                loaded["evaluation_version"],
                record["evaluation"]["evaluation_version"],
            )

    def test_evaluation_kind_only_rows_are_not_legacy(self) -> None:
        with AxiomStore(":memory:") as store:
            record = _evidence(
                evidence_window_id="window-kind-only-v2",
                evaluation_kind="ACTUAL_LEDGER",
                evaluator_invoked=False,
                evaluator_completed=False,
            )
            record["evaluation"] = {
                "evaluation_kind": "ACTUAL_LEDGER",
                "evaluator_invoked": False,
                "evaluator_completed": False,
            }
            self._save(store, record)

            loaded = store.list_strategy_evidence_windows("storage-v2")[0]
            self.assertFalse(loaded["evaluation_legacy"])
            self.assertEqual(loaded["evaluation_kind"], "ACTUAL_LEDGER")
            self.assertFalse(loaded["evaluator_invoked"])
            self.assertFalse(loaded["evaluator_completed"])

    @staticmethod
    def _save_strategy_only(store: AxiomStore, record: dict[str, object]) -> None:
        store.save_strategy_version(_strategy(str(record["strategy_version_id"])))


if __name__ == "__main__":
    unittest.main()
