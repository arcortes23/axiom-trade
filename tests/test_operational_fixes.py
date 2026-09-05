from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock

from axiom.bootstrap import crypto_universe_dataset_id
from axiom.cli import main
from axiom.dashboard import _DashboardHandler, _hermes_row
from axiom.director import research_summary, validate_hermes_proposal
from axiom.storage import AxiomStore


T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _catalog(
    dataset_id: str,
    version: str,
    *,
    market_type: str,
    instrument: str,
    source_type: str = "HISTORICAL",
    timeframe: str = "event",
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "dataset_version": version,
        "provider": "fixture-provider",
        "instrument": instrument,
        "market_type": market_type,
        "timeframe": timeframe,
        "start_timestamp": T0,
        "end_timestamp": T0 + timedelta(days=1),
        "row_count": 2,
        "completeness": 1.0,
        "missing_ranges": (),
        "quality": "PRICE_PROXY" if market_type == "prediction" else "OHLCV",
        "source_type": source_type,
        "snapshot_id": f"{dataset_id}:{version}",
        "metadata": metadata or {},
    }


def _save_catalog(store: AxiomStore, record: dict[str, object]) -> None:
    store.save_dataset_catalog(**record)  # type: ignore[arg-type]


def _prediction_proposal(dataset_id: str, dataset_version: str) -> dict[str, object]:
    return {
        "proposal_id": "listed-dataset-proposal",
        "statement": "The listed historical price relationship is testable.",
        "source": "Axiom persisted Polymarket history",
        "tests": ["chronological train-validation-holdout"],
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "time_split": "train-validation-holdout",
        "paper_only": True,
        "experiment_plan": {
            "schema_version": "1",
            "market_type": "prediction",
            "template": "probability_mispricing",
            "allowed_features": ["timestamp", "market_id", "yes_mid", "expiry", "settlement"],
            "parameters": {"threshold": [0.05]},
            "filters": {},
            "regime_restrictions": {},
            "target": {"market_ids": ["559651"]},
            "metrics": ["sample_count"],
            "dataset_selector": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
                "timeframe": "event",
            },
            "methodology": {"time_split": "train-validation-holdout"},
            "max_variants": 1,
            "min_samples": 1,
            "min_trades": 0,
            "paper_only": True,
        },
    }


class ResearchableDatasetTests(unittest.TestCase):
    def test_summary_exposes_exact_historical_prediction_bindings_only(self) -> None:
        with AxiomStore(":memory:") as store:
            aggregate_version = "sha256:aggregate-v1"
            constituent_version = "sha256:constituent-v1"
            _save_catalog(
                store,
                _catalog(
                    "Polymarket-historical",
                    aggregate_version,
                    market_type="prediction",
                    instrument="POLYMARKET",
                    metadata={
                        "market_versions": [
                            {"market_id": "559651", "version": constituent_version, "records": 2}
                        ]
                    },
                ),
            )
            _save_catalog(
                store,
                _catalog(
                    "prediction:559651",
                    constituent_version,
                    market_type="prediction",
                    instrument="559651",
                    metadata={"market_id": "559651"},
                ),
            )
            forward_timestamp = "2026-09-05T13:21:05.640628+00:00"
            _save_catalog(
                store,
                _catalog(
                    "prediction:559651",
                    forward_timestamp,
                    market_type="prediction",
                    instrument="559651",
                    source_type="FORWARD_COLLECTED",
                    metadata={"market_id": "559651"},
                ),
            )

            summary = research_summary(store)
            datasets = summary["researchable_datasets"]
            prediction = datasets["prediction"]
            self.assertEqual(prediction[0]["dataset_id"], "Polymarket-historical")
            self.assertEqual(prediction[0]["dataset_version"], aggregate_version)
            self.assertEqual(prediction[1]["dataset_id"], "prediction:559651")
            self.assertEqual(prediction[1]["market_id"], "559651")
            self.assertNotIn(forward_timestamp, json.dumps(summary, default=str))

    def test_crypto_summary_exposes_exact_dataset_and_universe_provenance(self) -> None:
        with AxiomStore(":memory:") as store:
            universe_id = "TOP_2_MARKET_CAP_BINANCE_USDT"
            universe_version = "sha256:universe-v1"
            _save_catalog(
                store,
                _catalog(
                    f"universe:{universe_id}",
                    universe_version,
                    market_type="crypto_spot",
                    instrument=universe_id,
                    source_type="FORWARD_COLLECTED",
                    timeframe="point_in_time",
                    metadata={
                        "universe_id": universe_id,
                        "snapshot_hash": universe_version,
                        "point_in_time": True,
                    },
                ),
            )
            dataset_id = crypto_universe_dataset_id(universe_id, universe_version, "ETHUSDT", "1d")
            dataset_version = "sha256:eth-history-v1"
            _save_catalog(
                store,
                _catalog(
                    dataset_id,
                    dataset_version,
                    market_type="crypto_spot",
                    instrument="ETHUSDT",
                    timeframe="1d",
                    metadata={
                        "selected_symbol": "ETHUSDT",
                        "universe_id": universe_id,
                        "universe_version": universe_version,
                        "universe_snapshot_hash": universe_version,
                        "survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT",
                    },
                ),
            )

            crypto = research_summary(store)["researchable_datasets"]["crypto_spot"]
            self.assertEqual(len(crypto), 1)
            self.assertEqual(crypto[0]["dataset_id"], dataset_id)
            self.assertEqual(crypto[0]["dataset_version"], dataset_version)
            self.assertEqual(crypto[0]["symbol"], "ETHUSDT")
            self.assertEqual(crypto[0]["universe_id"], universe_id)
            self.assertEqual(crypto[0]["universe_version"], universe_version)
            self.assertEqual(crypto[0]["survivorship_label"], "SURVIVORSHIP_BIAS_PRESENT")

    def test_listed_prediction_binding_passes_and_timestamp_binding_is_rejected_before_queue(self) -> None:
        aggregate_version = "sha256:aggregate-v1"
        with AxiomStore(":memory:") as store:
            _save_catalog(
                store,
                _catalog(
                    "Polymarket-historical",
                    aggregate_version,
                    market_type="prediction",
                    instrument="POLYMARKET",
                ),
            )
            accepted = validate_hermes_proposal(
                _prediction_proposal("Polymarket-historical", aggregate_version),
                store=store,
            )
            self.assertTrue(accepted.accepted, accepted.reasons)
            self.assertEqual(accepted.normalized["dataset_id"], "Polymarket-historical")  # type: ignore[index]

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "submission.sqlite")
            proposal = _prediction_proposal("prediction:559651", "2026-09-05T13:21:05.640628+00:00")
            output = io.StringIO()
            with unittest.mock.patch("sys.stdout", output):
                result = main(
                    [
                        "submit-proposal",
                        "--db",
                        path,
                        "--proposal",
                        json.dumps(proposal),
                    ]
                )
            payload = json.loads(output.getvalue())
            self.assertEqual(result, 1)
            self.assertIn("DATASET_NOT_FOUND", payload["reasons"])
            with AxiomStore(path) as store:
                self.assertEqual(store.list_research_items(limit=10), [])


    def test_mismatched_prediction_catalog_is_rejected_before_enqueue(self) -> None:
        with AxiomStore(":memory:") as store:
            dataset_id = "prediction:559651"
            dataset_version = "sha256:constituent-v1"
            _save_catalog(
                store,
                _catalog(
                    dataset_id,
                    dataset_version,
                    market_type="prediction",
                    instrument="559651",
                    metadata={"market_id": "different-market"},
                ),
            )
            validation = validate_hermes_proposal(
                _prediction_proposal(dataset_id, dataset_version),
                store=store,
            )
            self.assertFalse(validation.accepted)
            self.assertIn("DATASET_NOT_FOUND", validation.reasons)

    def test_researchable_dataset_summary_is_bounded(self) -> None:
        with AxiomStore(":memory:") as store:
            for index in range(1_001):
                _save_catalog(
                    store,
                    _catalog(
                        f"prediction:market-{index}",
                        f"sha256:version-{index}",
                        market_type="prediction",
                        instrument=f"market-{index}",
                        metadata={"market_id": f"market-{index}"},
                    ),
                )
            prediction = research_summary(store)["researchable_datasets"]["prediction"]
            self.assertLessEqual(len(prediction), 9)
            self.assertTrue(all(item["dataset_version"].startswith("sha256:") for item in prediction))


class DashboardDisconnectTests(unittest.TestCase):
    def _handler(self, path: str, *, data: object) -> _DashboardHandler:
        handler = _DashboardHandler.__new__(_DashboardHandler)
        handler.path = path
        handler.server = SimpleNamespace(dashboard_data=data)
        return handler

    def test_client_disconnect_during_body_write_is_quiet(self) -> None:
        class DisconnectingWriter:
            def __init__(self, error: BaseException) -> None:
                self.error = error
                self.writes = 0

            def write(self, body: bytes) -> int:
                self.writes += 1
                raise self.error

        for error_type in (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            with self.subTest(error_type=error_type.__name__):
                handler = _DashboardHandler.__new__(_DashboardHandler)
                handler.wfile = DisconnectingWriter(error_type())
                handler.send_response = Mock()
                handler.send_header = Mock()
                handler.end_headers = Mock()
                handler._send(200, {"ok": True})
                self.assertEqual(handler.wfile.writes, 1)
                handler.send_response.assert_called_once_with(200)

    def test_disconnect_during_response_does_not_trigger_second_send(self) -> None:
        data = SimpleNamespace(snapshot=Mock(return_value={"ok": True}))
        handler = self._handler("/api/operator", data=data)
        handler._send = Mock(side_effect=ConnectionAbortedError())
        handler.do_GET()
        handler._send.assert_called_once()

    def test_data_generation_failure_still_returns_503(self) -> None:
        data = SimpleNamespace(snapshot=Mock(side_effect=RuntimeError("dashboard data failed")))
        handler = self._handler("/api/operator", data=data)
        handler._send = Mock()
        handler.do_GET()
        handler._send.assert_called_once_with(
            503,
            {"error": "data unavailable", "detail": "dashboard data failed"},
        )

    def test_hermes_outcome_distinguishes_dataset_rejection_and_preserves_binding(self) -> None:
        proposal_rejection = _hermes_row(
            {
                "item_id": "proposal-1",
                "item_type": "hypothesis",
                "status": "REJECTED",
                "payload": {
                    "dataset_id": "prediction:559651",
                    "dataset_version": "sha256:exact",
                },
                "result": {"reason_code": "DATASET_NOT_FOUND"},
            }
        )
        experiment_rejection = _hermes_row(
            {
                "item_id": "proposal-2",
                "item_type": "hypothesis",
                "status": "REJECTED",
                "payload": {"dataset_id": "Polymarket-historical", "dataset_version": "sha256:exact"},
                "result": {"reason_code": "ROBUSTNESS_FAILED"},
            }
        )
        self.assertEqual(proposal_rejection["outcome_label"], "PROPOSAL REJECTED")
        self.assertEqual(experiment_rejection["outcome_label"], "EXPERIMENT REJECTED")
        self.assertEqual(proposal_rejection["dataset_id"], "prediction:559651")
        self.assertEqual(proposal_rejection["dataset_version"], "sha256:exact")


if __name__ == "__main__":
    unittest.main()
