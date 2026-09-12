from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections.abc import Mapping
import json
import unittest
from unittest.mock import patch

from axiom.autonomous import AutonomousResearchProcessor
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)
SYNTHETIC_LABEL = "SYNTHETIC_CAMPAIGN_ACCEPTANCE"
DATASET_ID = "synthetic-polymarket-campaign-history"


def _rows(*, midpoint: float) -> list[dict[str, object]]:
    return [
        {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "timestamp": T0.isoformat(),
            "market_id": "synthetic-market-1",
            "yes_mid": midpoint,
            "yes_bid": midpoint - 0.01,
            "yes_ask": midpoint + 0.01,
            "settlement": "open",
            "source_snapshot_id": f"{SYNTHETIC_LABEL}:snapshot:{midpoint}",
        }
    ]


def _large_rows(count: int = 18_141) -> list[dict[str, object]]:
    return [
        {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "timestamp": (T0 + timedelta(seconds=index)).isoformat(),
            "market_id": f"large-market-{index:05d}",
            "yes_mid": 0.50 + (index % 10) / 1_000,
            "yes_bid": 0.49 + (index % 10) / 1_000,
            "yes_ask": 0.51 + (index % 10) / 1_000,
            "settlement": "open",
            "source_snapshot_id": f"{SYNTHETIC_LABEL}:large:{index}",
        }
        for index in range(count)
    ]


def _save_attested_dataset(store: AxiomStore, version: str, *, midpoint: float) -> dict[str, object]:
    rows = _rows(midpoint=midpoint)
    metadata = {
        "fixture_label": SYNTHETIC_LABEL,
        "synthetic": True,
        "provider": SYNTHETIC_LABEL,
        "source_type": "HISTORICAL",
        "market_type": "prediction",
        "instrument": "POLYMARKET",
        "research_quality": "PRICE_PROXY",
    }
    store.save_dataset(DATASET_ID, version, rows, metadata=metadata, quality="PRICE_PROXY")
    store.save_dataset_catalog(
        DATASET_ID,
        version,
        provider=SYNTHETIC_LABEL,
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=T0,
        row_count=len(rows),
        completeness=1.0,
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id=f"{SYNTHETIC_LABEL}:catalog:{version}",
        metadata=metadata,
    )
    attestation = store.verify_dataset_integrity_attestation(DATASET_ID, version, force=True)
    assert attestation["status"] == "CURRENT"
    assert attestation["contamination_result"] == "PASS"
    return attestation


class AutonomousCampaignAcceptanceTests(unittest.TestCase):
    def test_large_campaign_uses_compact_locked_provenance(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                DATASET_ID,
                "large-v1",
                _large_rows(),
                metadata={
                    "source_type": "HISTORICAL",
                    "market_type": "prediction",
                    "instrument": "POLYMARKET",
                },
                quality="PRICE_PROXY",
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "large-compact-campaign",
                dataset_id=DATASET_ID,
                dataset_version="large-v1",
                now=T0,
            )
            queued = processor.bus.list_campaign_trials("large-compact-campaign", limit=10)
            self.assertEqual(len(queued), 1)
            plan = queued[0].payload["experiment_plan"]
            self.assertLess(len(json.dumps(plan, separators=(",", ":")).encode("utf-8")), 16_384)
            boundary = state["protocol"]["dataset_boundary"]
            self.assertEqual(boundary["row_count"], 18_141)
            self.assertTrue(boundary["ordered_row_manifest_digest"].startswith("sha256:"))
            self.assertNotIn("ordered_row_identities", boundary)
            self.assertNotIn("ordered_content_hashes", boundary)

    def test_synthetic_campaign_covers_protocol_progress_reassessment_and_exhaustion(self) -> None:
        """Exercise A/B/C/D/G against only labelled, attested offline fixtures."""
        with AxiomStore(":memory:") as store:
            first_attestation = _save_attested_dataset(store, "v1", midpoint=0.50)
            second_attestation = _save_attested_dataset(store, "v2", midpoint=0.52)
            self.assertNotEqual(
                first_attestation["attestation_hash"],
                second_attestation["attestation_hash"],
            )

            # Provider/attestation failures remain evidence for this job only;
            # this injectable fault must not escape into campaign scheduling.
            fault_processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with patch.object(
                store,
                "load_dataset_integrity_attestation",
                side_effect=RuntimeError("synthetic provider failure"),
            ):
                self.assertIsNone(
                    fault_processor._load_next_dataset_version(
                        dataset_id=DATASET_ID,
                        rejected_version="v1",
                    )
                )

            campaign_id = "synthetic-campaign-a-through-g"
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            initial = processor.start_polymarket_campaign(
                campaign_id,
                dataset_id=DATASET_ID,
                dataset_version="v1",
                now=T0,
            )
            campaign_job_name = processor.campaign_job_name(campaign_id)
            durable_job = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(durable_job)
            assert durable_job is not None
            self.assertEqual(durable_job["payload"]["protocol"], initial["protocol"])
            self.assertEqual(initial["status"], "RUNNING")
            self.assertEqual(initial["protocol"]["dataset_boundary"]["dataset_id"], DATASET_ID)
            self.assertEqual(initial["protocol"]["dataset_boundary"]["dataset_version"], "v1")
            first_queue = processor.bus.list_campaign_trials(campaign_id, limit=100)
            self.assertEqual(len(first_queue), 1)
            self.assertEqual(first_queue[0].payload["campaign_id"], campaign_id)

            # A: economic rejection advances to the next predeclared configuration
            # without changing the immutable dataset boundary.
            first_trial = first_queue[0]
            processor._advance_campaign_after_result(
                first_trial,
                {
                    "accepted": False,
                    "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
                    "candidate_id": "synthetic-candidate-a",
                },
                now=T0 + timedelta(minutes=1),
            )
            after_a = processor.campaign_state(campaign_id)
            self.assertEqual(after_a["status"], "RUNNING")
            self.assertEqual(after_a["counts"]["economic_rejection"], 1)
            self.assertEqual(after_a["budget_used"], 2)
            self.assertEqual(after_a["protocol"]["dataset_boundary"]["dataset_version"], "v1")
            self.assertEqual(after_a["trials"][0]["status"], "ECONOMIC_REJECTION")
            self.assertEqual(after_a["trials"][1]["status"], "RUNNING")
            self.assertNotEqual(
                after_a["trials"][0]["configuration_id"],
                after_a["trials"][1]["configuration_id"],
            )

            # B/C: data insufficiency pauses the trial and names the actual,
            second_trial = next(
                item
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                if item.payload["campaign_trial_id"] == after_a["trials"][1]["trial_id"]
            )
            processor._advance_campaign_after_result(
                second_trial,
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "synthetic-candidate-b",
                },
                now=T0 + timedelta(minutes=2),
            )
            waiting = processor.campaign_state(campaign_id)
            self.assertEqual(waiting["status"], "WAITING_FOR_DATA")
            self.assertEqual(waiting["counts"]["data_insufficient"], 1)
            next_real_job = waiting["next_real_job"]
            self.assertTrue(next_real_job)
            data_job = store.get_operator_job(next_real_job)
            self.assertIsNotNone(data_job)
            assert data_job is not None
            self.assertEqual(data_job["status"], "SCHEDULED")
            self.assertEqual(data_job["payload"]["campaign_id"], campaign_id)
            self.assertEqual(
                data_job["payload"]["producer_job"],
                f"polymarket-dataset-producer:{campaign_id}:{waiting['trials'][1]['trial_id']}",
            )
            self.assertEqual(data_job["payload"]["dataset_version"], "v1")
            # D: a changed, current attestation is accepted once, and duplicate
            # evidence cannot create a second reassessment or queue item.
            queue_before_reassessment = len(processor.bus.list_campaign_trials(campaign_id, limit=100))
            evidence_identity = str(second_attestation["attestation_hash"])
            self.assertNotEqual(evidence_identity, str(first_attestation["attestation_hash"]))
            processor.reassess_campaign(
                campaign_id,
                evidence_identity=evidence_identity,
                now=T0 + timedelta(minutes=3),
            )
            reassessed = processor.campaign_state(campaign_id)
            self.assertEqual(reassessed["reassessment_count"], 1)
            self.assertEqual(reassessed["status"], "RUNNING")
            self.assertEqual(reassessed["reassessment_evidence"]["identity"], evidence_identity)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                queue_before_reassessment + 1,
            )
            reassessment_id = f"{reassessed['trials'][1]['trial_id']}:reassessment-1"
            reassessment_trial = next(
                item for item in reassessed["trials"] if item["trial_id"] == reassessment_id
            )
            self.assertEqual(reassessment_trial["status"], "RUNNING")
            reassessment_queue = [
                item
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                if item.payload["campaign_trial_id"] == reassessment_id
            ]
            self.assertEqual(len(reassessment_queue), 1)
            self.assertEqual(
                reassessment_queue[0].payload["experiment_plan"]["dataset_selector"]["dataset_version"],
                "v2",
            )
            self.assertTrue(
                any(item["status"] == "PLANNED" for item in reassessed["trials"])
            )
            processor.reassess_campaign(
                campaign_id,
                evidence_identity=evidence_identity,
                now=T0 + timedelta(minutes=4),
            )
            self.assertEqual(processor.campaign_state(campaign_id)["reassessment_count"], 1)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                queue_before_reassessment + 1,
            )

            # G: every remaining fixed configuration gets exactly one bounded
            # turn; exhaustion is durable and produces no successor queue item.
            for tick in range(32):
                state = processor.campaign_state(campaign_id)
                if state["status"] == "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY":
                    break
                running = next(
                    trial
                    for trial in state["trials"]
                    if trial["status"] == "RUNNING"
                )
                queue_item = next(
                    item
                    for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                    if item.payload["campaign_trial_id"] == running["trial_id"]
                )
                processor._advance_campaign_after_result(
                    queue_item,
                    {
                        "accepted": False,
                        "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
                        "candidate_id": f"synthetic-candidate-{tick + 3}",
                    },
                    now=T0 + timedelta(minutes=5 + tick),
                )
            else:
                self.fail("synthetic fixed campaign did not exhaust within its bounded plan")

            exhausted = processor.campaign_state(campaign_id)
            self.assertEqual(exhausted["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertEqual(exhausted["qualified_candidate_ids"], [])
            self.assertEqual(exhausted["next_real_job"], None)
            self.assertTrue(exhausted["final_assessment_evaluated"])
            self.assertEqual(exhausted["counts"]["economic_rejection"], 12)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                len(exhausted["trials"]),
            )
            durable_exhaustion = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(durable_exhaustion)
            assert durable_exhaustion is not None
            self.assertEqual(
                durable_exhaustion["status"],
                "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
            )
            self.assertFalse(durable_exhaustion["resumable"])
            persisted = store.load_dataset(DATASET_ID, "v1")
            persisted_rows = (
                persisted.get("records", [])
                if isinstance(persisted, Mapping)
                else persisted
            )
            self.assertEqual(persisted_rows[0]["fixture_label"], SYNTHETIC_LABEL)
            self.assertTrue(persisted_rows[0]["synthetic"])

    def test_final_assessment_rejects_zero_trades_with_sufficient_observations(self) -> None:
        campaign_grid = (
            {
                "configuration_id": "momentum:lookback-1:threshold-0.05",
                "template": "momentum",
                "parameters": {"lookback": 1, "threshold": 0.05},
            },
        )
        rows = [
            {
                "fixture_label": SYNTHETIC_LABEL,
                "synthetic": True,
                "timestamp": (T0 + timedelta(seconds=index)).isoformat(),
                "market_id": f"zero-trades-market-{index}",
                "yes_mid": 0.50,
                "yes_bid": 0.49,
                "yes_ask": 0.51,
                "settlement": "open",
                "source_snapshot_id": f"{SYNTHETIC_LABEL}:zero-trades:{index}",
            }
            for index in range(5)
        ]
        gates = {"min_expectancy": 0.0, "min_samples": 1, "min_trades": 1}
        with patch("axiom.autonomous.POLYMARKET_CAMPAIGN_GRID", campaign_grid), AxiomStore(":memory:") as store:
            store.save_dataset(DATASET_ID, "zero-trades-v1", rows, quality="PRICE_PROXY")
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "zero-trades-campaign",
                dataset_id=DATASET_ID,
                dataset_version="zero-trades-v1",
                qualification_gates=gates,
                now=T0,
            )
            self.assertEqual(state["protocol"]["qualification_gates"], gates)
            trial = processor.bus.list_campaign_trials("zero-trades-campaign")[0]

            with patch.object(
                processor,
                "_run_backtest",
                return_value={"sample_count": 1, "filled_trades": 0, "expectancy": 0.10},
            ):
                processor._advance_campaign_after_result(
                    trial,
                    {
                        "accepted": True,
                        "reason_code": "VALIDATION_QUALIFIED",
                        "candidate_id": "zero-trades-finalist",
                    },
                    now=T0 + timedelta(minutes=1),
                )

            terminal = processor.campaign_state("zero-trades-campaign")
            self.assertIsNotNone(terminal)
            assert terminal is not None
            self.assertEqual(terminal["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertEqual(terminal["qualified_candidate_ids"], [])
            self.assertTrue(terminal["final_assessment_evaluated"])
            assessment = terminal["final_assessment"]["assessments"][0]
            self.assertEqual(assessment["status"], "REJECTED")
            self.assertFalse(assessment["passed"])
            self.assertTrue(assessment["minimum_sample_check"]["checks"]["observations"])
            self.assertFalse(assessment["minimum_sample_check"]["checks"]["trades"])
            self.assertEqual(assessment["minimum_sample_check"]["trades"], 0)
            self.assertEqual(assessment["minimum_sample_check"]["min_trades"], 1)
            durable = store.get_operator_job(processor.campaign_job_name("zero-trades-campaign"))
            self.assertIsNotNone(durable)
            assert durable is not None
            self.assertEqual(durable["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertFalse(durable["resumable"])



if __name__ == "__main__":
    unittest.main()
