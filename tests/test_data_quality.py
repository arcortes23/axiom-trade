from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from typing import Any

from axiom.data_quality import (
    PRICE_PROXY,
    TIMESTAMPED_DEPTH,
    evaluate_prediction_data_quality,
)


DATASET_ID = "prediction:market-1"
DATASET_VERSION = "snapshot-1"
DATASET_VERSION_2 = "snapshot-2"


class _CountingRecords(list[dict[str, Any]]):
    def __init__(
        self,
        owner: "_QualityStore",
        dataset_version: str,
        rows: list[dict[str, Any]],
    ) -> None:
        super().__init__(rows)
        self._owner = owner
        self._dataset_version = dataset_version

    def __iter__(self):
        with self._owner._stats_lock:
            self._owner.scan_calls += 1
            self._owner.scan_calls_by_version[self._dataset_version] += 1
        return super().__iter__()


class _QualityStore:
    """Small store double that makes duplicate cold scans observable."""

    def __init__(self, *, failures_remaining: int = 0) -> None:
        self._lock = threading.RLock()
        self._stats_lock = threading.Lock()
        self._failures_remaining = failures_remaining
        self.load_calls = 0
        self.load_calls_by_version = {
            DATASET_VERSION: 0,
            DATASET_VERSION_2: 0,
        }
        self.scan_calls = 0
        self.scan_calls_by_version = {
            DATASET_VERSION: 0,
            DATASET_VERSION_2: 0,
        }
        self.active_loaders = 0
        self.max_active_loaders = 0
        self.catalogs = {
            DATASET_VERSION: {
                "dataset_id": DATASET_ID,
                "dataset_version": DATASET_VERSION,
                "provider": "fixture-provider",
                "instrument": "market-1",
                "market_type": "PREDICTION",
                "timeframe": "1h",
                "source_type": "HISTORICAL",
                "snapshot_id": "snapshot-1",
                "row_count": 2,
                "completeness": 1.0,
                "start_timestamp": "2025-01-01T00:00:00Z",
                "end_timestamp": "2025-01-02T00:00:00Z",
                "metadata": {
                    "provider": "fixture-provider",
                    "source_type": "HISTORICAL",
                },
            },
            DATASET_VERSION_2: {
                "dataset_id": DATASET_ID,
                "dataset_version": DATASET_VERSION_2,
                "provider": "fixture-provider",
                "instrument": "market-1",
                "market_type": "PREDICTION",
                "timeframe": "1h",
                "source_type": "HISTORICAL",
                "snapshot_id": "snapshot-2",
                "row_count": 3,
                "completeness": 1.0,
                "start_timestamp": "2025-02-01T00:00:00Z",
                "end_timestamp": "2025-02-02T00:00:00Z",
                "metadata": {
                    "provider": "fixture-provider",
                    "source_type": "HISTORICAL",
                },
            },
        }
        self.records_by_version = {
            DATASET_VERSION: _CountingRecords(
                self,
                DATASET_VERSION,
                [{"source_type": "HISTORICAL"}, {"source_type": "HISTORICAL"}],
            ),
            DATASET_VERSION_2: _CountingRecords(
                self,
                DATASET_VERSION_2,
                [
                    {"source_type": "HISTORICAL"},
                    {"source_type": "HISTORICAL"},
                    {"source_type": "HISTORICAL"},
                ],
            ),
        }

    def load_dataset_catalog(self, dataset_id: str, dataset_version: str) -> dict[str, Any] | None:
        if dataset_id != DATASET_ID:
            return None
        catalog = self.catalogs.get(dataset_version)
        return dict(catalog) if catalog is not None else None

    def load_dataset(self, dataset_id: str, dataset_version: str) -> _CountingRecords:
        if dataset_id != DATASET_ID or dataset_version not in self.records_by_version:
            raise AssertionError("unexpected dataset identity")
        with self._stats_lock:
            self.load_calls += 1
            self.load_calls_by_version[dataset_version] += 1
            self.active_loaders += 1
            self.max_active_loaders = max(self.max_active_loaders, self.active_loaders)
            should_fail = self._failures_remaining > 0
            if should_fail:
                self._failures_remaining -= 1
        try:
            if should_fail:
                raise RuntimeError("transient dataset load failure")
            # Keep the cold section open long enough for unsynchronized callers
            # to overlap and expose duplicate loads.
            time.sleep(0.03)
            return self.records_by_version[dataset_version]
        finally:
            with self._stats_lock:
                self.active_loaders -= 1




class _AttestedQualityStore(_QualityStore):
    """Store double exposing only bounded attestation metadata."""

    def __init__(self, attestation: dict[str, Any]) -> None:
        super().__init__()
        self.attestation = dict(attestation)
        self.attestation_load_calls = 0

    def load_dataset_integrity_attestation(
        self, dataset_id: str, dataset_version: str
    ) -> dict[str, Any]:
        self.attestation_load_calls += 1
        return dict(self.attestation)
class _SequencedAttestedQualityStore(_QualityStore):
    """Store double that exposes a stale loser before the durable winner."""

    def __init__(self, attestations: list[dict[str, Any]]) -> None:
        super().__init__()
        self._attestations = [dict(item) for item in attestations]
        self.attestation_load_calls = 0

    def load_dataset_integrity_attestation(
        self, dataset_id: str, dataset_version: str
    ) -> dict[str, Any]:
        self.attestation_load_calls += 1
        index = min(self.attestation_load_calls - 1, len(self._attestations) - 1)
        return dict(self._attestations[index])

def _payload(
    *,
    fidelity: str,
    provenance_dataset_id: str = DATASET_ID,
    dataset_version: str = DATASET_VERSION,
) -> dict[str, Any]:
    return {
        "dataset_id": DATASET_ID,
        "dataset_version": dataset_version,
        "market_type": "PREDICTION",
        "source_type": "HISTORICAL",
        "historical_execution_fidelity": fidelity,
        "dataset_provenance": {
            "dataset_id": provenance_dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
            "time_split": "train",
        },
    }


class PredictionDataQualityCacheTests(unittest.TestCase):
    def test_repeated_evaluations_scan_once_but_reproject_each_payload(self) -> None:
        store = _QualityStore()

        proxy = evaluate_prediction_data_quality(
            store,
            _payload(fidelity=PRICE_PROXY),
        )
        depth = evaluate_prediction_data_quality(
            store,
            _payload(fidelity=TIMESTAMPED_DEPTH),
        )
        mismatched_provenance = evaluate_prediction_data_quality(
            store,
            _payload(fidelity=TIMESTAMPED_DEPTH, provenance_dataset_id="prediction:other"),
        )

        self.assertTrue(proxy["historical_data_integrity_passed"])
        self.assertEqual(proxy["historical_execution_fidelity"], PRICE_PROXY)
        self.assertEqual(proxy["historical_execution_fidelity_score"], 0.35)
        self.assertTrue(proxy["canary_data_quality_acceptable"])

        self.assertTrue(depth["historical_data_integrity_passed"])
        self.assertEqual(depth["historical_execution_fidelity"], TIMESTAMPED_DEPTH)
        self.assertEqual(depth["historical_execution_fidelity_score"], 1.0)
        self.assertTrue(depth["canary_data_quality_acceptable"])

        self.assertFalse(mismatched_provenance["historical_data_integrity_passed"])
        self.assertFalse(mismatched_provenance["historical_provenance_complete"])
        self.assertEqual(mismatched_provenance["historical_execution_fidelity"], TIMESTAMPED_DEPTH)
        self.assertIn("HISTORICAL_PROVENANCE_INCOMPLETE", mismatched_provenance["reasons"])
        self.assertEqual(mismatched_provenance["historical_dataset_row_count"], 2)

        self.assertEqual(store.load_calls, 1)
        self.assertEqual(store.scan_calls, 1)


    def test_same_dataset_id_versions_load_and_scan_independently(self) -> None:
        store = _QualityStore()
        version_one_payload = _payload(fidelity=PRICE_PROXY)
        version_two_payload = _payload(
            fidelity=TIMESTAMPED_DEPTH,
            dataset_version=DATASET_VERSION_2,
        )

        version_one = evaluate_prediction_data_quality(store, version_one_payload)
        version_two = evaluate_prediction_data_quality(store, version_two_payload)
        version_one_again = evaluate_prediction_data_quality(store, version_one_payload)
        version_two_again = evaluate_prediction_data_quality(store, version_two_payload)

        self.assertEqual(version_one, version_one_again)
        self.assertEqual(version_two, version_two_again)
        self.assertNotEqual(version_one, version_two)
        self.assertTrue(version_one["historical_data_integrity_passed"])
        self.assertTrue(version_two["historical_data_integrity_passed"])
        self.assertEqual(version_one["dataset_id"], DATASET_ID)
        self.assertEqual(version_two["dataset_id"], DATASET_ID)
        self.assertEqual(version_one["dataset_version"], DATASET_VERSION)
        self.assertEqual(version_two["dataset_version"], DATASET_VERSION_2)
        self.assertEqual(version_one["historical_dataset_row_count"], 2)
        self.assertEqual(version_two["historical_dataset_row_count"], 3)
        self.assertEqual(version_one["historical_execution_fidelity"], PRICE_PROXY)
        self.assertEqual(version_two["historical_execution_fidelity"], TIMESTAMPED_DEPTH)
        self.assertEqual(
            store.load_calls_by_version,
            {DATASET_VERSION: 1, DATASET_VERSION_2: 1},
        )
        self.assertEqual(
            store.scan_calls_by_version,
            {DATASET_VERSION: 1, DATASET_VERSION_2: 1},
        )
        self.assertEqual(store.load_calls, 2)
        self.assertEqual(store.scan_calls, 2)

    def test_concurrent_cold_evaluations_share_one_serialized_scan(self) -> None:
        store = _QualityStore()
        payload = _payload(fidelity=PRICE_PROXY)

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: evaluate_prediction_data_quality(store, payload),
                    range(8),
                )
            )

        self.assertEqual(len(results), 8)
        self.assertTrue(all(result["historical_data_integrity_passed"] for result in results))
        self.assertTrue(all(result["historical_dataset_row_count"] == 2 for result in results))
        self.assertEqual(store.load_calls, 1)
        self.assertEqual(store.scan_calls, 1)
        self.assertEqual(store.max_active_loaders, 1)

    def test_failed_load_is_not_cached_and_later_success_recovers(self) -> None:
        store = _QualityStore(failures_remaining=1)
        payload = _payload(fidelity=PRICE_PROXY)

        failed = evaluate_prediction_data_quality(store, payload)
        recovered = evaluate_prediction_data_quality(store, payload)

        self.assertFalse(failed["historical_data_integrity_passed"])
        self.assertIn("HISTORICAL_ROWS_EMPTY_OR_MISMATCHED", failed["reasons"])
        self.assertTrue(recovered["historical_data_integrity_passed"])
        self.assertEqual(recovered["historical_dataset_row_count"], 2)
        self.assertEqual(store.load_calls, 2)
        self.assertEqual(store.scan_calls, 1)

    def test_stale_attestation_preserves_forward_contamination_reason_without_scan(self) -> None:
        store = _AttestedQualityStore(
            {
                "dataset_id": DATASET_ID,
                "dataset_version": DATASET_VERSION,
                "source_type": "HISTORICAL",
                "market_type": "prediction",
                "row_count": 2,
                "completeness": 1.0,
                "execution_fidelity": PRICE_PROXY,
                "contamination_result": "FAIL",
                "reason": "FORWARD_CONTAMINATION",
                "status": "STALE",
            }
        )

        quality = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))

        self.assertFalse(quality["historical_data_integrity_passed"])
        self.assertIn("HISTORICAL_FORWARD_CONTAMINATION", quality["reasons"])
        self.assertNotIn("HISTORICAL_DATASET_ATTESTATION_STALE", quality["reasons"])
        self.assertEqual(store.attestation_load_calls, 1)
        self.assertEqual(store.scan_calls, 0)

    def test_current_attestation_is_reused_without_materializing_rows(self) -> None:
        store = _AttestedQualityStore(
            {
                "dataset_id": DATASET_ID,
                "dataset_version": DATASET_VERSION,
                "source_type": "HISTORICAL",
                "market_type": "prediction",
                "row_count": 2,
                "completeness": 1.0,
                "execution_fidelity": PRICE_PROXY,
                "contamination_result": "PASS",
                "status": "CURRENT",
            }
        )

        first = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))
        second = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))

        self.assertTrue(first["historical_data_integrity_passed"])
        self.assertEqual(first, second)
        self.assertEqual(store.scan_calls, 0)

    def test_quality_reloads_durable_winner_after_cas_loser(self) -> None:
        store = _SequencedAttestedQualityStore(
            [
                {
                    "dataset_id": DATASET_ID,
                    "dataset_version": DATASET_VERSION,
                    "status": "STALE",
                    "reason": "ATTESTATION_CAS_LOST",
                },
                {
                    "dataset_id": DATASET_ID,
                    "dataset_version": DATASET_VERSION,
                    "source_type": "HISTORICAL",
                    "market_type": "prediction",
                    "row_count": 2,
                    "completeness": 1.0,
                    "execution_fidelity": PRICE_PROXY,
                    "contamination_result": "PASS",
                    "policy_version": "prediction-integrity-v2",
                    "attestation_hash": "sha256:winner",
                    "status": "CURRENT",
                },
            ]
        )

        loser = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))
        winner = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))

        self.assertFalse(loser["historical_data_integrity_passed"])
        self.assertEqual(loser["dataset_integrity_attestation_status"], "STALE")
        self.assertTrue(winner["historical_data_integrity_passed"])
        self.assertEqual(winner["dataset_integrity_attestation_status"], "CURRENT")
        self.assertEqual(winner["dataset_integrity_attestation_hash"], "sha256:winner")
        self.assertEqual(store.scan_calls, 0)

    def test_unknown_stale_attestation_fails_closed_with_generic_reason(self) -> None:
        store = _AttestedQualityStore({"status": "STALE"})

        quality = evaluate_prediction_data_quality(store, _payload(fidelity=PRICE_PROXY))

        self.assertFalse(quality["historical_data_integrity_passed"])
        self.assertEqual(quality["reasons"], ["HISTORICAL_DATASET_ATTESTATION_STALE"])
        self.assertEqual(store.scan_calls, 0)


if __name__ == "__main__":
    unittest.main()
