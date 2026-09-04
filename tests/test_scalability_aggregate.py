from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest

from axiom.domain import MarketType
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=lambda item: item.isoformat(), allow_nan=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _history(market_id: str) -> list[dict[str, object]]:
    return [
        {"timestamp": T0, "price": 0.40, "token_id": f"{market_id}-yes"},
        {"timestamp": T0 + timedelta(days=1), "price": 0.60, "token_id": f"{market_id}-yes"},
    ]


def _seed_constituent(store: AxiomStore, market_id: str, *, quality: str = "PRICE_PROXY") -> str:
    history = _history(market_id)
    version = _hash(history)
    for index, point in enumerate(history):
        store.save_polymarket_snapshot(
            f"historical:{market_id}:{index}",
            market_id,
            point["timestamp"],
            point["timestamp"],
            {
                "source_type": "HISTORICAL",
                "market_id": market_id,
                "timestamp": point["timestamp"],
                "price": point["price"],
                "yes_mid": point["price"],
                "token_id": point["token_id"],
                "research_quality": quality,
            },
            quality=quality,
        )
    store.save_dataset_catalog(
        f"prediction:{market_id}",
        version,
        provider="fixture",
        instrument=market_id,
        market_type=MarketType.PREDICTION,
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=T0 + timedelta(days=1),
        row_count=2,
        completeness=1.0,
        quality=quality,
        source_type="HISTORICAL",
        snapshot_id=f"constituent:{market_id}:{version}",
        metadata={"market_id": market_id, "token_ids": {"yes": f"{market_id}-yes"}, "research_quality": quality},
    )
    return version


def _seed_aggregate(store: AxiomStore, market_versions: list[dict[str, object]], *, version: str = "aggregate-v1") -> None:
    row_count = sum(int(item["records"]) for item in market_versions)
    end_timestamp = T0 + timedelta(days=1) if row_count > 1 else T0
    store.save_dataset_catalog(
        "Polymarket-historical",
        version,
        provider="fixture",
        instrument="POLYMARKET",
        market_type=MarketType.PREDICTION,
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=end_timestamp,
        row_count=row_count,
        completeness=1.0,
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id=f"aggregate:{version}",
        metadata={"market_versions": market_versions},
    )


class AggregateCatalogScalabilityTests(unittest.TestCase):
    def test_aggregate_loads_nonzero_rows_with_provenance(self) -> None:
        with AxiomStore(":memory:") as store:
            version = _seed_constituent(store, "m1")
            _seed_aggregate(store, [{"market_id": "m1", "version": version, "records": 2}])
            rows = store.load_dataset("Polymarket-historical", "aggregate-v1")
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["aggregate_dataset_id"] == "Polymarket-historical" for row in rows))
            self.assertTrue(all(row["aggregate_dataset_version"] == "aggregate-v1" for row in rows))
            self.assertTrue(all(row["constituent_dataset_id"] == "prediction:m1" for row in rows))
            self.assertTrue(all(row["constituent_dataset_version"] == version for row in rows))
            self.assertEqual([row["market_id"] for row in rows], ["m1", "m1"])
            self.assertEqual([row["source_timestamp"] for row in rows], [T0, T0 + timedelta(days=1)])

    def test_wrong_constituent_version_fails_closed(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_constituent(store, "m1")
            _seed_aggregate(store, [{"market_id": "m1", "version": "wrong", "records": 2}])
            self.assertEqual(store.load_dataset("Polymarket-historical", "aggregate-v1"), [])

    def test_forward_rows_are_excluded_and_quality_is_preserved(self) -> None:
        with AxiomStore(":memory:") as store:
            version = _seed_constituent(store, "m1", quality="PRICE_PROXY")
            store.save_polymarket_snapshot(
                "forward:m1",
                "m1",
                T0 + timedelta(days=1, hours=1),
                T0 + timedelta(days=1, hours=1),
                {"source_type": "FORWARD_COLLECTED", "yes_mid": 0.99},
                quality="ORDER_BOOK_SIMULATED",
            )
            _seed_aggregate(store, [{"market_id": "m1", "version": version, "records": 2}])
            rows = store.load_dataset("Polymarket-historical", "aggregate-v1")
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["quality"] == "PRICE_PROXY" for row in rows))
            self.assertTrue(all(row["source_type"] == "HISTORICAL" for row in rows))

    def test_yes_mid_rows_remain_compatible_with_entry_price_filters(self) -> None:
        with AxiomStore(":memory:") as store:
            version = _seed_constituent(store, "m1")
            _seed_aggregate(store, [{"market_id": "m1", "version": version, "records": 2}])
            rows = store.load_dataset("Polymarket-historical", "aggregate-v1")
            selected = [row for row in rows if 0.45 <= float(row["yes_mid"]) <= 0.65]
            self.assertEqual([row["yes_mid"] for row in selected], [0.60])

    def test_exact_first_hermes_shaped_dataset_load_is_nonempty(self) -> None:
        with AxiomStore(":memory:") as store:
            records = [
                {"source_type": "HISTORICAL", "market_id": "hermes-m1", "timestamp": T0, "yes_mid": 0.5},
            ]
            store.save_dataset("prediction:hermes-m1", "hermes-v1", records, metadata={"market_id": "hermes-m1"})
            store.save_dataset_catalog(
                "prediction:hermes-m1",
                "hermes-v1",
                provider="hermes",
                instrument="hermes-m1",
                market_type=MarketType.PREDICTION,
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0,
                row_count=1,
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="hermes:constituent",
                metadata={"market_id": "hermes-m1"},
            )
            _seed_aggregate(store, [{"market_id": "hermes-m1", "version": "hermes-v1", "records": 1}])
            rows = store.load_dataset("Polymarket-historical", "aggregate-v1")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["constituent_dataset_version"], "hermes-v1")


if __name__ == "__main__":
    unittest.main()
