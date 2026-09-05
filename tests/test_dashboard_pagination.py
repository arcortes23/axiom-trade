from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

from axiom.dashboard import DashboardData, DashboardServer, _dashboard_html
from axiom.domain import MarketType
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)
COMMON_PAGE_KEYS = {"items", "page", "page_size", "total", "pages"}
DATASET_COUNT = 24
MARKET_COUNT = 23
CANDIDATE_COUNT = 23
QUEUE_COUNT = 23
PAPER_COUNT = 23
ACTIVITY_COUNT = DATASET_COUNT + 1 + 45 + 25


class DashboardPaginationFixture(unittest.TestCase):
    """Small persisted fixture shared by endpoint tests.

    Every collection is large enough to cross a ten-row page boundary, while
    remaining tiny compared with the production scan bounds these APIs replace.
    """

    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        # Register teardown before any fixture writes so a seed failure does
        # not leave an open SQLite handle or a locked temporary directory.
        self.addCleanup(self._temporary_directory.cleanup)
        database_path = Path(self._temporary_directory.name) / "dashboard.sqlite3"
        self.store = AxiomStore(str(database_path))
        self.addCleanup(self.store.close)
        self._seed_datasets()
        self._seed_polymarket()
        self._seed_candidates()
        self._seed_queue()
        self._seed_paper()
        self.server = DashboardServer(port=0, data=DashboardData(store=self.store)).start()
        # unittest cleanups run last-in, first-out; close the HTTP server and
        # SQLite connection before removing the temporary database directory.
        self.addCleanup(self.server.stop)

    def _seed_datasets(self) -> None:
        for index in range(DATASET_COUNT):
            dataset_id = f"dataset-{index:02d}"
            source_type = "HISTORICAL" if index % 2 == 0 else "FORWARD_COLLECTED"
            market_type = MarketType.CRYPTO_SPOT.value if index % 3 else MarketType.PREDICTION.value
            timeframe = "1d" if index % 2 == 0 else "1h"
            quality = "HIGH" if index % 4 else "LOW"
            timestamp = T0 + timedelta(minutes=index // 2)
            missing_ranges = (
                [
                    {
                        "start": f"2024-02-{range_index + 1:02d}T00:00:00+00:00",
                        "end": f"2024-02-{range_index + 1:02d}T01:00:00+00:00",
                    }
                    for range_index in range(23)
                ]
                if index == 0
                else ()
            )
            self.store.save_dataset_catalog(
                dataset_id,
                "v1",
                provider="fixture-provider",
                instrument=f"instrument-{index:02d}",
                market_type=market_type,
                timeframe=timeframe,
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(hours=1),
                row_count=index + 1,
                completeness=0.75 + (index % 4) / 16,
                missing_ranges=missing_ranges,
                quality=quality,
                source_type=source_type,
                snapshot_id=f"snapshot-{index:02d}",
                metadata={"fixture": True},
                created_at=timestamp,
                updated_at=timestamp,
            )

        # The detail route must decode the path segment and return the actual
        # persisted dataset, not materialize the entire catalog in the index.
        self.detail_dataset_id = "dataset/detail with/slash"
        self.store.save_dataset(
            self.detail_dataset_id,
            "v1",
            [{"value": "detail-only"}],
            metadata={"fixture": True, "detail": True},
        )
        self.store.save_dataset_catalog(
            self.detail_dataset_id,
            "v1",
            provider="fixture-provider",
            instrument="detail-instrument",
            market_type=MarketType.CRYPTO_SPOT.value,
            timeframe="detail",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="DETAIL",
            source_type="FORWARD_COLLECTED",
            snapshot_id="detail-snapshot",
            metadata={"fixture": True, "detail": True},
            created_at=T0 + timedelta(minutes=100),
            updated_at=T0 + timedelta(minutes=100),
        )

    def _seed_polymarket(self) -> None:
        for index in range(MARKET_COUNT):
            market_id = f"market-{index:02d}"
            category = "weather" if index % 3 == 0 else "sports"
            timeframe = "1d" if index % 2 == 0 else "7d"
            settlement = "open" if index != 20 else "resolved_yes"
            quality = "ORDER_BOOK_SIMULATED" if index % 2 == 0 else "PRICE_PROXY"
            question = f"Will fixture event {index:02d} happen?"
            metadata = {
                "market_id": market_id,
                "question": question,
                "category": category,
                "timeframe": timeframe,
                "active": True,
                "closed": False,
            }
            snapshot = {
                "market_id": market_id,
                "question": question,
                "category": category,
                "timeframe": timeframe,
                "settlement": settlement,
                "yes_mid": 0.40 + index / 100,
                "liquidity": 1000 + index,
            }
            self.store.save_polymarket_market_metadata(
                market_id,
                metadata,
                observed_at=T0 + timedelta(minutes=index // 2),
            )
            self.store.save_polymarket_snapshot(
                f"market-snapshot-{index:02d}",
                market_id,
                T0 + timedelta(minutes=index // 2),
                T0 + timedelta(minutes=index // 2, seconds=1),
                {"snapshot": snapshot, "source_type": "FORWARD_COLLECTED"},
                quality=quality,
            )

    def _seed_candidates(self) -> None:
        for index in range(CANDIDATE_COUNT):
            candidate_id = f"candidate-{index:02d}"
            payload = {
                "strategy_id": f"strategy-{index:02d}",
                "experiment_family": "trend" if index % 2 == 0 else "mean_reversion",
                "market_type": MarketType.CRYPTO_SPOT.value,
                "generation": index % 3,
                "quality": "HIGH" if index % 2 == 0 else "LOW",
                "hypothesis": f"candidate hypothesis {index:02d}",
            }
            if index == 0:
                stage = "IDEA"
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    stage,
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                progression = (
                    "SCHEMA_VALIDATED",
                    "BACKTESTED",
                    "VALIDATED",
                    "ROBUSTNESS_CHECKED",
                    "FROZEN",
                    "PAPER_FORWARD",
                    "PAPER_PROMOTABLE",
                )
                for next_stage in progression:
                    payload = {**payload, "event_marker": next_stage}
                    self.store.save_candidate_lifecycle(
                        candidate_id,
                        next_stage,
                        payload,
                        from_stage=stage,
                        reason="fixture event pagination",
                        timestamp=T0,
                    )
                    stage = next_stage
                for marker in range(4):
                    payload = {**payload, "event_marker": f"repeat-{marker}"}
                    self.store.save_candidate_lifecycle(
                        candidate_id,
                        stage,
                        payload,
                        from_stage=stage,
                        reason="fixture event pagination",
                        timestamp=T0,
                    )
            elif index == 2:
                payload = {
                    **payload,
                    "strategy_hash": "fixture-strategy-hash-02",
                    "model_hash": "fixture-model-hash-02",
                    "config_hash": "fixture-config-hash-02",
                }
                payload["frozen_hash"] = hashlib.sha256(
                    "|".join(
                        payload[key] for key in ("strategy_hash", "model_hash", "config_hash")
                    ).encode()
                ).hexdigest()
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "FROZEN",
                    payload,
                    from_stage="IDEA",
                    reason="fixture seed",
                    timestamp=T0,
                )
            elif index % 2 == 0:
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "VALIDATED",
                    payload,
                    from_stage="IDEA",
                    reason="fixture seed",
                    timestamp=T0,
                )
            else:
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
        # Persist one historical-gates-passed candidate as CANARY_ELIGIBLE
        # before any paper-forward or promotion stage.
        self.store.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS canary_eligibility (
              candidate_id TEXT PRIMARY KEY, eligible_at TEXT NOT NULL,
              frozen_hash TEXT NOT NULL, evidence_json TEXT NOT NULL
            );
            """
        )
        eligible_record = self.store.load_candidate_lifecycle("candidate-02")
        assert isinstance(eligible_record, dict)
        eligible_payload = eligible_record["payload"]
        self.store.connection.execute(
            "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) VALUES (?,?,?,?)",
            (
                "candidate-02",
                T0.isoformat(),
                eligible_payload["frozen_hash"],
                json.dumps(eligible_payload, sort_keys=True),
            ),
        )
        self.store.connection.commit()
    def _seed_queue(self) -> None:
        for index in range(QUEUE_COUNT):
            self.store.enqueue_research_item(
                "hypothesis",
                {"label": f"hypothesis-{index:02d}", "market": "crypto_spot"},
                source="fixture-hermes",
                author="fixture",
                item_id=f"queue-{index:02d}",
                dedupe_key=f"fixture-queue-{index:02d}",
                priority=100 if index == 0 else index % 3,
            )
        claim_time = datetime.now(UTC) + timedelta(seconds=1)
        claimed = self.store.claim_research_item("fixture-worker", now=claim_time)
        self.assertIsNotNone(claimed)
        self.store.complete_research_item(
            "queue-00",
            "ACCEPTED",
            result={"fixture": True},
            now=claim_time + timedelta(seconds=1),
            worker="fixture-worker",
        )

    def _seed_paper(self) -> None:
        for index in range(PAPER_COUNT):
            self.store.save_paper_state(
                f"paper-{index:02d}",
                {
                    "status": "OPEN" if index % 2 == 0 else "CLOSED",
                    "market_id": f"market-{index:02d}",
                    "portfolio": {
                        "equity": 1000.0 + index,
                        "initial_cash": 1000.0,
                        "positions": {},
                    },
                },
                timestamp=T0 + timedelta(minutes=index // 2),
            )

    def _request(self, path: str, **params: object) -> tuple[int, object, str]:
        assert self.server.url is not None
        query = urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{self.server.url}/{path}"
        if query:
            url += "?" + query
        try:
            with urlopen(url, timeout=3) as response:
                body = response.read().decode("utf-8")
                return response.status, json.loads(body), body
        except HTTPError as error:
            body = error.read().decode("utf-8")
            try:
                payload: object = json.loads(body)
            except json.JSONDecodeError:
                payload = body
            return error.code, payload, body

    def _page(
        self,
        path: str,
        *,
        expected_page: int,
        expected_size: int,
        expected_total: int,
        **params: object,
    ) -> dict[str, object]:
        status, payload, _ = self._request(path, **params)
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertTrue(COMMON_PAGE_KEYS <= payload.keys())
        self.assertEqual(payload["page"], expected_page)
        self.assertEqual(payload["page_size"], expected_size)
        self.assertEqual(payload["total"], expected_total)
        self.assertEqual(payload["pages"], (expected_total + expected_size - 1) // expected_size)
        self.assertIsInstance(payload["items"], list)
        self.assertLessEqual(len(payload["items"]), expected_size)
        return payload

    def _assert_bad_request(self, path: str, **params: object) -> None:
        status, payload, _ = self._request(path, **params)
        self.assertEqual(status, 400)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertIn("error", payload)


class DashboardPaginationEndpointTests(DashboardPaginationFixture):
    def test_datasets_cover_page_navigation_filters_and_detail_path(self) -> None:
        first = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in first["items"]],
            [f"dataset-{index:02d}" for index in range(10)],
        )

        middle = self._page(
            "api/v2/datasets",
            page=2,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in middle["items"]],
            [f"dataset-{index:02d}" for index in range(10, 20)],
        )

        final = self._page(
            "api/v2/datasets",
            page=3,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in final["items"]],
            [f"dataset-{index:02d}" for index in range(20, 24)] + [self.detail_dataset_id],
        )

        out_of_range = self._page(
            "api/v2/datasets",
            page=4,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in out_of_range["items"]],
            [f"dataset-{index:02d}" for index in range(20, 24)] + [self.detail_dataset_id],
        )

        historical = self._page(
            "api/v2/datasets",
            page=2,
            page_size=10,
            source_type="HISTORICAL",
            timeframe="1d",
            sort="dataset_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual([item["dataset_id"] for item in historical["items"]], ["dataset-20", "dataset-22"])
        self.assertTrue(all(item["source_type"] == "HISTORICAL" for item in historical["items"]))

        crypto = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            market="crypto_spot",
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=17,
        )
        self.assertTrue(all(item["market_type"] == "crypto_spot" for item in crypto["items"]))

        high_quality = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            quality="HIGH",
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=18,
        )
        self.assertTrue(all(item["quality"] == "HIGH" for item in high_quality["items"]))

        status, detail, _ = self._request(f"api/v2/datasets/{quote(self.detail_dataset_id, safe='')}")
        self.assertEqual(status, 200)
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertTrue(detail["available"])
        self.assertEqual(detail["dataset_id"], self.detail_dataset_id)
        self.assertEqual(detail["catalog"]["dataset_id"], self.detail_dataset_id)
        self.assertEqual(detail["catalog"]["dataset_version"], "v1")
        self.assertEqual(detail["catalog"]["row_count"], 1)

    def test_dataset_missing_ranges_are_paginated(self) -> None:
        path = f"api/v2/datasets/{quote('dataset-00', safe='')}/missing-ranges"
        first = self._page(
            path,
            page=1,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in first["items"]], list(range(10)))
        self.assertEqual(first["items"][0]["dataset_id"], "dataset-00")
        self.assertEqual(first["items"][0]["dataset_version"], "v1")
        self.assertEqual(first["items"][0]["range"], first["items"][0]["missing_range"])
        self.assertEqual(first["items"][0]["range"]["start"], "2024-02-01T00:00:00+00:00")

        final = self._page(
            path,
            page=3,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in final["items"]], [20, 21, 22])

        out_of_range = self._page(
            path,
            page=4,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in out_of_range["items"]], [20, 21, 22])

    def test_candidate_lifecycle_events_are_paginated(self) -> None:
        path = f"api/v2/candidates/{quote('candidate-00', safe='')}/events"
        first = self._page(
            path,
            page=1,
            page_size=10,
            sort="event_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual(len(first["items"]), 10)
        self.assertTrue(all(item["candidate_id"] == "candidate-00" for item in first["items"]))
        self.assertTrue({"event_id", "candidate_id", "to_stage", "reason", "payload", "created_at"} <= first["items"][0].keys())

        final = self._page(
            path,
            page=2,
            page_size=10,
            sort="event_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual(len(final["items"]), 2)
        self.assertTrue(all(item["candidate_id"] == "candidate-00" for item in final["items"]))


    def test_candidate_pagination_filters_and_stable_tie_order(self) -> None:
        page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="updated_at",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        self.assertEqual(
            [item["candidate_id"] for item in page["items"]],
            [f"candidate-{index:02d}" for index in range(10)],
        )
        for item in page["items"]:
            self.assertTrue({"candidate_id", "stage", "payload", "updated_at"} <= item.keys())

        validated = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            stage="VALIDATED",
            filter="candidate-1",
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=5,
        )
        self.assertEqual(
            [item["candidate_id"] for item in validated["items"]],
            ["candidate-10", "candidate-12", "candidate-14", "candidate-16", "candidate-18"],
        )
        self.assertTrue(all(item["stage"] == "VALIDATED" for item in validated["items"]))

        descending = self._page(
            "api/v2/candidates",
            page=2,
            page_size=10,
            sort="candidate_id",
            direction="desc",
            expected_page=2,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        self.assertEqual(
            [item["candidate_id"] for item in descending["items"]],
            [f"candidate-{index:02d}" for index in range(12, 2, -1)],
        )
    def test_candidate_statuses_keep_canary_and_paper_lifecycle_distinct(self) -> None:
        page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        eligible_before_paper = next(item for item in page["items"] if item["candidate_id"] == "candidate-02")
        self.assertEqual(eligible_before_paper["stage"], "FROZEN")
        self.assertEqual(eligible_before_paper["historical_gates"], "PASSED")
        self.assertTrue(eligible_before_paper["canary_eligible"])
        self.assertEqual(eligible_before_paper["canary_status"], "ELIGIBLE")
        self.assertFalse(eligible_before_paper["paper_forward"])
        self.assertEqual(eligible_before_paper["paper_forward_status"], "NOT_STARTED")
        self.assertFalse(eligible_before_paper["paper_promotable"])
        self.assertEqual(eligible_before_paper["paper_promotable_status"], "NOT_YET")

        promoted = next(item for item in page["items"] if item["candidate_id"] == "candidate-00")
        self.assertTrue(promoted["paper_forward"])
        self.assertTrue(promoted["paper_promotable"])
        self.assertFalse(promoted["canary_eligible"])

        status, detail, _ = self._request("api/v2/candidates/candidate-02")
        self.assertEqual(status, 200)
        assert isinstance(detail, dict)
        self.assertEqual(detail["historical_gates"], "PASSED")
        self.assertEqual(detail["canary_status"], "ELIGIBLE")
        self.assertEqual(detail["paper_forward_status"], "NOT_STARTED")
        self.assertEqual(detail["paper_promotable_status"], "NOT_YET")

        status, operator, _ = self._request("api/operator")
        self.assertEqual(status, 200)
        assert isinstance(operator, dict)
        self.assertEqual(operator["candidate_status"]["canary_eligible"], 1)
        self.assertEqual(operator["candidate_status"]["paper_forward"], 1)
        self.assertEqual(operator["candidate_status"]["paper_promotable"], 1)
        self.store.connection.execute(
            "UPDATE canary_eligibility SET frozen_hash=? WHERE candidate_id=?",
            ("tampered-frozen-hash", "candidate-02"),
        )
        self.store.connection.commit()
        stale_page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        stale = next(item for item in stale_page["items"] if item["candidate_id"] == "candidate-02")
        self.assertFalse(stale["canary_eligible"])
        self.assertEqual(stale["canary_status"], "NOT_ELIGIBLE")
        status, operator, _ = self._request("api/operator")
        self.assertEqual(status, 200)
        assert isinstance(operator, dict)
        self.assertEqual(operator["candidate_status"]["canary_eligible"], 0)


    @staticmethod
    def _market_value(item: dict[str, object], key: str) -> object:
        if key in item:
            return item[key]
        payload = item.get("payload")
        if isinstance(payload, dict):
            if key in payload:
                return payload[key]
            for nested_key in ("snapshot", "metadata"):
                nested = payload.get(nested_key)
                if isinstance(nested, dict) and key in nested:
                    return nested[key]
        return None

    def test_polymarket_pagination_and_market_filters(self) -> None:
        page = self._page(
            "api/v2/polymarket",
            page=1,
            page_size=10,
            sort="market_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=MARKET_COUNT,
        )
        self.assertEqual(
            [item["market_id"] for item in page["items"]],
            [f"market-{index:02d}" for index in range(10)],
        )
        for item in page["items"]:
            self.assertTrue({"market_id", "payload", "observed_at", "quality"} <= item.keys())

        weather_count = len(range(0, MARKET_COUNT, 3))
        weather = self._page(
            "api/v2/polymarket",
            page=1,
            page_size=10,
            category="weather",
            sort="market_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=weather_count,
        )
        self.assertEqual(
            [item["market_id"] for item in weather["items"]],
            [f"market-{index:02d}" for index in range(0, MARKET_COUNT, 3)],
        )
        self.assertTrue(all(self._market_value(item, "category") == "weather" for item in weather["items"]))

        open_markets = self._page(
            "api/v2/polymarket",
            page=2,
            page_size=10,
            settlement="open",
            quality="ORDER_BOOK_SIMULATED",
            sort="market_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=11,
        )
        self.assertEqual(
            [item["market_id"] for item in open_markets["items"]],
            ["market-22"],
        )
        self.assertTrue(all(self._market_value(item, "settlement") == "open" for item in open_markets["items"]))
        self.assertTrue(all(item["quality"] == "ORDER_BOOK_SIMULATED" for item in open_markets["items"]))

    def test_activity_is_paginated_and_uses_deterministic_tie_breaking(self) -> None:
        page = self._page(
            "api/v2/activity",
            page=1,
            page_size=10,
            filter="dataset-",
            sort="timestamp",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=DATASET_COUNT,
        )
        self.assertTrue(all(item["kind"] == "dataset" for item in page["items"]))
        self.assertEqual(page["items"][0]["details"]["source_type"], "HISTORICAL")
        self.assertIn("dataset-00", page["items"][0]["message"])
        self.assertIn("dataset-01", page["items"][1]["message"])

        final = self._page(
            "api/v2/activity",
            page=3,
            page_size=10,
            filter="dataset-",
            sort="timestamp",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT,
        )
        self.assertEqual([item["message"] for item in final["items"]], [
            "Dataset dataset-20 published (21 rows)",
            "Dataset dataset-21 published (22 rows)",
            "Dataset dataset-22 published (23 rows)",
            "Dataset dataset-23 published (24 rows)",
        ])

    def test_hermes_and_paper_records_are_page_bounded(self) -> None:
        hermes = self._page(
            "api/v2/hermes",
            page=1,
            page_size=25,
            sort="item_id",
            direction="asc",
            expected_page=1,
            expected_size=25,
            expected_total=QUEUE_COUNT,
        )
        self.assertEqual(len(hermes["items"]), QUEUE_COUNT)
        self.assertEqual(hermes["items"][0]["item_id"], "queue-00")
        self.assertTrue(all({"item_id", "item_type", "status", "payload"} <= item.keys() for item in hermes["items"]))

        accepted = self._page(
            "api/v2/hermes",
            page=1,
            page_size=10,
            status="ACCEPTED",
            sort="item_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=1,
        )
        self.assertEqual(accepted["items"][0]["item_id"], "queue-00")
        self.assertEqual(accepted["items"][0]["status"], "ACCEPTED")

        first_paper = self._page(
            "api/v2/paper",
            page=1,
            page_size=10,
            sort="experiment_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=PAPER_COUNT,
        )
        self.assertEqual(
            [item["experiment_id"] for item in first_paper["items"]],
            [f"paper-{index:02d}" for index in range(10)],
        )
        self.assertTrue(all({"record_type", "experiment_id", "timestamp", "status", "payload"} <= item.keys() for item in first_paper["items"]))

        final_paper = self._page(
            "api/v2/paper",
            page=3,
            page_size=10,
            sort="experiment_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=PAPER_COUNT,
        )
        self.assertEqual(
            [item["experiment_id"] for item in final_paper["items"]],
            [f"paper-{index:02d}" for index in range(20, PAPER_COUNT)],
        )

        open_paper = self._page(
            "api/v2/paper",
            page=2,
            page_size=10,
            status="OPEN",
            sort="experiment_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual([item["experiment_id"] for item in open_paper["items"]], ["paper-20", "paper-22"])
        self.assertTrue(all(item["status"] == "OPEN" for item in open_paper["items"]))


    def test_every_ui_sort_column_has_a_supported_paged_endpoint(self) -> None:
        # These are the backend keys emitted by each table's sort buttons.
        # A click must not silently produce an empty page or a server error.
        sortable_columns = {
            "api/v2/datasets": (
                "dataset_id",
                "source_type",
                "market_type",
                "instrument",
                "timeframe",
                "quality",
                "row_count",
                "updated_at",
            ),
            "api/v2/activity": ("timestamp", "kind"),
            "api/v2/candidates": ("candidate_id", "stage", "updated_at"),
            "api/v2/polymarket": ("market_id", "category", "settlement", "quality"),
            "api/v2/hermes": ("item_id", "item_type", "status", "created_at"),
            "api/v2/paper": ("record_type", "experiment_id", "market_id", "status", "timestamp"),
        }
        totals = {
            "api/v2/datasets": DATASET_COUNT + 1,
            "api/v2/activity": ACTIVITY_COUNT,
            "api/v2/candidates": CANDIDATE_COUNT,
            "api/v2/polymarket": MARKET_COUNT,
            "api/v2/hermes": QUEUE_COUNT,
            "api/v2/paper": PAPER_COUNT,
        }
        for endpoint, columns in sortable_columns.items():
            for column in columns:
                page = self._page(
                    endpoint,
                    page=1,
                    page_size=10,
                    sort=column,
                    direction="asc",
                    expected_page=1,
                    expected_size=10,
                    expected_total=totals[endpoint],
                )
                self.assertLessEqual(len(page["items"]), 10)

    def test_all_allowed_page_sizes_return_bounded_numbered_pages(self) -> None:
        for size in (10, 25, 50, 100):
            page = self._page(
                "api/v2/datasets",
                page=1,
                page_size=size,
                sort="dataset_id",
                direction="asc",
                expected_page=1,
                expected_size=size,
                expected_total=DATASET_COUNT + 1,
            )
            self.assertLessEqual(len(page["items"]), size)

class DashboardPaginationSurfaceTests(DashboardPaginationFixture):
    def test_overview_and_list_responses_do_not_embed_unbounded_records(self) -> None:
        status, overview, overview_body = self._request("api/overview")
        self.assertEqual(status, 200)
        self.assertIsInstance(overview, dict)
        assert isinstance(overview, dict)
        self.assertNotIn("items", overview)
        self.assertNotIn("dataset-00", overview_body)
        self.assertLess(len(overview_body), 5000)

        status, datasets, datasets_body = self._request("api/v2/datasets", page=1, page_size=10)
        self.assertEqual(status, 200)
        self.assertIsInstance(datasets, dict)
        assert isinstance(datasets, dict)
        self.assertLessEqual(len(datasets["items"]), 10)
        self.assertLess(len(datasets_body), 50000)
        self.assertNotIn('"records"', datasets_body)

    def test_html_has_paginated_views_url_state_and_responsive_sticky_layout(self) -> None:
        html = _dashboard_html()
        for marker in (
            'id="view-datasets"',
            'id="view-activity"',
            'data-view="datasets"',
            'data-view="activity"',
            "URLSearchParams",
            "page_size",
            "Showing ${start}",
            "windowStart",
            "numbers.map",
            "Previous",
            "Next",
            "select.facet",
            "dataset.param",
            "datasets-market",
            "datasets-timeframe",
            "datasets-quality",
            "polymarket-settlement",
            "polymarket-quality",
            "fetch(",
            "canary_eligible",
            "historical_gates",
            "Historical gates",
            "Micro-live canary",
            "Paper forward status",
            "Paper promotable",
        ):
            self.assertIn(marker, html)
        self.assertRegex(html, r"history\.(?:replaceState|pushState)")
        self.assertRegex(html, r"body\s*\{[^}]*max-width|main\s*\{[^}]*max-width")
        self.assertRegex(html, r"overflow-x\s*:\s*auto")
        self.assertRegex(html, r"header\s*\{[^}]*position\s*:\s*sticky[^}]*top\s*:\s*0")
        self.assertIn("box-sizing: border-box", html)
        # Data is fetched after load; it is not rendered as a giant inline JS
        # literal in the initial HTML document.
        self.assertNotIn("dataset-00", html)
        self.assertNotIn("market-00", html)


if __name__ == "__main__":
    unittest.main()
