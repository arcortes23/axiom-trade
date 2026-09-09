from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

from axiom.canary import CanaryBlocked, CanaryService, CredentialStore, PolymarketClobV2Venue
from axiom.cli import _load_cli_universe, _run_cli_crypto_research, build_parser
from axiom.crypto_universe import TOP_50_MARKET_CAP_BINANCE_USDT, UniverseSnapshot, load_crypto_universe
from axiom.data import InMemoryCryptoProvider
from axiom.domain import MarketType, OHLCVBar
from axiom.experiment_plan import (
    ExperimentPlan,
    ExperimentPlanError,
    forward_market_matches,
    normalize_market_scope,
)
from axiom.market_scope import resolve_market_scope
from axiom.research import run_crypto_research, run_multi_symbol_crypto_research
from axiom.storage import AxiomStore
from axiom.dashboard import DashboardData


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)


class ForwardAuthorityPredicateTests(unittest.TestCase):
    def test_resolution_window_ages_from_expiry_and_supplied_now(self) -> None:
        market = {
            "timestamp": T0.isoformat(),
            "expiry": (T0 + timedelta(hours=2)).isoformat(),
            # This persisted value is the original observation-time window.
            "time_to_expiry_seconds": 2 * 60 * 60,
        }
        filters = {"maximum_hours_to_resolution": 1.5}

        self.assertFalse(forward_market_matches(market, filters, now=T0))
        self.assertTrue(
            forward_market_matches(
                market,
                filters,
                now=T0 + timedelta(hours=1),
            )
        )

    def test_max_spread_uses_collected_quote_spread(self) -> None:
        market = {
            "payload": {
                "quotes": {
                    "yes_bid": 0.46,
                    "yes_ask": 0.50,
                    "yes_spread": 0.04,
                    "no_spread": 0.08,
                }
            }
        }

        self.assertTrue(forward_market_matches(market, {"max_spread": 0.05}, now=T0))
        self.assertFalse(forward_market_matches(market, {"max_spread": 0.03}, now=T0))
    def test_numeric_forward_bounds_reject_text_booleans_and_non_finite_values(self) -> None:
        market = {
            "timestamp": T0,
            "time_to_expiry_seconds": 2 * 60 * 60,
            "yes_mid": 0.5,
            "yes_ask": 0.51,
            "liquidity": 100.0,
            "spread": 0.01,
        }
        invalid_filters = (
            {"entry_price": "0.5"},
            {"entry_price": True},
            {"entry_price": [0.4, float("nan")]},
            {"minimum_hours_to_resolution": float("inf")},
            {"maximum_hours_to_resolution": "2"},
            {"min_liquidity": False},
            {"max_spread": float("-inf")},
        )
        for filters in invalid_filters:
            with self.subTest(filters=filters):
                self.assertFalse(forward_market_matches(market, filters, now=T0))

    def test_target_instrument_matches_only_persisted_instrument_identity(self) -> None:
        self.assertTrue(
            forward_market_matches(
                {"market_id": "market-id", "instrument": " Venue "},
                {},
                target_instrument="venue",
            )
        )
        self.assertTrue(
            forward_market_matches(
                {"market_id": "market-id", "payload": {"metadata": {"symbol": "Venue"}}},
                {},
                target_instrument=" venue ",
            )
        )
        self.assertFalse(
            forward_market_matches(
                {"market_id": "market-id"},
                {},
                target_instrument="market-id",
            )
        )
        self.assertFalse(
            forward_market_matches(
                {"market_id": "market-id", "instrument": "Other"},
                {},
                target_instrument="venue",
            )
        )


    def test_candidate_authority_applies_target_instrument_to_persisted_market(self) -> None:
        plan = {
            "hypothesis_id": "target-instrument",
            "market_type": "prediction",
            "template": "probability_mispricing",
            "dataset_version": "v1",
            "target": {"instrument": "Venue", "market_ids": ["target-market"]},
            "paper_only": True,
        }
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "target-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "instrument": " venue ",
                    "snapshot": {
                        "market_id": "target-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_candidate_lifecycle(
                "candidate-target-instrument",
                "IDEA",
                {"experiment_plan": plan},
                timestamp=T0,
            )
            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-target-instrument"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["resolution"], "RESOLVED")
        self.assertEqual(candidate["market_ids"], ["target-market"])

    def test_filter_discovery_excludes_historical_validation_constituent(self) -> None:
        normalized_plan = ExperimentPlan.from_mapping(
            {
                "hypothesis_id": "historical-filter-authority",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_version": "v1",
                "filters": {"category": "politics"},
                "target": {
                    "instrument": "Venue",
                    "market_ids": ["historical-filter-market"],
                },
                "paper_only": True,
            }
        ).as_dict()
        with AxiomStore(":memory:") as store:
            for market_id in ("historical-filter-market", "current-filter-market"):
                store.save_polymarket_market_metadata(
                    market_id,
                    {
                        "source_type": "FORWARD_COLLECTED",
                        "active": True,
                        "closed": False,
                        "instrument": "Venue",
                        "metadata": {"category": "politics"},
                        "snapshot": {
                            "market_id": market_id,
                            "settlement": "open",
                            "expiry": (T0 + timedelta(days=1)).isoformat(),
                        },
                    },
                    observed_at=T0,
                    source_type="FORWARD_COLLECTED",
                )
            store.save_candidate_lifecycle(
                "candidate-historical-filter",
                "IDEA",
                {
                    "experiment_plan": normalized_plan,
                    "dataset_provenance": {
                        "source_type": "HISTORICAL",
                        "historical_market_ids": ["historical-filter-market"],
                    },
                },
                timestamp=T0,
            )
            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-historical-filter"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["historical_market_ids_ignored"], ["historical-filter-market"])
        self.assertEqual(candidate["market_ids"], ["current-filter-market"])
        self.assertEqual(candidate["permitted_market_ids"], ["current-filter-market"])
        self.assertEqual(requirements["market_ids"], ["current-filter-market"])
        self.assertEqual(candidate["resolution"], "RESOLVED")

    def test_many_candidates_sharing_validation_only_ids_have_empty_authority(self) -> None:
        normalized_plan = ExperimentPlan.from_mapping(
            {
                "hypothesis_id": "shared-validation-authority",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_version": "v1",
                "target": {"market_ids": ["validation-only-market"]},
                "paper_only": True,
            }
        ).as_dict()
        candidate_ids = [f"candidate-shared-{index:02d}" for index in range(40)]
        with AxiomStore(":memory:") as store:
            for candidate_id in candidate_ids:
                store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    {
                        "experiment_plan": normalized_plan,
                        "dataset_provenance": {
                            "source_type": "HISTORICAL",
                            "historical_market_ids": ["validation-only-market"],
                        },
                    },
                    timestamp=T0,
                )
            requirements = store.candidate_forward_requirements(
                candidate_ids=candidate_ids,
                now=T0,
            )

        self.assertEqual(requirements["market_ids"], [])
        self.assertEqual(requirements["candidate_references"], {})
        self.assertEqual(requirements["unresolved_candidates"], candidate_ids)
        self.assertEqual(len(requirements["candidates"]), len(candidate_ids))
        for candidate in requirements["candidates"]:
            self.assertEqual(candidate["resolution"], "UNRESOLVED")
            self.assertEqual(
                candidate["reason_code"],
                "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
            )
            self.assertEqual(
                candidate["historical_market_ids_ignored"],
                ["validation-only-market"],
            )
            self.assertEqual(candidate["market_ids"], [])

    def test_filter_only_candidate_resolves_current_market_without_declared_target(self) -> None:
        normalized_plan = ExperimentPlan.from_mapping(
            {
                "hypothesis_id": "filter-only-current-authority",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_version": "v1",
                "filters": {"category": "politics"},
                "paper_only": True,
            }
        ).as_dict()
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "current-filter-only-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "instrument": "Venue",
                    "metadata": {"category": "politics"},
                    "snapshot": {
                        "market_id": "current-filter-only-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_candidate_lifecycle(
                "candidate-filter-only",
                "IDEA",
                {"experiment_plan": normalized_plan},
                timestamp=T0,
            )
            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-filter-only"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["declared_market_ids"], [])
        self.assertEqual(candidate["resolution"], "RESOLVED")
        self.assertEqual(candidate["market_ids"], ["current-filter-only-market"])
        self.assertEqual(
            requirements["market_ids"],
            ["current-filter-only-market"],
        )

    def test_repeated_exact_targets_share_authority_and_health_diagnostics(self) -> None:
        normalized_plan = ExperimentPlan.from_mapping(
            {
                "hypothesis_id": "repeated-exact-authority",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_version": "v1",
                "target": {"market_ids": ["shared-exact-market"]},
                "paper_only": True,
            }
        ).as_dict()
        candidate_ids = ["candidate-exact-a", "candidate-exact-b"]
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "shared-exact-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "instrument": "Venue",
                    "snapshot": {
                        "market_id": "shared-exact-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_polymarket_snapshot(
                "shared-exact-market:snapshot",
                "shared-exact-market",
                T0,
                T0,
                {
                    "source_type": "FORWARD_COLLECTED",
                    "market_id": "shared-exact-market",
                    "settlement": "open",
                },
                source_type="FORWARD_COLLECTED",
            )
            for candidate_id in candidate_ids:
                store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    {"experiment_plan": normalized_plan},
                    timestamp=T0,
                )
            requirements = store.candidate_forward_requirements(
                candidate_ids=candidate_ids,
                now=T0,
            )
            health = store.polymarket_required_health(
                requirements=requirements,
                now=T0,
            )

        self.assertEqual(requirements["market_ids"], ["shared-exact-market"])
        self.assertEqual(
            requirements["candidate_references"],
            {"shared-exact-market": candidate_ids},
        )
        self.assertEqual(
            requirements["candidate_bound_markets"],
            {"candidate-exact-a": ["shared-exact-market"], "candidate-exact-b": ["shared-exact-market"]},
        )
        for candidate in requirements["candidates"]:
            self.assertEqual(candidate["resolution"], "RESOLVED")
            self.assertEqual(candidate["market_ids"], ["shared-exact-market"])
        self.assertEqual(health["grade"], "A")
        self.assertEqual(health["reason_code"], "REQUIRED_MARKETS_FRESH")
        self.assertEqual(
            health["candidate_references"],
            {"shared-exact-market": candidate_ids},
        )
        self.assertEqual(len(health["market_diagnostics"]), 1)
        self.assertEqual(
            health["market_diagnostics"][0]["candidate_references"],
            candidate_ids,
        )
        self.assertEqual(
            health["market_diagnostics"][0]["collection_state"],
            "fresh",
        )


    def test_normal_serialized_plan_declares_open_closed_and_filtered_targets(self) -> None:
        normalized_plan = ExperimentPlan.from_mapping(
            {
                "hypothesis_id": "declared-targets",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_version": "v1",
                "filters": {"category": "politics"},
                "target": {
                    "market_ids": [
                        "open-market",
                        "closed-market",
                        "filtered-market",
                        "open-market",
                    ]
                },
                "paper_only": True,
            }
        ).as_dict()
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "open-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "metadata": {"category": "politics"},
                    "snapshot": {
                        "market_id": "open-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_polymarket_market_metadata(
                "closed-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": False,
                    "closed": True,
                    "metadata": {"category": "politics"},
                    "snapshot": {
                        "market_id": "closed-market",
                        "settlement": "resolved_yes",
                        "expiry": (T0 - timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_polymarket_market_metadata(
                "filtered-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "metadata": {"category": "sports"},
                    "snapshot": {
                        "market_id": "filtered-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            store.save_candidate_lifecycle(
                "candidate-declared-targets",
                "IDEA",
                {"experiment_plan": normalized_plan},
                timestamp=T0,
            )
            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-declared-targets"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(
            candidate["declared_market_ids"],
            ["open-market", "closed-market", "filtered-market"],
        )
        self.assertEqual(candidate["market_ids"], ["open-market"])
        self.assertEqual(candidate["permitted_market_ids"], ["open-market"])
        self.assertEqual(candidate["resolution"], "RESOLVED")
        self.assertNotIn("closed-market", candidate["market_ids"])
        self.assertNotIn("filtered-market", candidate["market_ids"])

    def test_capacity_excluded_candidate_is_unresolved_without_broad_fallback(self) -> None:
        with AxiomStore(":memory:") as store:
            for market_id in ("admitted-market", "capacity-market"):
                store.save_polymarket_market_metadata(
                    market_id,
                    {
                        "source_type": "FORWARD_COLLECTED",
                        "active": True,
                        "closed": False,
                        "snapshot": {
                            "market_id": market_id,
                            "settlement": "open",
                            "expiry": (T0 + timedelta(days=1)).isoformat(),
                        },
                    },
                    observed_at=T0,
                    source_type="FORWARD_COLLECTED",
                )
            store.save_candidate_lifecycle(
                "candidate-admitted",
                "IDEA",
                {"market_ids": ["admitted-market"]},
                timestamp=T0,
            )
            store.save_candidate_lifecycle(
                "candidate-capacity",
                "IDEA",
                {"market_ids": ["capacity-market"]},
                timestamp=T0,
            )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-admitted", "candidate-capacity"],
                now=T0,
                max_total_markets=1,
            )
            health = store.polymarket_required_health(requirements=requirements, now=T0)

        by_id = {candidate["candidate_id"]: candidate for candidate in requirements["candidates"]}
        self.assertEqual(requirements["market_ids"], ["admitted-market"])
        self.assertEqual(
            requirements["candidate_references"],
            {"admitted-market": ["candidate-admitted"]},
        )
        self.assertEqual(
            requirements["capacity_excluded_candidates"],
            ["candidate-capacity"],
        )
        self.assertEqual(requirements["capacity_excluded_candidate_count"], 1)
        self.assertEqual(by_id["candidate-admitted"]["resolution"], "RESOLVED")
        self.assertEqual(by_id["candidate-capacity"]["resolution"], "UNRESOLVED")
        self.assertEqual(by_id["candidate-capacity"]["reason_code"], "COLLECTOR_CAPACITY_INSUFFICIENT")
        self.assertEqual(by_id["candidate-capacity"]["market_ids"], [])
        self.assertEqual(by_id["candidate-capacity"]["permitted_market_ids"], [])
        self.assertEqual(by_id["candidate-capacity"]["capacity_excluded_market_ids"], ["capacity-market"])
        self.assertEqual(health["reason_code"], "COLLECTOR_CAPACITY_INSUFFICIENT")



    def test_shared_admitted_market_is_retained_for_each_candidate(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "shared-market",
                {
                    "source_type": "FORWARD_COLLECTED",
                    "active": True,
                    "closed": False,
                    "snapshot": {
                        "market_id": "shared-market",
                        "settlement": "open",
                        "expiry": (T0 + timedelta(days=1)).isoformat(),
                    },
                },
                observed_at=T0,
                source_type="FORWARD_COLLECTED",
            )
            for candidate_id in ("candidate-a", "candidate-b"):
                store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    {"market_ids": ["shared-market"]},
                    timestamp=T0,
                )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-a", "candidate-b"],
                now=T0,
                max_total_markets=1,
            )

        self.assertEqual(requirements["market_ids"], ["shared-market"])
        self.assertEqual(
            requirements["candidate_bound_markets"],
            {
                "candidate-a": ["shared-market"],
                "candidate-b": ["shared-market"],
            },
        )
        self.assertEqual(
            requirements["candidate_references"],
            {"shared-market": ["candidate-a", "candidate-b"]},
        )
        self.assertEqual(
            [candidate["market_ids"] for candidate in requirements["candidates"]],
            [["shared-market"], ["shared-market"]],
        )


class _CredentialStore(CredentialStore):
    def __init__(self, configured: bool) -> None:
        self._is_configured = configured

    def configured(self, **_: object) -> bool:
        return self._is_configured

    def load(self, **_: object) -> dict[str, str]:
        if not self._is_configured:
            return {}
        return {
            "private_key": "private-key-fixture",
            "wallet_address": "wallet-fixture",
        }


class _NoCredentialVenue:
    def __init__(self) -> None:
        self.geoblock_calls = 0
        self.authentication_calls = 0
        self.account_calls = 0
        self.balance_calls = 0
        self.market_calls = 0

    @staticmethod
    def installed_sdk_version() -> str:
        return "0.9.2"

    def geoblock(self) -> dict[str, object]:
        self.geoblock_calls += 1
        return {"blocked": False, "close_only": False}

    def connectivity_check(self) -> bool:
        self.authentication_calls += 1
        raise AssertionError("authentication must not run without credentials")

    def account(self) -> dict[str, object]:
        self.account_calls += 1
        raise AssertionError("account must not run without credentials")

    def balance(self) -> Decimal:
        self.balance_calls += 1
        raise AssertionError("balance must not run without credentials")

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        self.market_calls += 1
        raise AssertionError("market lookup must not run without credentials")


class _ExplodingProvider:
    calls = 0

    def historical_ohlcv(self, *_: object, **__: object) -> tuple[object, ...]:
        type(self).calls += 1
        raise AssertionError("provider must not be consulted for an exact binding failure")

    def metadata(self, *_: object, **__: object) -> None:
        type(self).calls += 1
        raise AssertionError("provider metadata must not be consulted for an exact binding failure")


class _SDKClient:
    def __init__(self) -> None:
        self.wallet = "wallet-never-rendered"
        self.wallet_type = "fixture"
        self.market_calls: list[dict[str, object]] = []
        self.book_calls: list[dict[str, object]] = []
        self.balance_calls: list[dict[str, object]] = []
        self.create_calls: list[dict[str, object]] = []
        self.post_calls: list[object] = []
        self.place_calls: list[dict[str, object]] = []
        self.approval_calls: list[tuple[str, dict[str, object]]] = []
        self.update_calls: list[dict[str, object]] = []
        self.call_events: list[str] = []
        self.allowance = "1000000"
        self.allowances: dict[str, str] | None = None
        self.close_calls = 0
        self._ctx = {
            "environment_config": {
                "standard_exchange": "0xstandard",
                "neg_risk_exchange": "0xnegrisk",
                "exchange_v3": "0xexchange-v3",
            }
        }

    def get_market(self, **kwargs: object) -> dict[str, object]:
        self.market_calls.append(kwargs)
        return {
            "version": "v2",
            "state": {"accepting_orders": True},
            "outcomes": {
                "yes": {"label": "Yes", "token_id": "token-yes", "position_id": "position-yes"},
                "no": {"label": "No", "token_id": "token-no", "position_id": "position-no"},
            },
            "trading": {"fee_schedule": {"rate": "0.001"}},
        }

    def get_order_book(self, **kwargs: object) -> dict[str, object]:
        self.book_calls.append(kwargs)
        return {
            "min_order_size": "0.10",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.50", "size": "10"}],
        }

    def get_balance_allowance(self, **kwargs: object) -> dict[str, object]:
        self.balance_calls.append(kwargs)
        self.call_events.append("get_balance_allowance")
        allowances = (
            self.allowances
            if self.allowances is not None
            else {"0xexchange-v3": self.allowance}
        )
        return {
            "balance": "2500000",
            "allowances": allowances,
        }

    def create_limit_order(self, **kwargs: object) -> dict[str, object]:
        self.create_calls.append(kwargs)
        self.call_events.append("create_limit_order")
        return {"maker_amount": "1000000", "token_id": "position-yes", "side": "BUY"}

    def post_order(self, signed: object) -> dict[str, object]:
        self.post_calls.append(signed)
        self.call_events.append("post_order")
        return {"ok": True, "order_id": "mock-order", "status": "submitted", "trade_ids": []}

    def place_limit_order(self, **kwargs: object) -> dict[str, object]:
        self.place_calls.append(kwargs)
        return {"ok": True, "order_id": "unexpected-place", "status": "submitted"}

    def approve_erc20(self, **kwargs: object) -> object:
        self.approval_calls.append(("approve_erc20", kwargs))
        return object()

    def approve_erc1155_for_all(self, **kwargs: object) -> object:
        self.approval_calls.append(("approve_erc1155_for_all", kwargs))
        return object()

    def update_balance_allowance(self, **kwargs: object) -> object:
        self.update_calls.append(kwargs)
        return object()
    def close(self) -> None:
        self.close_calls += 1


class OperationalHealthTests(unittest.TestCase):
    def _snapshot(
        self,
        store: AxiomStore,
        snapshot_id: str,
        observed_at: datetime,
        *,
        source_type: str,
        market_id: str = "market-1",
    ) -> None:
        store.save_polymarket_snapshot(
            snapshot_id,
            market_id,
            observed_at,
            observed_at,
            {
                "source_type": source_type,
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "open",
                    "yes_mid": 0.50,
                    "expiry": (observed_at + timedelta(days=1)).isoformat(),
                },
            },
            quality="ORDER_BOOK_SIMULATED",
            source_type=source_type,
        )

    def _tracked_market(self, store: AxiomStore, observed_at: datetime, market_id: str = "market-1") -> None:
        store.save_polymarket_market_metadata(
            market_id,
            {"market_id": market_id, "active": True, "closed": False, "snapshot": {"settlement": "open"}},
            observed_at=observed_at,
            source_type="FORWARD_COLLECTED",
        )
    def test_empty_market_id_filters_do_not_return_broad_inventory(self) -> None:
        with AxiomStore(":memory:") as store:
            self._tracked_market(store, T0, market_id="tracked-market")

            self.assertEqual(
                store.tracked_polymarket_markets(
                    market_ids=[],
                    now=T0,
                ),
                [],
            )
            self.assertEqual(
                store.tracked_polymarket_markets(
                    market_ids=["", "  "],
                    now=T0,
                ),
                [],
            )


    def test_historical_only_rows_preserve_old_errors_but_current_health_is_f(self) -> None:
        with AxiomStore(":memory:") as store:
            self._snapshot(store, "historical-1", T0 - timedelta(days=2), source_type="HISTORICAL")
            store.save_collection_error(
                "market-1",
                T0 - timedelta(days=2),
                "historical_parse",
                "old malformed payload",
                source_type="HISTORICAL",
            )

            health = store.polymarket_health(
                now=T0,
                expected_interval_seconds=60,
                stale_after_seconds=180,
                recent_window_seconds=300,
            )

            self.assertEqual(health["grade"], "F")
            self.assertEqual(health["reason_code"], "NO_FORWARD_SNAPSHOTS")
            self.assertEqual(health["grade_scope"], "collector_health")
            self.assertEqual(health["collection_errors"], 0)
            self.assertEqual(health["historical_error_count"], 1)
            self.assertEqual(health["evidence_maturity"]["grade_scope"], "research_evidence_maturity")
            self.assertEqual(health["historical_maturity_grade"], health["evidence_maturity"]["grade"])
            self.assertEqual(len(store.list_collection_errors("market-1")), 1)

    def test_fresh_forward_snapshot_is_current_while_maturity_stays_separate(self) -> None:
        with AxiomStore(":memory:") as store:
            self._tracked_market(store, T0)
            self._snapshot(store, "forward-1", T0, source_type="FORWARD_COLLECTED")

            health = store.polymarket_health(
                now=T0 + timedelta(seconds=1),
                expected_interval_seconds=60,
                stale_after_seconds=180,
                recent_window_seconds=300,
            )

            self.assertEqual(health["grade"], "A")
            self.assertIsNone(health["reason_code"])
            self.assertEqual(health["snapshots"], 1)
            self.assertEqual(health["historical_maturity_grade"], health["evidence_maturity"]["grade"])
            self.assertNotEqual(health["grade_scope"], health["evidence_maturity"]["grade_scope"])
    def test_forward_health_source_bound_survives_historical_append(self) -> None:
        with AxiomStore(":memory:") as store:
            self._tracked_market(store, T0)
            self._snapshot(store, "forward-before-history", T0, source_type="FORWARD_COLLECTED")
            store.save_collection_error(
                "market-1",
                T0 - timedelta(days=1),
                "historical_parse",
                "retained historical error",
                source_type="HISTORICAL",
            )
            for index in range(10_001):
                self._snapshot(
                    store,
                    f"historical-after-forward-{index:05d}",
                    T0 - timedelta(days=1),
                    source_type="HISTORICAL",
                )
                store.save_polymarket_market_metadata(
                    "market-1",
                    {
                        "market_id": "market-1",
                        "active": False,
                        "closed": True,
                        "history_index": index,
                    },
                    observed_at=T0 - timedelta(days=1),
                    source_type="HISTORICAL",
                )

            health = store.polymarket_health(
                now=T0 + timedelta(seconds=1),
                expected_interval_seconds=60,
                stale_after_seconds=180,
                recent_window_seconds=300,
            )

            self.assertEqual(health["grade"], "A")
            self.assertEqual(health["snapshots"], 1)
            self.assertEqual(health["markets_with_snapshots"], 1)
            self.assertEqual(health["metadata_records"], 1)
            self.assertEqual(health["collection_errors"], 0)
            self.assertEqual(health["historical_error_count"], 1)
            audit = store.list_collection_errors("market-1")
            self.assertEqual(len(audit), 1)
            self.assertEqual(audit[0]["source_type"], "HISTORICAL")

    def test_forward_health_time_window_keeps_early_market_after_large_append(self) -> None:
        with AxiomStore(":memory:") as store:
            early_market = "early-market"
            late_market = "late-market"
            self._tracked_market(store, T0, early_market)
            self._tracked_market(store, T0, late_market)
            self._snapshot(store, "early-forward", T0, source_type="FORWARD_COLLECTED", market_id=early_market)
            for index in range(10_001):
                self._snapshot(
                    store,
                    f"late-forward-{index:05d}",
                    T0,
                    source_type="FORWARD_COLLECTED",
                    market_id=late_market,
                )

            health = store.polymarket_health(
                now=T0 + timedelta(seconds=1),
                expected_interval_seconds=60,
                stale_after_seconds=180,
                recent_window_seconds=300,
            )

            self.assertEqual(health["grade"], "A")
            self.assertIsNone(health["reason_code"])
            self.assertNotIn("NO_FORWARD_SNAPSHOTS", {item["code"] for item in health["reasons"]})
            self.assertEqual(health["snapshots"], 10_002)
            self.assertEqual(health["markets_with_snapshots"], 2)
            self.assertEqual(health["stale_markets"], [])



    def test_trade_count_is_bounded_to_current_window_while_maturity_stays_historical(self) -> None:
        with AxiomStore(":memory:") as store:
            old_market = "historical-market"
            current_market = "current-market"
            self._snapshot(
                store,
                "historical-trade-market",
                T0 - timedelta(days=2),
                source_type="HISTORICAL",
                market_id=old_market,
            )
            self._snapshot(
                store,
                "current-trade-market",
                T0 - timedelta(seconds=1),
                source_type="FORWARD_COLLECTED",
                market_id=current_market,
            )
            store.save_polymarket_trade(
                old_market,
                {"timestamp": (T0 - timedelta(days=2)).isoformat(), "trade_id": "historical-trade"},
            )
            store.save_polymarket_trade(
                current_market,
                {"timestamp": (T0 - timedelta(seconds=1)).isoformat(), "trade_id": "current-trade"},
            )
            store.save_polymarket_trade(
                current_market,
                {"timestamp": (T0 + timedelta(seconds=1)).isoformat(), "trade_id": "future-trade"},
            )

            health = store.polymarket_health(
                now=T0,
                expected_interval_seconds=60,
                stale_after_seconds=180,
                recent_window_seconds=300,
            )

            self.assertEqual(health["trades"], 1)
            self.assertEqual(health["collector_health"]["trades"], 1)
            self.assertEqual(health["evidence_maturity"]["trade_markets"], 2)


    def test_stale_tracked_market_and_recent_malformed_error_degrade(self) -> None:
        with AxiomStore(":memory:") as store:
            old = T0 - timedelta(seconds=120)
            self._tracked_market(store, old)
            self._snapshot(store, "stale-1", old, source_type="FORWARD_COLLECTED")
            store.save_collection_error(
                "market-1", T0 - timedelta(seconds=1), "malformed_record", "bad current record", source_type="FORWARD_COLLECTED"
            )

            health = store.polymarket_health(
                now=T0,
                expected_interval_seconds=30,
                stale_after_seconds=60,
                recent_window_seconds=30,
            )
            codes = {item["code"] for item in health["reasons"]}

            self.assertEqual(health["grade"], "D")
            self.assertEqual(health["stale_markets"], ["market-1"])
            self.assertEqual(health["collector_health"]["malformed_records"], 1)
            self.assertIn("STALE_MARKETS", codes)
            self.assertIn("MALFORMED_RECORDS", codes)
            self.assertIn("CURRENT_COLLECTION_FAILURES", codes)
            self.assertEqual(health["collection_errors"], 1)

    def test_fresh_cycle_recovers_current_health_without_erasing_old_error(self) -> None:
        with AxiomStore(":memory:") as store:
            old = T0 - timedelta(seconds=120)
            self._tracked_market(store, old)
            self._snapshot(store, "before-recovery", old, source_type="FORWARD_COLLECTED")
            store.save_collection_error(
                "market-1", old, "malformed_record", "retained old error", source_type="FORWARD_COLLECTED"
            )
            before = store.polymarket_health(now=T0, expected_interval_seconds=30, stale_after_seconds=60, recent_window_seconds=30)
            self.assertEqual(before["grade"], "D")

            self._snapshot(store, "after-recovery", T0, source_type="FORWARD_COLLECTED")
            after = store.polymarket_health(now=T0, expected_interval_seconds=30, stale_after_seconds=60, recent_window_seconds=30)

            self.assertEqual(after["grade"], "A")
            self.assertIsNone(after["reason_code"])
            self.assertEqual(after["collection_errors"], 0)
            self.assertEqual(len(store.list_collection_errors("market-1")), 1)


class CanaryReadinessTests(unittest.TestCase):
    def test_no_credential_connectivity_fails_closed_without_wallet_or_signer(self) -> None:
        with AxiomStore(":memory:") as store:
            venue = _NoCredentialVenue()
            service = CanaryService(store, credentials=_CredentialStore(False), clock=lambda: T0)
            result = service.connectivity_check(candidate_id=None, venue=venue, market_id="market-1", token_id="yes")

            self.assertFalse(result["ready"])
            self.assertIn("CREDENTIALS_NOT_CONFIGURED", result["failures"])
            self.assertEqual(result["diagnostics"]["authentication"]["reason"], "CREDENTIALS_NOT_CONFIGURED")
            self.assertEqual(result["diagnostics"]["account"]["reason"], "CREDENTIALS_NOT_CONFIGURED")
            self.assertEqual(venue.authentication_calls, 0)
            self.assertEqual(venue.account_calls, 0)
            self.assertEqual(venue.balance_calls, 0)
            self.assertEqual(venue.market_calls, 0)
            rendered = json.dumps(result, sort_keys=True).lower()
            self.assertNotIn("wallet", rendered)
            self.assertNotIn("signer", rendered)
    def _seed_official_submission_fixture(self, store: AxiomStore, service: CanaryService) -> str:
        """Seed the canonical prediction candidate required by official submit."""
        dataset_id = "prediction-history"
        dataset_version = "v1"
        market_id = "market-1"
        source_timestamp = T0
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
            "strategy_id": "candidate-1",
        }
        model_document = {"probability": 0.80}
        market_scope = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": [market_id],
                "filters": {},
                "provenance": "canonical",
            }
        )
        experiment_plan = ExperimentPlan.from_mapping(
            {
                "plan_id": "official-submit-plan",
                "hypothesis_id": "official-submit-hypothesis",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "market_scope": market_scope.as_dict(),
                "dataset_selector": {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "source_type": "HISTORICAL",
                },
                "strategy_document": strategy_document,
                "model_document": model_document,
                "paper_only": True,
                "min_samples": 30,
                "min_trades": 10,
                "max_variants": 1,
            }
        )
        experiment_plan_payload = experiment_plan.as_dict()
        strategy_document = dict(experiment_plan_payload["strategy_document"])
        model_document = dict(experiment_plan_payload["model_document"])
        strategy_hash = service._document_hash(strategy_document)
        model_hash = service._document_hash(model_document)
        self.assertIsNotNone(strategy_hash)
        self.assertIsNotNone(model_hash)
        config_hash = "sha256:official-submit-config"
        frozen_hash = hashlib.sha256(
            f"{strategy_hash}|{model_hash}|{config_hash}".encode("utf-8")
        ).hexdigest()
        rows = [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}]
        store.save_dataset(
            dataset_id,
            dataset_version,
            rows,
            metadata={
                "provider": "polymarket-fixture",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
            quality="PRICE_PROXY",
        )
        store.save_dataset_catalog(
            dataset_id,
            dataset_version,
            provider="polymarket-fixture",
            instrument="POLYMARKET",
            market_type=MarketType.PREDICTION,
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=len(rows),
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{dataset_id}:{dataset_version}",
            metadata={
                "provider": "polymarket-fixture",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
            created_at=T0,
            updated_at=T0,
        )
        dataset_attestation = store.verify_dataset_integrity_attestation(
            dataset_id,
            dataset_version,
            force=True,
        )
        payload = {
            "candidate_id": "candidate-1",
            "strategy_id": "candidate-1",
            "experiment_family": "official-submit",
            "market_type": "prediction",
            "instrument": "POLYMARKET",
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "dataset_selector": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
            },
            "dataset_provenance": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "dataset_attestation": dataset_attestation,
            "experiment_plan": experiment_plan_payload,
            "plan_hash": experiment_plan.plan_hash,
            "market_scope": market_scope.as_dict(),
            "market_scope_hash": market_scope.scope_hash,
            "market_scope_version": market_scope.scope_version,
            "market_ids": [market_id],
            "target_market_ids": [market_id],
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "frozen": True,
            "holdout_used": False,
            "data_quality": "PRICE_PROXY",
            "validation_expectancy": 0.10,
            "validation_confidence_lower_bound": 0.05,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "sample_count": 100,
            "trade_count": 50,
            "validation_execution_quality": 0.90,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "min_observations": 30,
                "min_trades": 10,
                "checks": {"sample_count": True, "trade_count": True},
            },
            "strategy_document": strategy_document,
            "model_document": model_document,
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
        }
        store.save_candidate_lifecycle("candidate-1", "IDEA", payload, timestamp=T0)
        store.save_candidate_lifecycle("candidate-1", "FROZEN", payload, timestamp=T0)
        market_expiry = (T0 + timedelta(days=1)).isoformat()
        store.save_polymarket_market_metadata(
            market_id,
            {
                "source_type": "FORWARD_COLLECTED",
                "instrument": "POLYMARKET",
                "venue": "POLYMARKET",
                "market_type": "prediction",
                "active": True,
                "closed": False,
                "metadata": {
                    "instrument": "POLYMARKET",
                    "category": "politics",
                    "active": True,
                    "closed": False,
                },
                "snapshot": {
                    "market_id": market_id,
                    "condition_id": "condition-1",
                    "yes_token_id": "yes",
                    "no_token_id": "no",
                    "token_ids": {"yes": "yes", "no": "no"},
                    "category": "politics",
                    "settlement": "open",
                    "expiry": market_expiry,
                    "active": True,
                    "closed": False,
                    "accepting_orders": True,
                    "enable_order_book": True,
                },
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )
        book_timestamp = source_timestamp.isoformat()
        current_snapshot = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": {
                "market_id": market_id,
                "condition_id": "condition-1",
                "timestamp": book_timestamp,
                "yes_mid": "0.50",
                "yes_ask": "0.50",
                "no_mid": "0.50",
                "no_ask": "0.50",
                "yes_token_id": "yes",
                "no_token_id": "no",
                "token_ids": {"yes": "yes", "no": "no"},
                "yes_order_book": {
                    "asks": [{"price": "0.50", "size": "100"}],
                    "bids": [{"price": "0.49", "size": "100"}],
                    "timestamp": book_timestamp,
                    "token_id": "yes",
                },
                "no_order_book": {
                    "asks": [{"price": "0.50", "size": "100"}],
                    "bids": [{"price": "0.49", "size": "100"}],
                    "timestamp": book_timestamp,
                    "token_id": "no",
                },
                "settlement": "open",
                "expiry": market_expiry,
                "active": True,
                "closed": False,
                "accepting_orders": True,
                "enable_order_book": True,
            },
            "active": True,
            "closed": False,
            "settlement": "open",
        }
        store.save_polymarket_snapshot(
            "official-submit-snapshot",
            market_id,
            source_timestamp,
            T0,
            current_snapshot,
            quality="ORDER_BOOK_SIMULATED",
            source_type="FORWARD_COLLECTED",
        )
        resolution = resolve_market_scope(
            "candidate-1",
            {"market_scope": market_scope.as_dict()},
            [
                {
                    "market_id": market_id,
                    "condition_id": "condition-1",
                    "yes_token_id": "yes",
                    "no_token_id": "no",
                    "instrument": "POLYMARKET",
                    "venue": "POLYMARKET",
                    "source_type": "CURRENT",
                    "active": True,
                    "open": True,
                    "closed": False,
                    "settlement": "open",
                    "accepting_orders": True,
                    "enable_order_book": True,
                    "metadata": {"category": "politics"},
                    "metadata_provenance": {
                        "source_type": "CURRENT",
                        "metadata_hash": "sha256:market-1",
                    },
                }
            ],
            resolved_at=T0,
        )
        store.save_market_scope_resolution(resolution)
        service.mark_eligible("candidate-1")
        with patch.object(store, "polymarket_health", return_value={"grade": "A"}):
            signal = service.generate_signal("candidate-1")
        self.assertIsInstance(signal, dict)
        self.assertEqual(signal.get("status"), "READY")
        return str(signal["signal_id"])


    def _submit_official_fixture(self, client: _SDKClient) -> None:
        secure_factory = MagicMock(spec=["_create"])
        secure_factory._create.return_value = client
        credentials = _CredentialStore(True)
        venue = PolymarketClobV2Venue()
        sdk_module = MagicMock()
        sdk_module.SecureClient = secure_factory
        snapshot = {
            "micro_live_canary": "ARMED",
            "candidate": "candidate-1",
            "expiry": (T0 + timedelta(hours=1)).isoformat(),
            "control_generation": 1,
            "today_orders": 0,
            "open_positions": 0,
            "today_realized_pnl": 0.0,
            "total_exposure": 0.0,
            "limits": {
                "target_notional_usd": "1.00",
                "max_exposure_usd": "5.00",
                "max_daily_loss_usd": "2.00",
                "max_open_positions": 3,
                "max_orders_per_day": 5,
                "max_slippage_bps": 100,
            },
        }
        context = {
            "asset_id": "position-yes",
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }
        with patch.dict(sys.modules, {"polymarket": sdk_module}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.2",
        ), patch.object(
            CredentialStore,
            "load",
            return_value=credentials.load(),
        ), patch.object(
            PolymarketClobV2Venue,
            "geoblock",
            return_value={"blocked": False, "close_only": False},
        ), AxiomStore(":memory:") as store:
            service = CanaryService(store, credentials=credentials, clock=lambda: T0)
            signal_id = self._seed_official_submission_fixture(store, service)
            expiry = (T0 + timedelta(hours=1)).isoformat()
            store.connection.execute(
                "INSERT INTO canary_control("
                "singleton,state,candidate_id,venue,armed_at,expires_at,limits_json,"
                "integrity_hash,updated_at,control_generation) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    1,
                    "ARMED",
                    "candidate-1",
                    "polymarket",
                    T0.isoformat(),
                    expiry,
                    json.dumps(snapshot["limits"], sort_keys=True),
                    service._integrity(
                        "candidate-1",
                        "polymarket",
                        expiry,
                        snapshot["limits"],
                    ),
                    T0.isoformat(),
                    1,
                ),
            )
            store.connection.commit()
            with patch.object(service, "authoritative_status", return_value=snapshot), patch.object(
                store,
                "polymarket_health",
                return_value={"grade": "A"},
            ), patch.object(
                PolymarketClobV2Venue,
                "market_context",
                return_value=context,
            ):
                service.submit(
                    signal_id=signal_id,
                    candidate_id="candidate-1",
                    market_id="market-1",
                    token_id="yes",
                    side="BUY",
                    paper_expected_price=Decimal("0.50"),
                    venue=venue,
                )

    def test_official_insufficient_allowance_never_posts_or_approves(self) -> None:
        for allowances in (
            {"0xexchange-v3": "999999"},
            {"0xother-spender": "100000000"},
            {},
        ):
            client = _SDKClient()
            client.allowances = allowances
            with self.assertRaisesRegex(
                CanaryBlocked,
                "CANARY_ALLOWANCE_INSUFFICIENT",
            ):
                self._submit_official_fixture(client)

            self.assertEqual(len(client.create_calls), 1)
            self.assertEqual(client.post_calls, [])
            self.assertEqual(client.place_calls, [])
            self.assertEqual(client.approval_calls, [])
            self.assertEqual(client.update_calls, [])
            self.assertEqual(
                client.call_events[-2:],
                ["create_limit_order", "get_balance_allowance"],
            )
            self.assertGreater(client.close_calls, 0)

    def test_official_allowance_targets_are_protocol_specific(self) -> None:
        from axiom.canary import _order_balance_allowance_target

        self.assertEqual(
            _order_balance_allowance_target(
                side="BUY",
                asset_id="collateral-not-used",
                market_version="v1",
            ),
            ("COLLATERAL", None),
        )
        self.assertEqual(
            _order_balance_allowance_target(
                side="SELL",
                asset_id="legacy-token",
                market_version="v1",
            ),
            ("CONDITIONAL", "legacy-token"),
        )
        self.assertEqual(
            _order_balance_allowance_target(
                side="SELL",
                asset_id="v2-position",
                market_version="v2",
            ),
            ("CONDITIONAL-V2", "v2-position"),
        )

    def test_official_venue_uses_read_only_asset_aware_sdk_calls(self) -> None:
        client = _SDKClient()
        secure_factory = MagicMock(spec=["_create"])
        secure_factory._create.return_value = client
        credentials = _CredentialStore(True)
        venue = PolymarketClobV2Venue()
        sdk_module = MagicMock()
        sdk_module.SecureClient = secure_factory
        with patch.dict(sys.modules, {"polymarket": sdk_module}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.2",
        ), patch.object(
            CredentialStore,
            "load",
            return_value=credentials.load(),
        ), patch.object(
            PolymarketClobV2Venue,
            "geoblock",
            return_value={"blocked": False, "close_only": False},
        ):
            with AxiomStore(":memory:") as store:
                result = CanaryService(store, credentials=credentials, clock=lambda: T0).connectivity_check(
                    candidate_id=None, venue=venue, market_id="market-1", token_id="yes"
                )

            self.assertIn("market", result["diagnostics"])
            self.assertEqual(client.market_calls, [{"id": "market-1"}])
            self.assertEqual(client.book_calls, [{"asset_id": "position-yes"}])
            self.assertEqual(client.balance_calls, [{"asset_type": "COLLATERAL"}, {"asset_type": "COLLATERAL"}])
            self.assertEqual(client.post_calls, [])
            self.assertEqual(result["diagnostics"]["market"]["asset_id"], "position-yes")
            self.assertEqual(secure_factory._create.call_count, 4)
            secure_factory._create.assert_called_with(
                private_key="private-key-fixture",
                wallet="wallet-fixture",
                validate_credentials=True,
            )

            self.assertFalse(hasattr(venue, "submit_limit_order"))
            self.assertEqual(client.post_calls, [])
            self.assertFalse(hasattr(venue, "_bind_service_capability"))
            self.assertFalse(hasattr(venue, "_secure_client"))
            self.assertFalse(hasattr(venue, "_submit_limit_order"))
            self.assertFalse(hasattr(venue, "_read_only"))
            self.assertEqual(client.post_calls, [])
            self.assertFalse(hasattr(venue, "_client"))
            self.assertFalse(
                any("place_limit_order" in name for name in dir(venue))
            )
            self.assertFalse(hasattr(CanaryService, "_submit_official_order"))
            with AxiomStore(":memory:") as store:
                service = CanaryService(store, credentials=credentials, clock=lambda: T0)
                signal_id = self._seed_official_submission_fixture(store, service)
                snapshot = {
                    "micro_live_canary": "ARMED",
                    "candidate": "candidate-1",
                    "expiry": (T0 + timedelta(hours=1)).isoformat(),
                    "control_generation": 1,
                    "today_orders": 0,
                    "open_positions": 0,
                    "today_realized_pnl": 0.0,
                    "total_exposure": 0.0,
                    "limits": {
                        "target_notional_usd": "1.00",
                        "max_exposure_usd": "5.00",
                        "max_daily_loss_usd": "2.00",
                        "max_open_positions": 3,
                        "max_orders_per_day": 5,
                        "max_slippage_bps": 100,
                    },
                }
                context = {
                    "asset_id": "position-yes",
                    "market_version": "v2",
                    "neg_risk": False,
                    "accepting_orders": True,
                    "min_order_size": "1",
                    "tick_size": "0.01",
                    "bids": [{"price": "0.49", "size": "100"}],
                    "asks": [{"price": "0.50", "size": "100"}],
                    "fee_bps": "10",
                }
                store.connection.execute(
                    "INSERT INTO canary_control("
                    "singleton,state,candidate_id,venue,armed_at,expires_at,limits_json,"
                    "integrity_hash,updated_at,control_generation) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        1,
                        "ARMED",
                        "candidate-1",
                        "polymarket",
                        T0.isoformat(),
                        snapshot["expiry"],
                        json.dumps(snapshot["limits"], sort_keys=True),
                        service._integrity(
                            "candidate-1",
                            "polymarket",
                            snapshot["expiry"],
                            snapshot["limits"],
                        ),
                        T0.isoformat(),
                        1,
                    ),
                )
                store.connection.commit()
                with patch.object(service, "authoritative_status", return_value=snapshot), patch.object(
                    store,
                    "polymarket_health",
                    return_value={"grade": "A"},
                ), patch.object(
                    PolymarketClobV2Venue,
                    "geoblock",
                    return_value={"blocked": False, "close_only": False},
                ), patch.object(PolymarketClobV2Venue, "market_context", return_value=context):
                    service.submit(
                        signal_id=signal_id,
                        candidate_id="candidate-1",
                        market_id="market-1",
                        token_id="yes",
                        side="BUY",
                        paper_expected_price=Decimal("0.50"),
                        venue=venue,
                    )
            self.assertEqual(
                client.create_calls,
                [
                    {
                        "asset_id": "position-yes",
                        "side": "BUY",
                        "price": "0.50",
                        "size": "2.00",
                    }
                ],
            )
            self.assertEqual(len(client.post_calls), 1)
            self.assertEqual(client.post_calls[0]["maker_amount"], "1000000")
            self.assertEqual(
                client.balance_calls[-1],
                {"asset_type": "COLLATERAL"},
            )
            self.assertEqual(
                client.call_events[-3:],
                ["create_limit_order", "get_balance_allowance", "post_order"],
            )
            self.assertEqual(client.place_calls, [])
            self.assertEqual(client.approval_calls, [])
            self.assertEqual(client.update_calls, [])
            self.assertEqual(client.close_calls, 6)
    def test_official_venue_rejects_custom_geoblock_url(self) -> None:
        with self.assertRaises(TypeError):
            PolymarketClobV2Venue(
                geoblock_url="https://attacker.invalid/geoblock"
            )

    def test_official_venue_fails_closed_without_read_only_constructor(self) -> None:
        relayer_factory = MagicMock(return_value={"key": "api-key"})
        secure_factory = MagicMock(spec=["create"])
        secure_factory.create.return_value = _SDKClient()
        credentials = _CredentialStore(True)
        venue = PolymarketClobV2Venue()
        sdk_module = MagicMock()
        sdk_module.RelayerApiKey = relayer_factory
        sdk_module.SecureClient = secure_factory
        with patch.dict(sys.modules, {"polymarket": sdk_module}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.2",
        ), patch.object(
            CredentialStore,
            "load",
            return_value=credentials.load(),
        ):
            with self.assertRaises(CanaryBlocked) as raised:
                venue.connectivity_check()

        self.assertEqual(str(raised.exception), "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE")
        secure_factory.create.assert_not_called()
        relayer_factory.assert_not_called()


class ExactBindingTests(unittest.TestCase):
    @staticmethod
    def _plan(**overrides: object) -> dict[str, object]:
        value: dict[str, object] = {
            "hypothesis_id": "exact-binding",
            "market_type": "crypto_spot",
            "template": "momentum",
            "target": {"instrument": "BTC/USDT"},
            "dataset_id": "crypto:BTCUSDT",
            "dataset_version": "fixture-v1",
            "dataset_timeframe": "1h",
            "dataset_source": "fixture",
            "dataset_source_type": "HISTORICAL",
            "survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT",
            "universe": {
                "universe_id": "spot-major",
                "universe_version": "spot-major-v1",
                "snapshot_hash": "sha256:spot-major-v1",
                "methodology": "fixed fixture",
                "instruments": ["BTC/USDT"],
            },
            "allowed_features": ["timestamp", "close"],
            "parameters": {"lookback": [5], "threshold": [0.02]},
            "metrics": ["total_return"],
            "min_samples": 1,
            "max_variants": 1,
            "paper_only": True,
        }
        value.update(overrides)
        return value

    def test_latest_missing_and_mismatched_dataset_bindings_reject_before_provider(self) -> None:
        provider = _ExplodingProvider()
        with AxiomStore(":memory:") as store:
            with self.assertRaisesRegex(ValueError, "rejects latest"):
                run_crypto_research(
                    provider,
                    symbol="BTC/USDT",
                    store=store,
                    dataset_id="crypto:BTCUSDT",
                    dataset_version="latest",
                    timeframe="1d",
                    source_type="HISTORICAL",
                )
            with self.assertRaisesRegex(ValueError, "persisted dataset catalog is required"):
                run_crypto_research(
                    provider,
                    symbol="BTC/USDT",
                    store=store,
                    dataset_id="missing",
                    dataset_version="fixture-v1",
                    timeframe="1d",
                    source_type="HISTORICAL",
                )
            store.save_dataset(
                "crypto:BTCUSDT",
                "fixture-v1",
                [],
                metadata={"instrument": "ETH/USDT", "timeframe": "1d", "source_type": "HISTORICAL"},
            )
            with self.assertRaisesRegex(ValueError, "persisted dataset catalog is required"):
                run_crypto_research(
                    provider,
                    symbol="BTC/USDT",
                    store=store,
                    dataset_id="crypto:BTCUSDT",
                    dataset_version="fixture-v1",
                    timeframe="1d",
                    source_type="HISTORICAL",
                )
            store.save_dataset_catalog(
                "crypto:BTCUSDT",
                "fixture-v1",
                provider="fixture",
                instrument="ETH/USDT",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1d",
                row_count=0,
                completeness=0.0,
                quality="OHLCV",
                source_type="HISTORICAL",
                snapshot_id="crypto:BTCUSDT:fixture-v1",
            )
            with self.assertRaisesRegex(ValueError, "instrument does not match"):
                run_crypto_research(
                    provider,
                    symbol="BTC/USDT",
                    store=store,
                    dataset_id="crypto:BTCUSDT",
                    dataset_version="fixture-v1",
                    timeframe="1d",
                    source_type="HISTORICAL",
                )

        self.assertEqual(provider.calls, 0)

    def test_latest_universe_version_is_rejected_as_non_immutable(self) -> None:
        with self.assertRaisesRegex(ExperimentPlanError, "universe_version must be immutable") as raised:
            ExperimentPlan.from_mapping(self._plan(universe={"universe_id": "spot-major", "universe_version": "latest", "snapshot_hash": "hash", "methodology": "fixed", "instruments": ["BTC/USDT"]}))
        self.assertEqual(raised.exception.reason, "INSUFFICIENT_DATA")


class MultiSymbolProvenanceTests(unittest.TestCase):
    @staticmethod
    def _bars(symbol: str) -> list[OHLCVBar]:
        rows: list[OHLCVBar] = []
        for index in range(12):
            close = 100.0 + index if symbol.startswith("BTC") else 120.0 - index
            rows.append(
                OHLCVBar(
                    timestamp=T0 + timedelta(days=index),
                    open=close - 0.25,
                    high=close + 1.0,
                    low=close - 1.0,
                    close=close,
                    volume=100.0 + index,
                )
            )
        return rows
    @staticmethod
    def _persist_universe(store: AxiomStore, symbols: list[str], version: str = "universe-v7") -> None:
        metadata = {
            "universe_id": "spot-major",
            "snapshot_hash": f"sha256:{version}",
            "methodology": "fixed ranked fixture",
            "survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT",
            "point_in_time": True,
        }
        rows = [{"symbol": symbol, "selected": True} for symbol in symbols]
        store.save_dataset("universe:spot-major", version, rows, metadata=metadata, quality="HIGH")
        store.save_dataset_catalog(
            "universe:spot-major",
            version,
            provider="fixture",
            instrument="spot-major",
            market_type=MarketType.CRYPTO_SPOT,
            timeframe="point_in_time",
            row_count=len(rows),
            completeness=1.0,
            quality="HIGH",
            source_type="FORWARD_COLLECTED",
            snapshot_id=f"spot-major:{version}",
            metadata=metadata,
        )

    def test_report_persists_exact_per_symbol_dataset_and_universe_provenance(self) -> None:
        symbols = ["BTC/USDT", "ETH/USDT"]
        universe = {
            "universe_id": "spot-major",
            "universe_version": "universe-v7",
            "snapshot_hash": "sha256:universe-v7",
            "methodology": "fixed ranked fixture",
            "survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT",
            "selected_symbols": symbols,
        }
        with AxiomStore(":memory:") as store:
            universe_metadata = {
                "universe_id": "spot-major",
                "snapshot_hash": "sha256:universe-v7",
                "methodology": "fixed ranked fixture",
                "survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT",
                "point_in_time": True,
            }
            universe_rows = [{"symbol": symbol, "selected": True} for symbol in symbols]
            store.save_dataset("universe:spot-major", "universe-v7", universe_rows, metadata=universe_metadata, quality="HIGH")
            store.save_dataset_catalog(
                "universe:spot-major",
                "universe-v7",
                provider="fixture",
                instrument="spot-major",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="point_in_time",
                row_count=len(universe_rows),
                completeness=1.0,
                quality="HIGH",
                source_type="FORWARD_COLLECTED",
                snapshot_id="spot-major:universe-v7",
                metadata=universe_metadata,
            )
            dataset_ids: dict[str, str] = {}
            for symbol in symbols:
                dataset_id = f"crypto:{symbol.replace('/', '')}"
                dataset_ids[symbol] = dataset_id
                bars = self._bars(symbol)
                store.save_dataset(dataset_id, "v1", bars, metadata={"instrument": symbol, "timeframe": "1d", "source_type": "HISTORICAL"})
                store.save_bars(symbol, bars, dataset_id=dataset_id, dataset_version="v1")
                store.save_dataset_catalog(
                    dataset_id,
                    "v1",
                    provider="fixture",
                    instrument=symbol,
                    market_type=MarketType.CRYPTO_SPOT,
                    timeframe="1d",
                    row_count=len(bars),
                    completeness=1.0,
                    quality="OHLCV",
                    source_type="HISTORICAL",
                    snapshot_id=f"{dataset_id}:v1",
                    metadata={"survivorship_bias": "SURVIVORSHIP_BIAS_PRESENT"},
                )

            report = run_multi_symbol_crypto_research(
                symbols=symbols,
                providers=None,
                store=store,
                dataset_id=dataset_ids,
                dataset_version={symbol: "v1" for symbol in symbols},
                timeframe={symbol: "1d" for symbol in symbols},
                source_type={symbol: "HISTORICAL" for symbol in symbols},
                universe_provenance=universe,
            )
            persisted = store.list_reports(experiment_id="spot-major")

            self.assertEqual(len(persisted), 1)
            self.assertEqual(persisted[0]["report"], report)
            self.assertEqual(report["universe_provenance"], universe)
            for symbol in symbols:
                result = report["per_symbol"][symbol]
                provenance = result["dataset_provenance"]
                self.assertEqual(result["dataset_id"], dataset_ids[symbol])
                self.assertEqual(result["dataset_version"], "v1")
                self.assertEqual(result["timeframe"], "1d")
                self.assertEqual(result["source_type"], "HISTORICAL")
                self.assertEqual(provenance["dataset_id"], dataset_ids[symbol])
                self.assertEqual(provenance["instrument"], symbol)
                self.assertEqual(provenance["timeframe"], "1d")
                self.assertEqual(provenance["source_type"], "HISTORICAL")
    def test_forged_universe_claims_are_rejected_before_provider_access(self) -> None:
        symbols = ["BTC/USDT", "ETH/USDT"]
        with AxiomStore(":memory:") as store:
            self._persist_universe(store, symbols)
            forged = {
                "universe_id": "spot-major",
                "universe_version": "universe-v7",
                "snapshot_hash": "sha256:forged",
                "selected_symbols": symbols,
            }
            with self.assertRaisesRegex(ValueError, "snapshot_hash does not match"):
                run_crypto_research(
                    _ExplodingProvider(),
                    symbol="BTC/USDT",
                    store=store,
                    universe=forged,
                )

            forged["snapshot_hash"] = "sha256:universe-v7"
            forged["selected_symbols"] = ["BTC/USDT"]
            with self.assertRaisesRegex(ValueError, "selected membership"):
                run_crypto_research(
                    _ExplodingProvider(),
                    symbol="BTC/USDT",
                    store=store,
                    universe=forged,
                )

    def test_valid_universe_snapshot_delegates_through_multi_symbol_research(self) -> None:
        symbols = ["BTC/USDT", "ETH/USDT"]
        with AxiomStore(":memory:") as store:
            self._persist_universe(store, symbols)
            snapshot = load_crypto_universe(store, universe_id="spot-major", version="universe-v7")
            self.assertIsInstance(snapshot, UniverseSnapshot)
            assert snapshot is not None
            provider = InMemoryCryptoProvider(
                {
                    "BTCUSDT": self._bars("BTC/USDT"),
                    "ETHUSDT": self._bars("ETH/USDT"),
                }
            )
            report = run_crypto_research(
                provider,
                symbols=symbols,
                providers=provider,
                store=store,
                universe=snapshot,
            )

        self.assertEqual(report["symbols"], symbols)
        self.assertEqual(report["universe_id"], "spot-major")
        self.assertEqual(report["universe_version"], "universe-v7")
        self.assertEqual(report["universe_provenance"]["snapshot_hash"], "sha256:universe-v7")
        self.assertEqual(report["universe_provenance"]["selected_symbols"], symbols)


class RequiredPolymarketHealthTests(unittest.TestCase):
    @staticmethod
    def _metadata(store: AxiomStore, market_id: str, observed_at: datetime) -> None:
        store.save_polymarket_market_metadata(
            market_id,
            {
                "source_type": "FORWARD_COLLECTED",
                "active": True,
                "closed": False,
                "metadata": {"category": "politics"},
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "open",
                    "expiry": (observed_at + timedelta(days=1)).isoformat(),
                },
            },
            observed_at=observed_at,
            source_type="FORWARD_COLLECTED",
        )

    @staticmethod
    def _snapshot(
        store: AxiomStore,
        market_id: str,
        *,
        source_timestamp: datetime,
        observed_at: datetime,
    ) -> None:
        store.save_polymarket_snapshot(
            f"required-{market_id}-{observed_at.timestamp()}",
            market_id,
            source_timestamp,
            observed_at,
            {
                "source_type": "FORWARD_COLLECTED",
                "request_started_at": (observed_at - timedelta(seconds=1)).isoformat(),
                "provider_timestamp": source_timestamp.isoformat(),
                "response_received_at": observed_at.isoformat(),
                "observed_at": observed_at.isoformat(),
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "open",
                    "yes_mid": 0.5,
                },
            },
            quality="ORDER_BOOK_SIMULATED",
            source_type="FORWARD_COLLECTED",
        )

    def test_required_health_counts_fresh_stale_and_missing_and_ignores_unrelated_staleness(self) -> None:
        stale_source = T0 - timedelta(minutes=5)
        with AxiomStore(":memory:") as store:
            for market_id in (
                "required-fresh",
                "required-stale",
                "required-missing",
                "unrelated-discovery",
            ):
                self._metadata(store, market_id, T0)
            self._snapshot(
                store,
                "required-fresh",
                source_timestamp=T0 - timedelta(seconds=2),
                observed_at=T0,
            )
            self._snapshot(
                store,
                "required-stale",
                source_timestamp=stale_source,
                observed_at=stale_source,
            )
            self._snapshot(
                store,
                "unrelated-discovery",
                source_timestamp=stale_source,
                observed_at=stale_source,
            )
            requirements = {
                "market_ids": [
                    "required-fresh",
                    "required-stale",
                    "required-missing",
                ],
                "candidate_bound_markets": [
                    "required-fresh",
                    "required-stale",
                    "required-missing",
                ],
                "candidate_references": {
                    "required-fresh": ["candidate-health"],
                    "required-stale": ["candidate-health"],
                    "required-missing": ["candidate-health"],
                },
            }
            health = store.polymarket_required_health(
                requirements=requirements,
                scheduled_market_ids=[
                    "required-fresh",
                    "required-stale",
                    "required-missing",
                ],
                now=T0,
                stale_after_seconds=60,
            )

        self.assertEqual(
            health["candidate_bound_markets"],
            ["required-fresh", "required-stale", "required-missing"],
        )
        self.assertEqual(
            health["scheduled"],
            ["required-fresh", "required-stale", "required-missing"],
        )
        self.assertEqual(health["fresh"], ["required-fresh"])
        self.assertEqual(health["stale"], ["required-stale"])
        self.assertEqual(health["missing"], ["required-missing"])
        self.assertEqual(health["reason_code"], "REQUIRED_MARKETS_MISSING")
        self.assertEqual(health["grade"], "D")
        self.assertNotIn("unrelated-discovery", health["stale"])
        self.assertNotIn("unrelated-discovery", health["missing"])

        diagnostics = {
            item["market_id"]: item for item in health["market_diagnostics"]
        }
        self.assertEqual(
            diagnostics["required-fresh"]["candidate_references"],
            ["candidate-health"],
        )
        self.assertTrue(diagnostics["required-fresh"]["candidate_bound"])
        self.assertEqual(
            diagnostics["required-fresh"]["source_timestamp"],
            (T0 - timedelta(seconds=2)).isoformat(),
        )
        self.assertEqual(diagnostics["required-fresh"]["observed_at"], T0.isoformat())
        self.assertEqual(diagnostics["required-fresh"]["collection_state"], "fresh")
        self.assertEqual(diagnostics["required-stale"]["collection_state"], "stale")
        self.assertEqual(diagnostics["required-missing"]["collection_state"], "missing")

    def test_required_health_fresh_is_grade_a_and_retains_provider_timestamps(self) -> None:
        source_timestamp = T0 - timedelta(seconds=17)
        with AxiomStore(":memory:") as store:
            self._metadata(store, "required-market", T0)
            self._snapshot(
                store,
                "required-market",
                source_timestamp=source_timestamp,
                observed_at=T0,
            )
            requirements = {
                "market_ids": ["required-market"],
                "candidate_bound_markets": ["required-market"],
                "candidate_references": {"required-market": ["candidate-fresh"]},
            }
            health = store.polymarket_required_health(
                requirements=requirements,
                scheduled_market_ids=["required-market"],
                now=T0,
                stale_after_seconds=60,
            )
            stored = store.load_polymarket_snapshots("required-market")

        self.assertEqual(health["grade"], "A")
        self.assertEqual(health["reason_code"], "REQUIRED_MARKETS_FRESH")
        self.assertEqual(health["fresh"], ["required-market"])
        self.assertEqual(health["stale"], [])
        self.assertEqual(health["missing"], [])
        self.assertEqual(health["newest_required_snapshot"], source_timestamp.isoformat())
        self.assertEqual(health["oldest_required_snapshot"], source_timestamp.isoformat())
        self.assertEqual(stored[0]["source_timestamp"], source_timestamp)
        self.assertEqual(stored[0]["observed_at"], T0)
        self.assertEqual(stored[0]["payload"]["provider_timestamp"], source_timestamp.isoformat())
        self.assertEqual(stored[0]["payload"]["request_started_at"], (T0 - timedelta(seconds=1)).isoformat())
        self.assertEqual(stored[0]["payload"]["response_received_at"], T0.isoformat())
        self.assertEqual(stored[0]["payload"]["observed_at"], T0.isoformat())

class DashboardAndCliShapeTests(unittest.TestCase):
    def test_dashboard_exposes_exact_health_reason_provenance_and_bootstrap_progress(self) -> None:
        health = {
            "grade": "C",
            "grade_scope": "collector_health",
            "reason_code": "STALE_MARKETS",
            "reasons": [{"code": "STALE_MARKETS", "reason": "market missed the threshold"}],
            "source_type": "FORWARD_COLLECTED",
            "window_start": T0.isoformat(),
            "window_end": (T0 + timedelta(minutes=1)).isoformat(),
            "historical_maturity_grade": "B",
            "historical_error_count": 4,
        }
        with AxiomStore(":memory:") as store:
            store.save_dataset_bootstrap_state(
                "BTCUSDT-1d",
                {
                    "provider": "fixture",
                    "instrument": "BTCUSDT",
                    "market_type": "crypto_spot",
                    "timeframe": "1d",
                    "status": "RUNNING",
                    "requested_start": T0,
                    "requested_end": T0 + timedelta(days=10),
                    "next_timestamp": T0 + timedelta(days=4),
                    "records_staged": 4,
                    "errors": ["one retry"],
                },
            )
            data = __import__("axiom.dashboard", fromlist=["DashboardData"]).DashboardData(store=store, data={"dataset-health": health})
            operator = data.operator_data()
            self.assertEqual(operator["health_grade"], "C")
            self.assertEqual(operator["reason_code"], "STALE_MARKETS")
            self.assertEqual(operator["dataset_health"]["reasons"], health["reasons"])
            self.assertEqual(operator["source_type"], "FORWARD_COLLECTED")
            self.assertEqual(operator["historical_maturity_grade"], "B")
            self.assertEqual(operator["components"][1]["detail"]["reason_code"], "STALE_MARKETS")
            progress = operator["btc"]["bootstrap_progress"]
            self.assertEqual(len(progress), 1)
            self.assertEqual(progress[0]["status"], "RUNNING")
            self.assertAlmostEqual(progress[0]["progress"], 0.4)
            self.assertEqual(progress[0]["errors"], ["one retry"])

    def test_dashboard_projection_exposes_required_health_without_provider_access(self) -> None:
        health = {
            "candidate_bound_markets": ["required-market"],
            "scheduled": ["required-market"],
            "fresh": ["required-market"],
            "stale": [],
            "missing": [],
            "newest_required_source_timestamp": (T0 - timedelta(seconds=5)).isoformat(),
            "oldest_required_source_timestamp": (T0 - timedelta(seconds=5)).isoformat(),
            "newest_required_observed_at": T0.isoformat(),
            "oldest_required_observed_at": T0.isoformat(),
            "newest_required_snapshot": (T0 - timedelta(seconds=5)).isoformat(),
            "oldest_required_snapshot": (T0 - timedelta(seconds=5)).isoformat(),
            "grade": "A",
            "reason_code": "REQUIRED_MARKETS_FRESH",
            "reason_display": "All required forward market snapshots are fresh.",
            "candidate_references": {"required-market": ["candidate-dashboard"]},
            "market_diagnostics": [
                {
                    "market_id": "required-market",
                    "candidate_bound": True,
                    "candidate_references": ["candidate-dashboard"],
                    "source_timestamp": (T0 - timedelta(seconds=5)).isoformat(),
                    "observed_at": T0.isoformat(),
                    "freshness_age_seconds": 0.0,
                    "collection_state": "fresh",
                    "reason_code": "REQUIRED_MARKET_SNAPSHOT_FRESH",
                }
            ],
        }

        class ExplodingProvider:
            def markets(self, **_kwargs: object) -> None:
                raise AssertionError("dashboard projection called provider")

            def ticker(self, *_args: object, **_kwargs: object) -> None:
                raise AssertionError("dashboard projection called provider")

        with AxiomStore(":memory:") as store:
            store.save_worker_state(
                "health-monitor",
                "idle",
                health,
                started_at=T0,
                heartbeat_at=T0,
            )
            data = DashboardData(
                store=store,
                prediction_provider=ExplodingProvider(),
                crypto_provider=ExplodingProvider(),
            )
            overview = data.overview_summary()
            canary = data.canary_data()

        for payload in (overview, canary):
            evidence = payload["forward_evidence"]
            self.assertEqual(evidence["candidate_bound_markets"], ["required-market"])
            self.assertEqual(evidence["reason_display"], health["reason_code"])
            self.assertEqual(evidence["fresh"], ["required-market"])
            self.assertEqual(evidence["grade"], "A")
            self.assertEqual(evidence["reason_code"], "REQUIRED_MARKETS_FRESH")
            for timestamp_key in (
                "newest_required_source_timestamp",
                "oldest_required_source_timestamp",
                "newest_required_observed_at",
                "oldest_required_observed_at",
            ):
                self.assertEqual(evidence[timestamp_key], health[timestamp_key])
            self.assertEqual(
                evidence["market_diagnostics"][0]["candidate_references"],
                ["candidate-dashboard"],
            )

    def test_cli_parser_has_required_readiness_commands_and_argument_shapes(self) -> None:
        parser = build_parser()
        choices = parser._subparsers._group_actions[0].choices
        for command in ("dataset-health", "canary-connectivity-check", "crypto-research", "bootstrap-history"):
            self.assertIn(command, choices)

        health = parser.parse_args(["dataset-health", "--interval", "30"])
        self.assertEqual(health.command, "dataset-health")
        self.assertEqual(health.interval, 30.0)
        connectivity = parser.parse_args(["canary-connectivity-check", "--market", "m", "--token", "yes"])
        self.assertEqual(connectivity.command, "canary-connectivity-check")
        self.assertEqual(connectivity.market, "m")
        crypto = parser.parse_args(["crypto-research", "--universe", "latest", "--universe-id", "custom-cli"])
        self.assertEqual(crypto.universe, "latest")
        self.assertEqual(crypto.universe_id, "custom-cli")
        versioned = parser.parse_args(
            ["crypto-research-universe", "--universe-version", "custom-v1", "--universe-id", "custom-cli"]
        )
        self.assertEqual(versioned.universe_version, "custom-v1")
        self.assertEqual(versioned.universe_id, "custom-cli")
        bootstrap = parser.parse_args(
            ["bootstrap-history", "--universe", "--universe-version", "custom-v1", "--universe-id", "custom-cli"]
        )
        self.assertTrue(bootstrap.universe)
        self.assertEqual(bootstrap.universe_version, "custom-v1")
        self.assertEqual(bootstrap.universe_id, "custom-cli")
        with self.assertRaises(SystemExit):
            parser.parse_args(["crypto-research"])

    def test_cli_universe_selector_binds_custom_id_and_latest(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                "universe:custom-cli",
                "custom-v1",
                [{"binance_symbol": "BTCUSDT", "selected": True}],
                metadata={
                    "universe_id": "custom-cli",
                    "snapshot_hash": "sha256:custom-v1",
                    "status": "CURRENT",
                },
                quality="HIGH",
            )
            _, version, provenance, symbols = _load_cli_universe(
                store,
                "latest",
                universe_id="custom-cli",
            )

        self.assertEqual(version, "custom-v1")
        self.assertEqual(symbols, ("BTCUSDT",))
        self.assertEqual(provenance["universe_id"], "custom-cli")
        self.assertEqual(provenance["universe_version"], "custom-v1")
        self.assertEqual(provenance["snapshot_hash"], "sha256:custom-v1")


if __name__ == "__main__":
    unittest.main()
