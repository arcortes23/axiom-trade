from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

from axiom.canary import CanaryBlocked, CanaryService, CredentialStore, PolymarketClobV2Venue
from axiom.cli import _load_cli_universe, _run_cli_crypto_research, build_parser
from axiom.crypto_universe import TOP_50_MARKET_CAP_BINANCE_USDT, UniverseSnapshot, load_crypto_universe
from axiom.data import InMemoryCryptoProvider
from axiom.domain import MarketType, OHLCVBar
from axiom.experiment_plan import ExperimentPlan, ExperimentPlanError
from axiom.research import run_crypto_research, run_multi_symbol_crypto_research
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)


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
            "relayer_api_key": "relayer-key-fixture",
            "relayer_api_key_address": "relayer-address-fixture",
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
        self.place_calls: list[dict[str, object]] = []
        self.close_calls = 0

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
        return {"balance": "2500000"}

    def place_limit_order(self, **kwargs: object) -> dict[str, object]:
        self.place_calls.append(kwargs)
        return {"ok": True, "order_id": "mock-order", "status": "submitted", "trade_ids": []}
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

    def test_official_venue_uses_read_only_asset_aware_sdk_calls(self) -> None:
        client = _SDKClient()
        relayer_factory = MagicMock(return_value={"key": "api-key"})
        secure_factory = MagicMock(spec=["_create"])
        secure_factory._create.return_value = client
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
        ), patch.object(PolymarketClobV2Venue, "geoblock", return_value={"blocked": False, "close_only": False}):
            with AxiomStore(":memory:") as store:
                result = CanaryService(store, credentials=credentials, clock=lambda: T0).connectivity_check(
                    candidate_id=None, venue=venue, market_id="market-1", token_id="yes"
                )

            self.assertIn("market", result["diagnostics"])
            self.assertEqual(client.market_calls, [{"id": "market-1"}])
            self.assertEqual(client.book_calls, [{"asset_id": "position-yes"}])
            self.assertEqual(client.balance_calls, [{"asset_type": "COLLATERAL"}])
            self.assertEqual(client.place_calls, [])
            self.assertEqual(result["diagnostics"]["market"]["asset_id"], "position-yes")
            self.assertEqual(
                relayer_factory.call_count,
                4,
            )
            self.assertEqual(secure_factory._create.call_count, 4)
            secure_factory._create.assert_called_with(
                private_key="private-key-fixture",
                wallet="wallet-fixture",
                validate_credentials=True,
                api_key={"key": "api-key"},
            )

            self.assertFalse(hasattr(venue, "submit_limit_order"))
            self.assertEqual(client.place_calls, [])
            self.assertFalse(hasattr(venue, "_bind_service_capability"))
            self.assertFalse(hasattr(venue, "_secure_client"))
            self.assertFalse(hasattr(venue, "_submit_limit_order"))
            self.assertFalse(hasattr(venue, "_read_only"))
            self.assertFalse(hasattr(venue, "_credential_provider"))
            self.assertFalse(hasattr(venue, "_client"))
            self.assertFalse(
                any("place_limit_order" in name for name in dir(venue))
            )
            self.assertFalse(hasattr(CanaryService, "_submit_official_order"))
            with AxiomStore(":memory:") as store:
                service = CanaryService(store, credentials=credentials, clock=lambda: T0)
                snapshot = {
                    "micro_live_canary": "ARMED",
                    "candidate": "candidate-1",
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
                    "accepting_orders": True,
                    "min_order_size": "1",
                    "tick_size": "0.01",
                    "bids": [{"price": "0.49", "size": "100"}],
                    "asks": [{"price": "0.50", "size": "100"}],
                    "fee_bps": "10",
                }
                with patch.object(service, "status", return_value=snapshot), patch.object(
                    store,
                    "polymarket_health",
                    return_value={"grade": "A"},
                ), patch.object(
                    PolymarketClobV2Venue,
                    "geoblock",
                    return_value={"blocked": False, "close_only": False},
                ), patch.object(PolymarketClobV2Venue, "market_context", return_value=context):
                    service.submit(
                        signal_id="service-gated",
                        candidate_id="candidate-1",
                        market_id="market-1",
                        token_id="yes",
                        side="BUY",
                        paper_expected_price=Decimal("0.50"),
                        venue=venue,
                    )
            self.assertEqual(
                client.place_calls,
                [
                    {
                        "asset_id": "position-yes",
                        "side": "BUY",
                        "price": "0.50",
                        "size": "2.00",
                    }
                ],
            )
            self.assertEqual(client.close_calls, 7)
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
