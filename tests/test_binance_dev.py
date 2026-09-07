from __future__ import annotations

import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from axiom.cli import _main_impl, build_parser

from axiom.binance_dev import BinanceDevelopmentRuntime, PaperBinanceSpotVenue
from axiom.binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceRuntimeProfile,
    BinanceSpotRESTClient,
)


class _Worker:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.stopped = False

    def run(self, *, max_cycles=None):
        self.calls.append(max_cycles)
        return []

    def stop(self):
        self.stopped = True

    def status(self):
        return {"status": "IDLE"}


class _Server:
    def __init__(self, host, port, *, data):
        self.host = host
        self.port = port
        self.data = data
        self.started = False
        self.stopped = False
        self.url = f"http://{host}:{port}"

    def start(self):
        self.started = True
        return self

    def stop(self):
        self.stopped = True



class _IdentityVenue:
    def __init__(self, environment=BINANCE_SPOT_TESTNET, origin="https://testnet.binance.vision"):
        self.environment = environment
        self.origin = origin


class _MissingOriginVenue:
    environment = BINANCE_SPOT_TESTNET


class _VenueWrapper:
    def __init__(self, venue):
        self.venue = venue

class BinanceDevelopmentTests(unittest.TestCase):
    def profile(self, root: Path) -> BinanceRuntimeProfile:
        runtime_data = root / "runtime-data"
        runtime_data.mkdir(parents=True)
        return BinanceRuntimeProfile.development(root, runtime_data / "binance-dev.sqlite", environment=PAPER)

    def test_profile_and_runtime_use_isolated_identity(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        worker = _Worker()
        runtime = BinanceDevelopmentRuntime(
            root,
            profile=self.profile(root),
            worker=worker,
            dashboard_server_factory=_Server,
        )
        self.addCleanup(runtime.stop)
        self.assertEqual(runtime.environment, PAPER)
        self.assertIs(type(runtime.venue), PaperBinanceSpotVenue)
        self.assertEqual(runtime.venue.environment, PAPER)
        self.assertIsNone(runtime.venue.origin)
        self.assertEqual(runtime.profile.port, 8081)
        expected_runtime_data = (root / "runtime-data").resolve()
        expected_db = expected_runtime_data / "binance-dev.sqlite"
        db_path = Path(runtime.db_path).resolve()
        self.assertEqual(db_path, expected_db)
        self.assertEqual(db_path.name, "binance-dev.sqlite")
        self.assertEqual(db_path.parent, expected_runtime_data)
        for path, filename in (
            (runtime.lock_path, "binance-dev.lock"),
            (runtime.pid_path, "binance-dev.pid"),
        ):
            resolved_path = Path(path).resolve()
            self.assertEqual(resolved_path.parent, expected_runtime_data)
            self.assertEqual(resolved_path.name, filename)
        stale_stop = {
            "pid": 999999,
            "runtime_identity": "binance-dev",
            "owner_token": "stale-owner",
        }
        Path(runtime.stop_path).write_text(json.dumps(stale_stop), encoding="utf-8")
        main_runtime_markers = {
            root / "runtime-data" / "axiom.sqlite.lock": "main-runtime-lock",
            root / "runtime-data" / "axiom.sqlite.stop": "main-runtime-stop",
            root / "runtime-data" / "axiom.sqlite.log": "main-runtime-log",
        }
        for path, contents in main_runtime_markers.items():
            path.write_text(contents, encoding="utf-8")

        runtime.start(once=True)
        self.assertEqual(worker.calls, [1])
        self.assertFalse(Path(runtime.stop_path).exists())
        for path, contents in main_runtime_markers.items():
            self.assertEqual(path.read_text(encoding="utf-8"), contents)

        runtime.stop()
        self.assertFalse(Path(runtime.lock_path).exists())
        self.assertFalse(Path(runtime.pid_path).exists())
        self.assertTrue(Path(runtime.stop_path).exists())
        for path, contents in main_runtime_markers.items():
            self.assertEqual(path.read_text(encoding="utf-8"), contents)
        runtime.stop()
        self.assertTrue(Path(runtime.stop_path).exists())
        self.assertTrue(worker.stopped)

    def test_empty_store_once_cycle_reports_no_universe_without_public_services(self):
        class ForbiddenProvider:
            def __getattr__(self, name):
                raise AssertionError(f"public provider called: {name}")

        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        runtime = BinanceDevelopmentRuntime(
            root,
            profile=self.profile(root),
            provider=ForbiddenProvider(),
            dashboard_server_factory=_Server,
        )
        self.addCleanup(runtime.stop)
        with (
            patch.object(runtime.qualification, "rank_and_select", side_effect=AssertionError("qualification called")),
            patch.object(runtime.execution, "pause", side_effect=AssertionError("pause called")),
        ):
            runtime.start(once=True)

        worker_status = runtime.status()["worker"]
        self.assertIsNone(runtime.collector)
        self.assertEqual(worker_status["status"], "NO_TRADE")
        self.assertEqual(worker_status["no_trade_reason"], "NO_UNIVERSE")
        self.assertIsNone(worker_status["pause_reason"])
        self.assertIsNone(worker_status["error"])
        self.assertEqual(worker_status["last_cycle"]["status"], "NO_TRADE")
        self.assertEqual(worker_status["last_cycle"]["no_trade_reason"], "NO_UNIVERSE")
        self.assertIsNone(worker_status["last_cycle"]["error"])

        runtime.stop()
        self.assertFalse(Path(runtime.lock_path).exists())
        self.assertFalse(Path(runtime.pid_path).exists())
        self.assertTrue(Path(runtime.stop_path).exists())

    def test_paper_venue_orders_are_decimal_safe_and_owned(self):
        venue = PaperBinanceSpotVenue(
            initial_balances={"USDT": "100", "BTC": "1"},
            liquidity={"BTCUSDT": "0.25"},
        )
        first = venue.place_limit_order(
            symbol="BTC/USDT", side="BUY", quantity="0.50", price="10.00", time_in_force="IOC", new_client_order_id="owned-1"
        )
        payload = first["payload"]
        self.assertEqual(payload["status"], "PARTIALLY_FILLED")
        self.assertEqual(payload["executedQty"], "0.25")
        self.assertEqual(len(venue.my_trades(symbol="BTCUSDT")["payload"]["trades"]), 1)
        self.assertEqual(venue.query_order(symbol="BTCUSDT", orig_client_order_id="owned-1")["payload"]["orderId"], payload["orderId"])
        foreign = venue.cancel_owned_order(symbol="BTCUSDT", orig_client_order_id="other")
        self.assertEqual(foreign.status, "REJECTED")
        sell_venue = PaperBinanceSpotVenue(initial_balances={"USDT": "1", "BTC": "1"}, liquidity={"BTCUSDT": "1"})
        filled = sell_venue.place_limit_order(
            symbol="BTCUSDT", side="SELL", quantity="0.25", price="11.00", time_in_force="FOK", new_client_order_id="owned-2"
        )
        self.assertEqual(filled["payload"]["status"], "FILLED")
        self.assertEqual(sell_venue.account()["payload"]["balances"][0]["asset"], "BTC")

    def test_testnet_requires_explicit_credentials_and_venue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                BinanceDevelopmentRuntime(root, environment="BINANCE_SPOT_TESTNET")

    def test_explicit_testnet_requires_matching_identity_and_origin(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        runtime_data = root / "runtime-data"
        runtime_data.mkdir(parents=True)
        profile = BinanceRuntimeProfile.development(
            root,
            runtime_data / "binance-dev.sqlite",
            environment=BINANCE_SPOT_TESTNET,
        )
        venue = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "test-key", "api_secret": "test-secret"},
        )
        runtime = BinanceDevelopmentRuntime(
            root,
            profile=profile,
            environment=BINANCE_SPOT_TESTNET,
            credentials={"api_key": "test-key", "api_secret": "test-secret"},
            venue=venue,
            worker=_Worker(),
            dashboard_server_factory=_Server,
        )

        def assert_runtime_released():
            self.assertTrue(runtime._closed)
            self.assertFalse(Path(runtime.lock_path).exists())
            self.assertFalse(Path(runtime.pid_path).exists())
            with self.assertRaises(sqlite3.ProgrammingError):
                runtime.store.connection.execute("SELECT 1")

        # unittest cleanups run last-in, first-out; stop the runtime before
        # removing its temporary SQLite directory, even when an assertion fails.
        self.addCleanup(assert_runtime_released)
        self.addCleanup(runtime.stop)

        self.assertEqual(runtime.environment, BINANCE_SPOT_TESTNET)
        self.assertIs(runtime.venue, venue)
        self.assertEqual(runtime.venue.environment, BINANCE_SPOT_TESTNET)
        self.assertEqual(runtime.venue.origin, "https://testnet.binance.vision")

        runtime.start(once=True)
        self.assertTrue(Path(runtime.lock_path).exists())
        self.assertTrue(Path(runtime.pid_path).exists())
        runtime.stop()
        self.assertTrue(runtime._closed)
        self.assertFalse(Path(runtime.lock_path).exists())
        self.assertFalse(Path(runtime.pid_path).exists())

    def test_runtime_rejects_live_wrapped_missing_and_mismatched_venues_before_store(self):
        cases = (
            (
                PAPER,
                _IdentityVenue(BINANCE_SPOT_LIVE, "https://api.binance.com"),
                None,
            ),
            (
                BINANCE_SPOT_TESTNET,
                _IdentityVenue(BINANCE_SPOT_TESTNET, "https://testnet.binance.vision/"),
                {"api_key": "test-key", "api_secret": "test-secret"},
            ),
            (
                BINANCE_SPOT_TESTNET,
                _MissingOriginVenue(),
                {"api_key": "test-key", "api_secret": "test-secret"},
            ),
            (
                BINANCE_SPOT_TESTNET,
                _IdentityVenue(PAPER, None),
                {"api_key": "test-key", "api_secret": "test-secret"},
            ),
            (
                BINANCE_SPOT_TESTNET,
                _VenueWrapper(_IdentityVenue()),
                {"api_key": "test-key", "api_secret": "test-secret"},
            ),
        )
        for environment, venue, credentials in cases:
            with self.subTest(environment=environment, venue=type(venue).__name__):
                with tempfile.TemporaryDirectory() as directory:
                    calls: list[str] = []

                    def store_factory(path: str):
                        calls.append(path)
                        return None

                    with self.assertRaises(ValueError):
                        BinanceDevelopmentRuntime(
                            Path(directory),
                            environment=environment,
                            credentials=credentials,
                            venue=venue,
                            store_factory=store_factory,
                        )
                    self.assertEqual(calls, [])

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                BinanceDevelopmentRuntime(
                    Path(directory),
                    environment=BINANCE_SPOT_LIVE,
                    credentials={"api_key": "test-key", "api_secret": "test-secret"},
                    venue=_IdentityVenue(BINANCE_SPOT_LIVE, "https://api.binance.com"),
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                BinanceDevelopmentRuntime(
                    root,
                    profile=self.profile(root),
                    environment=BINANCE_SPOT_TESTNET,
                    credentials={"api_key": "test-key", "api_secret": "test-secret"},
                    venue=_IdentityVenue(),
                )

    def test_cli_binance_dev_surface_is_fixed_and_once_uses_runtime_only(self):
        args = build_parser().parse_args(["binance-dev", "--once"])
        self.assertEqual(args.command, "binance-dev")
        self.assertTrue(args.once)
        self.assertFalse(hasattr(args, "db"))
        self.assertFalse(hasattr(args, "port"))
        with patch("axiom.cli.BinanceDevelopmentRuntime") as runtime_type:
            runtime = runtime_type.return_value
            runtime.status.return_value = {"url": "http://127.0.0.1:8081", "paper_only": True}
            self.assertEqual(_main_impl(["binance-dev", "--once"]), 0)
            runtime.start.assert_called_once_with(once=True)
            runtime.stop.assert_called()
            self.assertNotIn("axiom.sqlite", json.dumps(runtime.status.return_value))
            self.assertNotIn("8080", json.dumps(runtime.status.return_value))


if __name__ == "__main__":
    unittest.main()
