from __future__ import annotations

import io
import json
import sqlite3
import threading
from contextlib import closing, redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from axiom.cli import _main_impl, build_parser
from axiom.storage import AxiomStore
from axiom.binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE
from axiom.binance_dev import BinanceDevelopmentRuntime, BinanceTestnetRuntime, PaperBinanceSpotVenue
from axiom.binance_operator import BinanceTestnetControlPlane
from axiom.binance_testnet import BinanceTestnetGateService
from axiom.binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceCredentialRef,
    BinanceCredentialStore,
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

class _FakeKeyring:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}

    def set_password(self, service: str, username: str, password: str) -> None:
        self.values[(service, username)] = password

    def get_password(self, service: str, username: str) -> str | None:
        return self.values.get((service, username))


class _CredentialStoreProbe:
    def __init__(self, ref, value=None, error=None) -> None:
        self.ref = ref
        self.value = value
        self.error = error
        self.load_calls = 0

    def load(self):
        self.load_calls += 1
        if self.error is not None:
            raise self.error
        return self.value




class _IdentityVenue:
    def __init__(self, environment=BINANCE_SPOT_TESTNET, origin="https://testnet.binance.vision"):
        self.environment = environment
        self.origin = origin


class _MissingOriginVenue:
    environment = BINANCE_SPOT_TESTNET


class _VenueWrapper:
    def __init__(self, venue):
        self.venue = venue

class _TestnetGate:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self) -> None:
        self.profile = None
        self.venue = None
        self._venue = None
        self.credential_store = None
        self.store = None
        self.calls: list[str] = []

    def check_connectivity(self, *, deadline_monotonic=None):
        self.calls.append("check_connectivity")
        return {"status": "PASS", "environment": BINANCE_SPOT_TESTNET}

    def validate_order(self, *, symbol=None, deadline_monotonic=None):
        self.calls.append(f"validate_order:{symbol}")
        return {
            "status": "PASS",
            "environment": BINANCE_SPOT_TESTNET,
            "symbol": symbol or "BTCUSDT",
        }

    def connectivity_status(self):
        self.calls.append("connectivity_status")
        return {"status": "PASS", "environment": BINANCE_SPOT_TESTNET}

    def validation_status(self):
        self.calls.append("validation_status")
        return {"status": "PASS", "environment": BINANCE_SPOT_TESTNET, "symbol": "BTCUSDT"}

    def probe_status(self):
        self.calls.append("probe_status")
        return {"status": "BLOCKED", "reason": "NOT_STARTED", "environment": BINANCE_SPOT_TESTNET}

class _TestnetControl:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self, gate, *, execution=None, worker=None, clock=None) -> None:
        self.gate = gate
        self.execution = execution
        self.worker = worker
        self.profile = getattr(gate, "profile", None)
        self.store = AxiomStore(self.profile.db_path) if isinstance(self.profile, BinanceRuntimeProfile) else None
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.state = "DISARMED"

    def status(self):
        return {
            "environment": BINANCE_SPOT_TESTNET,
            "state": self.state,
            "connectivity": {"status": "CONTROL_SHOULD_NOT_WIN"},
            "validation": {"status": "CONTROL_SHOULD_NOT_WIN"},
            "probe": {"status": "CONTROL_SHOULD_NOT_WIN"},
            "autonomous": {
                "enabled": self.worker is not None,
                "state": self.state,
            },
        }

    def action(self, action, payload=None):
        body = dict(payload or {})
        self.calls.append((action, body))
        if action == "PAUSE":
            self.state = "PAUSED"
        elif action == "DISARM":
            self.state = "DISARMED"
        elif action == "ENABLE":
            self.state = "ARMED"
        return {"ok": True, "action": action, "control": {"state": self.state}}

    def authorize_bounded_auto(self, confirmation, window_seconds, *, deadline_monotonic=None):
        self.calls.append(
            (
                "AUTHORIZE",
                {
                    "confirmation": confirmation,
                    "window_seconds": window_seconds,
                    **({"deadline_monotonic": deadline_monotonic} if deadline_monotonic is not None else {}),
                },
            )
        )
        self.state = "ARMED"
        return {"authorized": True, "control": {"state": self.state}}

    def list_actions(self, *, limit=25):
        return []
    def close(self):
        if self.store is not None:
            self.store.close()


class _AutoWorker(_Worker):
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self, *, error: BaseException | None = None) -> None:
        super().__init__()
        self.strategy = object()
        self.profile = None
        self.store = None
        self.execution = None
        self.qualification = None
        self._provider = None
        self._collector = None
        self.error = error
        self.run_once_thread_ids: list[int] = []

    def supports_persisted_strategy(self):
        return True

    def run_once(self):
        self.calls.append("run_once")
        self.run_once_thread_ids.append(threading.get_ident())
        if self.error is not None:
            raise self.error
        return {"status": "NO_TRADE"}

    def cycle(self):
        return self.run_once()

class _AutoStrategyState:
    def __init__(self) -> None:
        self.state = "DISARMED"


class _StatefulTestnetControl(_TestnetControl):
    def __init__(self, gate, *, strategy, execution=None, worker=None, clock=None) -> None:
        super().__init__(gate, execution=execution, worker=worker, clock=clock)
        self.strategy = strategy

    def authorize_bounded_auto(self, confirmation, window_seconds):
        result = super().authorize_bounded_auto(confirmation, window_seconds)
        self.strategy.state = "ARMED"
        return result

    def action(self, action, payload=None):
        result = super().action(action, payload)
        if action == "ENABLE":
            self.strategy.state = "ARMED"
        elif action in {"PAUSE", "DISARM"}:
            self.strategy.state = "DISARMED"
        return result


class _CleanupFailControl(_TestnetControl):
    def action(self, action, payload=None):
        if action == "DISARM":
            body = dict(payload or {})
            self.calls.append((action, body))
            return {"ok": False, "action": action, "reason": "DISARM_FAILED"}
        return super().action(action, payload)

class _NestedCleanupControl(_TestnetControl):
    def action(self, action, payload=None):
        body = dict(payload or {})
        self.calls.append((action, body))
        if action == "PAUSE":
            return {"ok": True, "action": action, "control": {"ok": False, "state": "PAUSED"}}
        return {"ok": True, "action": action, "control": {"state": "DISARMED"}}

class _Execution:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET
    entry_policy_hash = "BINANCE_TESTNET_CURRENT_QUALIFICATION_V1"

    def __init__(
        self,
        *,
        store=None,
        profile=None,
        environment=BINANCE_SPOT_TESTNET,
        venue=None,
        credentials=None,
        credential_store=None,
        credential_ref=None,
        qualification=None,
        entry_binding_authorizer=None,
        entry_policy_hash="BINANCE_TESTNET_CURRENT_QUALIFICATION_V1",
        risk_envelope=None,
        **_: object,
    ) -> None:
        self.calls: list[str] = []
        self.profile = profile
        self.store = store
        if self.store is None and isinstance(profile, BinanceRuntimeProfile):
            self.store = AxiomStore(profile.db_path)
        self.environment = getattr(environment, "value", environment)
        self.venue = venue
        self.credentials = credentials
        self.credential_store = credential_store
        self.credential_ref = credential_ref or getattr(credentials, "ref", None)
        self.qualification = qualification
        self.entry_binding_authorizer = entry_binding_authorizer or self._authorize
        self.entry_policy_hash = entry_policy_hash
        self.risk_envelope = risk_envelope or DEFAULT_BINANCE_RISK_ENVELOPE
    def _authorize(self, confirmation, **kwargs):
        self.calls.append("authorize")
        return True, "AUTHORIZED"

    def pause(self, **kwargs):
        self.calls.append("pause")
        return {"state": "PAUSED"}
    def close(self):
        if self.store is not None:
            self.store.close()

class BinanceDevelopmentTests(unittest.TestCase):
    def profile(self, root: Path) -> BinanceRuntimeProfile:
        runtime_data = root / "runtime-data"
        runtime_data.mkdir(parents=True)
        return BinanceRuntimeProfile.development(root, runtime_data / "binance-dev.sqlite", environment=PAPER)

    def configured_testnet_credentials(self, api_key="testnet-key", api_secret="testnet-secret"):
        store = BinanceCredentialStore(
            environment=BINANCE_SPOT_TESTNET,
            keyring_backend=_FakeKeyring(),
        )
        store.configure(api_key, api_secret)
        return store
    def configured_auto_runtime(
        self,
        root: Path,
        worker: _AutoWorker,
        *,
        gate=None,
        control=None,
    ):
        credentials = BinanceCredentialStore(
            environment=BINANCE_SPOT_TESTNET,
            keyring_backend=_FakeKeyring(),
        )
        credentials.configure("testnet-key", "testnet-secret")
        profile = BinanceRuntimeProfile.testnet(root, root / "runtime-data" / "binance-testnet.sqlite")
        venue = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "testnet-key", "api_secret": "testnet-secret"},
        )
        gate = gate or _TestnetGate()
        gate.profile = profile
        gate._venue = venue
        gate.venue = venue
        gate.credential_store = credentials
        execution_holder: dict[str, _Execution] = {}
        control = control or _TestnetControl(gate, execution=None, worker=worker)
        control.profile = profile
        if getattr(control, "store", None) is None:
            control.store = AxiomStore(profile.db_path)
        def make_execution(store, **kwargs):
            linked = _Execution(store=store, **kwargs)
            execution_holder["value"] = linked
            control.execution = linked
            return linked
        def make_worker(store, linked_execution, **kwargs):
            worker.profile = kwargs["profile"]
            worker.store = store
            worker.execution = linked_execution
            worker.qualification = kwargs["qualification"]
            worker._provider = kwargs.get("provider")
            worker._collector = kwargs.get("collector")
            return worker
        def make_gate(store, **kwargs):
            gate.store = store
            return gate
        runtime = BinanceTestnetRuntime(
            root,
            profile=profile,
            credential_store=credentials,
            venue=venue,
            gate_factory=make_gate,
            execution=None,
            execution_factory=make_execution,
            worker=None,
            strategy=worker.strategy,
            worker_factory=make_worker,
            control=control,
            dashboard_server_factory=_Server,
        )
        return runtime, control, execution_holder["value"]
    def test_development_runtime_uses_paper_venue_and_checkout_scoped_markers(self):
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

    def test_development_runtime_is_paper_only_before_construction(self):
        for environment in (BINANCE_SPOT_TESTNET, BINANCE_SPOT_LIVE):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as directory:
                calls: list[str] = []

                def store_factory(path: str):
                    calls.append("store")
                    raise AssertionError("store construction must not run")

                def worker_factory(*args, **kwargs):
                    calls.append("worker")
                    raise AssertionError("worker construction must not run")

                with self.assertRaises(ValueError):
                    BinanceDevelopmentRuntime(
                        Path(directory),
                        environment=environment,
                        credentials={"api_key": "test-key", "api_secret": "test-secret"},
                        store_factory=store_factory,
                        worker_factory=worker_factory,
                    )
                self.assertEqual(calls, [])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = BinanceRuntimeProfile.testnet(root)
            calls: list[str] = []

            def store_factory(path: str):
                calls.append(path)
                raise AssertionError("store construction must not run")

            with self.assertRaises(ValueError):
                BinanceDevelopmentRuntime(
                    root,
                    profile=profile,
                    store_factory=store_factory,
                )
            self.assertEqual(calls, [])

    def test_testnet_rejects_wrong_credential_ref_before_load_or_store(self):
        wrong_ref = BinanceCredentialRef(instance="binance-dev", environment=PAPER)
        for ref in (wrong_ref, None):
            with self.subTest(ref=ref), tempfile.TemporaryDirectory() as directory:
                credential_store = _CredentialStoreProbe(ref, {"api_key": "test-key", "api_secret": "test-secret"})
                store_calls: list[str] = []

                def store_factory(path: str):
                    store_calls.append(path)
                    raise AssertionError("store construction must not run")

                with self.assertRaises(ValueError):
                    BinanceTestnetRuntime(
                        Path(directory),
                        credential_store=credential_store,
                        store_factory=store_factory,
                    )
                self.assertEqual(credential_store.load_calls, 0)
                self.assertEqual(store_calls, [])

    def test_testnet_rejects_explicit_credentials_before_store_load(self):
        credential_store = _CredentialStoreProbe(
            BinanceCredentialRef(
                instance="binance-testnet",
                environment=BINANCE_SPOT_TESTNET,
                namespace="AXIOM-BINANCE-SPOT-TESTNET",
            ),
            {"api_key": "test-key", "api_secret": "test-secret"},
        )
        store_calls: list[str] = []

        def store_factory(path: str):
            store_calls.append(path)
            raise AssertionError("store construction must not run")

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                BinanceTestnetRuntime(
                    Path(directory),
                    credential_store=credential_store,
                    credentials={"api_key": "test-key", "api_secret": "test-secret"},
                    store_factory=store_factory,
                )
        self.assertEqual(credential_store.load_calls, 0)
        self.assertEqual(store_calls, [])

    def test_testnet_credential_store_load_error_fails_closed(self):
        credential_store = _CredentialStoreProbe(
            BinanceCredentialRef(
                instance="binance-testnet",
                environment=BINANCE_SPOT_TESTNET,
                namespace="AXIOM-BINANCE-SPOT-TESTNET",
            ),
            error=RuntimeError("keyring unavailable"),
        )
        store_calls: list[str] = []

        def store_factory(path: str):
            store_calls.append(path)
            raise AssertionError("store construction must not run")

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                BinanceTestnetRuntime(
                    Path(directory),
                    credential_store=credential_store,
                    store_factory=store_factory,
                )
        self.assertEqual(credential_store.load_calls, 1)
        self.assertEqual(store_calls, [])

    def test_testnet_execution_factory_typeerror_propagates_without_retry_and_closes_resources(self):
        execution_calls: list[dict[str, object]] = []
        stores: list[AxiomStore] = []

        def store_factory(path: str) -> AxiomStore:
            store = AxiomStore(path)
            stores.append(store)
            return store

        def execution_factory(store, **kwargs):
            execution_calls.append(kwargs)
            raise TypeError("execution validation failed")

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(TypeError, "execution validation failed"):
                BinanceTestnetRuntime(
                    Path(directory),
                    credential_store=self.configured_testnet_credentials(),
                    venue=BinanceSpotRESTClient(
                        BINANCE_SPOT_TESTNET,
                        {"api_key": "testnet-key", "api_secret": "testnet-secret"},
                    ),
                    store_factory=store_factory,
                    execution_factory=execution_factory,
                )

        self.assertEqual(len(execution_calls), 1)
        self.assertTrue(
            {
                "venue",
                "adapter",
                "profile",
                "environment",
                "credentials",
                "credential_store",
                "credential_ref",
                "owner_id",
                "entry_binding_authorizer",
                "entry_policy_hash",
            }.issubset(execution_calls[0])
        )
        self.assertEqual(
            execution_calls[0]["entry_policy_hash"],
            "BINANCE_TESTNET_CURRENT_QUALIFICATION_V1",
        )
        self.assertEqual(len(stores), 5)
        for store in stores:
            with self.assertRaises(sqlite3.ProgrammingError):
                store.connection.execute("SELECT 1")

    def test_testnet_rejects_reused_store_connection_and_closes_it(self):
        stores: list[AxiomStore] = []

        def store_factory(path: str) -> AxiomStore:
            if not stores:
                stores.append(AxiomStore(path))
            return stores[0]

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "distinct SQLite connections"):
                BinanceTestnetRuntime(
                    Path(directory),
                    credential_store=self.configured_testnet_credentials(),
                    venue=BinanceSpotRESTClient(
                        BINANCE_SPOT_TESTNET,
                        {"api_key": "testnet-key", "api_secret": "testnet-secret"},
                    ),
                    store_factory=store_factory,
                )
        self.assertEqual(len(stores), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            stores[0].connection.execute("SELECT 1")

    def test_testnet_rejects_fake_or_wrong_venue_identity(self):
        venues = (
            _IdentityVenue(),
            _MissingOriginVenue(),
            _VenueWrapper(_IdentityVenue()),
            BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "different-key", "api_secret": "different-secret"},
            ),
            BinanceSpotRESTClient(
                PAPER,
                {"api_key": "test-key", "api_secret": "test-secret"},
            ),
        )
        for venue in venues:
            with self.subTest(venue=type(venue).__name__), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    BinanceTestnetRuntime(
                        Path(directory),
                        credential_store=self.configured_testnet_credentials(
                            api_key="test-key",
                            api_secret="test-secret",
                        ),
                        venue=venue,
                        gate_factory=lambda *args, **kwargs: (_ for _ in ()).throw(
                            AssertionError("gate construction must not run")
                        ),
                        execution_factory=lambda *args, **kwargs: (_ for _ in ()).throw(
                            AssertionError("execution construction must not run")
                        ),
                        worker_factory=lambda *args, **kwargs: (_ for _ in ()).throw(
                            AssertionError("worker construction must not run")
                        ),
                        dashboard_server_factory=_Server,
                    )
    def test_testnet_rejects_gate_without_canonical_store_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = self.configured_testnet_credentials()
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "testnet-key", "api_secret": "testnet-secret"},
            )

            def gate_factory(store, **kwargs):
                gate = _TestnetGate()
                gate.profile = kwargs["profile"]
                gate._venue = kwargs["venue"]
                gate.venue = kwargs["venue"]
                gate.credential_store = kwargs["credential_store"]
                gate.credentials = kwargs["credentials"]
                return gate

            with self.assertRaisesRegex(ValueError, "gate store mismatch"):
                BinanceTestnetRuntime(
                    root,
                    credential_store=credentials,
                    venue=venue,
                    gate_factory=gate_factory,
                    dashboard_server_factory=_Server,
                )

    def test_testnet_rejects_metadata_free_direct_execution(self):
        class MetadataFreeExecution:
            def entry_binding_authorizer(self, confirmation, **kwargs):
                return True, "AUTHORIZED"

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "strict_testnet=True"):
                BinanceTestnetRuntime(
                    Path(directory),
                    credential_store=self.configured_testnet_credentials(),
                    execution=MetadataFreeExecution(),
                    dashboard_server_factory=_Server,
                )

    def test_testnet_rejects_conflicting_direct_execution_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = self.configured_testnet_credentials()
            profile = BinanceRuntimeProfile.testnet(root, root / "runtime-data" / "binance-testnet.sqlite")
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "testnet-key", "api_secret": "testnet-secret"},
            )
            execution = _Execution(profile=profile, venue=venue, credentials=credentials)
            execution.environment = PAPER
            with self.assertRaisesRegex(ValueError, "execution environment mismatch"):
                BinanceTestnetRuntime(
                    root,
                    profile=profile,
                    credential_store=credentials,
                    venue=venue,
                    execution=execution,
                    dashboard_server_factory=_Server,
                )

    def test_testnet_rejects_conflicting_factory_execution_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = self.configured_testnet_credentials()
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "testnet-key", "api_secret": "testnet-secret"},
            )

            def execution_factory(store, **kwargs):
                return _Execution(
                    profile=BinanceRuntimeProfile.testnet(
                        root / "other-checkout",
                        root / "other-checkout" / "runtime-data" / "binance-testnet.sqlite",
                    ),
                    venue=venue,
                    credentials=credentials,
                )

            with self.assertRaisesRegex(ValueError, "execution profile mismatch"):
                BinanceTestnetRuntime(
                    root,
                    credential_store=credentials,
                    venue=venue,
                    execution_factory=execution_factory,
                    dashboard_server_factory=_Server,
                )

    def test_cli_binance_credentials_accepts_only_testnet(self):
        parsed = build_parser().parse_args(
            ["binance-credentials", "configure", "--environment", "testnet"]
        )
        self.assertEqual(parsed.command, "binance-credentials")
        self.assertEqual(parsed.binance_credentials_command, "configure")
        self.assertEqual(parsed.environment, "testnet")
        parsed_status = build_parser().parse_args(
            ["binance-credentials", "status", "--environment", "testnet"]
        )
        self.assertEqual(parsed_status.binance_credentials_command, "status")
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["binance-credentials", "configure"])
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["binance-credentials", "status", "--environment", "mainnet"]
            )
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["binance-credentials", "configure", "--environment", "live"]
            )

    def test_cli_binance_credentials_writes_only_testnet_namespace_and_exposes_no_secrets(self):
        keyring = _FakeKeyring()
        api_key = "testnet-public-key"
        api_secret = "testnet-private-secret"

        def store_factory(*, ref):
            return BinanceCredentialStore(ref=ref, keyring_backend=keyring)

        configured_output = io.StringIO()
        with (
            patch("axiom.cli.BinanceCredentialStore", side_effect=store_factory),
            patch("axiom.cli.getpass.getpass", side_effect=[api_key, api_secret]),
            redirect_stdout(configured_output),
        ):
            self.assertEqual(
                _main_impl(
                    ["binance-credentials", "configure", "--environment", "testnet"]
                ),
                0,
            )
        configured_payload = json.loads(configured_output.getvalue())
        self.assertEqual(
            set(configured_payload),
            {"environment", "namespace", "configured", "secret_values_exposed"},
        )
        self.assertEqual(configured_payload["environment"], BINANCE_SPOT_TESTNET)
        self.assertEqual(
            configured_payload["namespace"], "AXIOM-BINANCE-SPOT-TESTNET"
        )
        self.assertTrue(configured_payload["configured"])
        self.assertFalse(configured_payload["secret_values_exposed"])
        self.assertNotIn(api_key, configured_output.getvalue())
        self.assertNotIn(api_secret, configured_output.getvalue())
        self.assertTrue(keyring.values)
        self.assertTrue(
            all(service == "AXIOM-BINANCE-SPOT-TESTNET" for service, _ in keyring.values)
        )
        self.assertTrue(
            all(
                "binance-testnet" in username
                and BINANCE_SPOT_TESTNET in username
                for _, username in keyring.values
            )
        )
        self.assertEqual(set(keyring.values.values()), {api_key, api_secret})

        status_output = io.StringIO()
        with (
            patch("axiom.cli.BinanceCredentialStore", side_effect=store_factory),
            redirect_stdout(status_output),
        ):
            self.assertEqual(
                _main_impl(
                    ["binance-credentials", "status", "--environment", "testnet"]
                ),
                0,
            )
        status_payload = json.loads(status_output.getvalue())
        self.assertTrue(status_payload["configured"])
        self.assertFalse(status_payload["secret_values_exposed"])
        self.assertNotIn(api_key, status_output.getvalue())
        self.assertNotIn(api_secret, status_output.getvalue())

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


    def test_testnet_missing_keyring_is_blocked_without_worker_or_venue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = BinanceTestnetRuntime(
                root,
                keyring_backend=_FakeKeyring(),
                dashboard_server_factory=_Server,
            )
            try:
                status = runtime.status()
                self.assertEqual(status["title"], "BINANCE SPOT TESTNET")
                self.assertEqual(status["environment"], BINANCE_SPOT_TESTNET)
                self.assertEqual(status["credentials"]["configured"], False)
                self.assertEqual(status["autonomous"]["state"], "BLOCKED")
                self.assertEqual(status["autonomous"]["blocked_reason"], "CREDENTIALS_NOT_CONFIGURED")
                self.assertIsNone(runtime.venue)
                self.assertIsNone(runtime.worker)
                self.assertEqual(runtime.profile.port, 8082)
                self.assertEqual(Path(runtime.db_path).name, "binance-testnet.sqlite")
                self.assertIs(type(runtime.gate), BinanceTestnetGateService)
                self.assertIs(type(runtime.control), BinanceTestnetControlPlane)
                self.assertNotIn("8080", json.dumps(status))
                self.assertNotIn("binance-dev", json.dumps(status))
            finally:
                runtime.stop()
                self.assertFalse(Path(runtime.lock_path).exists())
                self.assertFalse(Path(runtime.pid_path).exists())
                with closing(sqlite3.connect(runtime.db_path)) as reopened:
                    self.assertEqual(reopened.execute("SELECT 1").fetchone()[0], 1)
                Path(runtime.db_path).unlink()
    def test_testnet_components_use_distinct_sqlite_connections(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = self.configured_testnet_credentials(
                api_key="test-key",
                api_secret="test-secret",
            )
            profile = BinanceRuntimeProfile.testnet(root, root / "runtime-data" / "binance-testnet.sqlite")
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "test-key", "api_secret": "test-secret"},
            )
            gate = _TestnetGate()
            gate.profile = profile
            gate._venue = venue
            gate.venue = venue
            gate.credential_store = credentials
            worker = _AutoWorker()
            execution_holder: dict[str, _Execution] = {}
            control = _TestnetControl(gate, execution=None, worker=worker)
            control.profile = profile

            def make_execution(store, **kwargs):
                linked = _Execution(store=store, **kwargs)
                execution_holder["value"] = linked
                control.execution = linked
                return linked
            def make_worker(store, linked_execution, **kwargs):
                worker.profile = kwargs["profile"]
                worker.store = store
                worker.execution = linked_execution
                worker.qualification = kwargs["qualification"]
                worker._provider = kwargs.get("provider")
                worker._collector = kwargs.get("collector")
                return worker
            def make_gate(store, **kwargs):
                gate.store = store
                return gate

            runtime = BinanceTestnetRuntime(
                root,
                profile=profile,
                credential_store=credentials,
                venue=venue,
                gate_factory=make_gate,
                execution=None,
                execution_factory=make_execution,
                worker=None,
                strategy=worker.strategy,
                worker_factory=make_worker,
                control=control,
                dashboard_server_factory=_Server,
            )
            try:
                connections = (
                    runtime.dashboard_store.connection,
                    runtime.gate_store.connection,
                    runtime.execution_store.connection,
                    runtime.operator_store.connection,
                )
                self.assertEqual(len({id(connection) for connection in connections}), 4)
                self.assertIs(runtime.dashboard_data.store, runtime.dashboard_store)
                self.assertIsNot(runtime.operator_store.connection, runtime.dashboard_store.connection)
            finally:
                runtime.stop()
    def test_testnet_configured_uses_keyring_identity_and_bounded_auto_pauses(self):
        keyring = _FakeKeyring()
        credentials = BinanceCredentialStore(
            ref=BinanceCredentialStore(
                environment=BINANCE_SPOT_TESTNET
            ).ref,
            keyring_backend=keyring,
        )
        credentials.configure("testnet-key", "testnet-secret")
        gate = _TestnetGate()
        worker = _AutoWorker()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = BinanceRuntimeProfile.testnet(root, root / "runtime-data" / "binance-testnet.sqlite")
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "testnet-key", "api_secret": "testnet-secret"},
            )
            gate.profile = profile
            gate._venue = venue
            gate.venue = venue
            gate.credential_store = credentials
            execution_holder: dict[str, _Execution] = {}
            control = _TestnetControl(gate, execution=None, worker=worker)
            control.profile = profile

            def make_execution(store, **kwargs):
                linked = _Execution(store=store, **kwargs)
                execution_holder["value"] = linked
                control.execution = linked
                return linked
            def make_worker(store, linked_execution, **kwargs):
                worker.store = store
                worker.profile = kwargs["profile"]
                worker.execution = linked_execution
                worker.qualification = kwargs["qualification"]
                worker._provider = kwargs.get("provider")
                worker._collector = kwargs.get("collector")
                return worker
            def make_gate(store, **kwargs):
                gate.store = store
                return gate
            runtime = BinanceTestnetRuntime(
                root,
                profile=profile,
                credential_store=credentials,
                venue=venue,
                gate_factory=make_gate,
                execution=None,
                execution_factory=make_execution,
                worker=None,
                strategy=worker.strategy,
                worker_factory=make_worker,
                control=control,
                dashboard_server_factory=_Server,
            )
            try:
                before = list(gate.calls)
                status = runtime.status()
                self.assertEqual(gate.calls, before + ["connectivity_status", "validation_status", "probe_status"])
                self.assertEqual(status["connectivity"]["status"], "PASS")
                self.assertEqual(status["validation"]["status"], "PASS")
                self.assertEqual(status["probe"]["status"], "BLOCKED")
                self.assertEqual(runtime.profile.port, 8082)
                self.assertTrue(status["credentials"]["configured"])
                invalid = runtime.action("EXECUTION_PROBE", {"confirmation": "RUN BINANCE TESTNET EXECUTION PROBE "})
                self.assertFalse(invalid["ok"])
                self.assertEqual(invalid["reason"], "EXACT_CONFIRMATION_REQUIRED")
                with self.assertRaises(ValueError):
                    runtime.auto(confirmation="wrong", window_seconds=30)
                with self.assertRaises(ValueError):
                    runtime.auto(confirmation=runtime.TESTNET_CONFIRMATION, window_seconds=901)
                with self.assertRaisesRegex(
                    ValueError,
                    "window_seconds must be a finite integer between 30 and 900",
                ):
                    runtime.auto(confirmation=runtime.TESTNET_CONFIRMATION, window_seconds=29)
                with self.assertRaises(ValueError):
                    runtime.auto(confirmation=runtime.TESTNET_CONFIRMATION, window_seconds=30.0)
                with self.assertRaises(ValueError):
                    runtime.auto(confirmation=runtime.TESTNET_CONFIRMATION, window_seconds=True)
                result = runtime.auto(
                    confirmation=runtime.TESTNET_CONFIRMATION,
                    window_seconds=30,
                )
                self.assertTrue(result["ok"])
                self.assertTrue(result["paused"])
                self.assertEqual(result["window_seconds"], 30)
                self.assertEqual(result["requested_window_seconds"], 30)
                self.assertEqual(result["cycles_started"], 1)
                self.assertEqual(result["cycles_completed"], 1)
                self.assertEqual(result["worker_method"], "run_once")
                self.assertTrue(result["supervised"])
                self.assertFalse(result["background"])
                self.assertLessEqual(result["started_at"], result["deadline_at"])
                self.assertIsNotNone(result["finished_at"])
                self.assertEqual(control.calls[0][1]["confirmation"], runtime.TESTNET_CONFIRMATION)
                self.assertEqual(control.calls[0][1]["window_seconds"], 30)
                self.assertIn("deadline_monotonic", control.calls[0][1])
                self.assertFalse(Path(runtime.lock_path).exists())
            finally:
                runtime.stop()
                self.assertFalse(Path(runtime.lock_path).exists())
                self.assertFalse(Path(runtime.pid_path).exists())
                with closing(sqlite3.connect(runtime.db_path)) as reopened:
                    self.assertEqual(reopened.execute("SELECT 1").fetchone()[0], 1)
                Path(runtime.db_path).unlink()

    def test_testnet_dashboard_actions_honor_credential_removal_and_rotation(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime, control, _ = self.configured_auto_runtime(Path(directory), _AutoWorker())
            try:
                self.assertIs(runtime.dashboard_data.binance_canary, runtime)
                credential_store = runtime.credential_store
                keyring = credential_store._keyring

                keyring.values.clear()
                removed = runtime.dashboard_data.binance_canary.action("PAUSE", {})
                self.assertFalse(removed["ok"])
                self.assertEqual(removed["reason"], "CREDENTIALS_NOT_CONFIGURED")
                self.assertEqual(control.calls, [])

                credential_store.configure("rotated-key", "rotated-secret")
                rotated = runtime.dashboard_data.binance_canary.action("PAUSE", {})
                self.assertFalse(rotated["ok"])
                self.assertEqual(rotated["reason"], "CREDENTIALS_CHANGED")
                self.assertEqual(control.calls, [])
            finally:
                runtime.stop()

    def test_testnet_auto_refuses_probe_unknown_before_authorization(self):
        class UnknownProbeGate(_TestnetGate):
            def probe_status(self):
                self.calls.append("probe_status")
                return {
                    "status": "UNKNOWN",
                    "reason": "AMBIGUOUS_VENUE_RESULT",
                    "risk": {"reasons": ["PROBE_UNKNOWN"]},
                }

        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker()
            gate = UnknownProbeGate()
            runtime, control, _ = self.configured_auto_runtime(Path(directory), worker, gate=gate)
            try:
                result = runtime.auto(
                    confirmation=runtime.TESTNET_CONFIRMATION,
                    window_seconds=30,
                )
                self.assertEqual(result["reason"], "PROBE_UNRESOLVED")
                self.assertNotIn("AUTHORIZE", [name for name, _ in control.calls])
                self.assertEqual(worker.calls, [])
            finally:
                runtime.stop()

    def test_testnet_auto_elapsed_deadline_skips_worker_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker()
            runtime, control, _ = self.configured_auto_runtime(Path(directory), worker)
            try:
                with patch(
                    "axiom.binance_dev.time.monotonic",
                    side_effect=(0.0, 31.0, 31.0, 31.0, 31.0, 31.0),
                ):
                    result = runtime.auto(
                        confirmation=runtime.TESTNET_CONFIRMATION,
                        window_seconds=30,
                    )
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "DEADLINE_ELAPSED")
                self.assertEqual(result["cycles_started"], 0)
                self.assertEqual(result["cycles_completed"], 0)
                self.assertIsNone(result["worker_method"])
                self.assertEqual(worker.calls, [])
                self.assertIsNone(runtime._worker_thread)
                self.assertEqual([action for action, _ in control.calls], [])
                self.assertFalse(Path(runtime.lock_path).exists())
            finally:
                runtime.stop()

    def test_testnet_auto_worker_exception_pauses_and_releases_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker(error=RuntimeError("cycle failed"))
            runtime, control, _ = self.configured_auto_runtime(Path(directory), worker)
            try:
                result = runtime.auto(
                    confirmation=runtime.TESTNET_CONFIRMATION,
                    window_seconds=30,
                )
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "RuntimeError")
                self.assertEqual(result["cycles_started"], 1)
                self.assertEqual(result["cycles_completed"], 0)
                self.assertEqual(worker.calls, ["run_once"])
                self.assertEqual(worker.run_once_thread_ids, [threading.get_ident()])
                self.assertEqual([action for action, _ in control.calls], ["AUTHORIZE", "PAUSE", "DISARM"])
                self.assertFalse(Path(runtime.lock_path).exists())
                self.assertFalse(runtime._started)
                self.assertIsNone(runtime._worker_thread)
            finally:
                runtime.stop()

    def test_testnet_auto_cleanup_failure_reports_both_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker()
            gate = _TestnetGate()
            execution = _Execution()
            control = _CleanupFailControl(gate, execution=execution, worker=worker)
            runtime, control, _ = self.configured_auto_runtime(
                Path(directory),
                worker,
                gate=gate,
                control=control,
            )
            try:
                result = runtime.auto(
                    confirmation=runtime.TESTNET_CONFIRMATION,
                    window_seconds=30,
                )
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "CLEANUP_FAILED")
                self.assertFalse(result["cleanup"]["ok"])
                self.assertTrue(result["cleanup"]["pause"]["ok"])
                self.assertFalse(result["cleanup"]["disarm"]["ok"])
                self.assertEqual(
                    [item["action"] for item in result["cleanup"]["attempts"]],
                    ["PAUSE", "DISARM"],
                )
                self.assertFalse(result["cleanup"]["terminal_verified"])
                self.assertEqual(
                    [action for action, _ in control.calls],
                    ["AUTHORIZE", "PAUSE", "DISARM"],
                )
                self.assertFalse(Path(runtime.lock_path).exists())
            finally:
                runtime.stop()

    def test_testnet_auto_cleanup_rejects_nested_failure_even_with_outer_ok(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker()
            gate = _TestnetGate()
            execution = _Execution()
            control = _NestedCleanupControl(gate, execution=execution, worker=worker)
            runtime, control, _ = self.configured_auto_runtime(
                Path(directory),
                worker,
                gate=gate,
                control=control,
            )
            try:
                result = runtime.auto(
                    confirmation=runtime.TESTNET_CONFIRMATION,
                    window_seconds=30,
                )
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "CLEANUP_FAILED")
                self.assertFalse(result["cleanup"]["pause"]["ok"])
                self.assertFalse(result["cleanup"]["ok"])
                self.assertEqual([action for action, _ in control.calls], ["AUTHORIZE", "PAUSE", "DISARM"])
            finally:
                runtime.stop()

    def test_testnet_auto_keyboard_interrupt_propagates_after_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = _AutoWorker(error=KeyboardInterrupt())
            runtime, control, _ = self.configured_auto_runtime(Path(directory), worker)
            try:
                with self.assertRaises(KeyboardInterrupt):
                    runtime.auto(
                        confirmation=runtime.TESTNET_CONFIRMATION,
                        window_seconds=30,
                    )
                self.assertEqual(worker.calls, ["run_once"])
                self.assertEqual([action for action, _ in control.calls], ["AUTHORIZE", "PAUSE", "DISARM"])
                self.assertFalse(Path(runtime.lock_path).exists())
                self.assertFalse(runtime._started)
                self.assertIsNone(runtime._worker_thread)
            finally:
                runtime.stop()

    def test_testnet_auto_lock_failure_never_arms_and_runtime_closes(self):
        keyring = _FakeKeyring()
        credentials = BinanceCredentialStore(
            ref=BinanceCredentialStore(
                environment=BINANCE_SPOT_TESTNET
            ).ref,
            keyring_backend=keyring,
        )
        credentials.configure("testnet-key", "testnet-secret")
        gate = _TestnetGate()
        worker = _AutoWorker()
        strategy = _AutoStrategyState()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = BinanceRuntimeProfile.testnet(root, root / "runtime-data" / "binance-testnet.sqlite")
            venue = BinanceSpotRESTClient(
                BINANCE_SPOT_TESTNET,
                {"api_key": "testnet-key", "api_secret": "testnet-secret"},
            )
            gate.profile = profile
            gate._venue = venue
            gate.venue = venue
            gate.credential_store = credentials
            execution_holder: dict[str, _Execution] = {}
            control = _StatefulTestnetControl(
                gate,
                strategy=strategy,
                execution=None,
                worker=worker,
            )
            control.profile = profile

            def make_execution(store, **kwargs):
                linked = _Execution(store=store, **kwargs)
                execution_holder["value"] = linked
                control.execution = linked
                return linked
            def make_worker(store, linked_execution, **kwargs):
                worker.profile = kwargs["profile"]
                worker.store = store
                worker.execution = linked_execution
                worker.qualification = kwargs["qualification"]
                worker._provider = kwargs.get("provider")
                worker._collector = kwargs.get("collector")
                return worker
            def make_gate(store, **kwargs):
                gate.store = store
                return gate
            runtime = BinanceTestnetRuntime(
                root,
                profile=profile,
                credential_store=credentials,
                venue=venue,
                gate_factory=make_gate,
                execution=None,
                execution_factory=make_execution,
                worker=None,
                strategy=strategy,
                worker_factory=make_worker,
                control=control,
                dashboard_server_factory=_Server,
            )
            server = runtime.server
            try:
                with patch.object(
                    runtime,
                    "_acquire_testnet",
                    side_effect=OSError("TESTNET_PROFILE_LOCK_BUSY"),
                ):
                    with self.assertRaisesRegex(OSError, "TESTNET_PROFILE_LOCK_BUSY"):
                        runtime.auto(
                            confirmation=runtime.TESTNET_CONFIRMATION,
                            window_seconds=30,
                        )
                self.assertNotEqual(strategy.state, "ARMED")
                self.assertEqual(control.calls, [])
            finally:
                runtime.stop()
            self.assertTrue(server.stopped)
            self.assertFalse(Path(runtime.lock_path).exists())
            self.assertFalse(Path(runtime.pid_path).exists())
            self.assertFalse(Path(runtime.stop_path).exists())

    def test_binance_testnet_cli_mutations_use_profile_locked_wrapper(self):
        cases = (
            (
                ["binance-testnet", "connectivity"],
                "CONNECTIVITY_CHECK",
                (),
            ),
            (
                ["binance-testnet", "validate", "--symbol", "BTC/USDT"],
                "ORDER_VALIDATION_TEST",
                ({"symbol": "BTC/USDT"},),
            ),
            (
                [
                    "binance-testnet",
                    "probe",
                    "--symbol",
                    "BTCUSDT",
                    "--confirmation",
                    "RUN BINANCE TESTNET EXECUTION PROBE",
                ],
                "EXECUTION_PROBE",
                (
                    {
                        "symbol": "BTCUSDT",
                        "confirmation": "RUN BINANCE TESTNET EXECUTION PROBE",
                    },
                ),
            ),
            (
                ["binance-testnet", "reconcile"],
                "RECONCILE_PROBE",
                (),
            ),
        )
        with patch("axiom.cli.BinanceTestnetRuntime") as runtime_type:
            runtime = runtime_type.return_value
            runtime.locked_action.return_value = {"ok": True}
            for argv, expected_action, expected_args in cases:
                runtime.reset_mock()
                runtime.locked_action.return_value = {"ok": True}
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(_main_impl(argv), 0)
                self.assertEqual(runtime.locked_action.call_args.args[0], expected_action)
                self.assertEqual(runtime.locked_action.call_args.args[1:], expected_args)
                runtime.stop.assert_called_once_with()

    def test_binance_testnet_cli_command_shape_is_fixed(self):
        self.assertEqual(
            build_parser().parse_args(["binance-testnet", "status"]).binance_testnet_command,
            "status",
        )
        validate = build_parser().parse_args(
            ["binance-testnet", "validate", "--symbol", "BTC/USDT"]
        )
        self.assertEqual(validate.symbol, "BTC/USDT")
        probe = build_parser().parse_args(
            [
                "binance-testnet",
                "probe",
                "--symbol",
                "BTCUSDT",
                "--confirmation",
                "RUN BINANCE TESTNET EXECUTION PROBE",
            ]
        )
        self.assertEqual(probe.binance_testnet_command, "probe")
        auto = build_parser().parse_args(
            [
                "binance-testnet",
                "auto",
                "--confirmation",
                "ENABLE BINANCE TESTNET AUTO CANARY",
                "--window-seconds",
                "30",
            ]
        )
        self.assertEqual(auto.window_seconds, 30)
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["binance-testnet", "probe"])

if __name__ == "__main__":
    unittest.main()
