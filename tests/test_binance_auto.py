from __future__ import annotations

from datetime import datetime, timezone
import json
import tempfile
import time
import unittest

from axiom.binance_auto import BinanceAutonomousWorker, _strategy_digest
from axiom.binance_execution import ARMED, DISABLED, KILLED
from axiom.binance_research import project_testnet_execution_binding
from axiom.crypto_universe import UniverseSnapshot
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, tzinfo=UTC)


def universe(*symbols: str, status: str = "CURRENT") -> UniverseSnapshot:
    rows = tuple(
        {
            "symbol": symbol,
            "binance_symbol": symbol,
            "selected": True,
            "rank": index,
        }
        for index, symbol in enumerate(symbols, 1)
    )
    return UniverseSnapshot(
        universe_id="universe-test",
        version="u-v1",
        snapshot_hash="sha256:universe-test",
        observed_at=T0,
        status=status,
        records=rows,
    )


def market(symbol: str, *, tradable: bool = True, new_entry_allowed: bool = True, status: str = "TRADING", error: str | None = None) -> dict:
    return {
        "symbol": symbol,
        "status": status,
        "tradable": tradable,
        "new_entry_allowed": new_entry_allowed,
        "ticker_fresh": True,
        "book_fresh": True,
        "depth": {"bid_levels": 1, "ask_levels": 1, "fresh": True},
        "spread": "0.10",
        "fill_evidence": {
            "buy": {"complete": True, "price": "10"},
            "sell": {"complete": True, "price": "9.90"},
        },
        "error": error,
    }


def entry_signal(signal_id: str, symbol: str, candidate: str) -> dict:
    return {
        "signal_id": signal_id,
        "candidate_id": candidate,
        "binding_hash": "binding-" + candidate,
        "symbol": symbol,
        "environment": "PAPER",
        "decision_interval": "1d",
        "decision_at": T0.isoformat(),
        "intent": "ENTRY",
        "side": "BUY",
    }


class FakeExecution:
    def __init__(
        self,
        *,
        state: str = ARMED,
        authorized: bool = True,
        positions: list[dict] | None = None,
        reconcile_result: dict | None = None,
        order_rows: list[dict] | None = None,
    ):
        self.state = state
        self.authorized = authorized
        self.position_rows = list(positions or [])
        self.order_rows = list(order_rows or [])
        self.reconcile_result = reconcile_result or {"status": "SUCCESS"}
        self.reconcile_calls = 0
        self.connectivity_calls = 0
        self.pause_reasons: list[str] = []
        self.submissions: list[dict] = []

    def reconcile(self):
        self.reconcile_calls += 1
        if isinstance(self.reconcile_result, BaseException):
            raise self.reconcile_result
        return dict(self.reconcile_result)

    def control(self):
        return {"state": self.state, "authorized": self.authorized}

    def check_connectivity(self):
        self.connectivity_calls += 1
        return {"status": "OK"}

    def positions(self):
        return list(self.position_rows)

    def orders(self):
        return list(self.order_rows)

    def pause(self, reason: str):
        self.pause_reasons.append(reason)
        self.state = "PAUSED"
        self.authorized = False
        return self.control()

    def submit_signal(self, signal, **kwargs):
        self.submissions.append({"signal": dict(signal), **kwargs})
        return {"state": "ACKNOWLEDGED", "signal_id": signal["signal_id"]}


class FakeCollector:
    def __init__(self, records, *, failure: BaseException | None = None):
        self.records = list(records)
        self.failure = failure
        self.calls: list[dict] = []

    def collect(self, snapshot, *, interval="1d", limit=1000, exit_symbols=(), reconciliation=False, now=None):
        self.calls.append({
            "snapshot": snapshot,
            "interval": interval,
            "limit": limit,
            "exit_symbols": tuple(exit_symbols),
            "reconciliation": reconciliation,
            "now": now,
        })
        if self.failure is not None:
            raise self.failure
        return list(self.records)


class FakeQualification:
    def __init__(self, ranking, *, status="CURRENT"):
        self.ranking = dict(ranking)
        self.selection_status = status
        self.calls: list[dict] = []

    def rank_and_select(self, feasibility, *, limit=3, now=None):
        self.calls.append({"feasibility": dict(feasibility), "limit": limit, "now": now})
        return dict(self.ranking)

    def status(self):
        return {"selection_status": self.selection_status}


class FakeEngine:
    def __init__(self, binding, intent, signal=None):
        self.binding = dict(binding)
        self.intent = intent
        self.signal = signal

    def evaluate(self, snapshot, *, now=None, positions=None):
        return self.signal


class BinanceAutonomousWorkerTests(unittest.TestCase):
    def setUp(self):
        self.store = AxiomStore(":memory:")

    def tearDown(self):
        self.store.close()

    def worker(self, execution, collector, qualification=None, **kwargs):
        return BinanceAutonomousWorker(
            self.store,
            execution,
            universe=universe("BTCUSDT", "ETHUSDT"),
            collector=collector,
            qualification=qualification,
            clock=lambda: T0,
            interval_seconds=0,
            **kwargs,
        )
    def persisted_candidate(self, candidate_id="frozen-entry", symbol="BTCUSDT"):
        strategy = {
            "version": 1,
            "market_type": "crypto_spot",
            "family": "trend",
            "parameters": {"lookback": 5},
            "operations": [],
            "metadata": {
                "api_token": "immutable-token",
                "secret_key": "immutable-secret",
                "key": "immutable-key",
            },
        }
        strategy_hash = _strategy_digest(strategy)
        self.store.save_strategy_if_absent("trend", strategy, version="1")
        source_binding = {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "frozen_hash": "frozen-" + candidate_id,
            "strategy_hash": strategy_hash,
            "model_hash": "model-" + candidate_id,
            "config_hash": "config-" + candidate_id,
            "plan_hash": "plan-" + candidate_id,
            "universe_id": "universe-test",
            "universe_version": "u-v1",
            "universe_snapshot": "sha256:universe-test",
            "asset_symbol_mapping": {"BASE": symbol},
            "dataset_id": "crypto",
            "dataset_version": "crypto-v1",
            "timeframe": "1d",
            "source": "HISTORICAL",
            "quality": "HIGH",
            "survivorship": "point_in_time",
            "environment": "PAPER",
            "venue": "BINANCE_SPOT",
            "adapter_version": "fixture",
            "strategy_ref": {
                "strategy_id": "trend",
                "strategy_version": "1",
                "strategy_hash": strategy_hash,
            },
        }
        successor, source_hash = project_testnet_execution_binding(source_binding)
        source_binding["binding_hash"] = source_hash
        payload = {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "binding": source_binding,
            "strategy_ref": source_binding["strategy_ref"],
            "strategy_document": strategy,
            "qualification_hash": "qualification-" + candidate_id,
            "immutable_hashes": {
                "frozen_hash": source_binding["frozen_hash"],
                "secret_key": "immutable-evidence",
            },
        }
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(
            candidate_id, "FROZEN", payload, from_stage="IDEA", timestamp=T0
        )
        row = {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "rank": 1,
            "qualified": True,
            "binding": source_binding,
            "binding_hash": source_hash,
            "qualification_hash": payload["qualification_hash"],
            "immutable_hashes": payload["immutable_hashes"],
            "strategy_ref": source_binding["strategy_ref"],
        }
        successor_map = successor.as_dict()
        successor_map["binding_hash"] = successor.binding_hash
        return strategy, row, dict(source_binding), successor_map, source_hash

    def persisted_qualification(self, ranking):
        qualification = FakeQualification(ranking)
        qualification.store = self.store
        return qualification

    def test_reconciles_before_entries_when_disabled_and_killed(self):
        ranking = {"selection_status": "CURRENT", "rankings": [{"candidate_id": "c1", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]}
        for state in (DISABLED, KILLED):
            with self.subTest(state=state):
                execution = FakeExecution(state=state, authorized=False)
                collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
                qualification = FakeQualification(ranking)
                worker = self.worker(execution, collector, qualification, worker_id="worker-" + state)
                result = worker.cycle(now=T0)
                self.assertEqual(execution.reconcile_calls, 1)
                self.assertEqual(execution.submissions, [])
                self.assertEqual(result["control_state"], state)
                self.assertTrue(result["no_trade_reason"].startswith("CONTROL_"))

    def test_missing_universe_is_no_trade_without_market_or_qualification_calls(self):
        class ForbiddenProvider:
            def __getattr__(self, name):
                raise AssertionError(f"provider called: {name}")

        class ForbiddenQualification:
            def rank_and_select(self, *args, **kwargs):
                raise AssertionError("qualification called")

        execution = FakeExecution(state=ARMED, authorized=True)
        collector = FakeCollector([], failure=AssertionError("collector called"))
        worker = BinanceAutonomousWorker(
            self.store,
            execution,
            collector=collector,
            provider=ForbiddenProvider(),
            universe_loader=lambda **_: None,
            qualification=ForbiddenQualification(),
            clock=lambda: T0,
            interval_seconds=0,
            worker_id="missing-universe",
        )

        result = worker.cycle(now=T0)

        self.assertEqual(execution.reconcile_calls, 1)
        self.assertEqual(execution.connectivity_calls, 0)
        self.assertEqual(execution.pause_reasons, [])
        self.assertEqual(result["status"], "NO_TRADE")
        self.assertEqual(result["no_trade_reason"], "NO_UNIVERSE")
        self.assertEqual(result["entries"], [])
        self.assertEqual(result["exits"], [])
        self.assertEqual(result["events"], [])
        self.assertIsNone(result["error"])
        self.assertIsNone(result["pause_reason"])
        row = self.store.connection.execute(
            "SELECT status,no_trade_reason,pause_reason FROM binance_auto_state WHERE singleton=1"
        ).fetchone()
        self.assertEqual(tuple(row), ("NO_TRADE", "NO_UNIVERSE", None))


    def test_collects_exact_snapshot_membership_and_exit_symbols(self):
        execution = FakeExecution(state=DISABLED, authorized=False, positions=[{"symbol": "XRPUSDT", "quantity": "1"}])
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT"), market("XRPUSDT")])
        result = self.worker(execution, collector).cycle(now=T0)
        self.assertEqual(result["provenance"]["selected_symbols"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(collector.calls[0]["snapshot"].snapshot_hash, "sha256:universe-test")
        self.assertEqual(collector.calls[0]["exit_symbols"], ("XRPUSDT",))
        self.assertFalse(collector.calls[0]["reconciliation"])

    def test_successful_reconciliation_allows_normal_entry_but_not_owned_exit_only(self):
        position = {
            "symbol": "XRPUSDT",
            "quantity": "1",
            "candidate_id": "owned-candidate",
            "originating_binding": {
                "candidate_id": "owned-candidate",
                "symbol": "XRPUSDT",
                "binding_hash": "owned-binding",
                "timeframe": "1d",
            },
            "exit_policy": {"max_holding_bars": 2},
        }
        ranking = {
            "selection_status": "CURRENT",
            "rankings": [
                {"candidate_id": "owned-candidate", "symbol": "XRPUSDT", "rank": 1, "qualified": True},
                {"candidate_id": "normal-candidate", "symbol": "BTCUSDT", "rank": 2, "qualified": True},
            ],
        }
        execution = FakeExecution(positions=[position])
        btc_market, eth_market, xrp_market = market("BTCUSDT"), market("ETHUSDT"), market("XRPUSDT")
        collector = FakeCollector([btc_market, eth_market, xrp_market])
        def factory(**kwargs):
            if kwargs["intent"] == "EXIT":
                signal = entry_signal("exit-owned", "XRPUSDT", "owned-candidate")
                signal.update({"intent": "EXIT", "side": "SELL"})
            else:
                signal = entry_signal("entry-normal", "BTCUSDT", "normal-candidate")
            return FakeEngine(kwargs["binding"], kwargs["intent"], signal)

        result = self.worker(
            execution,
            collector,
            FakeQualification(ranking),
            signal_engine_factory=factory,
        ).cycle(now=T0)

        self.assertFalse(collector.calls[0]["reconciliation"])
        self.assertEqual([item["symbol"] for item in result["entries"]], ["BTCUSDT"])
        self.assertEqual([item["status"] for item in result["entries"]], ["SUBMITTED"])
        entry_submissions = [
            item for item in execution.submissions if item["signal"]["intent"] == "ENTRY"
        ]
        self.assertEqual(len(entry_submissions), 1)
        self.assertIs(
            next(item for item in execution.submissions if item["signal"]["intent"] == "EXIT")["market"],
            xrp_market,
        )
        self.assertIs(entry_submissions[0]["market"], btc_market)
        self.assertEqual(entry_submissions[0]["signal"]["symbol"], "BTCUSDT")
        self.assertEqual(
            [item["signal"]["intent"] for item in execution.submissions],
            ["EXIT", "ENTRY"],
        )

    def test_bounded_deterministic_ranking_uses_lower_rank_fallback(self):
        rows = [
            {"candidate_id": "top", "symbol": "BTCUSDT", "rank": 1, "total_score": "9", "qualified": True},
            {"candidate_id": "lower", "symbol": "ETHUSDT", "rank": 2, "total_score": "8", "qualified": True},
        ]
        # The production qualification service returns the winner separately
        # from its bounded fallback list.  A blocked winner must not truncate
        # that list before the lower candidate is evaluated.
        ranking = {"selection_status": "CURRENT", "selected": rows[0], "fallbacks": [rows[1]]}
        execution = FakeExecution()
        qualification = FakeQualification(ranking)
        collector = FakeCollector([market("BTCUSDT", new_entry_allowed=False), market("ETHUSDT")])
        def factory(**kwargs):
            binding = kwargs["binding"]
            candidate = str(binding["candidate_id"])
            symbol = str(binding["symbol"])
            signal = entry_signal(candidate + "-signal", symbol, candidate)
            return FakeEngine(binding, kwargs["intent"], signal)
        worker = self.worker(execution, collector, qualification, max_actionable=1, signal_engine_factory=factory)
        result = worker.cycle(now=T0)
        self.assertEqual(qualification.calls[0]["limit"], 1)
        self.assertEqual([row["candidate_id"] for row in result["entries"]], ["lower"])
        self.assertEqual(execution.submissions[0]["signal"]["symbol"], "ETHUSDT")
        evaluations = [
            event for event in result["events"]
            if event["event_type"] == "ENTRY_EVALUATION"
        ]
        self.assertEqual([event["candidate_id"] for event in evaluations], ["top", "lower"])
        self.assertEqual(evaluations[0]["reason"], "EXECUTION_EVIDENCE_INFEASIBLE")
        lower_events = [event for event in result["events"] if event["candidate_id"] == "lower"]
        self.assertEqual([event["event_type"] for event in lower_events], ["ENTRY_EVALUATION", "ENTRY_SUBMIT"])

    def test_no_trade_is_durable_and_status_survives_restart(self):
        execution = FakeExecution(state=DISABLED, authorized=False)
        worker = self.worker(execution, FakeCollector([market("BTCUSDT"), market("ETHUSDT")]), worker_id="restart-worker")
        first = worker.cycle(now=T0)
        self.assertEqual(first["status"], "NO_TRADE")
        row = self.store.connection.execute("SELECT status,no_trade_reason FROM binance_auto_state").fetchone()
        self.assertEqual(row[0], "NO_TRADE")
        self.assertIsNotNone(row[1])
        restarted = self.worker(execution, FakeCollector([market("BTCUSDT"), market("ETHUSDT")]), worker_id="restart-worker")
        self.assertEqual(restarted.status()["cycle_number"], 1)
        second = restarted.cycle(now=T0)
        self.assertEqual(second["cycle_number"], 2)

    def test_signal_interval_is_deduplicated_before_second_execution_call(self):
        execution = FakeExecution()
        qualification = FakeQualification({"selection_status": "CURRENT", "rankings": [{"candidate_id": "c1", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]})
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        signal = entry_signal("same-interval", "BTCUSDT", "c1")
        factory = lambda **kwargs: FakeEngine(kwargs["binding"], kwargs["intent"], signal)
        worker = self.worker(execution, collector, qualification, signal_engine_factory=factory)
        first = worker.cycle(now=T0)
        second = worker.cycle(now=T0)
        self.assertEqual(len(execution.submissions), 1)
        self.assertEqual(first["status"], "ACTIONED")
        self.assertEqual(second["entries"][0]["result"]["reason"], "DUPLICATE_INTERVAL")

    def test_stale_unqualified_and_non_trading_markets_block_entries(self):
        cases = (
            (universe("BTCUSDT", "ETHUSDT", status="STALE"), market("BTCUSDT"), "UNIVERSE_STALE"),
            (universe("BTCUSDT", "ETHUSDT"), market("BTCUSDT"), "QUALIFICATION_SELECTION_NOT_CURRENT"),
            (universe("BTCUSDT", "ETHUSDT"), market("BTCUSDT", status="BREAK"), "EXECUTION_EVIDENCE_INFEASIBLE"),
        )
        for index, (snapshot, btc, expected) in enumerate(cases):
            with self.subTest(expected=expected):
                execution = FakeExecution()
                ranking = {"selection_status": "CURRENT", "rankings": [{"candidate_id": "c1", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]}
                if expected == "QUALIFICATION_SELECTION_NOT_CURRENT":
                    ranking["selection_status"] = "STALE"
                worker = BinanceAutonomousWorker(
                    self.store,
                    execution,
                    universe=snapshot,
                    collector=FakeCollector([btc, market("ETHUSDT")]),
                    qualification=FakeQualification(ranking),
                    clock=lambda: T0,
                    interval_seconds=0,
                    worker_id="gate-" + str(index),
                )
                result = worker.cycle(now=T0)
                self.assertEqual(execution.submissions, [])
                self.assertEqual(result["no_trade_reason"], expected)

    def test_removed_position_exits_from_origin_policy_before_entry(self):
        position = {
            "symbol": "XRPUSDT",
            "quantity": "2",
            "candidate_id": "old-winner",
            "binding_hash": "old-binding",
            "originating_binding": {"candidate_id": "old-winner", "symbol": "XRPUSDT", "binding_hash": "old-binding", "timeframe": "1d"},
            "exit_policy": {"max_holding_bars": 1, "strategy": {"name": "origin-strategy"}},
        }
        execution = FakeExecution(positions=[position])
        ranking = {"selection_status": "CURRENT", "rankings": [{"candidate_id": "new", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]}
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT"), market("XRPUSDT")])
        seen: list[tuple[str, dict]] = []

        def factory(**kwargs):
            seen.append((kwargs["intent"], dict(kwargs["exit_policy"])))
            if kwargs["intent"] == "EXIT":
                return FakeEngine(kwargs["binding"], "EXIT", {**entry_signal("exit-old", "XRPUSDT", "old-winner"), "intent": "EXIT", "side": "SELL", "symbol": "XRPUSDT"})
            return FakeEngine(kwargs["binding"], "ENTRY", entry_signal("entry-new", "BTCUSDT", "new"))

        result = BinanceAutonomousWorker(
            self.store,
            execution,
            universe=universe("BTCUSDT", "ETHUSDT"),
            collector=collector,
            qualification=FakeQualification(ranking),
            signal_engine_factory=factory,
            clock=lambda: T0,
            interval_seconds=0,
        ).cycle(now=T0)
        self.assertEqual([item["status"] for item in result["exits"]], ["SUBMITTED"])
        self.assertEqual(seen[0], ("EXIT", {"max_holding_bars": 1, "strategy": {"name": "origin-strategy"}}))
        self.assertEqual(execution.submissions[0]["signal"]["intent"], "EXIT")
        self.assertEqual(execution.submissions[1]["signal"]["intent"], "ENTRY")

    def test_paused_restart_suppresses_exit_for_held_sell_reservation(self):
        position = {
            "symbol": "BTCUSDT",
            "quantity": "1",
            "candidate_id": "owned",
            "binding_hash": "owned-binding",
        }
        held_sell = {
            "symbol": "BTCUSDT",
            "intent": "EXIT",
            "side": "SELL",
            "state": "UNKNOWN",
            "risk_reservation": {"status": "HELD", "reserved_quantity": "1"},
        }
        execution = FakeExecution(
            state="PAUSED",
            authorized=False,
            positions=[position],
            order_rows=[held_sell],
        )
        first_collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        first = self.worker(execution, first_collector, worker_id="paused-sell-restart")
        first_result = first.cycle(now=T0)
        self.assertEqual(first_result["exits"], [])
        self.assertEqual(first_collector.calls[0]["exit_symbols"], ())
        self.assertEqual(execution.submissions, [])

        restarted_collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        restarted = self.worker(
            execution,
            restarted_collector,
            worker_id="paused-sell-restart",
        )
        second_result = restarted.cycle(now=T0)
        self.assertEqual(second_result["exits"], [])
        self.assertEqual(restarted_collector.calls[0]["exit_symbols"], ())
        self.assertEqual(execution.submissions, [])

    def test_polling_and_market_failures_pause_entries_without_thread_death(self):
        execution = FakeExecution(reconcile_result=ConnectionError("poll failed"))
        ranking = FakeQualification({"selection_status": "CURRENT", "rankings": [{"candidate_id": "c1", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]})
        worker = self.worker(execution, FakeCollector([market("BTCUSDT"), market("ETHUSDT")]), ranking, worker_id="poll-failure")
        results = worker.run(max_cycles=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "PAUSED")
        self.assertEqual(execution.submissions, [])

        execution = FakeExecution()
        worker = self.worker(execution, FakeCollector([], failure=RuntimeError("market failed")), ranking, worker_id="market-failure")
        results = worker.run(max_cycles=1)
        self.assertEqual(results[0]["status"], "PAUSED")
        self.assertEqual(execution.submissions, [])

    def test_owned_position_never_averages_down(self):
        # Account projections may include a zero net quantity alongside a
        # positive AXIOM-owned quantity, and a venue source is not foreign
        # ownership by itself.
        execution = FakeExecution(
            positions=[{
                "symbol": "BTCUSDT",
                "quantity": "0",
                "owned_quantity": "1",
                "source": "BINANCE",
                "candidate_id": "old",
            }]
        )
        ranking = FakeQualification({"selection_status": "CURRENT", "rankings": [{"candidate_id": "new", "symbol": "BTCUSDT", "rank": 1, "qualified": True}]})
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        evaluated: list[str] = []

        def factory(**kwargs):
            evaluated.append(kwargs["intent"])
            return FakeEngine(kwargs["binding"], kwargs["intent"], entry_signal("entry", "BTCUSDT", "new"))

        result = self.worker(execution, collector, ranking, signal_engine_factory=factory).cycle(now=T0)
        self.assertEqual(execution.submissions, [])
        self.assertEqual([item["status"] for item in result["exits"]], ["NO_TRADE"])
        self.assertEqual(result["exits"][0]["reason"], "NON_EXIT_SIGNAL")
        self.assertEqual(evaluated, ["EXIT"])
        self.assertEqual(result["no_trade_reason"], "OWNED_POSITION_NO_AVERAGING")
        evaluations = [
            event for event in result["events"]
            if event["event_type"] == "EXIT_EVALUATION"
        ]
        self.assertEqual(evaluations[0]["reason"], "NON_EXIT_SIGNAL")
        self.assertNotIn("ENTRY", evaluated)

    def test_autonomous_worker_does_not_construct_other_market_family_clients(self):
        execution = FakeExecution(state=DISABLED, authorized=False)
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        result = self.worker(execution, collector).cycle(now=T0)
        self.assertNotIn("polymarket", json.dumps(result).lower())
        self.assertNotIn("hermes", json.dumps(result).lower())


    def test_generated_frozen_candidate_hydrates_exact_strategy_and_evidence(self):
        strategy, row, source, successor, source_hash = self.persisted_candidate()
        ranking = {"selection_status": "CURRENT", "rankings": [row]}
        execution = FakeExecution()
        btc_market, eth_market = market("BTCUSDT"), market("ETHUSDT")
        collector = FakeCollector([btc_market, eth_market])
        captured = []

        def factory(**kwargs):
            captured.append(kwargs)
            binding = kwargs["binding"]
            signal = entry_signal("frozen-entry-signal", binding["symbol"], binding["candidate_id"])
            signal["binding_hash"] = binding["binding_hash"]
            return FakeEngine(binding, kwargs["intent"], signal)

        result = self.worker(
            execution,
            collector,
            self.persisted_qualification(ranking),
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(result["entries"][0]["status"], "SUBMITTED")
        self.assertIs(execution.submissions[0]["market"], btc_market)
        self.assertEqual(captured[0]["strategy"].to_dict(), strategy)
        submitted = execution.submissions[0]["signal"]
        self.assertEqual(submitted["binding_hash"], successor["binding_hash"])
        self.assertEqual(submitted["source_binding_hash"], source_hash)
        self.assertEqual(submitted["strategy_ref"]["strategy_id"], "trend")
        self.assertEqual(submitted["provenance"]["source_binding_hash"], source_hash)
        self.assertEqual(submitted["provenance"]["strategy_ref"]["strategy_hash"], _strategy_digest(strategy))
        altered = json.loads(json.dumps(strategy))
        altered["metadata"]["api_token"] = "changed-token"
        self.assertNotEqual(_strategy_digest(altered), _strategy_digest(strategy))

    def test_explicit_paper_execution_keeps_persisted_row_on_legacy_binding_path(self):
        strategy, row, *_ = self.persisted_candidate("paper-static", "BTCUSDT")
        execution = FakeExecution()
        execution.environment = "PAPER"
        captured = []

        def factory(**kwargs):
            captured.append(kwargs)
            binding = kwargs["binding"]
            return FakeEngine(
                binding,
                kwargs["intent"],
                entry_signal("paper-static-entry", "BTCUSDT", "paper-static"),
            )

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "CURRENT", "rankings": [row]}),
            strategy=strategy,
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(result["entries"][0]["status"], "SUBMITTED")
        self.assertEqual(captured[0]["binding"]["environment"], "PAPER")
        self.assertNotIn("source_binding_hash", captured[0]["binding"])


    def test_paper_persisted_candidate_hydrates_strategy_without_static_injection(self):
        strategy, row, *_ = self.persisted_candidate("paper-hydrated", "BTCUSDT")
        execution = FakeExecution()
        execution.environment = "PAPER"
        captured = []

        def factory(**kwargs):
            captured.append(kwargs)
            binding = kwargs["binding"]
            return FakeEngine(
                binding,
                kwargs["intent"],
                entry_signal("paper-hydrated-entry", "BTCUSDT", "paper-hydrated"),
            )

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "CURRENT", "rankings": [row]}),
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(result["entries"][0]["status"], "SUBMITTED")
        self.assertEqual(captured[0]["strategy"].to_dict(), strategy)

    def test_explicit_paper_execution_keeps_persisted_origin_on_legacy_exit_path(self):
        strategy, row, _, _, source_hash = self.persisted_candidate("paper-exit", "BTCUSDT")
        position = {
            "symbol": "BTCUSDT",
            "quantity": "1",
            "candidate_id": "paper-exit",
            "binding_hash": source_hash,
            "originating_binding": {**row["binding"], "binding_hash": source_hash},
            "originating_provenance": {
                "source_binding_hash": source_hash,
                "strategy_ref": row["strategy_ref"],
            },
            "exit_policy": {"strategy": strategy},
        }
        execution = FakeExecution(positions=[position])
        execution.environment = "PAPER"
        captured = []

        def factory(**kwargs):
            captured.append(kwargs)
            binding = kwargs["binding"]
            signal = entry_signal("paper-exit-signal", "BTCUSDT", "paper-exit")
            signal.update({"intent": "EXIT", "side": "SELL", "binding_hash": binding["binding_hash"]})
            return FakeEngine(binding, kwargs["intent"], signal)

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
            strategy=strategy,
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(result["status"], "ACTIONED")
        self.assertEqual(result["exits"][0]["status"], "SUBMITTED")
        self.assertEqual(captured[0]["binding"]["environment"], "PAPER")

    def test_invalid_frozen_hash_never_falls_back_to_static_strategy(self):
        static_strategy, row, *_ = self.persisted_candidate()
        row["binding_hash"] = "tampered-wrapper-hash"
        execution = FakeExecution()
        engines = []

        def factory(**kwargs):
            engines.append(kwargs)
            return FakeEngine(kwargs["binding"], kwargs["intent"], entry_signal("unexpected", "BTCUSDT", "frozen-entry"))

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "CURRENT", "rankings": [row]}),
            strategy=static_strategy,
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(execution.submissions, [])
        self.assertEqual(engines, [])
        self.assertIn("SOURCE_BINDING_HASH_MISMATCH", result["no_trade_reason"])

    def test_persisted_strategy_support_uses_canonical_same_database_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/auto.sqlite"
            worker_store = AxiomStore(path)
            qualification_store = AxiomStore(path)
            try:
                qualification = FakeQualification({"selection_status": "NONE", "rankings": []})
                qualification.store = qualification_store
                worker = BinanceAutonomousWorker(
                    worker_store,
                    FakeExecution(state=DISABLED, authorized=False),
                    universe=universe("BTCUSDT"),
                    collector=FakeCollector([market("BTCUSDT")]),
                    qualification=qualification,
                    clock=lambda: T0,
                    interval_seconds=0,
                )
                self.assertTrue(worker.supports_persisted_strategy())
            finally:
                qualification_store.close()
                worker_store.close()
    def test_different_database_candidate_id_collision_cannot_hydrate_local_lifecycle(self):
        self.persisted_candidate("collision", "BTCUSDT")
        foreign_store = AxiomStore(":memory:")
        try:
            qualification = FakeQualification({
                "selection_status": "CURRENT",
                "rankings": [{
                    "candidate_id": "collision",
                    "symbol": "BTCUSDT",
                    "rank": 1,
                    "qualified": True,
                }],
            })
            qualification.store = foreign_store
            execution = FakeExecution()
            result = self.worker(
                execution,
                FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                qualification,
                strategy={"market_type": "crypto_spot"},
            ).cycle(now=T0)
            self.assertEqual(execution.submissions, [])
            self.assertIn("PERSISTED_EVIDENCE_STORE_MISMATCH", result["no_trade_reason"])
        finally:
            foreign_store.close()

    def test_cross_database_nested_provenance_hash_is_not_row_proof(self):
        _, _, _, _, source_hash = self.persisted_candidate("nested-collision", "BTCUSDT")
        foreign_store = AxiomStore(":memory:")
        try:
            qualification = FakeQualification({
                "selection_status": "CURRENT",
                "rankings": [{
                    "candidate_id": "nested-collision",
                    "symbol": "BTCUSDT",
                    "rank": 1,
                    "qualified": True,
                    "provenance": {"source_binding_hash": source_hash},
                }],
            })
            qualification.store = foreign_store
            execution = FakeExecution()
            engines = []

            def factory(**kwargs):
                engines.append(kwargs)
                return FakeEngine(kwargs["binding"], kwargs["intent"], entry_signal("unexpected-nested", "BTCUSDT", "nested-collision"))

            result = self.worker(
                execution,
                FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                qualification,
                strategy={"market_type": "crypto_spot"},
                signal_engine_factory=factory,
            ).cycle(now=T0)
            self.assertEqual(execution.submissions, [])
            self.assertEqual(engines, [])
            self.assertIn("PERSISTED_EVIDENCE_STORE_MISMATCH", result["no_trade_reason"])
        finally:
            foreign_store.close()

    def test_cross_database_wrong_row_hash_fails_closed(self):
        _, row, _, _, _ = self.persisted_candidate("wrong-collision", "BTCUSDT")
        row = {
            "candidate_id": row["candidate_id"],
            "symbol": row["symbol"],
            "rank": 1,
            "qualified": True,
            "binding_hash": "wrong-public-proof",
        }
        foreign_store = AxiomStore(":memory:")
        try:
            qualification = FakeQualification({"selection_status": "CURRENT", "rankings": [row]})
            qualification.store = foreign_store
            execution = FakeExecution()
            engines = []

            def factory(**kwargs):
                engines.append(kwargs)
                return FakeEngine(kwargs["binding"], kwargs["intent"], entry_signal("unexpected-wrong", "BTCUSDT", "wrong-collision"))

            result = self.worker(
                execution,
                FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                qualification,
                strategy={"market_type": "crypto_spot"},
                signal_engine_factory=factory,
            ).cycle(now=T0)
            self.assertEqual(execution.submissions, [])
            self.assertEqual(engines, [])
            self.assertIn("SOURCE_BINDING_HASH_MISMATCH", result["no_trade_reason"])
        finally:
            foreign_store.close()

    def test_cross_database_exact_row_hash_can_bind_local_frozen_candidate(self):
        _, row, _, _, source_hash = self.persisted_candidate("exact-collision", "BTCUSDT")
        row = {
            "candidate_id": row["candidate_id"],
            "symbol": row["symbol"],
            "rank": 1,
            "qualified": True,
            "binding_hash": source_hash,
        }
        foreign_store = AxiomStore(":memory:")
        try:
            qualification = FakeQualification({"selection_status": "CURRENT", "rankings": [row]})
            qualification.store = foreign_store
            execution = FakeExecution()
            engines = []

            def factory(**kwargs):
                engines.append(kwargs)
                binding = kwargs["binding"]
                signal = entry_signal("exact-collision-entry", "BTCUSDT", "exact-collision")
                signal["binding_hash"] = binding["binding_hash"]
                return FakeEngine(binding, kwargs["intent"], signal)

            result = self.worker(
                execution,
                FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                qualification,
                strategy={"market_type": "crypto_spot"},
                signal_engine_factory=factory,
            ).cycle(now=T0)
            self.assertEqual(len(engines), 1)
            self.assertEqual(len(execution.submissions), 1)
            self.assertEqual(result["entries"][0]["status"], "SUBMITTED")
        finally:
            foreign_store.close()

    def test_same_database_minimal_row_uses_local_frozen_candidate(self):
        self.persisted_candidate("same-db-minimal", "BTCUSDT")
        qualification = self.persisted_qualification({
            "selection_status": "CURRENT",
            "rankings": [{
                "candidate_id": "same-db-minimal",
                "symbol": "BTCUSDT",
                "rank": 1,
                "qualified": True,
            }],
        })
        execution = FakeExecution()
        engines = []

        def factory(**kwargs):
            engines.append(kwargs)
            binding = kwargs["binding"]
            signal = entry_signal("same-db-minimal-entry", "BTCUSDT", "same-db-minimal")
            signal["binding_hash"] = binding["binding_hash"]
            return FakeEngine(binding, kwargs["intent"], signal)

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            qualification,
            strategy={"market_type": "crypto_spot"},
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(len(engines), 1)
        self.assertEqual(len(execution.submissions), 1)
        self.assertEqual(result["entries"][0]["status"], "SUBMITTED")


    def test_requested_entry_symbol_filters_entries_but_keeps_cross_symbol_exit(self):
        _, row, _, successor, _ = self.persisted_candidate("eth-entry", "ETHUSDT")
        ranking = {
            "selection_status": "CURRENT",
            "rankings": [
                {"candidate_id": "btc-entry", "symbol": "BTCUSDT", "rank": 1, "qualified": True},
                row,
            ],
        }
        position = {
            "symbol": "XRPUSDT",
            "quantity": "1",
            "candidate_id": "static-origin",
            "originating_binding": {
                "candidate_id": "static-origin",
                "symbol": "XRPUSDT",
                "binding_hash": "static-origin-hash",
            },
        }
        execution = FakeExecution(positions=[position])

        def factory(**kwargs):
            binding = kwargs["binding"]
            if kwargs["intent"] == "EXIT":
                signal = entry_signal("cross-exit", "XRPUSDT", "static-origin")
                signal.update({"intent": "EXIT", "side": "SELL", "binding_hash": binding.get("binding_hash")})
            else:
                signal = entry_signal("eth-entry-signal", "ETHUSDT", "eth-entry")
                signal["binding_hash"] = binding["binding_hash"]
            return FakeEngine(binding, kwargs["intent"], signal)

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT"), market("XRPUSDT")]),
            self.persisted_qualification(ranking),
            signal_engine_factory=factory,
        ).cycle(now=T0, symbol="ETHUSDT")
        self.assertEqual([item["symbol"] for item in result["entries"]], ["ETHUSDT"])
        self.assertEqual([item["symbol"] for item in result["exits"]], ["XRPUSDT"])
        self.assertEqual([item["signal"]["intent"] for item in execution.submissions], ["EXIT", "ENTRY"])

    def test_persisted_exit_rejects_non_paper_source_binding(self):
        for environment in ("LIVE", "BINANCE_SPOT_TESTNET"):
            with self.subTest(environment=environment):
                candidate_id = "invalid-origin-" + environment.lower()
                strategy, row, source, successor, source_hash = self.persisted_candidate(candidate_id)
                lifecycle = self.store.load_candidate_lifecycle(candidate_id)
                payload = dict(lifecycle["payload"])
                payload["binding"] = {**payload["binding"], "environment": environment}
                self.store.connection.execute(
                    "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id=?",
                    (json.dumps(payload, sort_keys=True), candidate_id),
                )
                self.store.connection.commit()
                position = {
                    "symbol": "BTCUSDT",
                    "quantity": "1",
                    "candidate_id": candidate_id,
                    "originating_binding": successor,
                    "originating_provenance": {
                        "source_binding_hash": source_hash,
                        "strategy_ref": source["strategy_ref"],
                    },
                }
                execution = FakeExecution(positions=[position])
                engines = []

                def factory(**kwargs):
                    engines.append(kwargs)
                    return FakeEngine(kwargs["binding"], kwargs["intent"], None)

                result = self.worker(
                    execution,
                    FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                    self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
                    signal_engine_factory=factory,
                    worker_id=candidate_id,
                ).cycle(now=T0)
                self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
                self.assertEqual(result["exits"][0]["reason"], "EXIT_ORIGIN_SOURCE_ENVIRONMENT_INVALID")
                self.assertEqual(execution.submissions, [])
                self.assertEqual(engines, [])

    def test_restart_exit_rejects_live_origin_with_matching_paper_lifecycle(self):
        strategy, row, source, successor, source_hash = self.persisted_candidate("live-origin-restart")
        live_origin = {**successor, "environment": "LIVE"}
        position = {
            "symbol": "BTCUSDT",
            "quantity": "1",
            "candidate_id": "live-origin-restart",
            "originating_binding": live_origin,
            "originating_provenance": {
                "source_binding_hash": source_hash,
                "strategy_ref": source["strategy_ref"],
            },
        }
        execution = FakeExecution(positions=[position])
        engines = []

        def factory(**kwargs):
            engines.append(kwargs)
            return FakeEngine(kwargs["binding"], kwargs["intent"], None)

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
            signal_engine_factory=factory,
            worker_id="live-origin-restart-worker",
        ).cycle(now=T0)
        self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
        self.assertEqual(result["exits"][0]["reason"], "EXIT_ORIGIN_ENVIRONMENT_INVALID")
        self.assertEqual(execution.submissions, [])
        self.assertEqual(engines, [])

    def test_restart_exit_rejects_conflicting_root_environment_evidence(self):
        for environment in ("PAPER", "LIVE"):
            with self.subTest(environment=environment):
                strategy, row, source, successor, source_hash = self.persisted_candidate("root-" + environment.lower())
                position = {
                    "symbol": "BTCUSDT",
                    "quantity": "1",
                    "candidate_id": "root-" + environment.lower(),
                    "environment": environment,
                    "originating_binding": successor,
                    "originating_provenance": {
                        "source_binding_hash": source_hash,
                        "strategy_ref": source["strategy_ref"],
                    },
                }
                execution = FakeExecution(positions=[position])
                engines = []

                def factory(**kwargs):
                    engines.append(kwargs)
                    return FakeEngine(kwargs["binding"], kwargs["intent"], None)

                result = self.worker(
                    execution,
                    FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                    self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
                    signal_engine_factory=factory,
                    worker_id="root-" + environment.lower(),
                ).cycle(now=T0)
                self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
                self.assertEqual(result["exits"][0]["reason"], "EXIT_ORIGIN_ENVIRONMENT_INVALID")
                self.assertEqual(execution.submissions, [])
                self.assertEqual(engines, [])

    def test_restart_exit_rejects_conflicting_root_hash_evidence(self):
        for field, expected_reason in (
            ("binding_hash", "EXIT_ORIGIN_BINDING_HASH_MISMATCH"),
            ("source_binding_hash", "EXIT_ORIGIN_PROVENANCE_MISMATCH"),
        ):
            with self.subTest(field=field):
                strategy, row, source, successor, source_hash = self.persisted_candidate("root-hash-" + field)
                position = {
                    "symbol": "BTCUSDT",
                    "quantity": "1",
                    "candidate_id": "root-hash-" + field,
                    "originating_binding": successor,
                    "originating_provenance": {
                        "source_binding_hash": source_hash,
                        "strategy_ref": source["strategy_ref"],
                    },
                    field: "contradictory-root-hash",
                }
                execution = FakeExecution(positions=[position])
                engines = []

                def factory(**kwargs):
                    engines.append(kwargs)
                    return FakeEngine(kwargs["binding"], kwargs["intent"], None)

                result = self.worker(
                    execution,
                    FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                    self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
                    signal_engine_factory=factory,
                    worker_id="root-hash-" + field,
                ).cycle(now=T0)
                self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
                self.assertEqual(result["exits"][0]["reason"], expected_reason)
                self.assertEqual(execution.submissions, [])
                self.assertEqual(engines, [])

    def test_restart_exit_rejects_projected_origin_missing_hash_evidence(self):
        for missing, expected_reason in (
            ("binding_hash", "EXIT_ORIGIN_BINDING_HASH_MISSING"),
            ("source_binding_hash", "EXIT_ORIGIN_PROVENANCE_MISMATCH"),
        ):
            with self.subTest(missing=missing):
                strategy, row, source, successor, source_hash = self.persisted_candidate("missing-" + missing)
                origin = dict(successor)
                provenance = {
                    "source_binding_hash": source_hash,
                    "strategy_ref": source["strategy_ref"],
                }
                if missing == "binding_hash":
                    origin.pop("binding_hash", None)
                else:
                    provenance.pop("source_binding_hash")
                position = {
                    "symbol": "BTCUSDT",
                    "quantity": "1",
                    "candidate_id": "missing-" + missing,
                    "originating_binding": origin,
                    "originating_provenance": provenance,
                }
                execution = FakeExecution(positions=[position])
                engines = []

                def factory(**kwargs):
                    engines.append(kwargs)
                    return FakeEngine(kwargs["binding"], kwargs["intent"], None)

                result = self.worker(
                    execution,
                    FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
                    self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
                    signal_engine_factory=factory,
                    worker_id="missing-" + missing,
                ).cycle(now=T0)
                self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
                self.assertEqual(result["exits"][0]["reason"], expected_reason)
                self.assertEqual(execution.submissions, [])
                self.assertEqual(engines, [])

    def test_restart_exit_rejects_contradictory_nested_payload_hash(self):
        strategy, row, source, successor, source_hash = self.persisted_candidate("contradictory-origin")
        lifecycle = self.store.load_candidate_lifecycle("contradictory-origin")
        payload = dict(lifecycle["payload"])
        payload["provenance"] = {"binding_hash": "contradictory-successor-hash"}
        self.store.connection.execute(
            "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id=?",
            (json.dumps(payload, sort_keys=True), "contradictory-origin"),
        )
        self.store.connection.commit()
        position = {
            "symbol": "BTCUSDT",
            "quantity": "1",
            "candidate_id": "contradictory-origin",
            "originating_binding": successor,
            "originating_provenance": {
                "source_binding_hash": source_hash,
                "strategy_ref": source["strategy_ref"],
            },
        }
        execution = FakeExecution(positions=[position])
        engines = []

        def factory(**kwargs):
            engines.append(kwargs)
            return FakeEngine(kwargs["binding"], kwargs["intent"], None)

        result = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            self.persisted_qualification({"selection_status": "NONE", "rankings": []}),
            signal_engine_factory=factory,
        ).cycle(now=T0)
        self.assertEqual(result["exits"][0]["status"], "NO_TRADE")
        self.assertEqual(result["exits"][0]["reason"], "EXIT_ORIGIN_PROVENANCE_MISMATCH")
        self.assertEqual(execution.submissions, [])
        self.assertEqual(engines, [])

    def test_restart_exit_rehydrates_exact_origin_and_rejects_missing_origin(self):
        strategy, row, source, successor, source_hash = self.persisted_candidate("restart-origin", "BTCUSDT")
        position = {
            "symbol": "BTCUSDT",
            "quantity": "1",
            "candidate_id": "restart-origin",
            "originating_binding": successor,
            "originating_provenance": {
                "source_binding_hash": source_hash,
                "strategy_ref": source["strategy_ref"],
            },
        }
        execution = FakeExecution(positions=[position])
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])

        def factory(**kwargs):
            binding = kwargs["binding"]
            signal = entry_signal("restart-exit", "BTCUSDT", "restart-origin")
            signal.update({"intent": "EXIT", "side": "SELL", "binding_hash": binding["binding_hash"]})
            return FakeEngine(binding, kwargs["intent"], signal)

        worker = self.worker(
            execution, collector, FakeQualification({"selection_status": "NONE", "rankings": []}),
            signal_engine_factory=factory,
        )
        first = worker.cycle(now=T0)
        self.assertEqual(first["exits"][0]["status"], "SUBMITTED")
        execution.position_rows = [{**position, "candidate_id": "missing-origin", "originating_binding": {**successor, "candidate_id": "missing-origin"}}]
        restarted = self.worker(
            execution,
            FakeCollector([market("BTCUSDT"), market("ETHUSDT")]),
            FakeQualification({"selection_status": "NONE", "rankings": []}),
            signal_engine_factory=factory,
            worker_id=worker.worker_id,
        )
        second = restarted.cycle(now=T0)
        self.assertEqual(second["exits"][0]["status"], "NO_TRADE")
        self.assertIn("EXIT_ORIGIN_EVIDENCE_MISMATCH", second["exits"][0]["reason"])
        self.assertEqual(len(execution.submissions), 1)

    def test_deadline_expiry_stops_after_reconcile_without_collection_or_submission(self):
        class SlowReconcile(FakeExecution):
            def reconcile(self, **kwargs):
                time.sleep(0.05)
                return {"status": "SUCCESS"}

        execution = SlowReconcile()
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT")])
        deadline = time.monotonic() + 0.005
        result = self.worker(execution, collector).cycle(
            now=T0, symbol="BTCUSDT", deadline_monotonic=deadline
        )
        self.assertEqual(result["no_trade_reason"], "AUTO_DEADLINE_EXPIRED")
        self.assertEqual(collector.calls, [])
        self.assertEqual(execution.submissions, [])
if __name__ == "__main__":
    unittest.main()
