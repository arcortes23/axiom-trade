from __future__ import annotations

from datetime import datetime, timezone
import json
import tempfile
import unittest

from axiom.binance_auto import BinanceAutonomousWorker
from axiom.binance_execution import ARMED, DISABLED, KILLED
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
        collector = FakeCollector([market("BTCUSDT"), market("ETHUSDT"), market("XRPUSDT")])

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


if __name__ == "__main__":
    unittest.main()
