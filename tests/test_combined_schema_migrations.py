from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from axiom.canary import CanaryService
from axiom.binance_auto import BinanceAutonomousWorker
from axiom.binance_dev import PaperBinanceSpotVenue
from axiom.binance_execution import BinanceExecutionService
from axiom.binance_operator import BinanceCanaryControlPlane
from axiom.binance_research import BinanceCryptoQualificationService
from axiom.canary_settings import CanarySettingsService
from axiom.dashboard import DashboardData
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, tzinfo=UTC)


# This is the smallest Polymarket control schema that predates the generation
# fence and readiness projection columns.  It intentionally contains no data
# from a checkout or runtime database.
_MAIN_ERA_POLYMARKET_DDL = """
CREATE TABLE canary_control (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    state TEXT NOT NULL,
    candidate_id TEXT,
    venue TEXT,
    armed_at TEXT,
    expires_at TEXT,
    limits_json TEXT NOT NULL,
    integrity_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO canary_control(
    singleton,state,candidate_id,venue,armed_at,expires_at,
    limits_json,integrity_hash,updated_at
) VALUES(
    1,'DISABLED',NULL,NULL,NULL,NULL,'{}','legacy-polymarket','2026-01-01T00:00:00+00:00'
);
CREATE TABLE canary_selection (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    ranking_run_id TEXT NOT NULL,
    candidate_id TEXT,
    rank INTEGER,
    total_score REAL,
    component_scores_json TEXT NOT NULL,
    evidence_versions_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    selected_at TEXT NOT NULL
);
INSERT INTO canary_selection(
    singleton,ranking_run_id,candidate_id,rank,total_score,
    component_scores_json,evidence_versions_json,reason,selected_at
) VALUES(
    1,'main-ranking','main-era-winner',1,0.9,'{}','{}','legacy winner','2026-01-01T00:00:00+00:00'
);
"""


# Binance first appeared with a control row, risk reservations, and a local
# operator audit table.  These declarations are deliberately pre-migration:
# the execution service must add entry_policy_hash and reserved_quantity, and
# the operator facade must add target, without importing any persisted state.
_BINANCE_ERA_DDL = _MAIN_ERA_POLYMARKET_DDL + """
CREATE TABLE binance_execution_control (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    state TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    environment TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    envelope_hash TEXT NOT NULL,
    credential_hash TEXT NOT NULL,
    authorized INTEGER NOT NULL DEFAULT 0,
    kill_requested INTEGER NOT NULL DEFAULT 0,
    pause_reason TEXT,
    updated_at TEXT NOT NULL
);
INSERT INTO binance_execution_control(
    singleton,state,generation,environment,binding_hash,envelope_hash,
    credential_hash,authorized,kill_requested,pause_reason,updated_at
) VALUES(
    1,'DISABLED',7,'PAPER',
    'dc29d27ea66c2100e520a10cca3931d8171a1260d68bda8ee8da38dbf8a992b2',
    'd1f90fc569997011c0c03ea1a8b427aa7790b3551556f9c7d6ab10a6deaa6ffe',
    '31bac4a3a86fe0ac346b1097a425ff68bdf0b0675cb028fd6b402ba646bc6aad',
    0,0,NULL,'2026-01-01T00:00:00+00:00'
);
CREATE TABLE binance_execution_risk_reservations (
    reservation_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    amount TEXT NOT NULL,
    fee_reserve TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
INSERT INTO binance_execution_risk_reservations(
    reservation_id,intent_id,symbol,side,amount,fee_reserve,status,created_at,released_at
) VALUES(
    'legacy-reservation','legacy-intent','BTCUSDT','SELL','3','0.03','HELD',
    '2026-01-01T00:00:00+00:00',NULL
);
CREATE TABLE binance_operator_actions (
    action_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    timestamp_pht TEXT NOT NULL,
    success INTEGER NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
INSERT INTO binance_operator_actions(
    action_id,action,attempted_at,completed_at,created_at,timestamp,
    timestamp_utc,timestamp_pht,success,reason,payload_json,result_json
) VALUES(
    'legacy-binance-action','PAUSE',
    '2026-01-01T00:00:00+00:00','2026-01-01T00:00:01+00:00',
    '2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',
    '2026-01-01T00:00:00+00:00','2026-01-01T08:00:00+08:00',
    0,'legacy sentinel','{"sentinel":"operator-payload"}','{"sentinel":"operator-result"}'
);
CREATE TABLE research_queue (
    item_id TEXT PRIMARY KEY,
    item_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    author TEXT NOT NULL DEFAULT '',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    schema_version TEXT NOT NULL DEFAULT '1',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    lease_owner TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    result_json TEXT
);
INSERT INTO research_queue(
    item_id,item_type,dedupe_key,status,priority,payload_json,source,author,
    lineage_json,schema_version,created_at,updated_at,available_at
) VALUES(
    'legacy-research-item','HYPOTHESIS','legacy-research-dedupe','COMPLETED',3,
    '{"statement":"historical research survives migration","paper_only":true}',
    'legacy-research','operator','[]','1',
    '2026-01-01T00:00:00+00:00','2026-01-01T00:01:00+00:00',
    '2026-01-01T00:00:00+00:00'
);
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
INSERT INTO fills(
    fill_id,order_id,timestamp,strategy_id,symbol,payload_json,created_at
) VALUES(
    'legacy-research-fill','legacy-research-order',
    '2026-01-01T00:01:00+00:00','legacy-strategy','BTCUSDT',
    '{"price":"10","quantity":"1","source":"historical"}',
    '2026-01-01T00:01:00+00:00'
);
CREATE TABLE operator_config (
    config_key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO operator_config(config_key,value_json,updated_at) VALUES(
    'canary-risk-limits',
    '{"target_notional_usd":"1.00","max_orders_per_day":1,"max_daily_loss_usd":"0.25"}',
    '2026-01-01T00:00:00+00:00'
);
CREATE TABLE canary_setting_configs (
    config_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    generation INTEGER NOT NULL,
    config_hash TEXT NOT NULL UNIQUE,
    values_json TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    activated_at TEXT,
    previous_config_id TEXT
);
INSERT INTO canary_setting_configs(
    config_id,state,generation,config_hash,values_json,actor,created_at,
    updated_at,activated_at,previous_config_id
) VALUES(
    'cfg-legacy-production','ACTIVE',4,'legacy-settings-hash',
    '{"target_notional_usd":"1.00","max_exposure_usd":"5.00","max_daily_loss_usd":"0.25","max_open_positions":3,"max_orders_per_day":1,"max_slippage_bps":100,"max_all_in_buy_usd":"1.00","max_fee_reserve_usd":"0.01","max_gross_daily_buy_usd":"5.00","max_aggregate_open_cost_usd":"5.00","max_aggregate_exposure_usd":"5.00","max_positions":3,"max_submitted_orders_per_day":1,"realized_loss_entry_stop_usd":"0.25","equity_loss_entry_stop_usd":"0.25","per_market_buy_cap_usd":null,"per_event_buy_cap_usd":null,"cumulative_buy_cap_usd":null}',
    'legacy-operator','2026-01-01T00:00:00+00:00',
    '2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',NULL
);
CREATE TABLE canary_risk_reservations (
    reservation_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE,
    side TEXT NOT NULL,
    market_id TEXT,
    event_id TEXT,
    requested_cost TEXT NOT NULL DEFAULT '0',
    filled_cost TEXT NOT NULL DEFAULT '0',
    remaining_cost TEXT NOT NULL DEFAULT '0',
    fee_reserve TEXT NOT NULL DEFAULT '0',
    quantity TEXT NOT NULL DEFAULT '0',
    filled_quantity TEXT NOT NULL DEFAULT '0',
    status TEXT NOT NULL,
    config_generation INTEGER,
    config_hash TEXT,
    created_at TEXT NOT NULL,
    submitted_at TEXT,
    updated_at TEXT NOT NULL,
    released_at TEXT
);
INSERT INTO canary_risk_reservations(
    reservation_id,intent_id,side,market_id,event_id,requested_cost,filled_cost,
    remaining_cost,fee_reserve,quantity,filled_quantity,status,config_generation,
    config_hash,created_at,submitted_at,updated_at,released_at
) VALUES(
    'legacy-canary-reservation','legacy-canary-intent','BUY','legacy-market',
    'legacy-event','1.00','0.50','0.50','0.01','2','1','HELD',4,
    'legacy-settings-hash','2026-01-01T00:00:00+00:00',
    '2026-01-01T00:00:01+00:00','2026-01-01T00:00:01+00:00',NULL
);
CREATE TABLE binance_execution_order_intents (
    intent_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    opportunity_id TEXT,
    candidate_id TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    symbol TEXT NOT NULL,
    environment TEXT NOT NULL,
    intent TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    notional TEXT NOT NULL,
    fee_reserve TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    exchange_order_id TEXT,
    state TEXT NOT NULL,
    generation INTEGER NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    reason TEXT,
    exit_policy_json TEXT,
    submitted_at TEXT,
    acknowledged_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    raw_json TEXT
);
INSERT INTO binance_execution_order_intents(
    intent_id,signal_id,candidate_id,binding_hash,symbol,environment,intent,side,
    price,quantity,notional,fee_reserve,client_order_id,exchange_order_id,state,
    generation,updated_at,raw_json
) VALUES(
    'legacy-order-intent','legacy-order-signal','legacy-candidate','legacy-binding',
    'BTCUSDT','PAPER','ENTRY','BUY','10','1','10','0.01',
    'legacy-client-order','legacy-exchange-order','FILLED',7,
    '2026-01-01T00:01:00+00:00','{"source":"historical-order"}'
);
"""


# A pre-settings database may have only the legacy operator/control envelopes.
# The UNKNOWN row and historical fill evidence must remain visible while the
# first versioned ACTIVE configuration is created.
_NO_ACTIVE_SETTINGS_DDL = _MAIN_ERA_POLYMARKET_DDL + """
CREATE TABLE operator_config (
    config_key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO operator_config(config_key,value_json,updated_at) VALUES(
    'canary-risk-limits',
    '{"target_notional_usd":"1.00","max_orders_per_day":1,"max_daily_loss_usd":"0.25"}',
    '2026-01-01T00:00:00+00:00'
);
UPDATE canary_control
SET state='DISABLED',
    limits_json='{"target_notional_usd":"0.50","max_orders_per_day":2,"max_daily_loss_usd":"0.50"}'
WHERE singleton=1;
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
INSERT INTO fills(
    fill_id,order_id,timestamp,strategy_id,symbol,payload_json,created_at
) VALUES(
    'legacy-research-fill','legacy-research-order',
    '2026-01-01T00:01:00+00:00','legacy-strategy','BTCUSDT',
    '{"price":"10","quantity":"1","source":"historical"}',
    '2026-01-01T00:01:00+00:00'
);
CREATE TABLE canary_ledger (
    event_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    timestamp TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    requested_notional TEXT NOT NULL,
    paper_expected_price TEXT NOT NULL,
    max_price TEXT NOT NULL,
    submitted_quantity TEXT,
    exchange_order_id TEXT,
    fill_quantity TEXT,
    actual_average_price TEXT,
    fees TEXT,
    status TEXT NOT NULL,
    realized_pnl TEXT,
    evidence_json TEXT NOT NULL
);
INSERT INTO canary_ledger(
    event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,
    requested_notional,paper_expected_price,max_price,submitted_quantity,
    fill_quantity,actual_average_price,fees,status,realized_pnl,evidence_json
) VALUES(
    'legacy-unknown-event','legacy-unknown-signal','2026-01-01T00:00:00+00:00',
    'legacy-candidate','polymarket','legacy-market','yes','BUY',
    '1.00','0.50','0.50','2','1','0.50','0.01','UNKNOWN','-0.25',
    '{"source":"legacy-canary"}'
);
"""


def _seed_database(path: Path, ddl: str) -> None:
    connection = sqlite3.connect(str(path))
    try:
        if ddl:
            connection.executescript(ddl)
        connection.commit()
    finally:
        connection.close()

_LEGACY_SENTINEL_ROWS = {
    "canary_control": (
        ("state", "candidate_id", "venue", "limits_json", "integrity_hash", "updated_at"),
        "singleton",
        1,
    ),
    "canary_selection": (
        ("ranking_run_id", "candidate_id", "rank", "total_score", "component_scores_json", "evidence_versions_json", "reason", "selected_at"),
        "singleton",
        1,
    ),
    "binance_execution_control": (
        ("state", "generation", "environment", "binding_hash", "envelope_hash", "credential_hash", "authorized", "kill_requested", "pause_reason", "updated_at"),
        "singleton",
        1,
    ),
    "binance_execution_risk_reservations": (
        ("reservation_id", "intent_id", "symbol", "side", "amount", "fee_reserve", "status", "created_at", "released_at"),
        "reservation_id",
        "legacy-reservation",
    ),
    "canary_setting_configs": (
        ("config_id", "state", "generation", "config_hash", "values_json", "actor", "updated_at"),
        "config_id",
        "cfg-legacy-production",
    ),
    "canary_risk_reservations": (
        ("reservation_id", "intent_id", "side", "market_id", "requested_cost", "filled_cost", "remaining_cost", "fee_reserve", "quantity", "filled_quantity", "status", "config_generation", "config_hash", "updated_at"),
        "reservation_id",
        "legacy-canary-reservation",
    ),
    "binance_operator_actions": (
        ("action_id", "action", "attempted_at", "completed_at", "created_at", "timestamp", "timestamp_utc", "timestamp_pht", "success", "reason", "payload_json", "result_json"),
        "action_id",
        "legacy-binance-action",
    ),
    "research_queue": (
        ("item_id", "status", "priority", "payload_json", "source", "author", "result_json"),
        "item_id",
        "legacy-research-item",
    ),
    "fills": (
        ("fill_id", "order_id", "timestamp", "strategy_id", "symbol", "payload_json"),
        "fill_id",
        "legacy-research-fill",
    ),
    "operator_config": (
        ("config_key", "value_json", "updated_at"),
        "config_key",
        "canary-risk-limits",
    ),
    "binance_execution_order_intents": (
        ("intent_id", "signal_id", "candidate_id", "symbol", "state", "client_order_id", "raw_json"),
        "intent_id",
        "legacy-order-intent",
    ),
}


def _legacy_sentinel_rows(path: Path) -> dict[str, dict[str, object]]:
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    result: dict[str, dict[str, object]] = {}
    try:
        for table, (columns, key, value) in _LEGACY_SENTINEL_ROWS.items():
            try:
                row = connection.execute(
                    f"SELECT {','.join(columns)} FROM {table} WHERE {key}=?",
                    (value,),
                ).fetchone()
            except sqlite3.OperationalError:
                row = None
            if row is not None:
                result[table] = dict(row)
    finally:
        connection.close()
    return result


def _assert_legacy_sentinel_rows(
    testcase: unittest.TestCase,
    store: AxiomStore,
    expected: dict[str, dict[str, object]],
) -> None:
    for table, expected_row in expected.items():
        columns = _LEGACY_SENTINEL_ROWS[table][0]
        key = _LEGACY_SENTINEL_ROWS[table][1]
        value = _LEGACY_SENTINEL_ROWS[table][2]
        row = store.connection.execute(
            f"SELECT {','.join(columns)} FROM {table} WHERE {key}=?",
            (value,),
        ).fetchone()
        testcase.assertIsNotNone(row, f"legacy sentinel disappeared from {table}")
        testcase.assertEqual(dict(row), expected_row)


def _table_names(store: AxiomStore) -> set[str]:
    rows = store.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _schema_signature(store: AxiomStore) -> dict[str, tuple[tuple[str, ...], int]]:
    result: dict[str, tuple[tuple[str, ...], int]] = {}
    for table in sorted(_table_names(store)):
        if not (table.startswith("canary_") or table.startswith("binance_")):
            continue
        columns = tuple(
            str(row[1])
            for row in store.connection.execute(f"PRAGMA table_info({table})").fetchall()
        )
        count = int(store.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        result[table] = (columns, count)
    return result


def _open_combined(path: Path) -> tuple[
    AxiomStore,
    CanaryService,
    BinanceExecutionService,
    BinanceCryptoQualificationService,
    BinanceAutonomousWorker,
    BinanceCanaryControlPlane,
    DashboardData,
]:
    store = AxiomStore(str(path))
    clock = lambda: T0
    polymarket = CanaryService(store, clock=clock)
    venue = PaperBinanceSpotVenue(clock=clock)
    execution = BinanceExecutionService(
        store,
        venue=venue,
        environment="PAPER",
        clock=clock,
        owner_id="combined-schema-test",
    )
    qualification = BinanceCryptoQualificationService(store, clock=clock)
    worker = BinanceAutonomousWorker(
        store,
        execution,
        qualification=qualification,
        worker_id="combined-schema-test",
        clock=clock,
        stop_event=threading.Event(),
    )
    binance = BinanceCanaryControlPlane(
        store,
        execution,
        qualification=qualification,
    )
    dashboard = DashboardData(store=store, binance_canary=binance)
    return store, polymarket, execution, qualification, worker, binance, dashboard


class CombinedSchemaMigrationTests(unittest.TestCase):
    def _assert_combined_state(
        self,
        path: Path,
        expected_polymarket_control_state: str,
        assert_legacy_columns: bool = False,
    ) -> None:
        sentinels_before = _legacy_sentinel_rows(path)
        opened = _open_combined(path)
        store, polymarket, execution, _qualification, _worker, binance, dashboard = opened
        try:
            names = _table_names(store)
            self.assertIn("canary_control", names)
            self.assertIn("canary_readiness_snapshot", names)
            self.assertIn("binance_execution_control", names)
            self.assertIn("binance_execution_connectivity", names)
            self.assertIn("binance_crypto_qualification", names)
            self.assertIn("binance_auto_state", names)
            self.assertIn("binance_operator_actions", names)
            _assert_legacy_sentinel_rows(self, store, sentinels_before)
            research_row = store.connection.execute(
                "SELECT status,payload_json,result_json FROM research_queue "
                "WHERE item_id='legacy-research-item'"
            ).fetchone()
            if research_row is not None:
                self.assertEqual(research_row["status"], "COMPLETED")
                self.assertEqual(
                    json.loads(research_row["payload_json"])["statement"],
                    "historical research survives migration",
                )
            fill_row = store.connection.execute(
                "SELECT order_id,symbol,payload_json FROM fills "
                "WHERE fill_id='legacy-research-fill'"
            ).fetchone()
            if fill_row is not None:
                self.assertEqual(fill_row["order_id"], "legacy-research-order")
                self.assertEqual(fill_row["symbol"], "BTCUSDT")
                self.assertEqual(json.loads(fill_row["payload_json"])["price"], "10")
            limits_row = store.connection.execute(
                "SELECT value_json FROM operator_config "
                "WHERE config_key='canary-risk-limits'"
            ).fetchone()
            if limits_row is not None:
                limits = json.loads(limits_row["value_json"])
                self.assertEqual(limits["target_notional_usd"], "1.00")
                self.assertEqual(limits["max_orders_per_day"], 1)
                self.assertEqual(limits["max_daily_loss_usd"], "0.25")
            order_row = store.connection.execute(
                "SELECT state,price,quantity,notional,raw_json "
                "FROM binance_execution_order_intents "
                "WHERE intent_id='legacy-order-intent'"
            ).fetchone()
            if order_row is not None:
                self.assertEqual(order_row["state"], "FILLED")
                self.assertEqual(order_row["price"], "10")
                self.assertEqual(order_row["quantity"], "1")
                self.assertEqual(order_row["notional"], "10")
                self.assertEqual(
                    json.loads(order_row["raw_json"])["source"],
                    "historical-order",
                )

            polymarket_control_columns = {
                str(row[1])
                for row in store.connection.execute(
                    "PRAGMA table_info(canary_control)"
                ).fetchall()
            }
            self.assertIn("control_generation", polymarket_control_columns)
            control_row = store.connection.execute(
                "SELECT control_generation FROM canary_control WHERE singleton=1"
            ).fetchone()
            if control_row is not None:
                self.assertEqual(control_row["control_generation"], 1)
            selection_columns = {
                str(row[1])
                for row in store.connection.execute(
                    "PRAGMA table_info(canary_selection)"
                ).fetchall()
            }
            self.assertIn("selection_status", selection_columns)
            readiness_columns = {
                str(row[1])
                for row in store.connection.execute(
                    "PRAGMA table_info(canary_readiness_snapshot)"
                ).fetchall()
            }
            self.assertTrue({"source_control_generation", "projection_version"} <= readiness_columns)
            polymarket_status = polymarket.status()
            self.assertEqual(polymarket_status["production_live_trading"], "DISABLED")
            self.assertEqual(
                polymarket_status["micro_live_canary"],
                expected_polymarket_control_state,
            )
            self.assertEqual(polymarket_status["readiness_snapshot_status"], "STALE")
            self.assertEqual(polymarket_status["selection_status"], "UNKNOWN")
            self.assertIsNone(polymarket_status["selection_valid"])

            binance_status = binance.status()
            self.assertEqual(binance_status["control"]["state"], "DISABLED")
            self.assertFalse(binance_status["control"]["authorized"])
            self.assertEqual(binance_status["connectivity"]["status"], "UNKNOWN")
            self.assertEqual(binance_status["readiness"]["status"], "NOT_READY")
            self.assertTrue(binance_status["connectivity"]["stale"])

            combined = dashboard.binance_canary_data()
            self.assertTrue(combined["available"])
            self.assertEqual(combined["status"]["control"]["state"], "DISABLED")
            self.assertEqual(combined["status"]["connectivity"]["status"], "UNKNOWN")
            self.assertEqual(dashboard.v2_snapshot("canary")["selection_status"], "UNKNOWN")

            if assert_legacy_columns:
                control_columns = {
                    str(row[1])
                    for row in store.connection.execute(
                        "PRAGMA table_info(binance_execution_control)"
                    ).fetchall()
                }
                reservation_columns = {
                    str(row[1])
                    for row in store.connection.execute(
                        "PRAGMA table_info(binance_execution_risk_reservations)"
                    ).fetchall()
                }
                operator_columns = {
                    str(row[1])
                    for row in store.connection.execute(
                        "PRAGMA table_info(binance_operator_actions)"
                    ).fetchall()
                }
                self.assertIn("entry_policy_hash", control_columns)
                self.assertIn("reserved_quantity", reservation_columns)
                self.assertIn("target", operator_columns)
                reservation = store.connection.execute(
                    "SELECT reserved_quantity FROM binance_execution_risk_reservations "
                    "WHERE reservation_id='legacy-reservation'"
                ).fetchone()
                self.assertIsNotNone(reservation)
                self.assertEqual(reservation["reserved_quantity"], "0")
                operator_action = store.connection.execute(
                    "SELECT target FROM binance_operator_actions "
                    "WHERE action_id='legacy-binance-action'"
                ).fetchone()
                self.assertIsNotNone(operator_action)
                self.assertEqual(operator_action["target"], "")

            # A Polymarket control transition cannot authorize the separate
            # Binance execution gate.
            store.connection.execute(
                "UPDATE canary_control SET state='ARMED',candidate_id='pm-only',venue='polymarket' WHERE singleton=1"
            )
            store.connection.commit()
            self.assertEqual(binance.status()["control"]["state"], "DISABLED")

            # Conversely, a Binance row is not read as Polymarket readiness or
            # selection state.  The latter remains its own persisted snapshot.
            store.connection.execute(
                "UPDATE binance_execution_control SET state='ARMED',authorized=1 WHERE singleton=1"
            )
            store.connection.commit()
            self.assertEqual(
                polymarket.status()["micro_live_canary"],
                expected_polymarket_control_state,
            )
            self.assertEqual(polymarket.status()["selection_status"], "UNKNOWN")

            # Restore both fixture control rows before the restart assertion;
            # the transition checks above must not become a new persisted
            # authorization state.
            store.connection.execute(
                "UPDATE canary_control SET state='DISABLED',candidate_id=NULL,venue=NULL WHERE singleton=1"
            )
            store.connection.execute(
                "UPDATE binance_execution_control SET state='DISABLED',authorized=0 WHERE singleton=1"
            )
            store.connection.commit()

            signature_before_restart = _schema_signature(store)
        finally:
            execution.close()
            store.close()

        reopened = _open_combined(path)
        reopened_store, reopened_polymarket, reopened_execution, _q, _w, reopened_binance, reopened_dashboard = reopened
        try:
            self.assertEqual(_schema_signature(reopened_store), signature_before_restart)
            _assert_legacy_sentinel_rows(self, reopened_store, sentinels_before)
            if assert_legacy_columns:
                reopened_reservation = reopened_store.connection.execute(
                    "SELECT reserved_quantity FROM binance_execution_risk_reservations "
                    "WHERE reservation_id='legacy-reservation'"
                ).fetchone()
                self.assertIsNotNone(reopened_reservation)
                self.assertEqual(reopened_reservation["reserved_quantity"], "0")
                reopened_operator_action = reopened_store.connection.execute(
                    "SELECT target FROM binance_operator_actions "
                    "WHERE action_id='legacy-binance-action'"
                ).fetchone()
                self.assertIsNotNone(reopened_operator_action)
                self.assertEqual(reopened_operator_action["target"], "")
            self.assertEqual(reopened_polymarket.status()["production_live_trading"], "DISABLED")
            self.assertEqual(
                reopened_polymarket.status()["micro_live_canary"],
                expected_polymarket_control_state,
            )
            self.assertEqual(reopened_polymarket.status()["readiness_snapshot_status"], "STALE")
            self.assertEqual(reopened_polymarket.status()["selection_status"], "UNKNOWN")
            self.assertIsNone(reopened_polymarket.status()["selection_valid"])
            self.assertEqual(reopened_binance.status()["control"]["state"], "DISABLED")
            self.assertFalse(reopened_binance.status()["control"]["authorized"])
            self.assertEqual(reopened_binance.status()["connectivity"]["status"], "UNKNOWN")
            self.assertEqual(reopened_binance.status()["readiness"]["status"], "NOT_READY")
            self.assertTrue(reopened_dashboard.binance_canary_data()["available"])
            self.assertEqual(
                reopened_dashboard.binance_canary_data()["status"]["control"]["state"],
                "DISABLED",
            )
        finally:
            reopened_execution.close()
            reopened_store.close()

    def test_empty_origin_initializes_combined_namespaces_and_restart_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self._assert_combined_state(
                Path(directory) / "empty.sqlite",
                expected_polymarket_control_state="UNKNOWN",
            )

    def test_main_era_polymarket_origin_migrates_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "main-era.sqlite"
            _seed_database(path, _MAIN_ERA_POLYMARKET_DDL)
            self._assert_combined_state(
                path,
                expected_polymarket_control_state="DISABLED",
            )

    def test_binance_era_origin_migrates_idempotently_without_cross_venue_inheritance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "binance-era.sqlite"
            _seed_database(path, _BINANCE_ERA_DDL)
            self._assert_combined_state(
                path,
                expected_polymarket_control_state="DISABLED",
                assert_legacy_columns=True,
            )

    def test_missing_active_settings_preserve_legacy_envelope_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-settings.sqlite"
            _seed_database(path, _NO_ACTIVE_SETTINGS_DDL)
            store = AxiomStore(str(path))
            try:
                # The regular Polymarket initializer adds its projections, but
                # must not alter the pre-existing authorization state or ledger.
                CanaryService(store, clock=lambda: T0)
                before = store.connection.execute(
                    "SELECT status,fill_quantity,actual_average_price,fees,realized_pnl "
                    "FROM canary_ledger WHERE event_id='legacy-unknown-event'"
                ).fetchone()
                self.assertIsNotNone(before)
                legacy_fill_before = store.connection.execute(
                    "SELECT order_id,symbol,payload_json FROM fills "
                    "WHERE fill_id='legacy-research-fill'"
                ).fetchone()
                self.assertIsNotNone(legacy_fill_before)
                settings = CanarySettingsService(store, clock=lambda: T0)
                limits = settings.active_limits()
                self.assertEqual(limits["target_notional_usd"], "0.50")
                self.assertEqual(limits["max_all_in_buy_usd"], "0.50")
                self.assertEqual(limits["max_orders_per_day"], 1)
                self.assertEqual(limits["max_submitted_orders_per_day"], 1)
                self.assertEqual(limits["max_daily_loss_usd"], "0.25")
                self.assertEqual(limits["realized_loss_entry_stop_usd"], "0.25")
                self.assertEqual(
                    store.connection.execute(
                        "SELECT state FROM canary_control WHERE singleton=1"
                    ).fetchone()["state"],
                    "DISABLED",
                )
                self.assertEqual(
                    store.list_canary_setting_audit(limit=1)[0]["action"],
                    "MIGRATION_DEFAULT_ACTIVE",
                )
                # Re-opening the settings facade is the migration retry path;
                # it must not create another ACTIVE row or genesis audit.
                CanarySettingsService(store, clock=lambda: T0)
                self.assertEqual(
                    len(store.list_canary_setting_configs(state="ACTIVE")),
                    1,
                )
                self.assertEqual(len(store.list_canary_setting_audit(limit=10)), 1)
                after = store.connection.execute(
                    "SELECT status,fill_quantity,actual_average_price,fees,realized_pnl "
                    "FROM canary_ledger WHERE event_id='legacy-unknown-event'"
                ).fetchone()
                self.assertEqual(dict(after), dict(before))
                legacy_fill_after = store.connection.execute(
                    "SELECT order_id,symbol,payload_json FROM fills "
                    "WHERE fill_id='legacy-research-fill'"
                ).fetchone()
                self.assertEqual(dict(legacy_fill_after), dict(legacy_fill_before))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
