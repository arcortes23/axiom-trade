from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import threading
import unittest
from unittest.mock import Mock, patch

import axiom.canary_positions as position_module
from axiom.canary import CanaryBlocked, CanaryService, CredentialStore
from axiom.experiment_plan import normalize_market_scope
from axiom.market_scope import resolve_market_scope
from axiom.storage import AxiomStore


T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


class TestCredentials(CredentialStore):
    _VALUES = {
        "private_key": "fixture-private-key",
        "wallet_address": "0x0000000000000000000000000000000000000001",
    }

    def load(self, **kwargs: object) -> dict[str, str]:
        return dict(self._VALUES)

    def configured(self, **kwargs: object) -> bool:
        return True


class HealthyPositionStore(AxiomStore):
    def polymarket_health(self, **kwargs: object) -> dict[str, object]:
        return {"grade": "A", "errors": 0}


class OfflineOfficialVenue:
    """Read-only fixture standing in for the official venue adapter."""

    def __init__(self) -> None:
        self.order_status = "MATCHED"
        self.settlement_status: str | None = None
        self.settlement_order_id: str | None = None
        self.order_id = "exit-1"
        self.fail_reads = False
        self.trades: list[dict[str, str]] = []
        self.trades_by_order: dict[str, list[dict[str, str]]] = {}

    def geoblock(self) -> dict[str, object]:
        return {"blocked": False, "close_only": False, "country": "ZZ"}

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        return {
            "market_id": market_id,
            "token_id": token_id,
            "asset_id": token_id,
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "0.1",
            "fee_bps": "10",
            "best_bid": "0.49",
        }

    def get_order(self, order_id: str) -> dict[str, str]:
        if self.fail_reads:
            raise RuntimeError("temporary account read failure")
        result = {"id": order_id, "status": self.order_status}
        if self.settlement_status is not None and (
            self.settlement_order_id is None or self.settlement_order_id == order_id
        ):
            result["settlement_status"] = self.settlement_status
        return result

    def list_account_trades(self, order_id: str) -> list[dict[str, str]]:
        if self.fail_reads:
            raise RuntimeError("temporary account read failure")
        return [
            dict(trade)
            for trade in self.trades_by_order.get(order_id, self.trades)
        ]

    def submit_limit_order(self, **kwargs: object) -> dict[str, object]:
        raise AssertionError("production exits must use CanaryService.submit_position_order")


class CanaryPositionManagementTests(unittest.TestCase):
    def setUp(self) -> None:
        # The release fixture is isolated by default; fake venue/control
        # coverage opts into the exact production profile explicitly.
        self._production_profile = patch.dict(
            os.environ,
            {"AXIOM_EXECUTION_PROFILE": "production"},
        )
        self._production_profile.start()
        self.addCleanup(self._production_profile.stop)
        self.now = T0
        self.store = HealthyPositionStore(":memory:")
        self.service = CanaryService(
            self.store,
            credentials=TestCredentials(),
            clock=lambda: self.now,
        )
        self.venue = OfflineOfficialVenue()
        self._venue_type_patch = patch.object(
            position_module,
            "PolymarketClobV2Venue",
            OfflineOfficialVenue,
        )
        self._venue_type_patch.start()
        self._arm_reviewed_candidate()
        self._seed_owned_position()
        self.post_calls: list[dict[str, object]] = []
        self.next_order_id = "exit-1"
        self.service.submit_position_order = Mock(side_effect=self._shared_position_submit)

    def tearDown(self) -> None:
        self._venue_type_patch.stop()
        self.store.close()

    def _arm_reviewed_candidate(self) -> None:
        self.store.save_dataset(
            "prediction-history",
            "v1",
            [{"timestamp": self.now.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            "prediction-history",
            "v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=self.now,
            end_timestamp=self.now,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="prediction-history:v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        self.store.verify_dataset_integrity_attestation(
            "prediction-history",
            "v1",
        )
        parts = ("strategy-v1", "model-v1", "config-v1")
        payload: dict[str, object] = {
            "market_type": "prediction",
            "source_type": "HISTORICAL",
            "dataset_id": "prediction-history",
            "plan_hash": "sha256:test-plan",
            "dataset_version": "v1",
            "dataset_selector": {
                "dataset_id": "prediction-history",
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
            },
            "dataset_attestation": {
                "dataset_id": "prediction-history",
                "dataset_version": "v1",
                "status": "CURRENT",
                "policy_version": "v1",
                "attestation_hash": "fixture-attestation",
            },
            "dataset_provenance": {
                "dataset_id": "prediction-history",
                "dataset_version": "v1",
                "provider": "polymarket",
                "instrument": "POLYMARKET",
                "market_type": "prediction",
                "timeframe": "event",
                "source_type": "HISTORICAL",
                "snapshot_id": "prediction-history:v1",
                "time_split": "train-validation-holdout",
            },
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "data_quality": "PRICE_PROXY",
            "validation_expectancy": 0.10,
            "validation_confidence_lower_bound": 0.05,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_execution_quality": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 20,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 20,
                "min_observations": 30,
                "min_trades": 10,
                "checks": {"observations": True, "trades": True},
            },
            "experiment_plan": {
                "policy_version": "canary-sample-policy-v1",
                "min_independent_samples": 30,
                "min_trades": 10,
            },
            "exit_policy": {"type": "fixed_holding_period", "holding_period_seconds": 0},
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": parts[0],
            "model_hash": parts[1],
            "config_hash": parts[2],
            "frozen_hash": hashlib.sha256("|".join(parts).encode()).hexdigest(),
            "forward_evidence": {
                "forward_duration_seconds": 7 * 86400,
                "forward_independent_resolved_bets": 30,
                "forward_successful_order_attempts": 20,
                "forward_expectancy": 0.1,
                "forward_confidence_lower_bound": 0.0,
                "forward_stability": 0.6,
                "forward_calibration": 0.8,
                "forward_liquidity": 0.0,
                "forward_max_drawdown": 0.2,
                "forward_regime_count": 3,
            },
        }
        policy = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": ["market-1"],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        )
        payload["market_scope"] = policy.as_dict()
        payload["market_scope_hash"] = policy.scope_hash
        payload["market_scope_version"] = policy.scope_version
        self.store.save_candidate_lifecycle("candidate-1", "IDEA", payload, timestamp=self.now)
        for stage in (
            "SCHEMA_VALIDATED",
            "BACKTESTED",
            "VALIDATED",
            "ROBUSTNESS_CHECKED",
            "FROZEN",
            "PAPER_FORWARD",
            "PAPER_PROMOTABLE",
        ):
            self.store.save_candidate_lifecycle(
                "candidate-1",
                stage,
                payload,
                from_stage="IDEA" if stage == "SCHEMA_VALIDATED" else None,
                timestamp=self.now,
            )
        self.store.save_market_scope_resolution(
            resolve_market_scope(
                "candidate-1",
                {"market_scope": payload["market_scope"]},
                [
                    {
                        "market_id": "market-1",
                        "condition_id": "market-1-condition",
                        "yes_token_id": "token-yes",
                        "no_token_id": "token-no",
                        "source_type": "CURRENT",
                        "provider": "polymarket",
                        "instrument": "POLYMARKET",
                        "active": True,
                        "open": True,
                        "closed": False,
                        "settlement": "open",
                        "accepting_orders": True,
                        "order_book_available": True,
                        "expiry": (self.now + timedelta(hours=2)).isoformat(),
                    }
                ],
                resolved_at=self.now,
            )
        )
        self.service.mark_eligible("candidate-1", publish_readiness=False)
        settings = self.service.settings.snapshot(now=self.now)
        self.service.arm(
            "candidate-1",
            venue=self.venue,
            credentials_configured=True,
            config_id=str(settings["config_id"]),
            expected_generation=int(settings["generation"]),
        )
        self.config = self.service.settings.snapshot(now=self.now)

    def _seed_owned_position(self) -> None:
        from axiom.canary_positions import _ensure_schema

        _ensure_schema(self.service)
        config = self.config
        self.store.reserve_canary_capacity(
            intent_id="entry-intent-1",
            reservation_id="entry-reservation-1",
            side="BUY",
            requested_cost="0.50",
            fee_reserve="0",
            quantity="1",
            market_id="market-1",
            event_id="entry-event-1",
            config_id=str(config["config_id"]),
            config_generation=int(config["generation"]),
            config_hash=str(config["config_hash"]),
            control_generation=int(config["control_generation"]),
            detail={"side": "BUY", "token_id": "token-yes", "settlement_status": "CONFIRMED"},
            timestamp=self.now,
        )
        self.store.record_canary_fill(
            fill_id="entry-fill-1",
            reservation_id="entry-reservation-1",
            quantity="1",
            price="0.50",
            cost="0.50",
            fee="0",
            filled_at=self.now,
            detail={"side": "BUY", "token_id": "token-yes", "settlement_status": "CONFIRMED"},
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_position_lots(position_id,reservation_id,event_id,venue,market_id,token_id,candidate_id,strategy_id,strategy_version,strategy_hash,model_hash,config_id,config_generation,exit_policy_json,quantity,sold_quantity,cost_basis,fees,pending_exit_quantity,status,opened_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "position-1",
                    "entry-reservation-1",
                    "entry-event-1",
                    "POLYMARKET",
                    "market-1",
                    "token-yes",
                    "candidate-1",
                    "strategy-1",
                    "v1",
                    "strategy-hash",
                    "model-hash",
                    str(config["config_id"]),
                    int(config["generation"]),
                    json.dumps(
                        {"type": "fixed_holding_period", "holding_period_seconds": 0},
                        sort_keys=True,
                    ),
                    "1",
                    "0",
                    "0.50",
                    "0",
                    "0",
                    "OPEN",
                    self.now.isoformat(),
                    self.now.isoformat(),
                ),
            )

    def _shared_position_submit(self, **kwargs: object) -> dict[str, object]:
        before_post = kwargs["before_post"]
        on_send_started = kwargs["on_send_started"]
        assert callable(before_post)
        assert callable(on_send_started)
        before_post()
        on_send_started()
        self.post_calls.append(dict(kwargs))
        return {"ok": True, "order_id": self.next_order_id, "status": "SUBMITTED"}

    def _submit_exit(self) -> dict[str, object]:
        return dict(
            self.service.submit_exit(
                "position-1",
                self.venue,
                expected_generation=int(self.config["generation"]),
                config_id=str(self.config["config_id"]),
            )
        )

    def _request(self) -> dict[str, object]:
        row = self.store.connection.execute(
            "SELECT * FROM canary_position_requests ORDER BY submitted_at,request_id LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def _lot(self) -> dict[str, object]:
        row = self.store.connection.execute(
            "SELECT * FROM canary_position_lots WHERE position_id='position-1'"
        ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def _reservation(self, reservation_id: str) -> dict[str, object]:
        row = self.store.connection.execute(
            "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def test_delayed_sdk_exit_acceptance_is_durable(self) -> None:
        def delayed_submit(**kwargs: object) -> dict[str, object]:
            before_post = kwargs["before_post"]
            on_send_started = kwargs["on_send_started"]
            assert callable(before_post)
            assert callable(on_send_started)
            before_post()
            on_send_started()
            return {"ok": True, "order_id": "exit-delayed", "status": "delayed"}

        self.service.submit_position_order = Mock(side_effect=delayed_submit)
        submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
        self.assertEqual(submitted["order_id"], "exit-delayed")
        self.assertEqual(self._request()["status"], "SUBMITTED")

    def test_matched_remains_pending_then_confirmed_closes_with_exact_pnl(self) -> None:
        self.venue.trades = [
            {
                "trade_id": "sell-fill-1",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "MATCHED",
            }
        ]
        submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
        matched = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(matched["status"], "RECONCILED")
        self.assertEqual(self._request()["status"], "MATCHED")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        reservation = self._reservation(str(submitted["reservation_id"]))
        self.assertEqual(
            reservation["status"],
            "OPEN",
            msg=(
                f"submitted={submitted!r}; matched={matched!r}; "
                f"request={self._request()!r}; lot={self._lot()!r}; "
                f"reservation={reservation!r}"
            ),
        )
        equity = self.store.canary_risk_accounting(self.now)
        self.assertEqual(matched["equity"]["status"], "KNOWN")
        self.assertEqual(
            equity["equity_status"],
            "KNOWN",
            msg=(
                f"matched={matched!r}; accounting={equity!r}; "
                f"lot={self._lot()!r}; reservation={reservation!r}"
            ),
        )
        self.assertEqual(Decimal(str(equity["equity_loss_usd"])), Decimal("0.01049"))

        self.venue.trades[0]["status"] = "CONFIRMED"
        confirmed = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(confirmed["status"], "RECONCILED")
        self.assertEqual(self._request()["status"], "MATCHED")
        self.assertEqual(self._lot()["status"], "EXIT_PENDING")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        confirmed_reservation = self._reservation(str(submitted["reservation_id"]))
        self.assertEqual(confirmed_reservation["status"], "FILLED")
        self.assertIsNone(confirmed_reservation["released_at"])
        self.venue.settlement_status = "SETTLED"
        settled = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(settled["status"], "RECONCILED")
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "SETTLED")
        self.assertEqual(lot["status"], "CLOSED")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(Decimal(str(lot["gross_proceeds"])), Decimal("0.49"))
        self.assertEqual(Decimal(str(lot["exit_fees"])), Decimal("0.00049"))
        self.assertEqual(Decimal(str(lot["realized_pnl"])), Decimal("-0.01049"))
        risk_fill = self.store.connection.execute(
            "SELECT detail_json FROM canary_risk_fills WHERE fill_id='sell-fill-1'"
        ).fetchone()
        self.assertIsNotNone(risk_fill)
        detail = json.loads(risk_fill["detail_json"])
        self.assertEqual(detail["entry_cost_usd"], "0.50")
        self.assertEqual(detail["proceeds_usd"], "0.48951")
        self.assertEqual(detail["realized_pnl_usd"], "-0.01049")
        position_fill = self.store.connection.execute(
            "SELECT price,fee,filled_at FROM canary_position_fills "
            "WHERE fill_id='sell-fill-1'"
        ).fetchone()
        self.assertIsNotNone(position_fill)
        assert position_fill is not None
        self.assertEqual(position_fill["price"], "0.49")
        self.assertEqual(position_fill["fee"], "0.00049")
        self.assertEqual(position_fill["filled_at"], self.now.isoformat())
        final_accounting = self.store.canary_risk_accounting(self.now)
        self.assertEqual(final_accounting["equity_status"], "KNOWN")
        self.assertEqual(Decimal(str(final_accounting["realized_loss_usd"])), Decimal("0.01049"))

    def test_failed_settlement_creates_no_inventory_or_proceeds(self) -> None:
        self.venue.order_status = "FAILED"
        self.venue.trades = [
            {
                "trade_id": "failed-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "FAILED",
            }
        ]
        submitted = self._submit_exit()
        result = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(result["status"], "RECONCILED")
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "FAILED")
        self.assertEqual(lot["sold_quantity"], "0")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(lot["gross_proceeds"], "0")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM canary_position_fills").fetchone()[0], 0)
        self.assertEqual(self._reservation(str(submitted["reservation_id"]))["status"], "RELEASED")

    def test_canceled_partial_releases_remainder_for_a_new_exit(self) -> None:
        self.venue.order_status = "CANCELED"
        self.venue.trades = [
            {
                "trade_id": "cancel-fill",
                "quantity": "0.4",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        self._submit_exit()
        position_module.reconcile_pending(self.service, self.venue)
        lot = self._lot()
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(lot["sold_quantity"], "0.4")
        self.assertEqual(lot["pending_exit_quantity"], "0")

        self.next_order_id = "exit-2"
        self.venue.order_status = "MATCHED"
        self.venue.trades = []
        retry = self._submit_exit()
        self.assertEqual(retry["order_id"], "exit-2")
        self.assertEqual(self.service.submit_position_order.call_count, 2)

    def test_matched_then_failed_preserves_inventory_and_releases_pending_exit(self) -> None:
        self.venue.order_status = "MATCHED"
        self.venue.trades = [
            {
                "trade_id": "provisional-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "MATCHED",
            }
        ]
        submitted = self._submit_exit()
        position_module.reconcile_pending(self.service, self.venue)
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "MATCHED")
        self.assertEqual(lot["sold_quantity"], "0")
        self.assertEqual(lot["pending_exit_quantity"], "1")
        reservation = self._reservation(str(submitted["reservation_id"]))
        self.assertEqual(
            reservation["status"],
            "OPEN",
            msg=(
                f"submitted={submitted!r}; request={request!r}; "
                f"lot={lot!r}; reservation={reservation!r}"
            ),
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills"
            ).fetchone()[0],
            0,
        )

        self.venue.order_status = "FAILED"
        self.venue.trades = []
        position_module.reconcile_pending(self.service, self.venue)
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "FAILED")
        self.assertEqual(lot["sold_quantity"], "0")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(self._reservation(str(submitted["reservation_id"]))["status"], "RELEASED")

    def test_confirmed_trade_without_individual_evidence_is_unknown_and_blocking(self) -> None:
        submitted = self._submit_exit()
        self.venue.trades = [
            {
                "trade_id": "missing-evidence",
                "quantity": "1",
                "status": "CONFIRMED",
            }
        ]

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        request = self._request()
        self.assertEqual(request["status"], "UNKNOWN")
        self.assertEqual(request["last_error"], "CANARY_TRADE_PRICE_UNAVAILABLE")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills WHERE fill_id=?",
                ("missing-evidence",),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills WHERE fill_id=?",
                ("missing-evidence",),
            ).fetchone()[0],
            0,
        )
        reservation = self._reservation(str(submitted["reservation_id"]))
        self.assertEqual(
            reservation["status"],
            "OPEN",
            msg=(
                f"submitted={submitted!r}; reconciled={reconciled!r}; "
                f"request={request!r}; lot={self._lot()!r}; "
                f"reservation={reservation!r}"
            ),
        )

    def test_kill_during_blocked_pre_post_callback_prevents_send(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        original_status = self.service.authoritative_status
        status_calls = 0

        def blocked_status() -> dict[str, object]:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 2:
                entered.set()
                self.assertTrue(release.wait(2))
            return original_status()

        self.service.authoritative_status = blocked_status
        errors: list[BaseException] = []

        def submit() -> None:
            try:
                self._submit_exit()
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=submit)
        worker.start()
        self.assertTrue(entered.wait(2))
        self.service.kill()
        release.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], CanaryBlocked)
        self.assertIn("CANARY_KILLED", str(errors[0]))
        self.assertEqual(self.post_calls, [])
        self.assertEqual(self._request()["status"], "REJECTED")

    def test_disarm_never_calls_shared_position_submit_helper(self) -> None:
        self.service.disarm()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
            self._submit_exit()
        self.service.submit_position_order.assert_not_called()
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_reservations WHERE side='SELL'"
            ).fetchone()[0],
            0,
        )

    def test_invalid_exit_policy_is_visible_as_management_blocker(self) -> None:
        with self.store.connection:
            self.store.connection.execute(
                "UPDATE canary_position_lots SET exit_policy_json=? WHERE position_id=?",
                (json.dumps({"type": "unsupported"}), "position-1"),
            )

        managed = position_module.manage_positions(self.service, self.venue)

        self.assertEqual(managed["status"], "BLOCKED")
        self.assertEqual(managed["submitted"], 0)
        self.assertEqual(
            managed["blocked"],
            [{"position_id": "position-1", "reason": "CANARY_POSITION_MANAGEMENT_BLOCKED"}],
        )
        self.assertEqual(self.post_calls, [])

    def test_exit_timeout_after_transport_start_is_durably_unknown(self) -> None:
        entered_transport = threading.Event()
        release_transport = threading.Event()

        def blocking_submit(**kwargs: object) -> dict[str, object]:
            before_post = kwargs["before_post"]
            on_send_started = kwargs["on_send_started"]
            assert callable(before_post)
            assert callable(on_send_started)
            before_post()
            on_send_started()
            entered_transport.set()
            release_transport.wait(1)
            return {"order_id": "late-exit", "status": "SUBMITTED"}

        def timeout_after_transport_started(operation, _timeout_seconds):
            worker = threading.Thread(target=operation, daemon=True)
            worker.start()
            self.assertTrue(entered_transport.wait(1))
            raise TimeoutError("canary external submission timed out")

        self.service.submit_position_order.side_effect = blocking_submit
        try:
            with patch.object(
                position_module,
                "_call_with_timeout",
                side_effect=timeout_after_transport_started,
            ):
                with self.assertRaisesRegex(
                    CanaryBlocked,
                    "CANARY_SUBMISSION_UNKNOWN",
                ):
                    self._submit_exit()
        finally:
            release_transport.set()

        request = self._request()
        reservation = self._reservation(str(request["reservation_id"]))
        self.assertEqual(request["status"], "UNKNOWN")
        self.assertEqual(reservation["status"], "UNKNOWN")
        self.assertEqual(self._lot()["status"], "EXIT_PENDING")

    def test_unknown_restart_and_idempotent_fill_projection(self) -> None:
        self.next_order_id = "exit-unknown"
        self._submit_exit()
        self.venue.fail_reads = True
        first = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(first["status"], "DEGRADED")
        self.assertEqual(self._request()["status"], "UNKNOWN")

        self.venue.fail_reads = False
        self.venue.order_status = "MATCHED"
        self.venue.trades = [
            {
                "trade_id": "stable-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "MATCHED",
            }
        ]
        restarted = CanaryService(
            self.store,
            credentials=TestCredentials(),
            clock=lambda: self.now,
        )
        second = position_module.reconcile_pending(restarted, self.venue)
        self.assertEqual(second["status"], "RECONCILED")
        self.assertEqual(self._request()["status"], "MATCHED")
        self.assertEqual(self.service.submit_position_order.call_count, 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills WHERE fill_id='stable-fill'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills WHERE fill_id='stable-fill'"
            ).fetchone()[0],
            0,
        )

        self.venue.trades[0]["status"] = "CONFIRMED"
        self.venue.settlement_status = "SETTLED"
        final = position_module.reconcile_pending(restarted, self.venue)
        self.assertEqual(final["status"], "RECONCILED")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills WHERE fill_id='stable-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills WHERE fill_id='stable-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(self._request()["status"], "SETTLED")


    def test_partial_buy_sell_then_later_buy_preserves_open_inventory_and_pnl(self) -> None:
        config = self.config
        equity = position_module._mark_owned_equity(self.service, self.venue, self.now)
        self.assertEqual(
            equity["status"],
            "KNOWN",
            msg=f"authoritative exact-token equity result={equity!r}",
        )
        self.store.reserve_canary_capacity(
            intent_id="late-entry-intent",
            reservation_id="late-entry-reservation",
            side="BUY",
            requested_cost="0.875",
            fee_reserve="0.000875",
            quantity="2",
            market_id="market-1",
            event_id="late-entry-event",
            config_id=str(config["config_id"]),
            config_generation=int(config["generation"]),
            config_hash=str(config["config_hash"]),
            control_generation=int(config["control_generation"]),
            detail={"side": "BUY", "token_id": "token-yes"},
            timestamp=self.now,
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "late-entry-event",
                    "late-entry-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "token-yes",
                    "BUY",
                    "0.875",
                    "0.50",
                    "0.70",
                    "2",
                    "late-entry-order",
                    "0",
                    None,
                    "0",
                    "MATCHED",
                    "{}",
                    int(config["control_generation"]),
                ),
            )
        self.venue.order_status = "MATCHED"
        self.venue.trades_by_order["late-entry-order"] = [
            {
                "trade_id": "late-entry-fill-1",
                "quantity": "0.5",
                "match_time": (self.now - timedelta(days=1)).isoformat(),
                "price": "0.50",
                "fee_rate_bps": "10",
                "status": "CONFIRMED",
            }
        ]
        first = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(first["status"], "RECONCILED")
        fill_timestamp = self.store.connection.execute(
            "SELECT filled_at FROM canary_risk_fills WHERE fill_id='late-entry-fill-1'"
        ).fetchone()
        self.assertIsNotNone(fill_timestamp)
        assert fill_timestamp is not None
        self.assertEqual(fill_timestamp["filled_at"], (self.now - timedelta(days=1)).isoformat())
        late_lot = self.store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis,realized_pnl,status "
            "FROM canary_position_lots WHERE position_id='position:late-entry-event'"
        ).fetchone()
        self.assertIsNotNone(late_lot)
        assert late_lot is not None
        self.assertEqual(Decimal(late_lot["quantity"]), Decimal("0.5"))
        self.assertEqual(late_lot["status"], "OPEN")

        self.next_order_id = "late-exit-order"
        exit_request = position_module.submit_exit(
            self.service,
            "position:late-entry-event",
            self.venue,
            expected_generation=int(config["generation"]),
            config_id=str(config["config_id"]),
        )
        self.assertEqual(exit_request["status"], "SUBMITTED")
        self.venue.trades_by_order["late-exit-order"] = [
            {
                "trade_id": "late-exit-fill",
                "quantity": "0.5",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        self.venue.settlement_status = "SETTLED"
        self.venue.settlement_order_id = "late-exit-order"
        exited = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(exited["status"], "RECONCILED")
        self.assertEqual(exited["requests"][0]["status"], "SETTLED")
        late_lot = self.store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis,realized_pnl,status "
            "FROM canary_position_lots WHERE position_id='position:late-entry-event'"
        ).fetchone()
        assert late_lot is not None
        self.assertEqual(late_lot["status"], "CLOSED")
        self.assertEqual(Decimal(late_lot["sold_quantity"]), Decimal("0.5"))
        prior_pnl = Decimal(late_lot["realized_pnl"])
        self.assertEqual(prior_pnl, Decimal("-0.005495"))

        self.venue.order_status = "CANCELED"
        self.venue.trades_by_order["late-entry-order"] = [
            {
                "trade_id": "late-entry-fill-1",
                "quantity": "0.5",
                "price": "0.50",
                "fee_rate_bps": "10",
                "match_time": (self.now - timedelta(days=1)).isoformat(),
                "status": "CONFIRMED",
            },
            {
                "trade_id": "late-entry-fill-2",
                "quantity": "0.5",
                "price": "0.60",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            },
        ]
        later = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(later["status"], "RECONCILED")
        late_lot = self.store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis,realized_pnl,status "
            "FROM canary_position_lots WHERE position_id='position:late-entry-event'"
        ).fetchone()
        assert late_lot is not None
        self.assertEqual(Decimal(late_lot["quantity"]), Decimal("1"))
        self.assertEqual(Decimal(late_lot["sold_quantity"]), Decimal("0.5"))
        self.assertEqual(Decimal(late_lot["cost_basis"]), Decimal("0.55055"))
        self.assertEqual(Decimal(late_lot["realized_pnl"]), prior_pnl)
        self.assertEqual(late_lot["status"], "OPEN")
        self.venue.trades_by_order["late-entry-order"].append(
            {
                "trade_id": "late-entry-fill-3",
                "quantity": "0.5",
                "price": "0.65",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        )
        after_terminal = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(after_terminal["status"], "RECONCILED")
        late_lot = self.store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis,realized_pnl,status "
            "FROM canary_position_lots WHERE position_id='position:late-entry-event'"
        ).fetchone()
        assert late_lot is not None
        self.assertEqual(Decimal(late_lot["quantity"]), Decimal("1.5"))
        self.assertEqual(Decimal(late_lot["sold_quantity"]), Decimal("0.5"))
        self.assertEqual(Decimal(late_lot["cost_basis"]), Decimal("0.875875"))
        self.assertEqual(Decimal(late_lot["realized_pnl"]), prior_pnl)
        self.assertEqual(late_lot["status"], "OPEN")

if __name__ == "__main__":
    unittest.main()
