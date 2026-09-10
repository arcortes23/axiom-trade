from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest

from axiom.canary_settings import (
    CanarySettingsConflict,
    CanarySettingsService,
    CanarySettingsValidationError,
)
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 9, 10, 15, 59, 30, tzinfo=UTC)  # 23:59:30 PHT


class CanarySettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = AxiomStore(":memory:")
        self.service = CanarySettingsService(self.store, clock=lambda: T0)

    def tearDown(self) -> None:
        self.store.close()

    def test_migration_defaults_preserve_legacy_limits_and_persist(self) -> None:
        limits = self.service.active_limits()
        self.assertEqual(limits["target_notional_usd"], "1.00")
        self.assertEqual(limits["max_exposure_usd"], "5.00")
        self.assertEqual(limits["max_daily_loss_usd"], "2.00")
        self.assertEqual(limits["max_open_positions"], 3)
        self.assertEqual(limits["max_orders_per_day"], 5)
        self.assertEqual(limits["max_slippage_bps"], 100)
        self.assertEqual(len(self.store.list_canary_setting_configs(state="ACTIVE")), 1)
        self.assertEqual(self.store.list_canary_setting_audit(limit=1)[0]["action"], "MIGRATION_DEFAULT_ACTIVE")

        draft = self.service.save_draft({"max_submitted_orders_per_day": 10, "max_all_in_buy_usd": "2.00", "target_notional_usd": "2.00"}, "operator-a")
        self.assertEqual(draft["state"], "DRAFT")
        self.assertEqual(draft["values"]["max_orders_per_day"], 10)
        self.assertEqual(draft["values"]["target_notional_usd"], "2.00")
        self.assertEqual(self.service.active_limits()["max_orders_per_day"], 5)

    def test_active_reads_join_existing_transaction_without_mutation(self) -> None:
        before_changes = self.store.connection.total_changes
        self.store.connection.execute("BEGIN IMMEDIATE")
        try:
            self.assertEqual(self.service.active_limits()["max_orders_per_day"], 5)
            self.assertEqual(self.service.snapshot(now=T0)["generation"], 1)
            self.assertEqual(self.store.connection.total_changes, before_changes)
            self.assertTrue(self.store.connection.in_transaction)
        finally:
            self.store.connection.rollback()

    def test_missing_active_does_not_reset_established_settings(self) -> None:
        self.store.connection.execute("DELETE FROM canary_setting_configs WHERE state='ACTIVE'")
        self.store.connection.commit()
        audit_count = len(self.store.list_canary_setting_audit(limit=100))

        restarted = CanarySettingsService(self.store, clock=lambda: T0)

        self.assertIsNone(self.store.load_canary_setting_config(status="ACTIVE"))
        with self.assertRaises(RuntimeError):
            restarted.active_limits()
        self.assertEqual(len(self.store.list_canary_setting_audit(limit=100)), audit_count)

    def test_rejects_lossy_values_and_cross_field_violations(self) -> None:
        for field, value in (
            ("max_all_in_buy_usd", True),
            ("max_all_in_buy_usd", 1.25),
            ("max_all_in_buy_usd", "NaN"),
            ("max_all_in_buy_usd", "Infinity"),
            ("max_all_in_buy_usd", "-1.00"),
            ("max_all_in_buy_usd", "1.001"),
            ("max_positions", False),
            ("max_positions", "1.5"),
        ):
            with self.assertRaises(CanarySettingsValidationError, msg=(field, value)):
                self.service.save_draft({field: value}, "operator-a")
        with self.assertRaises(CanarySettingsValidationError):
            self.service.save_draft({"max_fee_reserve_usd": "2.00", "max_all_in_buy_usd": "1.00", "target_notional_usd": "1.00"}, "operator-a")
        with self.assertRaises(CanarySettingsValidationError):
            self.service.save_draft({"max_exposure_usd": "2.00", "max_aggregate_exposure_usd": "3.00"}, "operator-a")
        with self.assertRaises(CanarySettingsValidationError):
            self.service.save_draft({"unknown_limit": "1.00"}, "operator-a")

    def test_activation_is_atomic_audited_and_generation_fenced(self) -> None:
        draft = self.service.save_draft({"max_submitted_orders_per_day": 10}, {"actor_id": "operator-a"})
        activated = self.service.activate_draft(draft["config_id"], "operator-a", expected_generation=1)
        self.assertEqual(activated["state"], "ACTIVE")
        self.assertEqual(activated["generation"], 2)
        self.assertEqual(self.service.active_limits()["max_orders_per_day"], 10)
        audit = self.store.list_canary_setting_audit(limit=1)[0]
        self.assertEqual(audit["action"], "ACTIVATED")
        self.assertEqual(audit["actor"], "operator-a")
        self.assertEqual(audit["previous_generation"], 1)
        self.assertEqual(audit["new_generation"], 2)
        self.assertEqual(audit["previous_config_id"], self.store.list_canary_setting_configs(state="ARCHIVED")[0]["config_id"])

        stale = self.service.save_draft({"max_submitted_orders_per_day": 20}, "operator-b")
        with self.assertRaises(CanarySettingsConflict):
            self.service.activate_draft(stale["config_id"], "operator-b", expected_generation=1)
        self.assertEqual(self.service.active_limits()["max_orders_per_day"], 10)

    def test_snapshot_reports_independent_usage_remaining_and_pht_reset(self) -> None:
        snapshot = self.service.snapshot(
            tighter_candidate={"max_submitted_orders_per_day": 2, "max_gross_daily_buy_usd": "3.00"},
            usage={"submitted_orders": 1, "gross_daily_buy_usd": "1.50", "all_in_buy_reserved_usd": "0.75", "external_flow_usd": "8.00"},
        )
        self.assertEqual(snapshot["effective_limits"]["max_orders_per_day"], 2)
        self.assertEqual(snapshot["effective_limits"]["max_gross_daily_buy_usd"], "3.00")
        self.assertEqual(snapshot["remaining"]["submitted_orders"], 1)
        self.assertEqual(snapshot["remaining"]["gross_daily_buy_usd"], "1.50")
        self.assertEqual(snapshot["usage"]["external_flow_usd"], "8.00")
        self.assertTrue(snapshot["entry_block_only"])
        self.assertTrue(snapshot["liquidation_allowed_when_tighter"])
        self.assertEqual(snapshot["pht_next_reset"], "2026-09-10T16:00:00+00:00")
        json.dumps(snapshot)

    def test_cumulative_reset_requires_disarmed_audit(self) -> None:
        with self.assertRaises(CanarySettingsConflict):
            self.service.reset_cumulative_buy_usage("operator-a")
        result = self.service.reset_cumulative_buy_usage("operator-a", disarmed=True)
        self.assertTrue(result["reset"])
        self.assertEqual(self.store.list_canary_setting_audit(limit=1)[0]["action"], "CUMULATIVE_USAGE_RESET")


class CanaryRiskAccountingTests(unittest.TestCase):
    def test_fill_is_counted_on_pht_fill_day(self) -> None:
        store = AxiomStore(":memory:")
        CanarySettingsService(store, clock=lambda: T0)
        try:
            created = datetime(2026, 9, 9, 15, 59, tzinfo=UTC)
            filled = datetime(2026, 9, 10, 16, 30, tzinfo=UTC)
            reservation = store.reserve_canary_capacity(
                intent_id="prior-day",
                side="BUY",
                requested_cost="0.50",
                detail={"token_id": "token-prior"},
                timestamp=created,
            )
            store.record_canary_fill(
                fill_id="fill-next-day",
                reservation_id=reservation["reservation_id"],
                quantity="1",
                price="0.50",
                cost="0.50",
                filled_at=filled,
                detail={"settlement_status": "CONFIRMED"},
            )
            usage = store.canary_risk_accounting(filled)
            self.assertEqual(usage["buy_filled_usd"], "0.50")
        finally:
            store.close()
        store = AxiomStore(":memory:")
        CanarySettingsService(store, clock=lambda: T0)
        try:
            limits = {
                "max_all_in_buy_usd": "3.00",
                "max_gross_daily_buy_usd": "10.00",
                "max_aggregate_exposure_usd": "10.00",
                "max_submitted_orders_per_day": 20,
            }
            buy = store.reserve_canary_capacity(
                intent_id="buy-1",
                side="BUY",
                requested_cost="0.50",
                fee_reserve="0.01",
                quantity="2",
                market_id="m1",
                limits=limits,
                detail={"token_id": "token-m1"},
                timestamp=T0,
            )
            store.record_canary_submission_attempt(
                attempt_id="attempt-1",
                intent_id="buy-1",
                side="BUY",
                attempted_at=T0,
            )
            store.record_canary_fill(
                fill_id="fill-1",
                reservation_id=buy["reservation_id"],
                quantity="1",
                price="0.50",
                cost="0.50",
                filled_at=T0,
                detail={"settlement_status": "CONFIRMED"},
            )
            store.record_canary_equity_mark(
                mark_id="mark-buy-1",
                observed_at=T0,
                market_id="m1",
                token_id="token-m1",
                side="SELL",
                quantity="1",
                mark_price="0.5005",
                cost_basis_usd="0.50",
                mark_fee="0.0005",
                detail={"position_id": "position:buy-1"},
            )
            unknown = store.reserve_canary_capacity(
                intent_id="buy-unknown",
                side="BUY",
                requested_cost="0.50",
                fee_reserve="0.01",
                quantity="1",
                market_id="m2",
                limits=limits,
                timestamp=T0,
            )
            store.record_canary_submission_attempt(
                attempt_id="attempt-2",
                intent_id="buy-unknown",
                side="BUY",
                attempted_at=T0,
            )
            store.release_canary_capacity(unknown["reservation_id"], status="UNKNOWN", timestamp=T0)
            store.reserve_canary_capacity(
                intent_id="sell-1",
                side="SELL",
                requested_cost="0",
                fee_reserve="0",
                quantity="1",
                market_id="m1",
                timestamp=T0,
            )
            store.record_canary_external_flow(
                flow_id="deposit-1",
                amount="8.00",
                kind="DEPOSIT",
                timestamp=T0,
            )
            store.record_canary_external_flow(
                flow_id="loss-1",
                amount="0.25",
                kind="EQUITY_LOSS",
                timestamp=T0,
            )
            usage = store.canary_risk_accounting(T0)
            self.assertEqual(usage["submitted_orders"], 2)
            self.assertEqual(usage["buy_filled_usd"], "0.50")
            self.assertEqual(usage["buy_unknown_usd"], "0.51")
            self.assertEqual(usage["buy_pending_usd"], "0.52")
            self.assertEqual(usage["aggregate_open_cost_usd"], "0.50")
            self.assertEqual(usage["aggregate_exposure_usd"], "1.02")
            self.assertEqual(usage["open_positions"], 2)
            self.assertEqual(usage["external_flow_usd"], "8.00")
            self.assertEqual(usage["equity_loss_usd"], "0.25")
            self.assertEqual(usage["equity_status"], "UNKNOWN")
        finally:
            store.close()
    def test_partial_cancel_keeps_filled_inventory(self) -> None:
        store = AxiomStore(":memory:")
        CanarySettingsService(store, clock=lambda: T0)
        try:
            reservation = store.reserve_canary_capacity(
                intent_id="partial-cancel",
                side="BUY",
                requested_cost="1.00",
                quantity="2",
                market_id="market-partial",
                detail={"token_id": "token-partial"},
                timestamp=T0,
            )
            store.record_canary_fill(
                fill_id="partial-fill",
                reservation_id=reservation["reservation_id"],
                quantity="1",
                price="0.50",
                cost="0.50",
                filled_at=T0,
                detail={"settlement_status": "CONFIRMED"},
            )
            store.release_canary_capacity(
                reservation["reservation_id"],
                status="CANCELLED",
                timestamp=T0,
            )
            usage = store.canary_risk_accounting(T0)
            self.assertEqual(usage["aggregate_open_cost_usd"], "0.50")
            self.assertEqual(usage["aggregate_exposure_usd"], "0.50")
            self.assertEqual(usage["open_positions"], 1)
            self.assertEqual(usage["buy_pending_usd"], "0")
        finally:
            store.close()

    def test_exact_fill_cost_and_actual_overrun_are_recorded(self) -> None:
        store = AxiomStore(":memory:")
        CanarySettingsService(store, clock=lambda: T0)
        try:
            reservation = store.reserve_canary_capacity(
                intent_id="exact-fraction",
                side="BUY",
                requested_cost="0.50",
                quantity="2",
                market_id="market-fraction",
                detail={"token_id": "token-fraction"},
                timestamp=T0,
            )
            with self.assertRaises(ValueError):
                store.record_canary_fill(
                    fill_id="bad-cost",
                    reservation_id=reservation["reservation_id"],
                    quantity="1",
                    price="0.25",
                    cost="0.20",
                    filled_at=T0,
                )
            store.record_canary_fill(
                fill_id="fraction-fill",
                reservation_id=reservation["reservation_id"],
                quantity="0.333",
                price="0.30",
                cost="0.09990",
                filled_at=T0,
                detail={"settlement_status": "CONFIRMED"},
            )
            store.record_canary_equity_mark(
                mark_id="mark-fraction",
                observed_at=T0,
                market_id="market-fraction",
                token_id="token-fraction",
                side="SELL",
                quantity="0.333",
                mark_price="0.30",
                cost_basis_usd="0.09990",
                mark_fee="0",
                detail={"position_id": "position:fraction"},
            )
            usage = store.canary_risk_accounting(T0)
            self.assertEqual(usage["aggregate_open_cost_usd"], "0.09990")

            overrun = store.reserve_canary_capacity(
                intent_id="actual-overrun",
                side="BUY",
                requested_cost="0.50",
                quantity="1",
                market_id="market-overrun",
                timestamp=T0,
            )
            store.record_canary_fill(
                fill_id="overrun-fill",
                reservation_id=overrun["reservation_id"],
                quantity="2",
                price="0.50",
                cost="1.00",
                filled_at=T0,
            )
            usage = store.canary_risk_accounting(T0)
            self.assertEqual(usage["aggregate_open_cost_usd"], "0.09990")
            self.assertEqual(usage["risk_breaker"], "ACTUAL_FILL_OVER_PLAN")
        finally:
            store.close()

    def test_reservation_identity_and_active_generation_are_fenced(self) -> None:
        store = AxiomStore(":memory:")
        service = CanarySettingsService(store, clock=lambda: T0)
        try:
            active = store.load_canary_setting_config(state="ACTIVE")
            assert active is not None
            first = store.reserve_canary_capacity(
                intent_id="identity-conflict",
                side="BUY",
                requested_cost="0.50",
                quantity="1",
                market_id="m1",
                timestamp=T0,
            )
            with self.assertRaises(ValueError):
                store.reserve_canary_capacity(
                    intent_id="identity-conflict",
                    side="BUY",
                    requested_cost="0.40",
                    quantity="1",
                    market_id="m1",
                    timestamp=T0,
                )
            draft = service.save_draft(
                {"target_notional_usd": "0.50", "max_all_in_buy_usd": "0.50"},
                "operator-a",
            )
            service.activate_draft(draft["config_id"], "operator-a", expected_generation=1)
            with self.assertRaises(ValueError):
                store.record_canary_submission_attempt(
                    attempt_id="stale-attempt",
                    intent_id="identity-conflict",
                    side="BUY",
                    attempted_at=T0,
                    config_generation=int(active["generation"]),
                    config_hash=str(active["config_hash"]),
                )
            self.assertEqual(first["intent_id"], "identity-conflict")
        finally:
            store.close()


    def test_loss_stop_blocks_buy_but_allows_owned_sell(self) -> None:
        store = AxiomStore(":memory:")
        service = CanarySettingsService(store, clock=lambda: T0)
        try:
            buy = store.reserve_canary_capacity(
                intent_id="loss-stop-entry",
                side="BUY",
                requested_cost="0.50",
                quantity="1",
                market_id="loss-market",
                detail={"token_id": "loss-token"},
                timestamp=T0,
            )
            store.record_canary_fill(
                fill_id="loss-stop-entry-fill",
                reservation_id=buy["reservation_id"],
                quantity="1",
                price="0.50",
                cost="0.50",
                filled_at=T0,
                detail={"settlement_status": "CONFIRMED"},
            )
            store.record_canary_equity_mark(
                mark_id="mark-loss-stop-entry",
                observed_at=T0,
                market_id="loss-market",
                token_id="loss-token",
                side="SELL",
                quantity="1",
                mark_price="0.50",
                cost_basis_usd="0.50",
                mark_fee="0",
                detail={"position_id": "position:loss-stop-entry"},
            )
            draft = service.save_draft(
                {"max_daily_loss_usd": "0.01", "realized_loss_entry_stop_usd": "0.01", "equity_loss_entry_stop_usd": "0.01"},
                "operator-a",
            )
            service.activate_draft(draft["config_id"], "operator-a", expected_generation=1)
            store.record_canary_external_flow(
                flow_id="equity-stop",
                amount="0.10",
                kind="EQUITY_LOSS",
                timestamp=T0,
            )
            with self.assertRaises(ValueError):
                store.reserve_canary_capacity(
                    intent_id="blocked-entry",
                    side="BUY",
                    requested_cost="0.10",
                    quantity="1",
                    market_id="other-market",
                    timestamp=T0,
                )
            sell = store.reserve_canary_capacity(
                intent_id="allowed-exit",
                side="SELL",
                requested_cost="0",
                quantity="1",
                market_id="loss-market",
                detail={"token_id": "loss-token"},
                timestamp=T0,
            )
            self.assertEqual(sell["side"], "SELL")
        finally:
            store.close()
if __name__ == "__main__":
    unittest.main()
