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
        self.market_version = "v1"
        self.position_id: str | None = None
        self.asset_id: str | None = None
        self.trades: list[dict[str, str]] = []
        self.trades_by_order: dict[str, list[dict[str, str]]] = {}
        self.order_assets_by_order: dict[str, str] = {}
        self.order_tokens_by_order: dict[str, str] = {}
        self.order_sides_by_order: dict[str, str] = {}
        self.order_prices_by_order: dict[str, str] = {}
        self.market_versions_by_token: dict[str, str] = {}
        self.position_ids_by_token: dict[str, str] = {}
    def geoblock(self) -> dict[str, object]:
        return {"blocked": False, "close_only": False, "country": "ZZ"}

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        version = self.market_versions_by_token.get(
            token_id,
            str(self.market_version or "").strip().lower(),
        )
        position_id = self.position_ids_by_token.get(token_id, self.position_id)
        asset_id = self.asset_id or (
            position_id if version == "v2" else token_id
        )
        return {
            "market_id": market_id,
            "token_id": token_id,
            "asset_id": asset_id,
            "market_version": version,
            "outcome_index": 0,
            "identity_bindings": (
                [
                    {
                        "index": 0,
                        "outcome": "yes",
                        "token_id": token_id,
                        "position_id": position_id,
                    }
                ]
                if position_id
                else []
            ),
            "position_id": position_id,
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "0.1",
            "fee_bps": "10",
            "best_bid": "0.49",
        }

    def get_order(self, order_id: str) -> dict[str, str]:
        if self.fail_reads:
            raise RuntimeError("temporary account read failure")
        side = self.order_sides_by_order.get(
            order_id,
            "BUY" if order_id == "late-entry-order" else "SELL",
        )
        original_size = (
            "2"
            if order_id == "late-entry-order"
            else "0.5"
            if order_id == "late-exit-order"
            else "1"
        )
        token_id = self.order_tokens_by_order.get(order_id, "token-yes")
        result = {
            "id": order_id,
            "status": self.order_status,
            "side": side,
            "market_id": "market-1",
            "token_id": token_id,
            "original_size": original_size,
            "price": self.order_prices_by_order.get(
                order_id,
                "0.70" if order_id == "late-entry-order" else "0.49",
            ),
        }
        asset_id = self.order_assets_by_order.get(order_id)
        if asset_id is not None:
            result["asset_id"] = asset_id
        if self.settlement_status is not None and (
            self.settlement_order_id is None or self.settlement_order_id == order_id
        ):
            result["settlement_status"] = self.settlement_status
        return result

    def list_account_trades(self, order_id: str) -> list[dict[str, str]]:
        if self.fail_reads:
            raise RuntimeError("temporary account read failure")
        side = self.order_sides_by_order.get(
            order_id,
            "BUY" if order_id == "late-entry-order" else "SELL",
        )
        asset_id = self.order_assets_by_order.get(order_id)
        rows: list[dict[str, str]] = []
        for trade in self.trades_by_order.get(order_id, self.trades):
            row = {
                **dict(trade),
                "order_id": trade.get("order_id", order_id),
                "side": trade.get("side", side),
                "market_id": trade.get("market_id", "market-1"),
                "token_id": trade.get("token_id", "token-yes"),
            }
            if asset_id is not None:
                row.setdefault("asset_id", asset_id)
            rows.append(row)
        return rows

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
    def test_exit_requires_explicit_true_accepting_orders(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        invalid_values = (False, None, 0, 1, "true", [], {})
        for value in invalid_values:
            with self.subTest(value=value):
                invalid_context = {
                    **context,
                    "accepting_orders": value,
                }
                with patch.object(
                    self.venue,
                    "market_context",
                    return_value=invalid_context,
                ):
                    with self.assertRaisesRegex(
                        CanaryBlocked,
                        "CANARY_MARKET_NOT_ACCEPTING_ORDERS",
                    ):
                        self._submit_exit()
                self.assertEqual(self.post_calls, [])
        submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
    def test_invalid_book_prices_block_mark_without_sink_or_loss_change(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        before = self.store.canary_risk_accounting(self.now)
        invalid_values = ("-1", "0", "2", "NaN", "Infinity")
        for value in invalid_values:
            with self.subTest(value=value):
                with patch.object(
                    self.venue,
                    "market_context",
                    return_value={**context, "best_bid": value},
                ):
                    marked = position_module._mark_owned_equity(
                        self.service,
                        self.venue,
                        self.now,
                    )
                self.assertEqual(marked["status"], "UNKNOWN")
                self.assertEqual(marked["marks"], [])
                self.assertEqual(
                    marked["blocked"],
                    [
                        {
                            "position_id": "position-1",
                            "reason": "CANARY_EXIT_PRICE_UNAVAILABLE",
                        }
                    ],
                )
                self.assertEqual(
                    self.store.connection.execute(
                        "SELECT COUNT(*) FROM canary_equity_marks"
                    ).fetchone()[0],
                    0,
                )
                after = self.store.canary_risk_accounting(self.now)
                for key in (
                    "aggregate_open_cost_usd",
                    "aggregate_exposure_usd",
                    "realized_loss_usd",
                    "equity_loss_usd",
                    "equity_status",
                ):
                    self.assertEqual(after[key], before[key], msg=f"value={value}")

    def test_invalid_book_prices_block_exit_without_reservation_or_submission(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        before = self.store.canary_risk_accounting(self.now)
        invalid_values = ("-1", "0", "2", "NaN", "Infinity")
        for value in invalid_values:
            with self.subTest(value=value):
                with patch.object(
                    self.venue,
                    "market_context",
                    return_value={**context, "best_bid": value},
                ):
                    with self.assertRaisesRegex(
                        CanaryBlocked,
                        "CANARY_EXIT_PRICE_UNAVAILABLE",
                    ):
                        self._submit_exit()
                self.assertEqual(self.post_calls, [])
                self.assertEqual(
                    self.store.connection.execute(
                        "SELECT COUNT(*) FROM canary_position_requests "
                        "WHERE side='SELL'"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    self.store.connection.execute(
                        "SELECT COUNT(*) FROM canary_risk_reservations "
                        "WHERE side='SELL'"
                    ).fetchone()[0],
                    0,
                )
                lot = self._lot()
                self.assertEqual(lot["status"], "OPEN")
                self.assertEqual(lot["pending_exit_quantity"], "0")
                after = self.store.canary_risk_accounting(self.now)
                for key in (
                    "aggregate_open_cost_usd",
                    "aggregate_exposure_usd",
                    "realized_loss_usd",
                    "equity_loss_usd",
                    "equity_status",
                ):
                    self.assertEqual(after[key], before[key], msg=f"value={value}")

    def test_boundary_adjacent_book_prices_are_valid_for_marks(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        for value in ("0.0001", "0.9999"):
            with self.subTest(value=value):
                with patch.object(
                    self.venue,
                    "market_context",
                    return_value={**context, "best_bid": value},
                ):
                    marked = position_module._mark_owned_equity(
                        self.service,
                        self.venue,
                        self.now,
                    )
                self.assertEqual(marked["status"], "KNOWN")
                self.assertEqual(marked["blocked"], [])
                self.assertEqual(marked["marks"][0]["mark_price"], value)

    def test_upper_boundary_adjacent_book_price_is_valid_for_exit(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        with patch.object(
            self.venue,
            "market_context",
            return_value={**context, "best_bid": "0.9999"},
        ):
            submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
        self.assertEqual(self.post_calls[-1]["price"], Decimal("0.9999"))


    def _assert_exit_context_blocked(
        self,
        context: dict[str, object],
        reason: str = "CANARY_EXIT_MARKET_CONTEXT_INVALID",
    ) -> None:
        with patch.object(self.venue, "market_context", return_value=context):
            with self.assertRaisesRegex(CanaryBlocked, reason):
                self._submit_exit()
        self.assertEqual(self.post_calls, [])
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_requests WHERE side='SELL'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_reservations WHERE side='SELL'"
            ).fetchone()[0],
            0,
        )
        lot = self._lot()
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(lot["pending_exit_quantity"], "0")

    def test_exit_rejects_wrong_v1_asset_before_reservation(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        self._assert_exit_context_blocked({**context, "asset_id": "wrong-asset"})

    def test_exit_rejects_wrong_v2_selected_token_before_reservation(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        self._assert_exit_context_blocked(
            {
                **context,
                "market_version": "v2",
                "token_id": "token-no",
                "position_id": "position-no",
                "asset_id": "position-no",
            }
        )

    def test_exit_rejects_conflicting_v2_position_before_reservation(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        self._assert_exit_context_blocked(
            {
                **context,
                "market_version": "v2",
                "position_id": "position-yes",
                "asset_id": "other-position",
            }
        )

    def test_exit_rejects_missing_context_identities_before_reservation(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        for missing in (
            {"market_version": "v1", "token_id": "token-yes", "asset_id": None},
            {"market_version": "v2", "token_id": None, "position_id": "position-yes", "asset_id": "position-yes"},
            {"market_version": "v2", "token_id": "token-yes", "position_id": None, "asset_id": "position-yes"},
            {"market_version": "v2", "token_id": "token-yes", "position_id": "position-yes", "asset_id": None},
        ):
            with self.subTest(missing=missing):
                self._assert_exit_context_blocked({**context, **missing})

    def test_exit_rejects_unknown_market_version_before_reservation(self) -> None:
        context = self.venue.market_context("market-1", "token-yes")
        self._assert_exit_context_blocked({**context, "market_version": "v3"})

    def test_valid_v1_exit_uses_lot_token_asset(self) -> None:
        submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
        call = self.service.submit_position_order.call_args.kwargs
        self.assertEqual(call["market_version"], "v1")
        self.assertEqual(call["asset_id"], "token-yes")

    def test_valid_v2_exit_uses_canonical_position_asset(self) -> None:
        self.venue.market_version = "v2"
        self.venue.position_id = "position-yes"
        evidence = {
            "market_version": "v2",
            "outcome_index": 0,
            "identity_bindings": [
                {
                    "index": 0,
                    "outcome": "yes",
                    "token_id": "token-yes",
                    "position_id": "position-yes",
                }
            ],
            "selected_token_id": "token-yes",
            "selected_position_id": "position-yes",
            "resolved_asset_id": "position-yes",
        }
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "entry-event-1",
                    "v2-fixture-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "token-yes",
                    "BUY",
                    "0.50",
                    "0.50",
                    "0.49",
                    "1",
                    None,
                    "1",
                    "0.50",
                    "0",
                    "CONFIRMED",
                    json.dumps(evidence, sort_keys=True),
                    int(self.config["control_generation"]),
                ),
            )
        submitted = self._submit_exit()
        self.assertEqual(submitted["status"], "SUBMITTED")
        call = self.service.submit_position_order.call_args.kwargs
        self.assertEqual(call["market_version"], "v2")
        self.assertEqual(call["asset_id"], "position-yes")
    def test_v2_buy_evidence_sync_preserves_lot_token_for_sell(self) -> None:
        from axiom.canary_positions import _ensure_schema, _sync_entry_lots

        self.assertEqual(
            position_module._mark_owned_equity(self.service, self.venue, self.now)["status"],
            "KNOWN",
        )
        _ensure_schema(self.service)
        event_id = "v2-entry-event"
        reservation_id = "v2-entry-reservation"
        token_id = "token-v2-yes"
        asset_id = "position-v2-yes"
        config = self.config
        self.store.reserve_canary_capacity(
            intent_id="v2-entry-intent",
            reservation_id=reservation_id,
            side="BUY",
            requested_cost="0.50",
            fee_reserve="0",
            quantity="1",
            market_id="market-1",
            event_id=event_id,
            config_id=str(config["config_id"]),
            config_generation=int(config["generation"]),
            config_hash=str(config["config_hash"]),
            control_generation=int(config["control_generation"]),
            detail={"side": "BUY", "token_id": token_id},
            timestamp=self.now,
        )
        self.store.record_canary_fill(
            fill_id="v2-entry-fill",
            reservation_id=reservation_id,
            quantity="1",
            price="0.50",
            cost="0.50",
            fee="0",
            filled_at=self.now,
            detail={"side": "BUY", "token_id": token_id},
        )
        evidence = {
            "market_version": "v2",
            "outcome_index": 0,
            "identity_bindings": [
                {
                    "index": 0,
                    "outcome": "yes",
                    "token_id": token_id,
                    "position_id": asset_id,
                }
            ],
            "selected_token_id": token_id,
            "selected_position_id": asset_id,
            "resolved_asset_id": asset_id,
            "exit_policy": {
                "type": "fixed_holding_period",
                "holding_period_seconds": 0,
            },
        }
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    "v2-entry-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    token_id,
                    "BUY",
                    "0.50",
                    "0.50",
                    "0.70",
                    "1",
                    "v2-entry-order",
                    "0",
                    None,
                    "0",
                    "MATCHED",
                    json.dumps(evidence, sort_keys=True),
                    int(config["control_generation"]),
                ),
            )
        self.venue.order_assets_by_order["v2-entry-order"] = asset_id
        self.venue.order_tokens_by_order["v2-entry-order"] = token_id
        self.venue.order_sides_by_order["v2-entry-order"] = "BUY"
        self.venue.order_prices_by_order["v2-entry-order"] = "0.70"
        self.venue.market_versions_by_token[token_id] = "v2"
        self.venue.position_ids_by_token[token_id] = asset_id
        self.venue.trades_by_order["v2-entry-order"] = [
            {
                "trade_id": "v2-entry-fill",
                "quantity": "1",
                "price": "0.50",
                "token_id": token_id,
                "fee_rate_bps": "0",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        reconciled = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(reconciled["status"], "RECONCILED")
        self.assertEqual(reconciled["entries"][0]["status"], "CONFIRMED")

        lot_row = self.store.connection.execute(
            "SELECT token_id,quantity FROM canary_position_lots WHERE position_id=?",
            ("position:" + event_id,),
        ).fetchone()
        self.assertIsNotNone(lot_row)
        assert lot_row is not None
        self.assertEqual(lot_row["token_id"], token_id)
        self.assertEqual(lot_row["quantity"], "1")

        self.venue.market_version = "v2"
        self.venue.position_id = asset_id
        self.next_order_id = "v2-exit-order"
        submitted = self.service.submit_exit(
            "position:" + event_id,
            self.venue,
            expected_generation=int(config["generation"]),
            config_id=str(config["config_id"]),
        )
        self.assertEqual(submitted["status"], "SUBMITTED")
        self.assertEqual(self.post_calls[-1]["asset_id"], asset_id)
        request_row = self.store.connection.execute(
            "SELECT token_id,asset_id,market_version FROM canary_position_requests "
            "WHERE request_id=?",
            (submitted["request_id"],),
        ).fetchone()
        self.assertIsNotNone(request_row)
        assert request_row is not None
        self.assertEqual(request_row["token_id"], token_id)
        self.assertEqual(request_row["asset_id"], asset_id)
        self.assertEqual(request_row["market_version"], "v2")
        marks_before = self.store.connection.execute(
            "SELECT COUNT(*) FROM canary_equity_marks"
        ).fetchone()[0]
        valid_context = self.venue.market_context("market-1", token_id)
        wrong_pair = {
            **valid_context,
            "position_id": "position-v2-no",
            "asset_id": "position-v2-no",
        }
        with patch.object(self.venue, "market_context", return_value=wrong_pair):
            marked = position_module._mark_owned_equity(
                self.service,
                self.venue,
                self.now + timedelta(seconds=1),
            )
        self.assertEqual(marked["status"], "UNKNOWN")
        self.assertEqual(
            marked["blocked"][0]["reason"],
            "CANARY_EQUITY_IDENTITY_CONFLICT",
        )
        marks_after = self.store.connection.execute(
            "SELECT COUNT(*) FROM canary_equity_marks"
        ).fetchone()[0]
        self.assertEqual(marks_after, marks_before)


    def test_legacy_v2_mixed_case_version_alias_is_canonicalized(self) -> None:
        from axiom.canary_positions import _legacy_entry_identity

        normalized, reason = _legacy_entry_identity(
            {"token_id": "legacy-token"},
            {
                "marketVersion": "V2",
                "asset_id": None,
                "selected_token_id": "legacy-token",
                "resolved_asset_id": "legacy-position",
            },
        )
        self.assertIsNone(reason)
        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["market_version"], "v2")
        self.assertNotIn("marketVersion", normalized)
        self.assertEqual(normalized["resolved_asset_id"], "legacy-position")

    def test_legacy_v2_fractional_outcome_index_is_rejected(self) -> None:
        from axiom.canary_positions import _legacy_entry_identity

        normalized, reason = _legacy_entry_identity(
            {"token_id": "legacy-token"},
            {
                "market_version": "v2",
                "outcome_index": 0.5,
                "selected_token_id": "legacy-token",
                "resolved_asset_id": "legacy-position",
            },
        )
        self.assertIsNone(normalized)
        self.assertEqual(reason, "CANARY_POSITION_IDENTITY_CONFLICT")

    def test_legacy_v2_unindexed_bindings_collapse_to_selected_identity(self) -> None:
        from axiom.canary_positions import _legacy_entry_identity

        normalized, reason = _legacy_entry_identity(
            {"token_id": "legacy-token"},
            {
                "market_version": "v2",
                "selected_token_id": "legacy-token",
                "resolved_asset_id": "legacy-position",
                "identity_bindings": [
                    {"token_id": "legacy-token", "position_id": "legacy-position"},
                    {"token_id": "other-token", "position_id": "other-position"},
                ],
            },
        )
        self.assertIsNone(reason)
        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertTrue(normalized["legacy_identity_binding"])
        self.assertEqual(
            normalized["identity_bindings"],
            [{"token_id": "legacy-token", "position_id": "legacy-position"}],
        )

    def test_legacy_terminal_settlement_is_not_overwritten(self) -> None:
        from axiom.canary_positions import _ensure_schema

        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation,settlement) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "terminal-legacy-entry",
                    "terminal-legacy-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "terminal-token",
                    "BUY",
                    "0.50",
                    "0.50",
                    "0.50",
                    "1",
                    "terminal-order",
                    "1",
                    "0.50",
                    "0",
                    "CONFIRMED",
                    json.dumps(
                        {
                            "market_version": "v2",
                            "selected_token_id": "terminal-token",
                        },
                        sort_keys=True,
                    ),
                    int(self.config["control_generation"]),
                    "TERMINAL",
                ),
            )
        _ensure_schema(self.service)
        row = self.store.connection.execute(
            "SELECT status,settlement,evidence_json FROM canary_ledger "
            "WHERE event_id='terminal-legacy-entry'"
        ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["status"], "CONFIRMED")
        self.assertEqual(row["settlement"], "TERMINAL")
        self.assertNotIn("legacy_migration", json.loads(row["evidence_json"]))

    def test_legacy_terminal_status_without_settlement_is_quarantined(self) -> None:
        from axiom.canary_positions import (
            LEGACY_ENTRY_NON_RESUMABLE,
            _ensure_schema,
        )

        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "terminal-status-no-settlement",
                    "terminal-status-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "terminal-token",
                    "BUY",
                    "0.50",
                    "0.50",
                    "0.50",
                    "1",
                    "terminal-status-order",
                    "0",
                    None,
                    "0",
                    "CANCELED",
                    json.dumps(
                        {
                            "market_version": "v2",
                            "selected_token_id": "terminal-token",
                        },
                        sort_keys=True,
                    ),
                    int(self.config["control_generation"]),
                ),
            )
        _ensure_schema(self.service)
        row = self.store.connection.execute(
            "SELECT status,settlement,evidence_json FROM canary_ledger "
            "WHERE event_id='terminal-status-no-settlement'"
        ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["status"], LEGACY_ENTRY_NON_RESUMABLE)
        self.assertEqual(row["settlement"], "MANUAL_RESOLUTION_REQUIRED")
        self.assertTrue(json.loads(row["evidence_json"])["non_resumable"])

    def test_pending_sell_blank_or_malformed_price_is_non_resumable(self) -> None:
        from axiom.canary_positions import _ensure_schema, LEGACY_REQUEST_NON_RESUMABLE

        with self.store.connection:
            for request_id, requested_price in (
                ("legacy-blank-price", " "),
                ("legacy-malformed-price", "not-a-price"),
            ):
                self.store.connection.execute(
                    "INSERT INTO canary_position_requests("
                    "request_id,position_id,reservation_id,event_id,venue,market_id,"
                    "token_id,asset_id,market_version,side,requested_quantity,"
                    "requested_price,status,expected_generation,config_id,submitted_at,"
                    "updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request_id,
                        "position-1",
                        "legacy-reservation-" + request_id,
                        request_id,
                        "polymarket",
                        "market-1",
                        "token-yes",
                        "token-yes",
                        "v1",
                        "SELL",
                        "1",
                        requested_price,
                        "SUBMITTED",
                        int(self.config["generation"]),
                        str(self.config["config_id"]),
                        self.now.isoformat(),
                        self.now.isoformat(),
                    ),
                )
        _ensure_schema(self.service)
        statuses = self.store.connection.execute(
            "SELECT request_id,status,last_error FROM canary_position_requests "
            "WHERE request_id IN ('legacy-blank-price','legacy-malformed-price') "
            "ORDER BY request_id"
        ).fetchall()
        self.assertEqual(
            [(row["status"], row["last_error"]) for row in statuses],
            [
                (LEGACY_REQUEST_NON_RESUMABLE, "LEGACY_REQUEST_PRICE_UNAVAILABLE"),
                (LEGACY_REQUEST_NON_RESUMABLE, "LEGACY_REQUEST_PRICE_UNAVAILABLE"),
            ],
        )

    def test_pre339_v2_buy_evidence_migrates_without_remote_identity_guess(self) -> None:
        from axiom.canary_positions import _ensure_schema

        _ensure_schema(self.service)
        event_id = "legacy-v2-entry-event"
        reservation_id = "legacy-v2-entry-reservation"
        token_id = "legacy-token-yes"
        asset_id = "legacy-position-yes"
        config = self.config
        self.assertEqual(
            position_module._mark_owned_equity(self.service, self.venue, self.now)["status"],
            "KNOWN",
        )
        self.store.reserve_canary_capacity(
            intent_id="legacy-v2-entry-intent",
            reservation_id=reservation_id,
            side="BUY",
            requested_cost="0.70",
            fee_reserve="0",
            quantity="1",
            market_id="market-1",
            event_id=event_id,
            config_id=str(config["config_id"]),
            config_generation=int(config["generation"]),
            config_hash=str(config["config_hash"]),
            control_generation=int(config["control_generation"]),
            detail={"side": "BUY", "token_id": token_id},
            timestamp=self.now,
        )
        self.store.record_canary_fill(
            fill_id="legacy-v2-entry-fill",
            reservation_id=reservation_id,
            quantity="1",
            price="0.70",
            cost="0.70",
            fee="0",
            filled_at=self.now,
            detail={"side": "BUY", "token_id": token_id},
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    "legacy-v2-entry-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    token_id,
                    "BUY",
                    "0.70",
                    "0.70",
                    "0.70",
                    "1",
                    "legacy-v2-entry-order",
                    "0",
                    None,
                    "0",
                    "MATCHED",
                    json.dumps(
                        {
                            "market_version": "v2",
                            "selected_token_id": token_id,
                            "resolved_asset_id": asset_id,
                            "exit_policy": {
                                "type": "fixed_holding_period",
                                "holding_period_seconds": 0,
                            },
                        },
                        sort_keys=True,
                    ),
                    int(config["control_generation"]),
                ),
            )
        self.venue.order_assets_by_order["legacy-v2-entry-order"] = asset_id
        self.venue.order_tokens_by_order["legacy-v2-entry-order"] = token_id
        self.venue.order_sides_by_order["legacy-v2-entry-order"] = "BUY"
        self.venue.order_prices_by_order["legacy-v2-entry-order"] = "0.70"
        self.venue.market_versions_by_token[token_id] = "v2"
        self.venue.position_ids_by_token[token_id] = asset_id
        self.venue.trades_by_order["legacy-v2-entry-order"] = [
            {
                "trade_id": "legacy-v2-entry-fill",
                "quantity": "1",
                "price": "0.70",
                "token_id": token_id,
                "fee_rate_bps": "0",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        reconciled = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(reconciled["status"], "RECONCILED")
        evidence_row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_ledger WHERE event_id=?",
            (event_id,),
        ).fetchone()
        self.assertIsNotNone(evidence_row)
        assert evidence_row is not None
        migrated = json.loads(evidence_row["evidence_json"])
        self.assertTrue(migrated["legacy_identity_binding"])
        self.assertEqual(migrated["identity_bindings"][0]["token_id"], token_id)
        self.assertEqual(migrated["identity_bindings"][0]["position_id"], asset_id)
        lot = self.store.connection.execute(
            "SELECT token_id,asset_id,market_version FROM canary_position_lots "
            "WHERE position_id=?",
            ("position:" + event_id,),
        ).fetchone()
        self.assertIsNotNone(lot)
        assert lot is not None
        self.assertEqual(lot["token_id"], token_id)
        self.assertEqual(lot["asset_id"], asset_id)
        self.assertEqual(lot["market_version"], "v2")

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
        request = self._request()
        self.assertEqual(request["request_id"], submitted["request_id"])
        self.assertEqual(request["position_id"], submitted["position_id"])
        self.assertEqual(request["reservation_id"], submitted["reservation_id"])
        self.assertEqual(request["event_id"], submitted["reservation_id"])
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
        self.assertEqual(Decimal(str(equity["aggregate_open_cost_usd"])), Decimal("0.50"))
        self.assertEqual(Decimal(str(equity["aggregate_exposure_usd"])), Decimal("0.50"))

        self.venue.trades[0]["status"] = "CONFIRMED"
        confirmed = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(confirmed["status"], "RECONCILED")
        self.assertEqual(self._request()["status"], "MATCHED")
        self.assertEqual(self._lot()["status"], "EXIT_PENDING")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        confirmed_reservation = self._reservation(str(submitted["reservation_id"]))
        self.assertEqual(confirmed_reservation["status"], "FILLED")
        self.assertIsNone(confirmed_reservation["released_at"])
        confirmed_accounting = self.store.canary_risk_accounting(self.now)
        self.assertEqual(
            Decimal(str(confirmed_accounting["aggregate_open_cost_usd"])),
            Decimal("0.50"),
        )
        self.assertEqual(
            Decimal(str(confirmed_accounting["aggregate_exposure_usd"])),
            Decimal("0.50"),
        )
        self.venue.order_status = "FILLED"
        filled = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(filled["status"], "RECONCILED")
        self.assertEqual(self._request()["status"], "FILLED")
        filled_accounting = self.store.canary_risk_accounting(self.now)
        self.assertEqual(
            Decimal(str(filled_accounting["aggregate_open_cost_usd"])),
            Decimal("0.50"),
        )
        self.assertEqual(
            Decimal(str(filled_accounting["aggregate_exposure_usd"])),
            Decimal("0.50"),
        )
        self.venue.order_status = "MATCHED"
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
            "SELECT fill_id,reservation_id,detail_json FROM canary_risk_fills "
            "WHERE fill_id='sell-fill-1'"
        ).fetchone()
        self.assertIsNotNone(risk_fill)
        assert risk_fill is not None
        self.assertEqual(risk_fill["reservation_id"], request["reservation_id"])
        self.assertEqual(risk_fill["fill_id"], "sell-fill-1")
        detail = json.loads(risk_fill["detail_json"])
        self.assertEqual(detail["position_id"], request["position_id"])
        self.assertEqual(detail["request_id"], request["request_id"])
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
        self.assertEqual(Decimal(str(final_accounting["aggregate_open_cost_usd"])), Decimal("0"))
        self.assertEqual(Decimal(str(final_accounting["aggregate_exposure_usd"])), Decimal("0"))
        self.assertEqual(
            Decimal(str(final_accounting["today_realized_pnl_usd"])),
            Decimal("-0.01049"),
        )
        self.assertEqual(final_accounting["equity_status"], "KNOWN")
        self.assertEqual(Decimal(str(final_accounting["realized_loss_usd"])), Decimal("0.01049"))
        repeated_accounting = self.store.canary_risk_accounting(self.now)
        self.assertEqual(
            repeated_accounting["aggregate_open_cost_usd"],
            final_accounting["aggregate_open_cost_usd"],
        )
        self.assertEqual(
            repeated_accounting["aggregate_exposure_usd"],
            final_accounting["aggregate_exposure_usd"],
        )
        self.assertEqual(
            repeated_accounting["today_realized_pnl_usd"],
            final_accounting["today_realized_pnl_usd"],
        )

    def test_mixed_unrelated_fill_is_fail_closed_without_lot_or_risk_mutation(self) -> None:
        submitted = self._submit_exit()
        self.venue.trades = [
            {
                "trade_id": "owned-fill",
                "quantity": "0.5",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            },
            {
                "trade_id": "unrelated-fill",
                "order_id": "different-order",
                "quantity": "0.5",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            },
        ]

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        self.assertEqual(self._request()["status"], "UNKNOWN")
        self.assertEqual(self._lot()["sold_quantity"], "0")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "OPEN",
        )

    def test_duplicate_fill_owned_by_another_request_is_explicit_conflict(self) -> None:
        original_lot = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_position_lots WHERE position_id='position-1'"
            ).fetchone()
        )
        original_lot.update(
            {
                "position_id": "position-2",
                "event_id": "entry-event-2",
                "reservation_id": "entry-reservation-2",
            }
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_position_lots("
                + ",".join(original_lot)
                + ") VALUES("
                + ",".join("?" for _ in original_lot)
                + ")",
                tuple(original_lot.values()),
            )
        entry = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_risk_reservations "
                "WHERE reservation_id='entry-reservation-1'"
            ).fetchone()
        )
        entry.update(
            {
                "reservation_id": "entry-reservation-2",
                "intent_id": "entry-intent-2",
                "event_id": "entry-event-2",
                "filled_cost": "0",
                "remaining_cost": "0.50",
                "filled_quantity": "0",
                "status": "HELD",
                "detail_json": json.dumps(
                    {"side": "BUY", "token_id": "token-yes"},
                    sort_keys=True,
                ),
                "created_at": self.now.isoformat(),
                "updated_at": self.now.isoformat(),
                "released_at": None,
            }
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_risk_reservations("
                + ",".join(entry)
                + ") VALUES("
                + ",".join("?" for _ in entry)
                + ")",
                tuple(entry.values()),
            )
        self.store.record_canary_fill(
            fill_id="entry-fill-2",
            reservation_id="entry-reservation-2",
            quantity="1",
            price="0.50",
            cost="0.50",
            fee="0",
            filled_at=self.now,
            detail={
                "side": "BUY",
                "token_id": "token-yes",
                "settlement_status": "CONFIRMED",
            },
        )
        first = self._submit_exit()
        self.next_order_id = "exit-2"
        second = self.service.submit_exit(
            "position-2",
            self.venue,
            expected_generation=int(self.config["generation"]),
            config_id=str(self.config["config_id"]),
        )
        self.venue.trades_by_order["exit-1"] = [
            {
                "trade_id": "cross-request-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        self.venue.trades_by_order["exit-2"] = [
            {
                "trade_id": "cross-request-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        first_request = self.store.connection.execute(
            "SELECT status FROM canary_position_requests WHERE request_id=?",
            (first["request_id"],),
        ).fetchone()
        second_request = self.store.connection.execute(
            "SELECT status FROM canary_position_requests WHERE request_id=?",
            (second["request_id"],),
        ).fetchone()
        self.assertEqual(first_request["status"], "MATCHED")
        self.assertEqual(second_request["status"], "UNKNOWN")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills WHERE fill_id='cross-request-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills WHERE fill_id='cross-request-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self._reservation(str(second["reservation_id"]))["status"],
            "OPEN",
        )

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

    def test_authoritative_partial_settlement_releases_remainder_with_exact_open_basis(self) -> None:
        self.venue.order_status = "MATCHED"
        self.venue.trades = [
            {
                "trade_id": "partial-settled-fill",
                "quantity": "0.4",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        self.venue.settlement_status = "SETTLED_PARTIAL"
        submitted = self._submit_exit()

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "RECONCILED")
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "SETTLED_PARTIAL")
        self.assertEqual(request["settlement_status"], "SETTLED_PARTIAL")
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(lot["quantity"], "1")
        self.assertEqual(lot["sold_quantity"], "0.4")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(Decimal(lot["cost_basis"]), Decimal("0.50"))
        self.assertEqual(Decimal(lot["gross_proceeds"]), Decimal("0.196"))
        self.assertEqual(Decimal(lot["exit_fees"]), Decimal("0.000196"))
        self.assertEqual(Decimal(lot["realized_pnl"]), Decimal("-0.004196"))
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "SETTLED",
        )
        self.assertEqual(position_module._request_rows(self.service), [])
        accounting = self.store.canary_risk_accounting(self.now)
        self.assertEqual(
            Decimal(str(accounting["aggregate_open_cost_usd"])),
            Decimal("0.30"),
        )
        self.assertEqual(
            Decimal(str(accounting["aggregate_exposure_usd"])),
            Decimal("0.30"),
        )
        positions = position_module.list_positions(self.service)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].quantity, Decimal("0.6"))
        self.assertEqual(positions[0].cost_basis, Decimal("0.50"))

        self.next_order_id = "exit-2"
        self.venue.order_status = "MATCHED"
        self.venue.settlement_status = None
        self.venue.trades = []
        retry = self._submit_exit()
        self.assertEqual(retry["order_id"], "exit-2")
        self.assertEqual(retry["quantity"], "0.6")
        self.assertEqual(Decimal(str(self.post_calls[-1]["size"])), Decimal("0.6"))

    def test_authoritative_zero_fill_malformed_trade_payload_stays_pending(self) -> None:
        self.venue.order_status = "MATCHED"
        self.venue.settlement_status = "SETTLED"
        submitted = self._submit_exit()
        self.venue.list_account_trades = lambda order_id: {"trades": "bad"}  # type: ignore[method-assign]

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        self.assertEqual(reconciled["requests"][0]["reason"], "CANARY_TRADE_RESPONSE_INVALID")
        self.assertEqual(self._request()["status"], "UNKNOWN")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "OPEN",
        )

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

    def test_missing_authoritative_order_price_is_unknown_and_blocking(self) -> None:
        submitted = self._submit_exit()
        original_get_order = self.venue.get_order

        def order_without_price(order_id: str) -> dict[str, str]:
            result = original_get_order(order_id)
            result.pop("price", None)
            return result

        with patch.object(
            self.venue,
            "get_order",
            side_effect=order_without_price,
        ):
            reconciled = position_module.reconcile_pending(
                self.service,
                self.venue,
            )

        self.assertEqual(reconciled["status"], "DEGRADED")
        request = self._request()
        self.assertEqual(request["status"], "UNKNOWN")
        self.assertEqual(request["last_error"], "CANARY_ORDER_PRICE_UNAVAILABLE")
        self.assertEqual(self._lot()["pending_exit_quantity"], "1")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills "
                "WHERE fill_id != 'entry-fill-1'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "OPEN",
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

    def test_unrecognized_venue_status_is_unknown_and_retryable(self) -> None:
        submitted = self._submit_exit()
        self.venue.order_status = "PROCESSING"

        first = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(first["status"], "RECONCILED")
        self.assertEqual(first["requests"][0]["status"], "UNKNOWN")
        self.assertEqual(self._request()["status"], "UNKNOWN")
        self.assertEqual(self._reservation(str(submitted["reservation_id"]))["status"], "OPEN")
        self.assertEqual(len(position_module._request_rows(self.service)), 1)

        self.venue.order_status = "MATCHED"
        retry = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(retry["status"], "RECONCILED")
        self.assertEqual(retry["requests"][0]["status"], "MATCHED")
        self.assertEqual(self._request()["status"], "MATCHED")

    def test_stale_post_settled_reconciliation_cannot_reopen_or_change_basis(self) -> None:
        self.venue.trades = [
            {
                "trade_id": "settled-fill",
                "quantity": "1",
                "price": "0.49",
                "fee_rate_bps": "10",
                "match_time": self.now.isoformat(),
                "status": "CONFIRMED",
            }
        ]
        submitted = self._submit_exit()
        self.venue.settlement_status = "SETTLED"
        settled = position_module.reconcile_pending(self.service, self.venue)
        self.assertEqual(settled["status"], "RECONCILED")

        before = self.store.canary_risk_accounting(self.now)
        stale_request = self._request()
        self.venue.settlement_status = None
        self.venue.order_status = "PROCESSING"
        stale_order = self.venue.get_order(str(submitted["order_id"]))

        replay = position_module._apply_reconciled_request(
            self.service,
            stale_request,
            stale_order,
            [],
            self.now,
        )

        self.assertEqual(replay["status"], "SETTLED")
        self.assertEqual(self._request()["status"], "SETTLED")
        self.assertEqual(self._lot()["status"], "CLOSED")
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "SETTLED",
        )
        after = self.store.canary_risk_accounting(self.now)
        self.assertEqual(after["aggregate_open_cost_usd"], before["aggregate_open_cost_usd"])
        self.assertEqual(after["aggregate_exposure_usd"], before["aggregate_exposure_usd"])
        self.assertEqual(after["today_realized_pnl_usd"], before["today_realized_pnl_usd"])


    def test_interleaved_stale_reconciliation_applies_confirmed_partial_fill_once(self) -> None:
        submitted = self._submit_exit()
        stale_request = self._request()
        order_id = str(submitted["order_id"])
        stale_order = {
            "id": order_id,
            "status": "MATCHED",
            "side": "SELL",
            "market_id": "market-1",
            "token_id": "token-yes",
            "original_size": "1",
            "price": "0.49",
        }
        settled_order = dict(stale_order)
        settled_order["settlement_status"] = "SETTLED"
        trade = {
            "trade_id": "interleaved-fill",
            "order_id": order_id,
            "quantity": "0.4",
            "price": "0.49",
            "fee_rate_bps": "10",
            "match_time": self.now.isoformat(),
            "status": "CONFIRMED",
            "side": "SELL",
            "market_id": "market-1",
            "token_id": "token-yes",
        }
        entered = threading.Event()
        resume = threading.Event()
        errors: list[BaseException] = []
        stale_result: list[dict[str, object]] = []
        validation_calls = 0
        original_validate = position_module._validate_venue_identity

        def interleaving_validate(**kwargs: object) -> tuple[str, Decimal]:
            nonlocal validation_calls
            validation_calls += 1
            if validation_calls == 1:
                entered.set()
                self.assertTrue(resume.wait(2))
            return original_validate(**kwargs)  # type: ignore[arg-type]

        def stale_worker() -> None:
            try:
                stale_result.append(
                    position_module._apply_reconciled_request(
                        self.service,
                        stale_request,
                        stale_order,
                        [],
                        self.now,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        with patch.object(
            position_module,
            "_validate_venue_identity",
            side_effect=interleaving_validate,
        ):
            worker = threading.Thread(target=stale_worker)
            worker.start()
            self.assertTrue(entered.wait(2))
            settled_result = position_module._apply_reconciled_request(
                self.service,
                stale_request,
                settled_order,
                [trade],
                self.now,
            )
            resume.set()
            worker.join(2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(settled_result["status"], "SETTLED_PARTIAL")
        self.assertEqual(stale_result[0]["status"], "SETTLED_PARTIAL")
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "SETTLED_PARTIAL")
        self.assertEqual(request["filled_quantity"], "0.4")
        self.assertEqual(request["fees"], "0.000196")
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(lot["sold_quantity"], "0.4")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(lot["cost_basis"], "0.50")
        self.assertEqual(lot["gross_proceeds"], "0.196")
        self.assertEqual(lot["exit_fees"], "0.000196")
        self.assertEqual(lot["realized_pnl"], "-0.004196")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills "
                "WHERE fill_id='interleaved-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills "
                "WHERE fill_id='interleaved-fill'"
            ).fetchone()[0],
            1,
        )
        risk_detail_row = self.store.connection.execute(
            "SELECT detail_json FROM canary_risk_fills "
            "WHERE fill_id='interleaved-fill'"
        ).fetchone()
        self.assertIsNotNone(risk_detail_row)
        assert risk_detail_row is not None
        risk_detail = json.loads(risk_detail_row["detail_json"])
        self.assertEqual(risk_detail["entry_cost_usd"], "0.200")
        self.assertEqual(risk_detail["realized_pnl_usd"], "-0.004196")

    def test_atomic_sell_projection_blocks_terminalizer_between_risk_and_local(self) -> None:
        submitted = self._submit_exit()
        request = self._request()
        order_id = str(submitted["order_id"])
        order = {
            "id": order_id,
            "status": "MATCHED",
            "settlement_status": "SETTLED",
            "side": "SELL",
            "market_id": "market-1",
            "token_id": "token-yes",
            "original_size": "1",
            "price": "0.49",
        }
        trade = {
            "trade_id": "atomic-interleave-fill",
            "order_id": order_id,
            "quantity": "0.4",
            "price": "0.49",
            "fee_rate_bps": "10",
            "match_time": self.now.isoformat(),
            "status": "CONFIRMED",
            "side": "SELL",
            "market_id": "market-1",
            "token_id": "token-yes",
        }
        risk_written = threading.Event()
        allow_local_projection = threading.Event()
        terminalizer_done = threading.Event()
        results: list[dict[str, object]] = []
        errors: list[BaseException] = []
        original_record = self.store.record_canary_fill

        def pausing_record(**kwargs: object) -> dict[str, object]:
            result = original_record(**kwargs)
            risk_written.set()
            if not allow_local_projection.wait(2):
                raise AssertionError("timed out before local fill projection")
            return result

        def reconcile() -> None:
            try:
                results.append(
                    position_module._apply_reconciled_request(
                        self.service,
                        request,
                        order,
                        [trade],
                        self.now,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        def terminalize() -> None:
            try:
                position_module._apply_reconciled_request(
                    self.service,
                    request,
                    order,
                    [trade],
                    self.now,
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                terminalizer_done.set()

        with patch.object(
            self.store,
            "record_canary_fill",
            side_effect=pausing_record,
        ):
            first = threading.Thread(target=reconcile)
            first.start()
            self.assertTrue(risk_written.wait(2))

            second = threading.Thread(target=terminalize)
            second.start()
            # The competing terminalizer must remain outside the projection
            # until both halves of the first fill are in one transaction.
            self.assertFalse(terminalizer_done.wait(0.2))

            allow_local_projection.set()
            first.join(2)
            second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertFalse(errors)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "SETTLED_PARTIAL")
        request_row = self._request()
        lot = self._lot()
        self.assertEqual(request_row["status"], "SETTLED_PARTIAL")
        self.assertEqual(request_row["filled_quantity"], "0.4")
        self.assertEqual(request_row["fees"], "0.000196")
        self.assertEqual(lot["status"], "OPEN")
        self.assertEqual(lot["sold_quantity"], "0.4")
        self.assertEqual(lot["pending_exit_quantity"], "0")
        self.assertEqual(lot["gross_proceeds"], "0.196")
        self.assertEqual(lot["exit_fees"], "0.000196")
        self.assertEqual(lot["realized_pnl"], "-0.004196")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills "
                "WHERE fill_id='atomic-interleave-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills "
                "WHERE fill_id='atomic-interleave-fill'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills AS risk "
                "LEFT JOIN canary_position_fills AS local "
                "ON local.fill_id=risk.fill_id "
                "WHERE risk.fill_id='atomic-interleave-fill' "
                "AND local.fill_id IS NULL"
            ).fetchone()[0],
            0,
        )

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
                "order_id": "entry-taker-order",
                "maker_order_ids": ["late-entry-order"],
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
                "order_id": "exit-taker-order",
                "maker_order_ids": ["late-exit-order"],
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

    def test_canceled_exit_with_truncated_trade_history_stays_unknown_and_reserved(self) -> None:
        submitted = self._submit_exit()
        self.venue.order_status = "CANCELED"
        visible_fill = {
            "trade_id": "visible-before-truncation",
            "quantity": "1",
            "price": "0.49",
            "fee_rate_bps": "10",
            "match_time": self.now.isoformat(),
            "status": "CONFIRMED",
        }
        self.venue.list_account_trades = (  # type: ignore[method-assign]
            lambda order_id: {
                "trades": [visible_fill],
                "truncated": True,
            }
        )

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        self.assertEqual(reconciled["requests"][0]["status"], "UNKNOWN")
        self.assertEqual(reconciled["requests"][0]["reason"], "CANARY_TRADE_HISTORY_INCOMPLETE")
        request = self._request()
        lot = self._lot()
        self.assertEqual(request["status"], "UNKNOWN")
        self.assertEqual(lot["status"], "EXIT_PENDING")
        self.assertEqual(lot["sold_quantity"], "0")
        self.assertEqual(lot["pending_exit_quantity"], "1")
        self.assertEqual(
            self._reservation(str(submitted["reservation_id"]))["status"],
            "OPEN",
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_fills"
            ).fetchone()[0],
            0,
        )

    def test_no_current_fill_does_not_revoke_prior_confirmed_ownership(self) -> None:
        config = self.config
        event_id = "entry-event-1"
        reservation_id = "entry-reservation-1"
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_ledger("
                "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,"
                "side,requested_notional,paper_expected_price,max_price,submitted_quantity,"
                "exchange_order_id,fill_quantity,actual_average_price,fees,status,evidence_json,"
                "control_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    "entry-prior-owned-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "token-yes",
                    "BUY",
                    "0.50",
                    "0.50",
                    "0.49",
                    "2",
                    "late-entry-order",
                    "1",
                    "0.50",
                    "0",
                    "CONFIRMED",
                    json.dumps(
                        {
                            "market_version": "v1",
                            "selected_token_id": "token-yes",
                            "resolved_asset_id": "token-yes",
                        },
                        sort_keys=True,
                    ),
                    int(config["control_generation"]),
                ),
            )
            self.store.connection.execute(
                "DELETE FROM canary_risk_fills WHERE reservation_id=?",
                (reservation_id,),
            )
        self.venue.order_sides_by_order["late-entry-order"] = "BUY"
        self.venue.order_prices_by_order["late-entry-order"] = "0.49"
        self.venue.order_status = "CANCELED"

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "RECONCILED")
        self.assertEqual(reconciled["entries"][0]["status"], "CONFIRMED")
        entry = self.store.connection.execute(
            "SELECT status,fill_quantity,actual_average_price,fees,settlement "
            "FROM canary_ledger WHERE event_id=?",
            (event_id,),
        ).fetchone()
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["status"], "CONFIRMED")
        self.assertEqual(entry["fill_quantity"], "1")
        self.assertEqual(entry["actual_average_price"], "0.50")
        self.assertEqual(entry["fees"], "0")
        self.assertIsNone(entry["settlement"])
        reservation = self.store.connection.execute(
            "SELECT status,released_at FROM canary_risk_reservations "
            "WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        self.assertIsNotNone(reservation)
        assert reservation is not None
        self.assertEqual(reservation["status"], "FILLED")
        self.assertIsNone(reservation["released_at"])
        lot = self.store.connection.execute(
            "SELECT quantity,cost_basis,status FROM canary_position_lots "
            "WHERE position_id=?",
            ("position-1",),
        ).fetchone()
        self.assertIsNotNone(lot)
        assert lot is not None
        self.assertEqual(lot["quantity"], "1")
        self.assertEqual(lot["cost_basis"], "0.50")
        self.assertEqual(lot["status"], "OPEN")

    def test_canceled_entry_without_fill_releases_reservation_terminally(self) -> None:
        config = self.config
        position_module._mark_owned_equity(self.service, self.venue, self.now)
        event_id = "canceled-entry-no-fill"
        reservation_id = "canceled-entry-reservation"
        self.store.reserve_canary_capacity(
            intent_id=event_id,
            reservation_id=reservation_id,
            side="BUY",
            requested_cost="1.00",
            fee_reserve="0",
            quantity="2",
            market_id="market-1",
            event_id=event_id,
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
                    event_id,
                    "canceled-entry-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "token-yes",
                    "BUY",
                    "1.00",
                    "0.50",
                    "0.51",
                    "2",
                    "late-entry-order",
                    "0",
                    None,
                    "0",
                    "CANCELED",
                    "{}",
                    int(config["control_generation"]),
                ),
            )
        self.venue.order_prices_by_order["late-entry-order"] = "0.51"
        self.venue.order_status = "CANCELED"

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "RECONCILED")
        self.assertEqual(reconciled["entries"][0]["status"], "CANCELED")
        entry = self.store.connection.execute(
            "SELECT status,fill_quantity,settlement FROM canary_ledger "
            "WHERE event_id=?",
            (event_id,),
        ).fetchone()
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["status"], "CANCELED")
        self.assertEqual(entry["fill_quantity"], "0")
        self.assertEqual(entry["settlement"], "TERMINAL")
        reservation = self.store.connection.execute(
            "SELECT status,released_at FROM canary_risk_reservations "
            "WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        self.assertIsNotNone(reservation)
        assert reservation is not None
        self.assertEqual(reservation["status"], "RELEASED")
        self.assertEqual(reservation["released_at"], self.now.isoformat())
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT 1 FROM canary_position_lots WHERE event_id=?",
                (event_id,),
            ).fetchone()
        )

    def test_canceled_entry_with_truncated_trade_history_stays_unknown_and_reserved(self) -> None:
        config = self.config
        position_module._mark_owned_equity(self.service, self.venue, self.now)
        self.store.reserve_canary_capacity(
            intent_id="truncated-entry-intent",
            reservation_id="truncated-entry-reservation",
            side="BUY",
            requested_cost="1.00",
            fee_reserve="0",
            quantity="2",
            market_id="market-1",
            event_id="truncated-entry-event",
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
                    "truncated-entry-event",
                    "truncated-entry-signal",
                    self.now.isoformat(),
                    "candidate-1",
                    "polymarket",
                    "market-1",
                    "token-yes",
                    "BUY",
                    "1.00",
                    "0.50",
                    "0.51",
                    "2",
                    "truncated-entry-order",
                    "0",
                    None,
                    "0",
                    "CANCELED",
                    "{}",
                    int(config["control_generation"]),
                ),
            )
        self.venue.order_status = "CANCELED"
        visible_fill = {
            "trade_id": "entry-visible-before-truncation",
            "quantity": "1",
            "price": "0.50",
            "fee_rate_bps": "10",
            "match_time": self.now.isoformat(),
            "status": "CONFIRMED",
        }
        self.venue.list_account_trades = (  # type: ignore[method-assign]
            lambda order_id: {
                "trades": [visible_fill],
                "truncated": True,
            }
        )

        reconciled = position_module.reconcile_pending(self.service, self.venue)

        self.assertEqual(reconciled["status"], "DEGRADED")
        self.assertEqual(reconciled["entries"][0]["status"], "UNKNOWN")
        self.assertEqual(
            reconciled["entries"][0]["reason"],
            "CANARY_TRADE_HISTORY_INCOMPLETE",
        )
        entry = self.store.connection.execute(
            "SELECT status,fill_quantity FROM canary_ledger "
            "WHERE event_id='truncated-entry-event'"
        ).fetchone()
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["status"], "UNKNOWN")
        self.assertEqual(entry["fill_quantity"], "0")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_risk_reservations "
                "WHERE reservation_id='truncated-entry-reservation'"
            ).fetchone()["status"],
            "HELD",
        )

if __name__ == "__main__":
    unittest.main()
