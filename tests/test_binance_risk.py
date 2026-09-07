from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from axiom.binance_risk import (
    BINANCE_RISK_ENVELOPE_VERSION,
    BinanceRisk,
    BinanceRiskAssessment,
    BinanceRiskEnvelope,
    BinanceRiskSnapshot,
    DEFAULT_BINANCE_RISK_ENVELOPE,
    PendingOrder,
    RiskAssessment,
    RiskEngine,
    RiskSnapshot,
    SizedOrder,
    SymbolRules,
    assess_entry,
    assess_exit,
    assess_order,
    size_limit_order,
)


D = Decimal
UTC = timezone.utc
T0 = datetime(2026, 1, 2, 23, 59, 30, tzinfo=UTC)


def make_rules(**overrides):
    values = {
        "symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "status": "TRADING",
        "spot_trading_allowed": True,
        "order_types": ("LIMIT", "MARKET"),
        "time_in_force": ("IOC", "FOK"),
        "min_price": D("0.01"),
        "max_price": D("100000"),
        "tick_size": D("0.01"),
        "min_qty": D("0.001"),
        "max_qty": D("1000"),
        "step_size": D("0.001"),
        "market_min_qty": D("0.01"),
        "market_max_qty": D("500"),
        "market_step_size": D("0.01"),
        "min_notional": D("1"),
        "max_notional": D("100000"),
    }
    values.update(overrides)
    return SymbolRules(**values)


def make_snapshot(**overrides):
    values = {
        "observed_at": T0,
        "quote_available": D("1000"),
        "aggregate_exposure": D("0"),
        "reserved_exposure": D("0"),
        "owned_inventory": {},
        "pending_orders": (),
        "positions": 0,
        "entry_submissions_today": 0,
        "exit_submissions_today": 0,
        "realized_loss_today": D("0"),
        "equity_loss_today": D("0"),
        "unrealized_pnl": D("0"),
        "fees_today": D("0"),
        "open_orders": 0,
        "account_order_count": 0,
        "exchange_order_count": 0,
        "account_paused": False,
        "rate_limited": False,
    }
    values.update(overrides)
    return RiskSnapshot(**values)


class BinanceRiskEnvelopeTests(unittest.TestCase):
    def test_envelope_is_decimal_only_immutable_and_canonical(self):
        envelope = BinanceRiskEnvelope(
            version=" test-envelope ",
            quote_asset="usdt",
            entry_notional="12.50",
            max_aggregate_exposure="40",
            max_reserved_exposure="35",
            realized_loss_entry_stop="4.5",
            equity_loss_entry_stop="6",
            max_positions="3",
            max_submissions_per_day="7",
            max_execution_deviation_bps="75",
            exit_order_reserve_per_position="2",
        )
        self.assertEqual(envelope.version, " test-envelope ")
        self.assertEqual(envelope.quote_asset, "USDT")
        self.assertEqual(envelope.entry_notional, D("12.50"))
        self.assertEqual(envelope.entry_cap, D("12.50"))
        self.assertEqual(envelope.entry_notional_usdt, D("12.50"))
        self.assertEqual(envelope.aggregate_exposure, D("40"))
        self.assertEqual(envelope.reserved_exposure, D("35"))
        self.assertEqual(envelope.max_realized_loss, D("4.5"))
        self.assertEqual(envelope.max_equity_loss, D("6"))
        self.assertEqual(envelope.execution_deviation_bps, D("75"))
        self.assertEqual(envelope.exit_order_reserve, 2)
        self.assertEqual(envelope.to_dict(), envelope.as_dict())
        self.assertEqual(envelope.binding_hash, envelope.canonical_hash)
        self.assertEqual(len(envelope.canonical_hash), 64)
        self.assertEqual(DEFAULT_BINANCE_RISK_ENVELOPE.version, BINANCE_RISK_ENVELOPE_VERSION)
        with self.assertRaises((AttributeError, TypeError)):
            envelope.entry_notional = D("99")
        with self.assertRaises(ValueError):
            BinanceRiskEnvelope(quote_asset="BTC")
        with self.assertRaises(ValueError):
            BinanceRiskEnvelope(entry_notional="-1")
        with self.assertRaises(ValueError):
            BinanceRiskEnvelope(max_positions="1.5")


class SymbolRulesTests(unittest.TestCase):
    def test_exchange_info_parses_every_supported_filter_and_preserves_unknowns(self):
        info = {
            "symbol": "btcusdt",
            "baseAsset": "BTC",
            "quoteAsset": "usdt",
            "status": "trading",
            "isSpotTradingAllowed": True,
            "orderTypes": ["limit", "market"],
            "timeInForce": ["IOC", "FOK"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "50", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.01", "maxQty": "25", "stepSize": "0.01"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "1", "applyToMarket": True},
                {"filterType": "NOTIONAL", "minNotional": "2", "maxNotional": "90", "applyMinToMarket": False, "applyMaxToMarket": True},
                {"filterType": "PERCENT_PRICE", "multiplierUp": "1.2", "multiplierDown": "0.8", "avgPriceMins": 5},
                {"filterType": "PERCENT_PRICE_BY_SIDE", "bidMultiplierUp": "1.1", "bidMultiplierDown": "0.9", "askMultiplierUp": "1.3", "askMultiplierDown": "0.7", "avgPriceMins": 3},
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 8},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 7},
                {"filterType": "MAX_NUM_ICEBERG_ORDERS", "maxNumIcebergOrders": 6},
                {"filterType": "EXCHANGE_MAX_NUM_ORDERS", "maxNumOrders": 20},
                {"filterType": "EXCHANGE_MAX_ALGO_ORDERS", "maxNumOrders": 19},
                {"filterType": "MAX_POSITION", "maxPosition": "4.5"},
                {"filterType": "TRAILING_DELTA", "minTrailingAboveDelta": 10},
            ],
        }
        rules = SymbolRules.from_exchange_info(info)
        self.assertEqual(rules.symbol, "BTCUSDT")
        self.assertEqual(rules.base_asset, "BTC")
        self.assertEqual(rules.quote_asset, "USDT")
        self.assertEqual(rules.status, "TRADING")
        self.assertEqual(rules.order_types, ("LIMIT", "MARKET"))
        self.assertEqual(rules.time_in_force, ("IOC", "FOK"))
        for name, expected in {
            "min_price": "0.01", "max_price": "100", "tick_size": "0.01",
            "min_qty": "0.001", "max_qty": "50", "step_size": "0.001",
            "market_min_qty": "0.01", "market_max_qty": "25", "market_step_size": "0.01",
            "min_notional": "2", "max_notional": "90", "percent_multiplier_up": "1.2",
            "percent_multiplier_down": "0.8", "bid_multiplier_up": "1.1", "bid_multiplier_down": "0.9",
            "ask_multiplier_up": "1.3", "ask_multiplier_down": "0.7", "max_position": "4.5",
        }.items():
            self.assertEqual(getattr(rules, name), D(expected), name)
        self.assertTrue(rules.min_notional_apply_to_market is False)
        self.assertTrue(rules.max_notional_apply_to_market)
        self.assertEqual(rules.percent_avg_price_mins, 5)
        self.assertEqual(rules.side_percent_avg_price_mins, 3)
        self.assertEqual(rules.max_num_orders, 8)
        self.assertEqual(rules.max_num_algo_orders, 7)
        self.assertEqual(rules.max_num_iceberg_orders, 6)
        self.assertEqual(rules.exchange_max_num_orders, 20)
        self.assertEqual(rules.exchange_max_num_algo_orders, 19)
        self.assertEqual(rules.unknown_filters["TRAILING_DELTA"]["minTrailingAboveDelta"], 10)
        self.assertEqual(len(rules.raw_filters), len(info["filters"]))
        self.assertEqual(SymbolRules.parse(info), rules)
        self.assertEqual(SymbolRules.from_exchange_symbol(info), rules)
        with self.assertRaises(TypeError):
            rules.unknown_filters["NEW"] = {}
        with self.assertRaises(ValueError):
            SymbolRules.from_exchange_info({"filters": []})

    def test_side_rounding_market_rounding_and_zero_disabled_tick_step(self):
        rules = make_rules(tick_size=D("0.10"), step_size=D("0.10"), market_step_size=D("0.25"), max_qty=D("3"), market_max_qty=D("4"))
        self.assertEqual(rules.round_price("1.21", "BUY"), D("1.30"))
        self.assertEqual(rules.round_price("1.29", "SELL"), D("1.20"))
        self.assertEqual(rules.round_quantity("2.39", "BUY"), D("2.30"))
        self.assertEqual(rules.round_quantity("9", "SELL", available="2.34"), D("2.30"))
        self.assertEqual(rules.round_quantity("3.99", "BUY", market=True), D("3.75"))
        self.assertEqual(rules.round_order("BUY", "1.21", "2.39"), (D("1.30"), D("2.30")))

        disabled = SymbolRules.from_exchange_info({
            "symbol": "ETHUSDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0", "maxPrice": "0", "tickSize": "0"},
                {"filterType": "LOT_SIZE", "minQty": "0", "maxQty": "0", "stepSize": "0"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "0", "stepSize": "0"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "0", "applyToMarket": False},
                {"filterType": "NOTIONAL", "minNotional": "0", "maxNotional": "0", "applyMinToMarket": False, "applyMaxToMarket": False},
            ],
        })
        self.assertEqual(disabled.round_price("1.234", "BUY"), D("1.234"))
        self.assertEqual(disabled.round_quantity("2.345", "BUY"), D("2.345"))
        result = assess_order(disabled, "BUY", "1.234", "2.345", snapshot=make_snapshot(quote_available=D("100")), envelope=BinanceRiskEnvelope(entry_notional=D("100"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100")))
        self.assertTrue(result.allowed, result.reasons)
        self.assertEqual(result.price, D("1.234"))
        self.assertEqual(result.quantity, D("2.345"))

    def test_filter_reasons_cover_price_quantity_notional_percent_and_position_limits(self):
        generous = BinanceRiskEnvelope(entry_notional=D("10000"), max_aggregate_exposure=D("10000"), max_reserved_exposure=D("10000"), max_positions=50)
        state = make_snapshot(quote_available=D("10000"), owned_inventory={"BTCUSDT": D("2")}, positions=1)

        below = assess_order(make_rules(min_price=D("10"), tick_size=D("1")), "BUY", "9", "1", snapshot=state, envelope=generous)
        self.assertIn("PRICE_BELOW_MINIMUM", below.reasons)
        above = assess_order(make_rules(max_price=D("20"), tick_size=D("1")), "BUY", "21", "1", snapshot=state, envelope=generous)
        self.assertIn("PRICE_ABOVE_MAXIMUM", above.reasons)
        non_tick = assess_order(make_rules(min_price=D("11.5"), tick_size=D("1")), "BUY", "10.5", "1", snapshot=state, envelope=generous)
        self.assertIn("PRICE_TICK", non_tick.reasons)
        self.assertIn("PRICE_BELOW_MINIMUM", non_tick.reasons)

        qty_low = assess_order(make_rules(min_qty=D("2"), step_size=D("1")), "BUY", "10", "1", snapshot=state, envelope=generous)
        self.assertIn("QUANTITY_BELOW_MINIMUM", qty_low.reasons)
        qty_capped = assess_order(make_rules(max_qty=D("2"), step_size=D("1")), "BUY", "10", "3", snapshot=state, envelope=generous)
        self.assertEqual(qty_capped.quantity, D("2"))
        self.assertNotIn("QUANTITY_ABOVE_MAXIMUM", qty_capped.reasons)
        step_capped = assess_order(make_rules(step_size=D("1")), "BUY", "10", "2.9", snapshot=state, envelope=generous)
        self.assertEqual(step_capped.quantity, D("2"))
        self.assertNotIn("QUANTITY_STEP", step_capped.reasons)

        min_n = assess_order(make_rules(min_notional=D("20"), max_notional=D("100")), "BUY", "10", "1", snapshot=state, envelope=generous)
        self.assertIn("MIN_NOTIONAL", min_n.reasons)
        max_n = assess_order(make_rules(min_notional=D("1"), max_notional=D("20")), "BUY", "10", "3", snapshot=state, envelope=generous)
        self.assertIn("MAX_NOTIONAL", max_n.reasons)

        percent_rules = make_rules(percent_multiplier_up=D("1.1"), percent_multiplier_down=D("0.9"), bid_multiplier_up=D("1.05"), bid_multiplier_down=D("0.95"))
        high = assess_order(percent_rules, "BUY", "12", "1", market={"average_price": "10"}, snapshot=state, envelope=generous)
        self.assertIn("PERCENT_PRICE_ABOVE_MAX", high.reasons)
        self.assertIn("PERCENT_PRICE_BY_SIDE_ABOVE_MAX", high.reasons)
        low = assess_order(percent_rules, "BUY", "8", "1", market={"average_price": "10"}, snapshot=state, envelope=generous)
        self.assertIn("PERCENT_PRICE_BELOW_MIN", low.reasons)
        self.assertIn("PERCENT_PRICE_BY_SIDE_BELOW_MIN", low.reasons)
        unavailable = assess_order(percent_rules, "BUY", "10", "1", snapshot=state, envelope=generous)
        self.assertIn("FILTER_REFERENCE_UNAVAILABLE", unavailable.reasons)

        max_position = assess_order(make_rules(max_position=D("2")), "BUY", "10", "1", snapshot=state, envelope=generous)
        self.assertIn("MAX_POSITION", max_position.reasons)

    def test_order_type_time_in_force_status_and_spot_filters(self):
        state = make_snapshot(quote_available=D("100"))
        policy = make_rules(order_types=("LIMIT",), time_in_force=("IOC",))
        market = assess_order(policy, "BUY", "10", "1", order_type="MARKET", time_in_force="IOC", snapshot=state)
        self.assertIn("ORDER_TYPE_NOT_ALLOWED", market.reasons)
        bad_tif = assess_order(policy, "BUY", "10", "1", time_in_force="GTC", snapshot=state)
        self.assertIn("TIME_IN_FORCE_NOT_ALLOWED", bad_tif.reasons)
        self.assertIn("NOT_TRADING", assess_order(make_rules(status="HALT"), "BUY", "10", "1", snapshot=state).reasons)
        self.assertIn("SPOT_NOT_ALLOWED", assess_order(make_rules(spot_trading_allowed=False), "BUY", "10", "1", snapshot=state).reasons)
        self.assertIn("INVALID_SIDE", assess_order(make_rules(), "HOLD", "10", "1", snapshot=state).reasons)


class PendingAndSnapshotTests(unittest.TestCase):
    def test_pending_and_unknown_orders_remain_reserved_until_reconciled(self):
        unknown_buy = PendingOrder("btcusdt", "buy", "2", price="5", status="UNKNOWN", fee_reserve="0.10", order_id="u1")
        unknown_sell = PendingOrder("ETHUSDT", "SELL", "1", price="10", status="ACK_UNKNOWN", fee_reserve="0.20")
        pending_sell = PendingOrder("ETHUSDT", "SELL", "1", price="10", status="PARTIALLY_FILLED", fee_reserve="0.20")
        filled = PendingOrder("ETHUSDT", "SELL", "1", price="10", status="FILLED", fee_reserve="0.20")
        self.assertTrue(unknown_buy.unresolved)
        self.assertTrue(unknown_sell.unresolved)
        self.assertTrue(pending_sell.unresolved)
        self.assertFalse(filled.unresolved)
        self.assertEqual(unknown_buy.notional, D("10"))
        self.assertEqual(unknown_buy.reservation, D("10.10"))
        self.assertEqual(unknown_sell.reservation, D("10.20"))
        self.assertEqual(pending_sell.reservation, D("0"))
        state = make_snapshot(pending_orders=(unknown_buy, unknown_sell, pending_sell, filled))
        self.assertEqual(state.pending_reservation, D("20.30"))
        self.assertEqual(state.unresolved_exposure, D("20.30"))
        blocked = assess_entry(state, BinanceRiskEnvelope(max_reserved_exposure=D("20"), entry_notional=D("100"), max_aggregate_exposure=D("100")), requested_notional="0")
        self.assertFalse(blocked.allowed)
        self.assertIn("RESERVED_EXPOSURE", blocked.reasons)

    def test_from_account_normalizes_balances_and_rollover_resets_only_daily_fields(self):
        pending = {"symbol": "BTCUSDT", "side": "BUY", "origQty": "1", "price": "10", "status": "UNKNOWN", "fee": "0.1"}
        account = {
            "free_quote": "99.50",
            "inventory": [{"asset": "btcusdt", "free": "1.25"}, {"asset": "USDT", "free": "99.50"}],
            "aggregate_exposure": "7",
            "reserved_exposure": "2",
            "pending_orders": [pending],
            "positions": 1,
            "entry_count_today": 4,
            "exit_count_today": 2,
            "daily_realized_loss": "3",
            "daily_equity_loss": "1",
            "unrealized_pnl": "-0.5",
            "fees_today": "0.2",
            "open_order_count": 2,
            "account_order_count": 3,
            "exchange_order_count": 4,
        }
        state = RiskSnapshot.from_account(account, now=T0)
        self.assertIs(BinanceRiskSnapshot, RiskSnapshot)
        self.assertEqual(state.quote_available, D("99.50"))
        self.assertEqual(state.owned_inventory["BTCUSDT"], D("1.25"))
        self.assertEqual(state.entry_submissions_today, 4)
        self.assertEqual(state.exit_submissions_today, 2)
        self.assertEqual(state.submissions_today, 6)
        self.assertEqual(state.owned_positions, 1)
        self.assertEqual(state.utc_day.isoformat(), "2026-01-02")
        self.assertEqual(state.unresolved_exposure, D("12.10"))
        self.assertEqual(state.to_dict()["unresolved_exposure"], "12.10")

        next_day = T0 + timedelta(seconds=40)
        rolled = state.project(next_day)
        self.assertIsNot(rolled, state)
        self.assertEqual(rolled.observed_at, next_day)
        self.assertEqual(rolled.entry_submissions_today, 0)
        self.assertEqual(rolled.exit_submissions_today, 0)
        self.assertEqual(rolled.realized_loss_today, D("0"))
        self.assertEqual(rolled.equity_loss_today, D("0"))
        self.assertEqual(rolled.fees_today, D("0"))
        self.assertEqual(rolled.quote_available, state.quote_available)
        self.assertEqual(rolled.aggregate_exposure, state.aggregate_exposure)
        self.assertEqual(rolled.reserved_exposure, state.reserved_exposure)
        self.assertEqual(rolled.pending_reservation, state.pending_reservation)
        self.assertEqual(rolled.owned_inventory, state.owned_inventory)
        self.assertEqual(rolled.open_orders, state.open_orders)
        self.assertIs(state.project(T0), state)


class OrderSizingAndProjectionTests(unittest.TestCase):
    def test_size_limit_order_floors_buy_after_ceiling_price_and_accounts_for_fee(self):
        envelope = BinanceRiskEnvelope(entry_notional=D("10"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100"))
        sized = size_limit_order(make_rules(tick_size=D("0.01"), step_size=D("0.001")), "BUY", "3.333", "10", envelope=envelope, fee_bps="10")
        self.assertIsInstance(sized, SizedOrder)
        self.assertTrue(sized.valid, sized.reasons)
        self.assertEqual(sized.side, "BUY")
        self.assertEqual(sized.price, D("3.34"))
        self.assertEqual(sized.quantity, D("2.991"))
        self.assertEqual(sized.notional, D("9.98994"))
        self.assertEqual(sized.fee_reserve, D("0.00998994"))
        self.assertLessEqual(sized.notional + sized.fee_reserve, envelope.entry_notional)
        self.assertTrue(sized.allowed)
        self.assertEqual(sized.skip_reasons, ())

        assessed = assess_order(make_rules(tick_size=D("0.01"), step_size=D("0.001")), "BUY", "3.333", "10", snapshot=make_snapshot(quote_available=D("100"), aggregate_exposure=D("2"), reserved_exposure=D("1")), envelope=envelope, fee_bps="10")
        self.assertEqual(assessed.price, D("3.34"))
        self.assertEqual(assessed.quantity, D("2.991"))
        self.assertEqual(assessed.projected_exposure, D("11.98994"))
        self.assertEqual(assessed.projected_reserved, D("10.99992994"))
        self.assertEqual(assessed.projection["projected_exposure"], "11.98994")

    def test_sell_sizing_floors_owned_inventory_and_rejects_dust(self):
        rules = make_rules(step_size=D("0.01"), min_qty=D("0.05"), min_notional=D("1"))
        dust = size_limit_order(rules, "SELL", "10.009", "0.049", available_inventory="0.05")
        self.assertFalse(dust.valid)
        self.assertEqual(dust.price, D("10.00"))
        self.assertEqual(dust.quantity, D("0.04"))
        self.assertIn("EXIT_BELOW_MINIMUM", dust.reasons)
        self.assertIn("QUANTITY_BELOW_MINIMUM", dust.reasons)
        self.assertEqual(dust.reason, "; ".join(dust.reasons))

        zero = size_limit_order(rules, "SELL", "10", "0", available_inventory="0")
        self.assertFalse(zero.valid)
        self.assertIn("DUST", zero.reasons)
        self.assertIn("EXIT_BELOW_MINIMUM", zero.reasons)
        self.assertEqual(zero.quantity, D("0"))

    def test_risk_assessment_properties_and_facade_aliases(self):
        envelope = BinanceRiskEnvelope(entry_notional=D("100"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100"))
        result = assess_order(make_rules(), "BUY", "10", "1", snapshot=make_snapshot(quote_available=D("100")), envelope=envelope)
        self.assertIsInstance(result, RiskAssessment)
        self.assertIs(BinanceRiskAssessment, RiskAssessment)
        self.assertTrue(bool(result))
        self.assertTrue(result.ok)
        self.assertEqual(result.skip_reasons, result.reasons)
        self.assertEqual(result.reason, "")
        self.assertEqual(result.order["symbol"], "BTCUSDT")
        self.assertEqual(result.order["quantity"], D("1"))
        self.assertEqual(result.to_dict()["notional"], "10.00")
        facade = BinanceRisk(envelope)
        self.assertIs(RiskEngine, BinanceRisk)
        self.assertEqual(facade.assess(make_rules(), "BUY", "10", "1", snapshot=make_snapshot(quote_available=D("100"))), result)
        self.assertEqual(facade.assess_order(make_rules(), "BUY", "10", "1", snapshot=make_snapshot(quote_available=D("100"))), result)


class BudgetAssessmentTests(unittest.TestCase):
    def test_entry_budget_reasons_include_quote_cap_exposure_loss_position_and_pause_limits(self):
        envelope = BinanceRiskEnvelope(
            entry_notional=D("10"),
            max_aggregate_exposure=D("10"),
            max_reserved_exposure=D("10"),
            realized_loss_entry_stop=D("5"),
            equity_loss_entry_stop=D("5"),
            max_positions=1,
            max_submissions_per_day=1,
        )
        state = make_snapshot(
            quote_available=D("9"),
            aggregate_exposure=D("9"),
            reserved_exposure=D("9"),
            positions=1,
            entry_submissions_today=1,
            realized_loss_today=D("5"),
            equity_loss_today=D("3"),
            unrealized_pnl=D("-3"),
            fees_today=D("2"),
            account_paused=True,
            rate_limited=True,
        )
        result = assess_entry(state, envelope, requested_notional="10", reserved_fee="0.1")
        self.assertFalse(result.allowed)
        for reason in ("INSUFFICIENT_QUOTE", "ENTRY_CAP_EXCEEDED", "AGGREGATE_EXPOSURE", "RESERVED_EXPOSURE", "MAX_POSITIONS", "DAILY_REALIZED_LOSS", "DAILY_EQUITY_LOSS", "SUBMISSIONS_PER_DAY", "ACCOUNT_PAUSED", "RATE_LIMIT"):
            self.assertIn(reason, result.reasons)
        self.assertEqual(result.projected_exposure, D("19"))
        self.assertEqual(result.projected_reserved, D("19.1"))

    def test_order_limits_and_exit_reserve_keep_capacity_for_owned_positions(self):
        envelope = BinanceRiskEnvelope(entry_notional=D("100"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100"), max_positions=5, exit_order_reserve_per_position=1)
        rules = make_rules(max_num_orders=2, exchange_max_num_orders=3)
        state = make_snapshot(open_orders=2, account_order_count=1, exchange_order_count=3, positions=1, quote_available=D("100"))
        result = assess_order(rules, "BUY", "10", "1", snapshot=state, envelope=envelope)
        self.assertIn("MAX_NUM_ORDERS", result.reasons)
        self.assertIn("EXCHANGE_MAX_NUM_ORDERS", result.reasons)
        self.assertIn("EXIT_ORDER_RESERVE", result.reasons)

        no_reserve_for_exit = assess_order(rules, "SELL", "10", "1", snapshot=make_snapshot(owned_inventory={"BTCUSDT": D("1")}, positions=1, open_orders=0, account_order_count=0, exchange_order_count=0), envelope=envelope)
        self.assertNotIn("EXIT_ORDER_RESERVE", no_reserve_for_exit.reasons)

    def test_entry_projection_uses_usdt_notional_and_fee_without_float_math(self):
        envelope = BinanceRiskEnvelope(entry_notional=D("100"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100"))
        state = make_snapshot(quote_available=D("50"), aggregate_exposure=D("4"), reserved_exposure=D("2"))
        result = assess_entry(state, envelope, requested_notional="3.25", reserved_fee="0.0325")
        self.assertTrue(result.allowed, result.reasons)
        self.assertEqual(result.side, "BUY")
        self.assertEqual(result.notional, D("3.25"))
        self.assertEqual(result.fee_reserve, D("0.0325"))
        self.assertEqual(result.projected_exposure, D("7.25"))
        self.assertEqual(result.projected_reserved, D("5.2825"))


class ExitAssessmentTests(unittest.TestCase):
    def test_exit_reduces_exposure_and_is_not_blocked_by_loss_stops_or_entry_cap(self):
        envelope = BinanceRiskEnvelope(
            entry_notional=D("10"),
            max_aggregate_exposure=D("20"),
            max_reserved_exposure=D("20"),
            realized_loss_entry_stop=D("1"),
            equity_loss_entry_stop=D("1"),
            max_submissions_per_day=2,
        )
        state = make_snapshot(
            aggregate_exposure=D("40"),
            reserved_exposure=D("3"),
            owned_inventory={"BTCUSDT": D("3")},
            positions=1,
            realized_loss_today=D("10"),
            equity_loss_today=D("10"),
            unrealized_pnl=D("4"),
            fees_today=D("10"),
            pending_orders=(PendingOrder("BTCUSDT", "BUY", "1", price="5", status="UNKNOWN"),),
        )
        result = assess_exit(state, envelope, requested_notional="50", reserved_fee="0.5", symbol="btcusdt", quantity="2", minimum_notional="1")
        self.assertTrue(result.allowed, result.reasons)
        self.assertEqual(result.side, "SELL")
        self.assertEqual(result.symbol, "BTCUSDT")
        self.assertEqual(result.quantity, D("2"))
        self.assertEqual(result.notional, D("50"))
        self.assertEqual(result.fee_reserve, D("0.5"))
        self.assertEqual(result.projected_exposure, D("-10"))
        self.assertEqual(result.projected_reserved, D("8"))
        self.assertNotIn("ENTRY_CAP_EXCEEDED", result.reasons)
        self.assertNotIn("DAILY_REALIZED_LOSS", result.reasons)
        self.assertNotIn("DAILY_EQUITY_LOSS", result.reasons)

    def test_exit_rejects_foreign_inventory_oversell_dust_and_below_minimum(self):
        envelope = BinanceRiskEnvelope(max_submissions_per_day=5)
        state = make_snapshot(owned_inventory={"BTCUSDT": D("1")}, positions=1)
        foreign = assess_exit(state, envelope, requested_notional="10", symbol="ETHUSDT", quantity="1")
        self.assertFalse(foreign.allowed)
        self.assertIn("FOREIGN_INVENTORY", foreign.reasons)
        oversell = assess_exit(state, envelope, requested_notional="10", symbol="BTCUSDT", quantity="1.1")
        self.assertFalse(oversell.allowed)
        self.assertIn("INSUFFICIENT_OWNED_INVENTORY", oversell.reasons)
        dust = assess_exit(state, envelope, requested_notional="0", symbol="BTCUSDT", quantity="0")
        self.assertFalse(dust.allowed)
        self.assertIn("DUST", dust.reasons)
        self.assertIn("EXIT_BELOW_MINIMUM", dust.reasons)
        below_min = assess_exit(state, envelope, requested_notional="0.5", symbol="BTCUSDT", quantity="0.05", minimum_notional="1")
        self.assertFalse(below_min.allowed)
        self.assertIn("EXIT_BELOW_MINIMUM", below_min.reasons)

    def test_exit_submission_and_account_pause_rate_limit_reasons_are_observable(self):
        envelope = BinanceRiskEnvelope(max_submissions_per_day=1)
        state = make_snapshot(
            owned_inventory={"BTCUSDT": D("1")},
            positions=1,
            exit_submissions_today=1,
            account_paused=True,
            rate_limited=True,
        )
        result = assess_exit(state, envelope, requested_notional="5", symbol="BTCUSDT", quantity="0.5")
        self.assertFalse(result.allowed)
        self.assertIn("SUBMISSIONS_PER_DAY", result.reasons)
        self.assertIn("ACCOUNT_PAUSED", result.reasons)
        self.assertIn("RATE_LIMIT", result.reasons)

    def test_market_stale_thin_wide_crash_account_and_rate_reasons(self):
        envelope = BinanceRiskEnvelope(entry_notional=D("100"), max_aggregate_exposure=D("100"), max_reserved_exposure=D("100"), max_execution_deviation_bps=D("100"))
        market = {
            "status": "HALT",
            "isSpotTradingAllowed": False,
            "account_paused": True,
            "rate_limited": True,
            "crash": True,
            "depth_ok": False,
            "thin_book": True,
            "depth_available": "1",
            "min_depth": "2",
            "spread_bps": "20",
            "max_spread_bps": "5",
            "fresh": False,
            "observed_at": T0 - timedelta(seconds=30),
            "max_age_seconds": "1",
            "average_price": "10",
            "conservative_fill_price": "11.10",
        }
        result = assess_order(make_rules(), "BUY", "10", "1", market=market, snapshot=make_snapshot(quote_available=D("100")), envelope=envelope, now=T0)
        for reason in ("NOT_TRADING", "SPOT_NOT_ALLOWED", "ACCOUNT_PAUSED", "RATE_LIMIT", "CRASH", "THIN_BOOK", "WIDE_SPREAD", "STALE_MARKET", "EXECUTION_DEVIATION"):
            self.assertIn(reason, result.reasons)


if __name__ == "__main__":
    unittest.main()
