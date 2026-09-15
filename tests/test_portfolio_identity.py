from __future__ import annotations

from datetime import datetime, timezone
import unittest

from axiom.domain import Fill, MarketType, ResolvedContract, SettlementState, Side
from axiom.portfolio import OrderRequest, Portfolio


T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


def prediction_fill(
    symbol: str,
    side: Side,
    quantity: float,
    price: float,
    *,
    order_id: str,
    market_id: str | None = None,
    outcome: str | None = "yes",
) -> Fill:
    return Fill(
        timestamp=T0,
        market_type=MarketType.PREDICTION,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        fees=0.0,
        slippage=0.0,
        strategy_id="identity-test",
        order_id=order_id,
        market_id=market_id,
        metadata={} if outcome is None else {"outcome": outcome},
    )


class PortfolioMarketIdentityTests(unittest.TestCase):
    def test_prediction_sell_uses_only_requested_market_inventory(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("shared-symbol", Side.BUY, 2.0, 0.20, order_id="buy-a", market_id="market-a")
        )
        portfolio.apply_fill(
            prediction_fill("shared-symbol", Side.BUY, 0.75, 0.30, order_id="buy-b", market_id="market-b")
        )

        sell = portfolio.execute_order(
            OrderRequest(
                "shared-symbol",
                Side.SELL,
                2.0,
                market_type=MarketType.PREDICTION,
                market_id=" market-b ",
                outcome="YES",
            ),
            timestamp=T0,
            price=0.40,
            order_id="sell-b",
        )

        self.assertIsNotNone(sell)
        assert sell is not None
        self.assertEqual(sell.quantity, 0.75)
        self.assertEqual(
            portfolio.get_position("shared-symbol", outcome="yes", market_id="market-a").quantity,  # type: ignore[union-attr]
            2.0,
        )
        self.assertEqual(
            portfolio.get_position("shared-symbol", outcome="yes", market_id="market-b").quantity,  # type: ignore[union-attr]
            0.0,
        )

    def test_prediction_mark_and_exposure_use_bare_market_id(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("token-a", Side.BUY, 2.0, 0.20, order_id="mark-buy-a", market_id="market-a")
        )
        portfolio.apply_fill(
            prediction_fill("token-b", Side.BUY, 3.0, 0.30, order_id="mark-buy-b", market_id="market-b")
        )
        prices = {"market-a": 0.45, "market-b": 0.55}
        portfolio.mark(prices)


        self.assertAlmostEqual(
            portfolio.get_position("token-a", outcome="yes", market_id="market-a").last_price,  # type: ignore[union-attr]
            0.45,
        )
        self.assertAlmostEqual(
            portfolio.get_position("token-b", outcome="yes", market_id="market-b").last_price,  # type: ignore[union-attr]
            0.55,
        )
        self.assertAlmostEqual(portfolio.equity(), 101.25)
        self.assertAlmostEqual(portfolio.gross_exposure(prices), 2.55)

    def test_prediction_no_market_id_reuses_unique_existing_market(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("token", Side.BUY, 1.0, 0.20, order_id="explicit-buy", market_id="market-a")
        )

        portfolio.apply_fill(prediction_fill("token", Side.BUY, 0.5, 0.25, order_id="legacy-buy"))
        sell = portfolio.execute_order(
            OrderRequest("token", Side.SELL, 0.5, market_type=MarketType.PREDICTION, outcome="yes"),
            timestamp=T0,
            price=0.40,
            order_id="legacy-sell",
        )

        self.assertIsNotNone(sell)
        self.assertEqual(sell.market_id, "market-a")
        self.assertEqual(set(portfolio.positions), {"market-a|yes"})
        self.assertAlmostEqual(portfolio.positions["market-a|yes"].quantity, 1.0)

    def test_prediction_no_market_id_deduplicates_market_across_outcomes(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("token", Side.BUY, 1.0, 0.20, order_id="yes-buy", market_id="market-a")
        )
        portfolio.apply_fill(
            prediction_fill(
                "token",
                Side.BUY,
                1.0,
                0.30,
                order_id="no-buy",
                market_id="market-a",
                outcome="no",
            )
        )

        fill = portfolio.execute_order(
            OrderRequest("token", Side.BUY, 0.5, market_type=MarketType.PREDICTION),
            timestamp=T0,
            price=0.40,
            order_id="canonical-market-buy",
        )

        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill.market_id, "market-a")
        self.assertEqual(portfolio.positions["market-a|yes"].quantity, 1.5)
        self.assertEqual(portfolio.positions["market-a|no"].quantity, 1.0)

    def test_prediction_no_market_id_filters_candidates_by_requested_outcome(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("token", Side.BUY, 1.0, 0.20, order_id="yes-buy", market_id="market-a")
        )
        portfolio.apply_fill(
            prediction_fill(
                "token",
                Side.BUY,
                1.0,
                0.30,
                order_id="no-buy",
                market_id="market-b",
                outcome="no",
            )
        )

        fill = portfolio.execute_order(
            OrderRequest(
                "token",
                Side.BUY,
                0.5,
                market_type=MarketType.PREDICTION,
                outcome="YES",
            ),
            timestamp=T0,
            price=0.40,
            order_id="outcome-specific-market-buy",
        )

        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill.market_id, "market-a")

    def test_prediction_no_market_id_buy_respects_settlement_of_unique_market(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill(
                "token",
                Side.BUY,
                1.0,
                0.20,
                order_id="settled-buy",
                market_id="market-a",
                outcome="no",
            )
        )
        portfolio.resolve(
            ResolvedContract("market-a", SettlementState.RESOLVED_NO, T0, "identity regression")
        )

        with self.assertRaises(ValueError):
            portfolio.submit_order(
                OrderRequest("token", Side.BUY, 0.5, market_type=MarketType.PREDICTION)
            )
        with self.assertRaises(ValueError):
            portfolio.apply_fill(
                prediction_fill("token", Side.BUY, 0.5, 0.25, order_id="reopen-buy", outcome=None)
            )

    def test_prediction_no_market_id_fails_closed_when_symbol_is_ambiguous(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(
            prediction_fill("token", Side.BUY, 1.0, 0.20, order_id="ambiguous-buy-a", market_id="market-a")
        )
        portfolio.apply_fill(
            prediction_fill(
                "token",
                Side.BUY,
                1.0,
                0.30,
                order_id="ambiguous-buy-b",
                market_id="market-b",
                outcome="no",
            )
        )
        with self.assertRaises(ValueError):
            portfolio.submit_order(
                OrderRequest("token", Side.BUY, 0.5, market_type=MarketType.PREDICTION)
            )
        with self.assertRaises(ValueError):
            portfolio.apply_fill(
                prediction_fill("token", Side.BUY, 0.5, 0.25, order_id="ambiguous-legacy-buy", outcome=None)
            )

    def test_prediction_sell_without_market_id_keeps_symbol_fallback(self) -> None:
        portfolio = Portfolio(100.0)
        portfolio.apply_fill(prediction_fill("legacy-symbol", Side.BUY, 1.25, 0.20, order_id="legacy-buy"))

        sell = portfolio.execute_order(
            OrderRequest(
                "legacy-symbol",
                Side.SELL,
                2.0,
                market_type=MarketType.PREDICTION,
                outcome="yes",
            ),
            timestamp=T0,
            price=0.40,
            order_id="legacy-sell",
        )

        self.assertIsNotNone(sell)
        assert sell is not None
        self.assertEqual(sell.quantity, 1.25)
        self.assertEqual(sell.market_id, "legacy-symbol")
        self.assertEqual(portfolio.get_position("legacy-symbol", outcome="yes").quantity, 0.0)  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
