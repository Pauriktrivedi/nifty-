import logging
import os
from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)

class BreakoutRangeStrategy(BaseStrategy):
    def __init__(self, mode='paper', paper_trader=None, live_trader=None, risk_manager=None, symbol=None, range_high=None, range_low=None, quantity=100, strategy_id="breakout_range"):
        super().__init__(name="BreakoutRange", strategy_id=strategy_id, mode=mode, paper_trader=paper_trader, live_trader=live_trader, risk_manager=risk_manager)
        self.symbol = symbol or "nse_cm|Nifty 50" # Default testing symbol
        self.range_high = range_high
        self.range_low = range_low
        self.quantity = quantity
        self.position = 0 # 0 for flat, 1 for long, -1 for short
        self.has_traded = False
        self.reference_price = None
        self.dynamic_band_points = float(os.getenv("BREAKOUT_TEST_BUFFER_POINTS", "2"))

        self.dynamic_band = self.range_high is None or self.range_low is None
        if not self.dynamic_band:
            self.reference_price = (self.range_high + self.range_low) / 2.0

        logger.info(
            "Initialized BreakoutRangeStrategy for %s. Range: [%s, %s] (dynamic=%s)",
            self.symbol,
            self.range_low,
            self.range_high,
            self.dynamic_band,
        )

    def on_tick(self, tick_data: dict):
        if self.has_traded:
            return # Only take one trade per day for this simple example

        symbol = tick_data.get('symbol')
        if symbol != self.symbol:
            return

        ltp = tick_data.get('last_traded_price') or tick_data.get('ltp')
        if not ltp:
            return
        ltp = float(ltp)

        if self.dynamic_band and self.reference_price is None:
            self.reference_price = ltp
            self.range_high = round(ltp + self.dynamic_band_points, 2)
            self.range_low = round(max(0.01, ltp - self.dynamic_band_points), 2)
            logger.info(
                "[%s] armed around first tick %.2f. Breakout band = [%.2f, %.2f]",
                self.name,
                ltp,
                self.range_low,
                self.range_high,
            )
            return

        logger.debug(f"{self.symbol} LTP: {ltp} | Range: [{self.range_low}, {self.range_high}]")

        # Breakout to the upside
        if ltp > self.range_high and self.position == 0:
            logger.info(f"Upside breakout detected for {self.symbol} at {ltp}. Placing BUY order.")
            order_id = self.place_order(
                symbol=self.symbol,
                trading_symbol=tick_data.get("trading_symbol", self.symbol),
                qty=self.quantity,
                side="BUY",
                exchange_seg=tick_data.get("exchange_seg", "nse_cm"),
                order_type="LIMIT",
                price=round(ltp + 1.0, 2),
                instrument_type=tick_data.get("instrument_type", "EQ")
            )
            if order_id:
                self.position = 1
                self.has_traded = True

        # Breakout to the downside
        elif ltp < self.range_low and self.position == 0:
            logger.info(f"Downside breakout detected for {self.symbol} at {ltp}. Placing SELL order.")
            order_id = self.place_order(
                symbol=self.symbol,
                trading_symbol=tick_data.get("trading_symbol", self.symbol),
                qty=self.quantity,
                side="SELL",
                exchange_seg=tick_data.get("exchange_seg", "nse_cm"),
                order_type="LIMIT",
                price=round(max(0.01, ltp - 1.0), 2),
                instrument_type=tick_data.get("instrument_type", "EQ")
            )
            if order_id:
                self.position = -1
                self.has_traded = True

    def on_signal(self, signal_data: dict):
        pass

    def on_order_fill(self, trade_data: dict):
        logger.info(f"Breakout strategy trade filled: {trade_data}")
