import logging
from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class PaperTestStrategy(BaseStrategy):
    """
    One-shot test strategy used to verify that a strategy can place an order
    and receive a fill in paper mode.
    """

    def __init__(
        self,
        mode="paper",
        paper_trader=None,
        live_trader=None,
        risk_manager=None,
        symbol="nse_cm|Nifty 50",
        quantity=1,
        strategy_id="paper_test",
    ):
        super().__init__(
            name="PaperTestStrategy",
            strategy_id=strategy_id,
            mode=mode,
            paper_trader=paper_trader,
            live_trader=live_trader,
            risk_manager=risk_manager,
        )
        self.symbol = symbol
        self.quantity = quantity
        self.trade_executed = False

    def on_tick(self, tick_data: dict):
        if self.trade_executed:
            return

        symbol = tick_data.get("symbol") or tick_data.get("raw_symbol") or ""
        if symbol != self.symbol:
            return

        ltp = float(tick_data.get("last_traded_price") or tick_data.get("ltp") or 0.0)
        if ltp <= 0:
            return

        logger.info(
            "[%s] placing known paper-test BUY on %s at market reference %.2f",
            self.name,
            self.symbol,
            ltp,
        )
        order_id = self.place_order(
            symbol=self.symbol,
            trading_symbol=tick_data.get("trading_symbol", self.symbol),
            qty=self.quantity,
            side="BUY",
            exchange_seg=tick_data.get("exchange_seg", "nse_cm"),
            order_type="MARKET",
            price=ltp,
            instrument_type=tick_data.get("instrument_type", "INDEX"),
        )
        if order_id:
            self.trade_executed = True
            logger.info("[%s] paper-test order placed: %s", self.name, order_id)

    def on_signal(self, signal_data: dict):
        pass

    def on_order_fill(self, trade_data: dict):
        logger.info("[%s] fill received: %s", self.name, trade_data)
        self.trade_executed = True
