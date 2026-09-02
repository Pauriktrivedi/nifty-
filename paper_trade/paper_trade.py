import os
import time
import random
import threading
import logging
from datetime import datetime
from dotenv import load_dotenv
from database.database import SessionLocal
from database.models import Order, Trade, MarketData
from sqlalchemy import or_

logger = logging.getLogger(__name__)

class PaperTradeSimulator:
    def __init__(self, on_fill_callback=None):
        load_dotenv()
        self.virtual_cash = float(os.getenv("VIRTUAL_CASH", "500000"))
        self.starting_cash = self.virtual_cash
        self.running = False
        self.thread = None
        self.on_fill_callback = on_fill_callback

    def set_fill_callback(self, callback):
        self.on_fill_callback = callback

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._simulate_fills_loop, daemon=True)
        self.thread.start()
        logger.info("Paper Trade Simulator started in background.")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Paper Trade Simulator stopped.")

    def place_order(self, symbol, trading_symbol, qty, side, exchange_seg, order_type, price, instrument_type, strike_price, expiry_date, strategy_id=None):
        db = SessionLocal()
        try:
            order_ref = f"PAPER_{int(time.time()*1000)}"
            new_order = Order(
                symbol=symbol,
                quantity=int(qty),
                price=float(price) if price != '0' and price else None,
                order_type=order_type,
                side=side.upper(),
                status="PENDING",
                instrument_type=instrument_type,
                strike_price=strike_price,
                expiry_date=expiry_date,
                exchange_seg=exchange_seg,
                trading_symbol=trading_symbol,
                mode="paper",
                strategy_id=strategy_id,
                broker_order_id=order_ref,
                kotak_order_id=order_ref,
                timestamp=datetime.now()
            )
            db.add(new_order)
            db.commit()
            logger.info(f"Placed PAPER order {order_ref} for {side} {qty} {trading_symbol}")
            return order_ref
        except Exception as e:
            logger.error(f"Failed to place paper order: {e}")
            raise
        finally:
            db.close()

    def _simulate_fills_loop(self):
        while self.running:
            self._simulate_fills()
            time.sleep(1.0)

    def _simulate_fills(self):
        db = SessionLocal()
        try:
            pending_orders = db.query(Order).filter(Order.status == "PENDING", Order.mode == "paper").all()
            for order in pending_orders:
                # Fetch latest market data for the symbol
                latest_md = (
                    db.query(MarketData)
                    .filter(
                        or_(
                            MarketData.symbol == order.symbol,
                            MarketData.trading_symbol == order.trading_symbol,
                        )
                    )
                    .order_by(MarketData.timestamp.desc())
                    .first()
                )
                if not latest_md:
                    if order.order_type != "MARKET":
                        continue

                filled = False
                fill_price = 0.0
                slippage = 0.0

                if order.order_type == "MARKET":
                    filled = True
                    reference_price = 0.0
                    if latest_md:
                        reference_price = latest_md.ask_price if order.side == "BUY" else latest_md.bid_price
                        if reference_price <= 0:
                            reference_price = latest_md.last_traded_price
                    if reference_price <= 0:
                        reference_price = float(order.price or 0.0)
                    if reference_price <= 0:
                        continue
                    if order.side == "BUY":
                        slippage_pct = random.uniform(0.0001, 0.0005)
                        fill_price = reference_price * (1 + slippage_pct)
                    else:
                        slippage_pct = random.uniform(0.0001, 0.0005)
                        fill_price = reference_price * (1 - slippage_pct)
                    slippage = abs(fill_price - reference_price)

                elif order.order_type == "LIMIT":
                    if order.side == "BUY" and latest_md.last_traded_price <= order.price:
                        filled = True
                        fill_price = order.price
                    elif order.side == "SELL" and latest_md.last_traded_price >= order.price:
                        filled = True
                        fill_price = order.price

                if filled:
                    order.status = "FILLED"

                    new_trade = Trade(
                        order_id=order.id,
                        symbol=order.symbol,
                        quantity=order.quantity,
                        price=fill_price,
                        side=order.side,
                        slippage=slippage,
                        strategy_id=order.strategy_id,
                        timestamp=datetime.now()
                    )
                    db.add(new_trade)

                    # Update virtual cash roughly (assumes fully cash settled without margin for simplicity)
                    trade_val = order.quantity * fill_price
                    if order.side == "BUY":
                        self.virtual_cash -= trade_val
                    else:
                        self.virtual_cash += trade_val

                    db.commit()
                    logger.info(f"PAPER Fill: {order.side} {order.quantity} {order.symbol} @ {fill_price:.2f}")

                    if self.on_fill_callback:
                        try:
                            self.on_fill_callback(
                                {
                                    "order_id": order.id,
                                    "symbol": order.symbol,
                                    "quantity": order.quantity,
                                    "price": fill_price,
                                    "side": order.side,
                                    "slippage": slippage,
                                    "strategy_id": order.strategy_id,
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )
                        except Exception as callback_error:
                            logger.error(f"Error delivering paper fill callback: {callback_error}")

        except Exception as e:
            logger.error(f"Error in paper trade simulation loop: {e}")
            db.rollback()
        finally:
            db.close()

    def get_virtual_pnl(self):
        return {
            "starting_cash": self.starting_cash,
            "current_cash": self.virtual_cash,
            "cash_pnl": self.virtual_cash - self.starting_cash
        }

    def get_open_positions(self):
        db = SessionLocal()
        try:
            trades = db.query(Trade).join(Order).filter(Order.mode == "paper").all()
            positions = {}
            for t in trades:
                if t.symbol not in positions:
                    positions[t.symbol] = 0
                if t.side == "BUY":
                    positions[t.symbol] += t.quantity
                else:
                    positions[t.symbol] -= t.quantity

            # Remove closed positions
            return {sym: qty for sym, qty in positions.items() if qty != 0}
        finally:
            db.close()

    def get_open_positions_by_strategy(self):
        db = SessionLocal()
        try:
            trades = db.query(Trade).join(Order).filter(Order.mode == "paper").all()
            positions = {}
            for t in trades:
                strategy_id = t.strategy_id or "legacy"
                strategy_positions = positions.setdefault(strategy_id, {})
                strategy_positions[t.symbol] = strategy_positions.get(t.symbol, 0) + (t.quantity if t.side == "BUY" else -t.quantity)
            for strategy_id in list(positions.keys()):
                positions[strategy_id] = {sym: qty for sym, qty in positions[strategy_id].items() if qty != 0}
            return positions
        finally:
            db.close()
