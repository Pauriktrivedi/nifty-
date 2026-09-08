import logging
from datetime import date
from database.database import SessionLocal
from database.models import PnlSummary, Trade

logger = logging.getLogger(__name__)

class PnlReport:
    def __init__(self):
        pass

    def calculate_daily_pnl(self, run_date=None):
        if not run_date:
            run_date = date.today()

        db = SessionLocal()
        try:
            # Get all trades for the given date
            # Ensure we are filtering by date correctly. Depending on DB dialect,
            # cast to date or filter by range. Here we do simple range.
            from datetime import datetime, time
            start_dt = datetime.combine(run_date, time.min)
            end_dt = datetime.combine(run_date, time.max)

            trades = db.query(Trade).filter(Trade.timestamp >= start_dt, Trade.timestamp <= end_dt).all()

            realized_pnl = 0.0
            total_trades = len(trades)
            winning_trades = 0

            # Group trades by symbol to match buys and sells
            symbol_trades = {}
            for t in trades:
                if t.symbol not in symbol_trades:
                    symbol_trades[t.symbol] = []
                symbol_trades[t.symbol].append(t)

            for symbol, s_trades in symbol_trades.items():
                # Sort by time
                s_trades.sort(key=lambda x: x.timestamp)

                # Simple FIFO matching for realized PnL
                buy_queue = []
                sell_queue = []

                for t in s_trades:
                    qty = t.quantity
                    price = t.price

                    if t.side == "BUY":
                        while qty > 0 and sell_queue:
                            match = sell_queue[0]
                            match_qty = min(qty, match["qty"])
                            # We bought to close a short
                            pnl = (match["price"] - price) * match_qty
                            realized_pnl += pnl
                            if pnl > 0:
                                winning_trades += 1

                            qty -= match_qty
                            match["qty"] -= match_qty
                            if match["qty"] == 0:
                                sell_queue.pop(0)

                        if qty > 0:
                            buy_queue.append({"qty": qty, "price": price})

                    elif t.side == "SELL":
                        while qty > 0 and buy_queue:
                            match = buy_queue[0]
                            match_qty = min(qty, match["qty"])
                            # We sold to close a long
                            pnl = (price - match["price"]) * match_qty
                            realized_pnl += pnl
                            if pnl > 0:
                                winning_trades += 1

                            qty -= match_qty
                            match["qty"] -= match_qty
                            if match["qty"] == 0:
                                buy_queue.pop(0)

                        if qty > 0:
                            sell_queue.append({"qty": qty, "price": price})

            win_rate = (winning_trades / total_trades) if total_trades > 0 else 0.0

            # Store it
            summary = db.query(PnlSummary).filter(PnlSummary.date == run_date).first()
            if not summary:
                summary = PnlSummary(
                    date=run_date,
                    total_pnl=realized_pnl,
                    realized_pnl=realized_pnl,
                    unrealized_pnl=0.0,
                    win_rate=win_rate,
                    total_trades=total_trades,
                    winning_trades=winning_trades
                )
                db.add(summary)
            else:
                summary.total_pnl = realized_pnl
                summary.realized_pnl = realized_pnl
                summary.win_rate = win_rate
                summary.total_trades = total_trades
                summary.winning_trades = winning_trades

            db.commit()
            db.refresh(summary)
            # Create a detached copy (dict or simple object) or just return the values we need to avoid DetachedInstanceError.
            # Or simpler: access the attributes while the session is open so they are loaded,
            # but SQLAlchemy expunge doesn't always prevent DetachedInstanceError on related attributes or deferred cols.
            # Using expunge_all or just returning a dict is safer.
            summary_dict = {
                "total_pnl": summary.total_pnl,
                "realized_pnl": summary.realized_pnl,
                "win_rate": summary.win_rate,
                "total_trades": summary.total_trades,
                "winning_trades": summary.winning_trades
            }
            # Create a dummy object to mimic the model so main.py doesn't break
            class DummySummary:
                pass
            res = DummySummary()
            for k, v in summary_dict.items():
                setattr(res, k, v)
            return res
        except Exception as e:
            logger.error(f"Error calculating daily PnL: {e}")
            return None
        finally:
            db.close()

    def calculate_daily_pnl_by_strategy(self, run_date=None):
        if not run_date:
            run_date = date.today()

        db = SessionLocal()
        try:
            from datetime import datetime, time

            start_dt = datetime.combine(run_date, time.min)
            end_dt = datetime.combine(run_date, time.max)
            trades = db.query(Trade).filter(Trade.timestamp >= start_dt, Trade.timestamp <= end_dt).all()
            latest_prices = {}
            for symbol, price in db.query(Trade.symbol, Trade.price).filter(Trade.timestamp >= start_dt, Trade.timestamp <= end_dt).all():
                if symbol not in latest_prices:
                    latest_prices[symbol] = price

            summary = {}
            for trade in sorted(trades, key=lambda item: item.timestamp):
                strategy_id = trade.strategy_id or "legacy"
                bucket = summary.setdefault(strategy_id, {
                    "realized_pnl": 0.0,
                    "total_trades": 0,
                    "winning_trades": 0,
                    "positions": {},
                })
                bucket["total_trades"] += 1
                positions = bucket["positions"].setdefault(trade.symbol, {"buy_lots": [], "sell_lots": []})

                qty = int(trade.quantity)
                price = float(trade.price)

                if trade.side == "BUY":
                    while qty > 0 and positions["sell_lots"]:
                        lot = positions["sell_lots"][0]
                        close_qty = min(qty, lot["qty"])
                        pnl = (lot["price"] - price) * close_qty
                        bucket["realized_pnl"] += pnl
                        if pnl > 0:
                            bucket["winning_trades"] += 1
                        qty -= close_qty
                        lot["qty"] -= close_qty
                        if lot["qty"] == 0:
                            positions["sell_lots"].pop(0)
                    if qty > 0:
                        positions["buy_lots"].append({"qty": qty, "price": price})
                else:
                    while qty > 0 and positions["buy_lots"]:
                        lot = positions["buy_lots"][0]
                        close_qty = min(qty, lot["qty"])
                        pnl = (price - lot["price"]) * close_qty
                        bucket["realized_pnl"] += pnl
                        if pnl > 0:
                            bucket["winning_trades"] += 1
                        qty -= close_qty
                        lot["qty"] -= close_qty
                        if lot["qty"] == 0:
                            positions["buy_lots"].pop(0)
                    if qty > 0:
                        positions["sell_lots"].append({"qty": qty, "price": price})

            for strategy_id, bucket in summary.items():
                unrealized = 0.0
                open_positions = {}
                for symbol, lot_bucket in bucket["positions"].items():
                    ltp = latest_prices.get(symbol)
                    net_qty = sum(lot["qty"] for lot in lot_bucket["buy_lots"]) - sum(lot["qty"] for lot in lot_bucket["sell_lots"])
                    if net_qty != 0:
                        open_positions[symbol] = net_qty
                    if ltp is None:
                        continue
                    if lot_bucket["buy_lots"]:
                        buy_qty = sum(lot["qty"] for lot in lot_bucket["buy_lots"])
                        avg_buy_price = sum(lot["qty"] * lot["price"] for lot in lot_bucket["buy_lots"]) / buy_qty
                        unrealized += (ltp - avg_buy_price) * buy_qty
                    if lot_bucket["sell_lots"]:
                        sell_qty = sum(lot["qty"] for lot in lot_bucket["sell_lots"])
                        avg_sell_price = sum(lot["qty"] * lot["price"] for lot in lot_bucket["sell_lots"]) / sell_qty
                        unrealized += (avg_sell_price - ltp) * sell_qty
                bucket["unrealized_pnl"] = unrealized
                bucket["total_pnl"] = bucket["realized_pnl"] + unrealized
                bucket["open_positions"] = open_positions
                bucket["open_positions_count"] = len(open_positions)

            return summary
        except Exception as e:
            logger.error(f"Error calculating strategy PnL: {e}")
            return {}
        finally:
            db.close()

    def get_today_pnl(self):
        db = SessionLocal()
        try:
            summary = db.query(PnlSummary).filter(PnlSummary.date == date.today()).first()
            if summary:
                return summary.total_pnl
            return 0.0
        finally:
            db.close()
