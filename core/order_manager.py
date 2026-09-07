import logging
import os
import time
from datetime import datetime

from database.database import SessionLocal
from database.models import Order

from core.fyers_utils import get_fyers_client, to_fyers_symbol

logger = logging.getLogger(__name__)


class OrderException(Exception):
    pass


class OrderManager:
    def __init__(self, auth_session, auth_instance=None):
        self.auth_session = auth_session
        self.auth_instance = auth_instance
        self.client_id = auth_session.get("client_id")
        self.access_token = auth_session.get("access_token")
        self.default_product = os.getenv("FYERS_PRODUCT_TYPE", "MARGIN").upper()
        self.fyers = get_fyers_client(self.client_id, self.access_token)

    @staticmethod
    def _order_type_code(order_type: str) -> int:
        code_map = {
            "LIMIT": 1,
            "L": 1,
            "MARKET": 2,
            "MKT": 2,
            "SL-M": 3,
            "SL": 3,
            "STOP": 3,
            "STOP-MARKET": 3,
            "SL-L": 4,
            "STOPLIMIT": 4,
            "STOP-LIMIT": 4,
        }
        return code_map.get(str(order_type or "MARKET").upper(), 2)

    @staticmethod
    def _side_code(side: str) -> int:
        return 1 if str(side or "BUY").upper() == "BUY" else -1

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _save_order(
        self,
        broker_order_id,
        symbol,
        trading_symbol,
        qty,
        side,
        order_type,
        price,
        instrument_type,
        strike_price,
        expiry_date,
        exchange_seg,
        strategy_id,
    ):
        order_ref = str(broker_order_id or f"LIVE_{int(time.time() * 1000)}")
        kwargs = {
            "symbol": symbol,
            "quantity": int(qty),
            "price": self._to_float(price, None),
            "order_type": str(order_type).upper(),
            "side": str(side).upper(),
            "status": "PENDING",
            "instrument_type": instrument_type or "EQ",
            "strike_price": strike_price,
            "expiry_date": expiry_date,
            "exchange_seg": exchange_seg or "nse_cm",
            "trading_symbol": trading_symbol,
            "mode": "live",
            "strategy_id": strategy_id,
            "kotak_order_id": order_ref,
            "timestamp": datetime.now(),
        }
        if hasattr(Order, "broker_order_id"):
            kwargs["broker_order_id"] = order_ref

        db = SessionLocal()
        try:
            db.add(Order(**kwargs))
            db.commit()
        finally:
            db.close()
        return order_ref

    def _normalize_symbol(self, exchange_seg, symbol, trading_symbol):
        preferred = str(trading_symbol or "").strip() or str(symbol or "").strip()
        fyers_symbol = to_fyers_symbol(exchange_seg, preferred)
        if not fyers_symbol:
            raise OrderException(f"Unable to resolve FYERS symbol from '{preferred}'.")
        return fyers_symbol

    def place_order(
        self,
        symbol,
        trading_symbol,
        qty,
        side,
        exchange_seg,
        order_type='MKT',
        price='0',
        product='NRML',
        trigger_price='0',
        instrument_type='EQ',
        strike_price=None,
        expiry_date=None,
        strategy_id=None,
    ):
        fyers_symbol = self._normalize_symbol(exchange_seg, symbol, trading_symbol)
        order_type_code = self._order_type_code(order_type)

        limit_price = self._to_float(price, 0.0) if order_type_code in (1, 4) else 0.0
        stop_price = self._to_float(trigger_price, 0.0) if order_type_code in (3, 4) else 0.0

        payload = {
            "symbol": fyers_symbol,
            "qty": int(qty),
            "type": order_type_code,
            "side": self._side_code(side),
            "productType": self.default_product,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "stopLoss": 0,
            "takeProfit": 0,
        }

        logger.info("Placing FYERS order: %s", payload)
        response = self.fyers.place_order(payload)
        if response.get("s") != "ok":
            raise OrderException(
                f"FYERS place_order failed: {response.get('message') or response.get('code') or response}"
            )

        broker_order_id = response.get("id") or response.get("order_id")
        order_ref = self._save_order(
            broker_order_id=broker_order_id,
            symbol=symbol,
            trading_symbol=fyers_symbol,
            qty=qty,
            side=side,
            order_type=order_type,
            price=price,
            instrument_type=instrument_type,
            strike_price=strike_price,
            expiry_date=expiry_date,
            exchange_seg=exchange_seg,
            strategy_id=strategy_id,
        )

        logger.info("Placed FYERS order %s (%s %s %s)", order_ref, side, qty, fyers_symbol)
        return order_ref

    def modify_order(self, order_no, trading_symbol, qty, price, order_type, exchange_seg, product):
        payload = {
            "id": str(order_no),
            "qty": int(qty),
            "type": self._order_type_code(order_type),
            "limitPrice": self._to_float(price, 0.0),
            "stopPrice": 0.0,
        }
        response = self.fyers.modify_order(payload)
        if response.get("s") != "ok":
            raise OrderException(
                f"FYERS modify_order failed: {response.get('message') or response.get('code') or response}"
            )
        return response

    def cancel_order(self, order_no):
        response = self.fyers.cancel_order({"id": str(order_no)})
        if response.get("s") != "ok":
            raise OrderException(
                f"FYERS cancel_order failed: {response.get('message') or response.get('code') or response}"
            )
        return response

    def get_order_book(self):
        return self.fyers.orderbook()

    def get_positions(self):
        return self.fyers.positions()

    def get_trade_book(self):
        return self.fyers.tradebook()

    def check_margin(self, token, exchange_seg, qty, price, order_type, product, side):
        # FYERS SDK does not expose a direct margin-check endpoint equivalent here.
        # Returning funds + positions is still useful for diagnostics/UI.
        return {
            "s": "ok",
            "funds": self.fyers.funds(),
            "positions": self.fyers.positions(),
        }
