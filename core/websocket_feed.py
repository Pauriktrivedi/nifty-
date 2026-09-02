import logging
import threading
import time
from datetime import datetime

import pandas as pd
from fyers_apiv3.FyersWebsocket import data_ws


# Monkey-patch FyersDataSocket to retain 'OI' field that the SDK normally pops
_original_response_output = data_ws.FyersDataSocket._FyersDataSocket__response_output

def _patched_response_output(self, data, data_type):
    original_on_message = self.On_message

    def fake_on_message(response):
        if isinstance(data, dict) and "OI" in data:
            response["OI"] = data["OI"]
        original_on_message(response)

    self.On_message = fake_on_message
    try:
        _original_response_output(self, data, data_type)
    finally:
        self.On_message = original_on_message

data_ws.FyersDataSocket._FyersDataSocket__response_output = _patched_response_output


from core.fyers_utils import (
    get_fyers_client,
    infer_instrument_type,
    infer_strike,
    to_fyers_symbol,
    to_internal_symbol,
    to_ws_access_token,
)
from core.status import set_ws_metrics, set_ws_status
from database.database import SessionLocal
from database.models import MarketData

logger = logging.getLogger(__name__)


class WebSocketFeedHandler:
    def __init__(self, auth_session, instrument_tokens, on_tick_callback=None, instruments=None):
        self.auth_session = auth_session
        self.instrument_tokens = instrument_tokens or []
        self.on_tick_callback = on_tick_callback
        self.instruments = instruments

        self.client_id = auth_session.get("client_id")
        self.access_token = auth_session.get("access_token")
        self.running = False
        self.connected = False

        self._thread = None
        self._socket = None
        self._symbol_aliases = {}
        self._subscribed_symbols = []

    def _build_subscription_symbols(self):
        self._symbol_aliases = {}
        symbols = []
        for raw_token in self.instrument_tokens:
            fyers_symbol = to_fyers_symbol(None, raw_token)
            if not fyers_symbol:
                continue
            self._symbol_aliases.setdefault(fyers_symbol, set()).add(str(raw_token))
            symbols.append(fyers_symbol)

        # stable order + dedupe
        deduped = []
        seen = set()
        for symbol in symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            deduped.append(symbol)
        self._subscribed_symbols = deduped

    def start(self):
        self._build_subscription_symbols()
        self.running = True
        self._thread = threading.Thread(target=self._run_socket_loop, daemon=True)
        self._thread.start()
        set_ws_metrics(
            connected=False,
            reconnect_count=0,
            consecutive_failures=0,
            last_error=None,
            last_tick_at=None,
            last_message_at=None,
            last_connect_at=None,
            last_tick_epoch=None,
            last_message_epoch=None,
            reconnect_events=[],
            subscription_count=len(self._subscribed_symbols),
        )
        logger.info("FYERS websocket handler started for %s symbols.", len(self._subscribed_symbols))

    def stop(self):
        self.running = False
        try:
            if self._socket is not None:
                self._socket.close_connection()
        except Exception:
            pass
        if self._thread and self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=3)

        self.connected = False
        set_ws_metrics(connected=False)
        set_ws_status(False)
        logger.info("FYERS websocket handler stopped.")

    def _run_socket_loop(self):
        retry_delays = [2, 4, 8, 15, 30]
        retry_idx = 0
        stale_reconnect_grace_sec = 12

        while self.running:
            try:
                self.connected = False
                access = to_ws_access_token(self.client_id, self.access_token)
                self._socket = data_ws.FyersDataSocket(
                    access_token=access,
                    write_to_file=False,
                    litemode=False,
                    reconnect=False,
                    on_connect=self._on_connect,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._socket.connect()

                # FYERS connect() may return immediately and run callbacks on background threads.
                # Wait for connect callback, then keep this loop parked until disconnected.
                connected_wait_deadline = time.time() + 12.0
                while self.running and not self.connected and time.time() < connected_wait_deadline:
                    time.sleep(0.2)

                if not self.running:
                    break

                if self.connected:
                    retry_idx = 0
                    while self.running and self.connected:
                        # Guard against "connected but no tick stream" stalls.
                        if self._should_force_reconnect(stale_reconnect_grace_sec):
                            logger.warning(
                                "No websocket ticks for >%ss during market hours; forcing reconnect.",
                                stale_reconnect_grace_sec,
                            )
                            try:
                                if self._socket is not None:
                                    self._socket.close_connection()
                            except Exception:
                                pass
                            self.connected = False
                            try:
                                set_ws_metrics(connected=False, last_error="stale_tick_watchdog_reconnect")
                            except Exception:
                                pass
                            try:
                                set_ws_status(False)
                            except Exception:
                                pass
                            break
                        time.sleep(0.5)
                    continue

                raise RuntimeError("FYERS websocket did not reach connected state.")
            except Exception as exc:
                reconnect_events = list(self._read_metric("reconnect_events") or [])
                reconnect_events.append(time.time())
                reconnect_events = reconnect_events[-24:]
                logger.error("FYERS websocket connection error: %s", exc)
                set_ws_metrics(
                    connected=False,
                    last_error=str(exc),
                    consecutive_failures=int((self._read_metric("consecutive_failures") or 0)) + 1,
                    reconnect_count=int((self._read_metric("reconnect_count") or 0)) + 1,
                    reconnect_events=reconnect_events,
                )
                set_ws_status(False)
                delay = retry_delays[min(retry_idx, len(retry_delays) - 1)]
                time.sleep(delay)
                retry_idx = min(retry_idx + 1, len(retry_delays) - 1)

    @staticmethod
    def _is_market_window_now():
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        minute_of_day = now.hour * 60 + now.minute
        return (9 * 60 + 15) <= minute_of_day <= (15 * 60 + 30)

    def _read_metric(self, key):
        try:
            from core.status import get_ws_metrics

            return get_ws_metrics().get(key)
        except Exception:
            return None

    def _should_force_reconnect(self, stale_reconnect_grace_sec):
        if not self._is_market_window_now():
            return False
        last_tick_epoch = self._read_metric("last_tick_epoch")
        if last_tick_epoch is None:
            return False
        try:
            age_sec = time.time() - float(last_tick_epoch)
        except Exception:
            return False
        return age_sec > float(stale_reconnect_grace_sec)

    def _on_connect(self):
        if not self._socket:
            return
        try:
            if self._subscribed_symbols:
                self._socket.subscribe(symbols=self._subscribed_symbols, data_type="SymbolUpdate")
            self.connected = True
            set_ws_metrics(
                connected=True,
                last_connect_at=datetime.now().isoformat(),
                last_message_epoch=time.time(),
                last_error=None,
                consecutive_failures=0,
                subscription_count=len(self._subscribed_symbols),
            )
            set_ws_status(True)
            logger.info("FYERS websocket connected.")
        except Exception as exc:
            logger.error("FYERS websocket subscribe failed: %s", exc)
            self._on_error(exc)

    def _on_close(self, message):
        self.connected = False
        set_ws_metrics(connected=False, last_error=str(message or "closed"))
        set_ws_status(False)
        logger.info("FYERS websocket closed: %s", message)

    def _on_error(self, message):
        self.connected = False
        set_ws_metrics(connected=False, last_error=str(message))
        set_ws_status(False)
        logger.error("FYERS websocket error: %s", message)

    def _lookup_master_row(self, fyers_symbol, exchange_seg):
        if not self.instruments:
            return None

        try:
            if exchange_seg == "nse_fo" and self.instruments.fo_df is not None:
                df = self.instruments.fo_df
                match = df[df["pSymbol"].astype(str) == str(fyers_symbol)]
                if not match.empty:
                    return match.iloc[0]
            if exchange_seg == "nse_cm" and self.instruments.cm_df is not None:
                df = self.instruments.cm_df
                match = df[df["pSymbol"].astype(str) == str(fyers_symbol)]
                if not match.empty:
                    return match.iloc[0]
        except Exception:
            return None
        return None

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _to_int(value, default=0):
        try:
            return int(float(value))
        except Exception:
            return default

    def _normalize_tick(self, tick):
        fyers_symbol = str(tick.get("symbol") or "").strip()
        if not fyers_symbol:
            return None

        alias_candidates = list(self._symbol_aliases.get(fyers_symbol, []))
        internal_symbol, default_exchange = to_internal_symbol(fyers_symbol)
        exchange_seg = default_exchange

        # Prefer an explicit legacy alias if present.
        if alias_candidates:
            preferred = alias_candidates[0]
            if "|" in preferred:
                exchange_seg = preferred.split("|", 1)[0]
                internal_symbol = preferred

        row = self._lookup_master_row(fyers_symbol, exchange_seg)
        trading_symbol = fyers_symbol
        instrument_type = infer_instrument_type(fyers_symbol)
        strike_price = infer_strike(fyers_symbol)
        expiry_date = None

        if row is not None:
            trading_symbol = str(row.get("pTrdSymbol") or fyers_symbol)
            instrument_type = str(row.get("pOptionType") or instrument_type).upper()
            if instrument_type not in {"CE", "PE"}:
                instrument_type = str(row.get("pInstType") or instrument_type).upper()
            strike_price = self._to_float(row.get("dStrikePrice"), strike_price or 0.0) if pd.notna(row.get("dStrikePrice")) else strike_price
            expiry_date = str(row.get("lExpiryDate") or "").strip() or None

        ltp = self._to_float(tick.get("ltp", tick.get("lp", 0.0)), 0.0)
        bid = self._to_float(tick.get("bid_price", tick.get("bid", ltp)), ltp)
        ask = self._to_float(tick.get("ask_price", tick.get("ask", ltp)), ltp)
        volume = self._to_int(tick.get("vol_traded_today", tick.get("volume", 0)), 0)
        oi = self._to_int(tick.get("OI", tick.get("oi", 0)), 0)

        now_iso = datetime.now().isoformat()
        normalized = {
            "symbol": internal_symbol,
            "raw_symbol": fyers_symbol,
            "trading_symbol": trading_symbol,
            "exchange_seg": exchange_seg,
            "instrument_type": instrument_type,
            "strike_price": strike_price,
            "expiry_date": expiry_date,
            "last_traded_price": ltp,
            "bid_price": bid,
            "ask_price": ask,
            "volume": volume,
            "oi": oi,
            "timestamp": now_iso,
            "raw": tick,
        }
        return normalized

    def _store_tick(self, normalized):
        db = SessionLocal()
        try:
            db.add(
                MarketData(
                    symbol=normalized["symbol"],
                    trading_symbol=normalized["trading_symbol"],
                    exchange_seg=normalized["exchange_seg"],
                    instrument_type=normalized["instrument_type"],
                    bid_price=normalized["bid_price"],
                    ask_price=normalized["ask_price"],
                    last_traded_price=normalized["last_traded_price"],
                    volume=normalized["volume"],
                    oi=normalized["oi"],
                    strike_price=normalized["strike_price"],
                    expiry_date=normalized["expiry_date"],
                    timestamp=datetime.now(),
                )
            )
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.error("DB error storing FYERS tick: %s", exc)
        finally:
            db.close()

    def _handle_single_message(self, payload):
        if not isinstance(payload, dict):
            return
        if "symbol" not in payload:
            payload = self._coerce_symbol_update_payload(payload)
        if "symbol" not in payload:
            return

        normalized = self._normalize_tick(payload)
        if not normalized:
            return

        try:
            now_epoch = time.time()
            set_ws_metrics(
                last_tick_at=normalized["timestamp"],
                last_message_at=normalized["timestamp"],
                last_tick_epoch=now_epoch,
                last_message_epoch=now_epoch,
            )
        except Exception:
            # Status telemetry is best-effort; never block tick persistence.
            pass

        if self.on_tick_callback:
            try:
                self.on_tick_callback(normalized)
            except Exception as exc:
                logger.error("Tick callback failed: %s", exc)

        self._store_tick(normalized)

    @staticmethod
    def _coerce_symbol_update_payload(payload):
        """
        FYERS sometimes sends SymbolUpdate frames as {"n": "<symbol>", "v": {...}}
        inside wrapper messages. Convert that shape into the flat payload used by
        _normalize_tick.
        """
        if not isinstance(payload, dict):
            return payload
        symbol = str(payload.get("n") or payload.get("symbol") or "").strip()
        values = payload.get("v")
        if not symbol or not isinstance(values, dict):
            return payload
        return {
            "symbol": symbol,
            "ltp": values.get("lp", values.get("ltp")),
            "bid_price": values.get("bid", values.get("bid_price")),
            "ask_price": values.get("ask", values.get("ask_price")),
            "vol_traded_today": values.get("volume", values.get("vol_traded_today")),
            "oi": values.get("oi", values.get("OI")),
        }

    def _on_message(self, message):
        try:
            set_ws_metrics(last_message_at=datetime.now().isoformat(), last_message_epoch=time.time())
        except Exception:
            pass

        if isinstance(message, list):
            for item in message:
                self._handle_single_message(item)
            return

        if isinstance(message, dict):
            # Some FYERS control frames arrive as dicts without symbol.
            if "symbol" in message:
                self._handle_single_message(message)
                return
            # FYERS can also wrap updates as {"d": [...]} or {"d": {...}}.
            nested = message.get("d")
            if isinstance(nested, list):
                for item in nested:
                    self._handle_single_message(item)
                return
            if isinstance(nested, dict):
                self._handle_single_message(nested)
                return


class QuotesClient:
    def __init__(self, auth_session):
        self.client_id = auth_session.get("client_id")
        self.access_token = auth_session.get("access_token")
        self.fyers = get_fyers_client(self.client_id, self.access_token)

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def get_quote(self, exchange_seg, symbol):
        fyers_symbol = to_fyers_symbol(exchange_seg, symbol)
        if not fyers_symbol:
            return None

        try:
            response = self.fyers.quotes({"symbols": fyers_symbol})
            if response.get("s") != "ok":
                return None
            data = response.get("d") or []
            if not data:
                return None

            entry = data[0] if isinstance(data, list) else data
            values = entry.get("v") or {}

            ltp = self._to_float(values.get("lp", values.get("ltp", 0.0)), 0.0)
            bid = self._to_float(values.get("bid", values.get("bid_price", ltp)), ltp)
            ask = self._to_float(values.get("ask", values.get("ask_price", ltp)), ltp)
            volume = int(self._to_float(values.get("volume", values.get("vol_traded_today", 0.0)), 0.0))
            oi = int(self._to_float(values.get("oi", values.get("OI", 0.0)), 0.0))

            return {
                "display_symbol": entry.get("n") or fyers_symbol,
                "ltp": ltp,
                "open_int": oi,
                "last_volume": volume,
                "depth": {
                    "buy": [{"price": bid}],
                    "sell": [{"price": ask}],
                },
            }
        except Exception as exc:
            logger.debug("Quote fetch failed for %s: %s", fyers_symbol, exc)
            return None
