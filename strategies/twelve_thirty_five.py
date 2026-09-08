import logging
from datetime import date, datetime, time

from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class TwelveThirtyFiveStrategy(BaseStrategy):
    """
    Executes a short ATM Call and 50-point ITM Put around 12:35 PM daily.
    Applies a strict 25-point independent Stop Loss on both legs based on
    the option entry price and exits any remaining positions at 03:25 PM.
    """

    def __init__(
        self,
        mode="paper",
        instrument_master=None,
        underlying_symbol="NIFTY",
        qty=50,
        strategy_id="twelve_thirty_five",
        quote_fetcher=None,
    ):
        super().__init__(name="12:35_Options_Selling", strategy_id=strategy_id, mode=mode)
        self.instrument_master = instrument_master
        self.quote_fetcher = quote_fetcher
        self.underlying_symbol = underlying_symbol
        self.qty = qty

        self.executed_today = False
        self.execution_date = None
        self.positions = {}  # symbol -> {entry_price, sl_price, status}

        self.entry_time = time(12, 35)
        self.exit_time = time(15, 25)
        self.sl_points = 25.0

        # Track LTP to use for execution at 12:35
        self.current_underlying_price = 0.0

    @staticmethod
    def _normalize_key(value):
        return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

    @staticmethod
    def _coerce_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _find_column(self, columns_map, *candidates):
        for candidate in candidates:
            match = columns_map.get(self._normalize_key(candidate))
            if match:
                return match
        return None

    def _normalize_strike_value(self, value):
        strike = self._coerce_float(value, 0.0)
        if strike <= 0:
            return 0.0
        if strike >= 1000:
            strike = strike / 100.0
        return round(strike, 2)

    def _reset_daily_state_if_needed(self):
        today = date.today()
        if self.execution_date != today:
            self.execution_date = today
            self.executed_today = False

    def get_time_from_tick(self, tick_data):
        timestamp = tick_data.get("timestamp")
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                return dt.time()
            except Exception:
                return datetime.now().time()
        if isinstance(timestamp, datetime):
            return timestamp.time()
        return datetime.now().time()

    def _get_strikes(self, ltp):
        # NIFTY strike step is 50 points.
        atm_strike = round(ltp / 50) * 50
        itm_pe_strike = atm_strike + 50
        return float(atm_strike), float(itm_pe_strike)

    def _parse_expiry(self, expiry_value):
        if expiry_value is None:
            return None
        if isinstance(expiry_value, datetime):
            return expiry_value
        if isinstance(expiry_value, date):
            return datetime.combine(expiry_value, time.min)
        if isinstance(expiry_value, (int, float)):
            expiry_num = float(expiry_value)
            if expiry_num > 1_000_000_000_000:
                expiry_num /= 1000.0
            if expiry_num > 1_000_000_000:
                try:
                    return datetime.fromtimestamp(expiry_num)
                except Exception:
                    return None
        expiry_text = str(expiry_value).strip()
        if not expiry_text or expiry_text.lower() == "nan":
            return None
        if expiry_text.isdigit():
            try:
                expiry_num = float(expiry_text)
                if expiry_num > 1_000_000_000_000:
                    expiry_num /= 1000.0
                if expiry_num > 1_000_000_000:
                    return datetime.fromtimestamp(expiry_num)
            except Exception:
                return None
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(expiry_text, fmt)
            except Exception:
                continue
        return None

    def _expiry_label(self, expiry_value):
        parsed = self._parse_expiry(expiry_value)
        if parsed is None:
            return None
        return parsed.strftime("%d-%b-%Y")

    def _pick_nearest_expiry(self, expiry_values):
        expiries = []
        for value in expiry_values:
            parsed = self._parse_expiry(value)
            if parsed is not None:
                expiries.append(parsed)
        if not expiries:
            return None
        return sorted(expiries)[0]

    def _resolve_leg(self, current_opts, strike, option_type, expiry_label, strike_column="_strike_actual"):
        if strike_column not in current_opts.columns:
            return None
        leg = current_opts[
            (current_opts[strike_column] == strike)
            & (current_opts["pOptionType"] == option_type)
        ]
        if leg.empty:
            return None

        row = leg.iloc[0]
        token = f"nse_fo|{row['pSymbol']}"
        trading_symbol = str(row.get("pTrdSymbol") or row.get("pSymbol") or token).strip() or token
        reference_price = 0.0

        if self.quote_fetcher:
            quote = self.quote_fetcher(token)
            if quote:
                trading_symbol = str(
                    quote.get("display_symbol")
                    or quote.get("trading_symbol")
                    or trading_symbol
                ).strip() or trading_symbol
                reference_price = float(
                    quote.get("ltp")
                    or quote.get("lastPrice")
                    or quote.get("last_traded_price")
                    or 0.0
                )

        return {
                "symbol": token,
                "trading_symbol": trading_symbol,
                "reference_price": reference_price,
                "expiry_date": expiry_label,
            }

    def _execute_entry(self, tick_data):
        if not self.instrument_master:
            logger.error("InstrumentMaster not provided to 12:35 strategy.")
            return

        ltp = float(self.current_underlying_price or 0.0)
        if ltp <= 0:
            logger.warning("Underlying price is 0, cannot execute 12:35 entry.")
            return

        atm_strike, itm_pe_strike = self._get_strikes(ltp)
        logger.info(
            "[12:35 Entry] Underlying LTP: %.2f, ATM CE Strike: %.2f, ITM PE Strike: %.2f",
            ltp,
            atm_strike,
            itm_pe_strike,
        )

        ce_leg = None
        pe_leg = None

        try:
            df = getattr(self.instrument_master, "fo_df", None)
            if df is not None and not df.empty:
                working = df.copy()
                working.columns = [str(col).strip() for col in working.columns]
                column_map = {self._normalize_key(col): col for col in working.columns}
                symbol_col = self._find_column(column_map, "pSymbolName", "symbolName")
                inst_col = self._find_column(column_map, "pInstType", "instrumentType")
                strike_col = self._find_column(column_map, "dStrikePrice", "dStrikePrice;", "strikePrice")
                option_col = self._find_column(column_map, "pOptionType", "optionType")
                expiry_col = self._find_column(column_map, "lExpiryDate", "pExpiryDate")
                symbol_id_col = self._find_column(column_map, "pSymbol")

                if all([symbol_col, inst_col, strike_col, option_col, expiry_col, symbol_id_col]):
                    symbol_str = self.underlying_symbol.split("|")[-1] if "|" in self.underlying_symbol else self.underlying_symbol
                    if symbol_str.upper() in {"NIFTY 50", "NIFTY"}:
                        symbol_str = "NIFTY"

                    mask = (
                        (working[symbol_col].astype(str).str.upper() == symbol_str.upper())
                        & (working[inst_col].astype(str).str.contains("OPT", na=False))
                    )
                    opts = working[mask].copy()
                    if not opts.empty:
                        opts["_expiry_dt"] = opts[expiry_col].apply(self._parse_expiry)
                        nearest_expiry = self._pick_nearest_expiry(opts["_expiry_dt"].dropna().tolist())
                        if nearest_expiry:
                            current_opts = opts[opts["_expiry_dt"].apply(lambda value: value == nearest_expiry)].copy()
                            if not current_opts.empty:
                                current_opts["_strike_actual"] = current_opts[strike_col].apply(self._normalize_strike_value)
                                expiry_label = self._expiry_label(nearest_expiry)
                                ce_leg = self._resolve_leg(current_opts, atm_strike, "CE", expiry_label, strike_column="_strike_actual")
                                pe_leg = self._resolve_leg(current_opts, itm_pe_strike, "PE", expiry_label, strike_column="_strike_actual")
        except Exception as exc:
            logger.error("Error finding tokens dynamically: %s", exc)

        if not ce_leg or not pe_leg:
            logger.warning(
                "[12:35 Entry] Unable to resolve both option legs for ATM %.2f / ITM %.2f. cols=%s",
                atm_strike,
                itm_pe_strike,
                [col for col in getattr(getattr(self.instrument_master, "fo_df", None), "columns", [])][:12],
            )

        ce_order_id = None
        pe_order_id = None

        if ce_leg:
            ce_order_id = self.place_order(
                symbol=ce_leg["symbol"],
                trading_symbol=ce_leg["trading_symbol"],
                qty=self.qty,
                side="SELL",
                exchange_seg="nse_fo",
                order_type="MARKET",
                price=ce_leg["reference_price"] or ltp,
                instrument_type="CE",
                strike_price=atm_strike,
                expiry_date=ce_leg["expiry_date"],
            )

        if pe_leg:
            pe_order_id = self.place_order(
                symbol=pe_leg["symbol"],
                trading_symbol=pe_leg["trading_symbol"],
                qty=self.qty,
                side="SELL",
                exchange_seg="nse_fo",
                order_type="MARKET",
                price=pe_leg["reference_price"] or ltp,
                instrument_type="PE",
                strike_price=itm_pe_strike,
                expiry_date=pe_leg["expiry_date"],
            )

        if ce_order_id or pe_order_id:
            self.executed_today = True
            logger.info("[12:35 Entry] orders placed: CE=%s PE=%s", ce_order_id, pe_order_id)

    def _execute_exit_all(self):
        logger.info("[15:25 Auto-Exit] Exiting all remaining positions.")
        for symbol, pos in list(self.positions.items()):
            if pos["status"] == "OPEN":
                self.place_order(
                    symbol=symbol,
                    trading_symbol=symbol,
                    qty=self.qty,
                    side="BUY",
                    exchange_seg="nse_fo",
                    order_type="MARKET",
                )
                pos["status"] = "CLOSED"

    def on_tick(self, tick_data: dict):
        self._reset_daily_state_if_needed()

        symbol = tick_data.get("symbol", "")
        ltp = float(tick_data.get("last_traded_price", 0.0) or tick_data.get("ltp", 0.0) or 0.0)
        tick_time = self.get_time_from_tick(tick_data)

        # Track underlying price
        if symbol == self.underlying_symbol or "NIFTY 50" in str(symbol).upper():
            if ltp > 0:
                self.current_underlying_price = ltp

        # 12:35 PM Entry Check
        if not self.executed_today and self.entry_time <= tick_time < time(12, 40):
            self._execute_entry(tick_data)

        # Stop Loss Management for Open Positions
        for pos_symbol, pos in list(self.positions.items()):
            if pos["status"] == "OPEN" and pos_symbol == symbol:
                # Since we sold options, loss occurs when LTP > entry_price
                if ltp >= pos["sl_price"]:
                    logger.info(
                        "[Stop Loss Hit] %s LTP %.2f >= SL %.2f. Covering short.",
                        pos_symbol,
                        ltp,
                        pos["sl_price"],
                    )
                    self.place_order(
                        symbol=pos_symbol,
                        trading_symbol=pos_symbol,
                        qty=self.qty,
                        side="BUY",
                        exchange_seg="nse_fo",
                        order_type="MARKET",
                    )
                    pos["status"] = "CLOSED"

        # 15:25 PM Exit Check
        if self.executed_today and tick_time >= self.exit_time:
            if any(pos["status"] == "OPEN" for pos in self.positions.values()):
                self._execute_exit_all()

    def on_signal(self, signal_data: dict):
        pass

    def on_order_fill(self, trade_data: dict):
        symbol = trade_data["symbol"]
        side = trade_data["side"]
        fill_price = trade_data["price"]

        if side == "SELL":
            self.positions[symbol] = {
                "entry_price": fill_price,
                "sl_price": fill_price + self.sl_points,
                "status": "OPEN",
            }
            logger.info(
                "[Position Opened] %s sold at %.2f, SL set at %.2f",
                symbol,
                fill_price,
                fill_price + self.sl_points,
            )
        elif side == "BUY":
            if symbol in self.positions:
                self.positions[symbol]["status"] = "CLOSED"
                logger.info("[Position Closed] %s covered at %.2f", symbol, fill_price)
