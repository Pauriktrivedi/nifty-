import os
import sys
import time
import uvicorn
import logging
import threading
import socket
import atexit
import fcntl
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from datetime import datetime, time as dt_time

import pandas as pd
from database.database import init_db, SessionLocal
from database.models import MarketData
from core.auth import FyersAuth
from core.instruments import InstrumentMaster
from core.websocket_feed import WebSocketFeedHandler, QuotesClient
from core.order_manager import OrderManager
from paper_trade.paper_trade import PaperTradeSimulator
from risk.risk_manager import RiskManager
from scheduler.scheduler import TradingScheduler
from analytics.pnl_report import PnlReport
from core import strategy_runtime
from dashboard.dashboard import app as fastapi_app
from strategies.sample_strategy import SampleStrategy
from strategies.breakout_strategy import BreakoutRangeStrategy
from strategies.twelve_thirty_five import TwelveThirtyFiveStrategy
from strategies.paper_test_strategy import PaperTestStrategy

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("trading.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("System")
console = Console()
_INSTANCE_LOCK_FD = None
_INSTANCE_LOCK_PATH = "/tmp/nifty_main.lock"


def _acquire_single_instance_lock():
    global _INSTANCE_LOCK_FD
    fd = os.open(_INSTANCE_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        holder_pid = ""
        try:
            with os.fdopen(os.dup(fd), "r") as handle:
                holder_pid = handle.read().strip()
        except Exception:
            holder_pid = ""
        os.close(fd)
        raise RuntimeError(f"Another backend instance is already running (pid={holder_pid or 'unknown'}).")

    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode("utf-8"))
    os.fsync(fd)
    _INSTANCE_LOCK_FD = fd

    def _release_lock():
        global _INSTANCE_LOCK_FD
        if _INSTANCE_LOCK_FD is None:
            return
        try:
            fcntl.flock(_INSTANCE_LOCK_FD, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(_INSTANCE_LOCK_FD)
        except Exception:
            pass
        _INSTANCE_LOCK_FD = None

    atexit.register(_release_lock)

class SystemController:
    def __init__(self):
        load_dotenv()
        self.mode = "paper" if os.getenv("PAPER_MODE", "true").lower() == "true" else "live"
        self.virtual_cash = os.getenv("VIRTUAL_CASH", "500000")
        self.max_daily_loss = os.getenv("MAX_DAILY_LOSS", "10000")
        self.max_positions = os.getenv("MAX_OPEN_POSITIONS", "5")
        self.enable_strategies = str(os.getenv("ENABLE_STRATEGIES", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        
        self.auth = None
        self.session = None
        self.quotes_client = None
        self.instruments = None
        self.ws_handler = None
        self.order_manager = None
        self.paper_trader = None
        self.strategy_instances = {}
        self.strategy_risk_managers = {}
        self._strategy_lock = threading.Lock()
        self._index_bridge_running = False
        self._index_bridge_thread = None
        self._paper_test_bootstrap_done = False
        self._paper_test_bootstrap_running = False
        if self.enable_strategies:
            strategy_runtime.register_callbacks(
                start_cb=self.start_strategy,
                pause_cb=self.pause_strategy,
                resume_cb=self.resume_strategy,
                stop_cb=self.stop_strategy_by_id,
            )

    def _get_strategy_risk_manager(self, strategy_id):
        risk_manager = self.strategy_risk_managers.get(strategy_id)
        if risk_manager is None:
            risk_manager = RiskManager(
                max_daily_loss=self.max_daily_loss,
                per_trade_stop_loss=2000,
                max_open_positions=self.max_positions,
                persist_state=False,
                strategy_id=strategy_id,
            )
            self.strategy_risk_managers[strategy_id] = risk_manager
        return risk_manager

    def _build_strategy(self, strategy_name):
        risk_manager = self._get_strategy_risk_manager(strategy_name)
        if strategy_name == "breakout_range":
            return BreakoutRangeStrategy(
                mode=self.mode,
                paper_trader=self.paper_trader,
                live_trader=self.order_manager,
                risk_manager=risk_manager,
                symbol="nse_cm|Nifty 50",
                range_high=None,
                range_low=None,
                quantity=1,
                strategy_id=strategy_name,
            )
        if strategy_name == "twelve_thirty_five":
            strategy = TwelveThirtyFiveStrategy(
                mode=self.mode,
                instrument_master=self.instruments,
                underlying_symbol="nse_cm|Nifty 50",
                strategy_id=strategy_name,
                quote_fetcher=self._fetch_symbol_quote,
            )
            strategy.paper_trader = self.paper_trader
            strategy.live_trader = self.order_manager
            strategy.risk_manager = risk_manager
            return strategy
        if strategy_name == "paper_test":
            return PaperTestStrategy(
                mode=self.mode,
                paper_trader=self.paper_trader,
                live_trader=self.order_manager,
                risk_manager=risk_manager,
                symbol="nse_cm|Nifty 50",
                quantity=1,
                strategy_id=strategy_name,
            )
        return SampleStrategy(
            mode=self.mode,
            paper_trader=self.paper_trader,
            live_trader=self.order_manager,
            risk_manager=risk_manager,
            strategy_id=strategy_name,
        )

    def _dispatch_tick(self, tick):
        with self._strategy_lock:
            strategies = list(self.strategy_instances.items())
        for strategy_id, strategy in strategies:
            state = strategy_runtime.get_strategy_state(strategy_id)
            if str(state.get("status", "")).lower() != "live":
                continue
            try:
                strategy.on_tick(tick)
            except Exception as exc:
                logger.error("Strategy %s tick error: %s", strategy_id, exc)

    def _dispatch_fill(self, trade_data):
        strategy_id = trade_data.get("strategy_id") or "legacy"
        with self._strategy_lock:
            strategy = self.strategy_instances.get(strategy_id)
            risk_manager = self.strategy_risk_managers.get(strategy_id)

        if strategy:
            try:
                strategy.on_order_fill(trade_data)
            except Exception as exc:
                logger.error("Strategy %s fill error: %s", strategy_id, exc)

        if risk_manager:
            try:
                risk_manager.update_position(
                    trade_data.get("symbol"),
                    int(trade_data.get("quantity", 0)),
                    trade_data.get("side", "BUY"),
                )
            except Exception as exc:
                logger.error("Risk manager update failed for %s: %s", strategy_id, exc)

    def _is_market_window(self):
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        current_time = now.time()
        return dt_time(9, 15) <= current_time <= dt_time(15, 30)

    def _store_index_market_data(self, exchange_seg, symbol_name, payload):
        db = SessionLocal()
        try:
            depth = payload.get("depth", {}) or {}
            buy = (depth.get("buy") or [{}])[0] or {}
            sell = (depth.get("sell") or [{}])[0] or {}
            md = MarketData(
                symbol=f"{exchange_seg}|{symbol_name}",
                trading_symbol=str(payload.get("display_symbol") or symbol_name),
                exchange_seg=exchange_seg,
                instrument_type="INDEX",
                bid_price=float(buy.get("price", 0) or 0.0),
                ask_price=float(sell.get("price", 0) or 0.0),
                last_traded_price=float(payload.get("ltp", 0) or 0.0),
                volume=int(float(payload.get("last_volume", 0) or 0)),
                oi=int(float(payload.get("open_int", 0) or 0)),
                timestamp=datetime.now(),
            )
            db.add(md)
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.debug("Failed to store index market data for %s: %s", symbol_name, exc)
        finally:
            db.close()

    def _latest_index_market_data(self, exchange_seg, symbol_name):
        db = SessionLocal()
        try:
            symbol = f"{exchange_seg}|{symbol_name}"
            return (
                db.query(MarketData)
                .filter(MarketData.symbol == symbol)
                .order_by(MarketData.timestamp.desc())
                .first()
            )
        finally:
            db.close()

    def _market_data_row_to_quote(self, row, symbol_name):
        if row is None:
            return None
        return {
            "display_symbol": symbol_name,
            "ltp": float(row.last_traded_price or 0.0),
            "open_int": int(row.oi or 0),
            "last_volume": int(row.volume or 0),
            "depth": {
                "buy": [{"price": float(row.bid_price or 0.0)}],
                "sell": [{"price": float(row.ask_price or 0.0)}],
            },
        }

    def _dispatch_index_tick(self, exchange_seg, symbol_name, payload):
        depth = payload.get("depth", {}) or {}
        buy = (depth.get("buy") or [{}])[0] or {}
        sell = (depth.get("sell") or [{}])[0] or {}
        normalized_tick = {
            "symbol": f"{exchange_seg}|{symbol_name}",
            "raw_symbol": f"{exchange_seg}|{symbol_name}",
            "trading_symbol": str(payload.get("display_symbol") or symbol_name),
            "exchange_seg": exchange_seg,
            "instrument_type": "INDEX",
            "strike_price": None,
            "expiry_date": None,
            "last_traded_price": float(payload.get("ltp", 0) or 0.0),
            "bid_price": float(buy.get("price", 0) or 0.0),
            "ask_price": float(sell.get("price", 0) or 0.0),
            "volume": int(float(payload.get("last_volume", 0) or 0)),
            "oi": int(float(payload.get("open_int", 0) or 0)),
            "timestamp": datetime.now().isoformat(),
            "raw": payload,
        }
        self._dispatch_tick(normalized_tick)

    def _fetch_index_quote(self, session, exchange_seg, symbol_name):
        if self.quotes_client is None and session:
            self.quotes_client = QuotesClient(session)
        if self.quotes_client is None:
            return None
        return self.quotes_client.get_quote(exchange_seg, symbol_name)

    def _make_index_tick(self, exchange_seg, symbol_name, payload):
        depth = payload.get("depth", {}) or {}
        buy = (depth.get("buy") or [{}])[0] or {}
        sell = (depth.get("sell") or [{}])[0] or {}
        return {
            "symbol": f"{exchange_seg}|{symbol_name}",
            "raw_symbol": f"{exchange_seg}|{symbol_name}",
            "trading_symbol": str(payload.get("display_symbol") or symbol_name),
            "exchange_seg": exchange_seg,
            "instrument_type": "INDEX",
            "strike_price": None,
            "expiry_date": None,
            "last_traded_price": float(payload.get("ltp", 0) or 0.0),
            "bid_price": float(buy.get("price", 0) or 0.0),
            "ask_price": float(sell.get("price", 0) or 0.0),
            "volume": int(float(payload.get("last_volume", 0) or 0)),
            "oi": int(float(payload.get("open_int", 0) or 0)),
            "timestamp": datetime.now().isoformat(),
            "raw": payload,
        }

    def _fetch_symbol_quote(self, symbol_token):
        session = self.session
        if not session and self.auth:
            try:
                session = self.auth.get_session()
                self.session = session
            except Exception as exc:
                logger.debug("Unable to refresh session for quote fetch: %s", exc)
                return None

        if not session:
            return None

        if self.quotes_client is None:
            self.quotes_client = QuotesClient(session)
        return self.quotes_client.get_quote(None, symbol_token)

    def _maybe_bootstrap_paper_test_trade(self, session, payload=None):
        if self._paper_test_bootstrap_done or self._paper_test_bootstrap_running:
            return

        with self._strategy_lock:
            strategy = self.strategy_instances.get("paper_test")

        if strategy is None:
            return

        if str(strategy_runtime.get_strategy_state("paper_test").get("status", "")).lower() != "live":
            return

        self._paper_test_bootstrap_running = True
        try:
            if payload is None:
                payload = self._fetch_index_quote(session, "nse_cm", "Nifty 50")
            if not payload:
                payload = self._market_data_row_to_quote(
                    self._latest_index_market_data("nse_cm", "Nifty 50"),
                    "Nifty 50",
                )
            if not payload:
                return

            tick = self._make_index_tick("nse_cm", "Nifty 50", payload)
            logger.info("Bootstrapping paper_test from live Nifty 50 quote.")
            strategy.on_tick(tick)
            self._paper_test_bootstrap_done = bool(getattr(strategy, "trade_executed", False))
        except Exception as exc:
            logger.debug("Paper-test bootstrap skipped: %s", exc)
        finally:
            self._paper_test_bootstrap_running = False

    def trigger_time_based_strategy(self, strategy_id, exchange_seg="nse_cm", symbol_name="Nifty 50"):
        with self._strategy_lock:
            strategy = self.strategy_instances.get(strategy_id)

        if strategy is None:
            return False, f"Strategy {strategy_id} is not initialized."

        state = strategy_runtime.get_strategy_state(strategy_id)
        if str(state.get("status", "")).lower() != "live":
            return False, f"Strategy {strategy_id} is not live."

        session = self.session
        if not session and self.auth:
            try:
                session = self.auth.get_session()
                self.session = session
            except Exception as exc:
                return False, f"Unable to refresh session for {strategy_id}: {exc}"

        if not session:
            return False, f"No valid session available for {strategy_id}."

        try:
            payload = self._fetch_index_quote(session, exchange_seg, symbol_name)
            if not payload:
                payload = self._market_data_row_to_quote(
                    self._latest_index_market_data(exchange_seg, symbol_name),
                    symbol_name,
                )
            if not payload:
                return False, f"Unable to fetch latest {symbol_name} quote."

            self._store_index_market_data(exchange_seg, symbol_name, payload)
            tick = self._make_index_tick(exchange_seg, symbol_name, payload)
            strategy.on_tick(tick)
            return True, f"Triggered {strategy_id} using latest {symbol_name} quote."
        except Exception as exc:
            logger.error("Failed to trigger strategy %s from latest quote: %s", strategy_id, exc)
            return False, f"Failed to trigger {strategy_id}: {exc}"

    def _index_bridge_loop(self, session):
        targets = [
            ("nse_cm", "Nifty 50"),
            ("nse_cm", "Nifty Bank"),
        ]

        logger.info("Index quote bridge started.")
        while self._index_bridge_running:
            try:
                if not self._is_market_window():
                    time.sleep(10)
                    continue

                for exchange_seg, symbol_name in targets:
                    try:
                        payload = self._fetch_index_quote(session, exchange_seg, symbol_name)
                        if not payload:
                            payload = self._market_data_row_to_quote(
                                self._latest_index_market_data(exchange_seg, symbol_name),
                                symbol_name,
                            )
                        if not payload:
                            continue
                        row = payload
                        self._store_index_market_data(exchange_seg, symbol_name, row)
                        self._dispatch_index_tick(exchange_seg, symbol_name, row)
                        if exchange_seg == "nse_cm" and symbol_name == "Nifty 50":
                            self._maybe_bootstrap_paper_test_trade(session, payload=row)
                    except Exception as exc:
                        logger.debug("Index bridge fetch failed for %s: %s", symbol_name, exc)

                time.sleep(2)
            except Exception as exc:
                logger.error("Index bridge loop error: %s", exc)
                time.sleep(5)

    def _start_index_bridge(self, session):
        if self._index_bridge_running:
            return
        self._index_bridge_running = True
        self._index_bridge_thread = threading.Thread(target=self._index_bridge_loop, args=(session,), daemon=True)
        self._index_bridge_thread.start()

    def start_strategy(self, strategy_id):
        if not self.enable_strategies:
            return False, "Strategy engine is disabled in dashboard-only mode."
        with self._strategy_lock:
            strategy = self.strategy_instances.get(strategy_id)
            if strategy is None:
                strategy = self._build_strategy(strategy_id)
                self.strategy_instances[strategy_id] = strategy
        strategy_runtime.set_runtime_state(strategy_id, "Live")
        logger.info("Strategy started: %s", strategy_id)
        return True, f"Strategy {strategy_id} started."

    def pause_strategy(self, strategy_id):
        if not self.enable_strategies:
            return False, "Strategy engine is disabled in dashboard-only mode."
        with self._strategy_lock:
            if strategy_id not in self.strategy_instances:
                self.strategy_instances[strategy_id] = self._build_strategy(strategy_id)
        strategy_runtime.set_runtime_state(strategy_id, "Paused")
        logger.info(f"Strategy paused: {strategy_id}")
        return True, f"Strategy {strategy_id} paused."

    def resume_strategy(self, strategy_id):
        if not self.enable_strategies:
            return False, "Strategy engine is disabled in dashboard-only mode."
        with self._strategy_lock:
            if strategy_id not in self.strategy_instances:
                self.strategy_instances[strategy_id] = self._build_strategy(strategy_id)
        strategy_runtime.set_runtime_state(strategy_id, "Live")
        logger.info(f"Strategy resumed: {strategy_id}")
        return True, f"Strategy {strategy_id} resumed."

    def stop_strategy_by_id(self, strategy_id):
        if not self.enable_strategies:
            return False, "Strategy engine is disabled in dashboard-only mode."
        with self._strategy_lock:
            if strategy_id not in self.strategy_instances:
                self.strategy_instances[strategy_id] = self._build_strategy(strategy_id)
        strategy_runtime.set_runtime_state(strategy_id, "Stopped")
        logger.info(f"Strategy stopped: {strategy_id}")
        return True, f"Strategy {strategy_id} stopped."

    def start(self):
        logger.info(f"Initializing system in {self.mode.upper()} mode...")
        
        try:
            # 1. Init Risk
            # 2. Auth & Instruments
            self.auth = FyersAuth()
            session = self.auth.get_session()
            self.session = session
            self.quotes_client = QuotesClient(session)
            
            self.instruments = InstrumentMaster()
            self.instruments.download(session)
            self.instruments.load('nse_fo')
            self.instruments.load('nse_cm')
            
            # 3. Setup strategy execution (disabled by default for dashboard-only usage)
            if self.enable_strategies:
                if self.mode == 'paper':
                    self.paper_trader = PaperTradeSimulator(on_fill_callback=self._dispatch_fill)
                    self.paper_trader.start()
                else:
                    self.order_manager = OrderManager(session, self.auth)

                default_strategy_spec = os.getenv(
                    "ACTIVE_STRATEGIES",
                    os.getenv(
                        "ACTIVE_STRATEGY",
                        "breakout_range,twelve_thirty_five,sample_strategy,paper_test",
                    ),
                )
                strategy_ids = [s.strip() for s in default_strategy_spec.split(",") if s.strip()]
                for strategy_name in strategy_ids:
                    self.start_strategy(strategy_name)
            else:
                for strategy_id in strategy_runtime.STRATEGY_CATALOG:
                    strategy_runtime.set_runtime_state(strategy_id, "Stopped")
                logger.info("Dashboard-only mode active. Strategies and order simulators are disabled.")
                
            # 5. Setup Websocket 
            # Subscribe using existing internal symbols; websocket adapter maps to FYERS symbols.
            tokens = [
                "nse_cm|Nifty 50", "nse_cm|Nifty Bank"
            ]
            
            # Dynamically fetch correct option chain tokens using InstrumentMaster
            if self.instruments.fo_df is not None:
                try:
                    df = self.instruments.fo_df
                    symbol_filter = df['pSymbolName'].astype(str).str.upper()
                    inst_filter = df['pInstType'].astype(str).str.upper()

                    def _future_tokens_for(underlying_key, include_patterns, exclude_patterns=None):
                        future_df = df[
                            inst_filter.str.contains('FUT', na=False) &
                            symbol_filter.str.contains('|'.join(include_patterns), na=False, regex=True)
                        ].copy()
                        if future_df.empty:
                            return []
                        if exclude_patterns:
                            future_df = future_df[
                                ~future_df['pSymbolName'].astype(str).str.upper().str.contains(
                                    '|'.join(exclude_patterns), na=False, regex=True
                                )
                            ]
                        if underlying_key == "NIFTY":
                            future_df = future_df[
                                future_df['pSymbolName'].astype(str).str.upper().str.contains(
                                    r'(?:^|[^A-Z0-9])NIFTY(?:[^A-Z0-9]|$)', na=False, regex=True
                                )
                                & ~future_df['pSymbolName'].astype(str).str.upper().str.contains('BANKNIFTY', na=False)
                                & ~future_df['pSymbolName'].astype(str).str.upper().str.contains('FINNIFTY', na=False)
                                & ~future_df['pSymbolName'].astype(str).str.upper().str.contains('MIDCPNIFTY', na=False)
                                & ~future_df['pSymbolName'].astype(str).str.upper().str.contains('NIFTY BANK', na=False)
                            ]
                        if future_df.empty:
                            return []
                        expiry_col = 'lExpiryDate' if 'lExpiryDate' in future_df.columns else 'pExpiryDate'
                        expiry_rank = pd.to_datetime(future_df[expiry_col], errors='coerce', dayfirst=True)
                        today = pd.Timestamp.now().normalize()
                        future_df = future_df.assign(_expiry_rank=expiry_rank).sort_values(
                            by=['_expiry_rank', expiry_col],
                            na_position='last'
                        )
                        future_df = future_df[
                            future_df['_expiry_rank'].notna() & (future_df['_expiry_rank'] >= today)
                        ]
                        if future_df.empty:
                            return []
                        unique_expiries = []
                        for expiry in future_df[expiry_col].dropna().astype(str).tolist():
                            expiry = expiry.strip()
                            if expiry and expiry not in unique_expiries:
                                unique_expiries.append(expiry)
                            if len(unique_expiries) >= 3:
                                break

                        future_tokens = []
                        for expiry in unique_expiries:
                            contract = future_df[future_df[expiry_col].astype(str) == expiry]
                            if contract.empty:
                                continue
                            future_tokens.append(f"nse_fo|{contract.iloc[0]['pSymbol']}")
                        return future_tokens

                    future_tokens = []
                    future_tokens.extend(_future_tokens_for("NIFTY", [r"NIFTY"], [r"BANKNIFTY", r"NIFTY BANK"]))
                    future_tokens.extend(_future_tokens_for("BANKNIFTY", [r"BANKNIFTY", r"NIFTY BANK"]))
                    if future_tokens:
                        tokens.extend(future_tokens)

                    def _option_tokens_for(underlying_symbol, index_symbol_name, strikes_each_side=4):
                        symbol_series = df['pSymbolName'].astype(str).str.upper()
                        if underlying_symbol == "NIFTY":
                            underlying_mask = (
                                symbol_series.str.contains(r'(?:^|[^A-Z0-9])NIFTY(?:[^A-Z0-9]|$)', na=False, regex=True)
                                & ~symbol_series.str.contains('BANKNIFTY', na=False)
                                & ~symbol_series.str.contains('NIFTY BANK', na=False)
                                & ~symbol_series.str.contains('FINNIFTY', na=False)
                                & ~symbol_series.str.contains('MIDCPNIFTY', na=False)
                            )
                        elif underlying_symbol == "BANKNIFTY":
                            underlying_mask = symbol_series.str.contains(r'BANKNIFTY|NIFTY BANK', na=False, regex=True)
                        else:
                            underlying_mask = symbol_series.str.contains(underlying_symbol, na=False, regex=False)

                        option_df = df[
                            underlying_mask
                            & (df['pInstType'].astype(str).str.upper().str.contains('OPT', na=False))
                        ].copy()
                        if option_df.empty:
                            return []

                        expiry_col = 'lExpiryDate' if 'lExpiryDate' in option_df.columns else 'pExpiryDate'
                        option_df[expiry_col] = option_df[expiry_col].astype(str).str.strip()
                        option_df = option_df[option_df[expiry_col] != ""]
                        if option_df.empty:
                            return []

                        option_df['_expiry_rank'] = pd.to_datetime(
                            option_df[expiry_col], errors='coerce', dayfirst=True
                        )
                        today = pd.Timestamp.now().normalize()
                        option_df = option_df[
                            option_df['_expiry_rank'].notna() & (option_df['_expiry_rank'] >= today)
                        ]
                        if option_df.empty:
                            return []
                        option_df = option_df.sort_values(by=['_expiry_rank', expiry_col], na_position='last')
                        nearest_expiry = None
                        for value in option_df[expiry_col].dropna().astype(str):
                            value = value.strip()
                            if value:
                                nearest_expiry = value
                                break
                        if not nearest_expiry:
                            return []

                        current_expiry_opts = option_df[option_df[expiry_col].astype(str) == nearest_expiry].copy()
                        if current_expiry_opts.empty:
                            return []

                        strike_series = pd.to_numeric(current_expiry_opts['dStrikePrice'], errors='coerce').dropna()
                        strikes = sorted(strike_series.unique())
                        if not strikes:
                            return []

                        underlying_ltp = 0.0
                        quote = self._fetch_index_quote(session, "nse_cm", index_symbol_name)
                        if quote:
                            underlying_ltp = float(quote.get("ltp") or 0.0)
                        if underlying_ltp <= 0:
                            md = self._latest_index_market_data("nse_cm", index_symbol_name)
                            if md is not None:
                                underlying_ltp = float(md.last_traded_price or 0.0)

                        if underlying_ltp > 0:
                            nearest_idx = min(range(len(strikes)), key=lambda idx: abs(float(strikes[idx]) - underlying_ltp))
                        else:
                            nearest_idx = len(strikes) // 2

                        left = max(0, nearest_idx - strikes_each_side)
                        right = min(len(strikes), nearest_idx + strikes_each_side + 1)
                        selected_strikes = [float(strike) for strike in strikes[left:right]]

                        option_tokens = []
                        strike_values = pd.to_numeric(current_expiry_opts['dStrikePrice'], errors='coerce')
                        for strike in selected_strikes:
                            strike_rows = current_expiry_opts[strike_values == strike]
                            for option_type in ("CE", "PE"):
                                leg = strike_rows[
                                    strike_rows['pOptionType'].astype(str).str.upper() == option_type
                                ]
                                if not leg.empty:
                                    option_tokens.append(f"nse_fo|{leg.iloc[0]['pSymbol']}")
                        return option_tokens

                    tokens.extend(_option_tokens_for("NIFTY", "Nifty 50"))
                    tokens.extend(_option_tokens_for("BANKNIFTY", "Nifty Bank"))

                    deduped_tokens = []
                    seen = set()
                    for token in tokens:
                        key = str(token).strip()
                        if not key or key in seen:
                            continue
                        seen.add(key)
                        deduped_tokens.append(key)
                    tokens = deduped_tokens

                    logger.info("Dynamically added %s derivative subscriptions.", max(0, len(tokens) - 2))
                except Exception as e:
                    logger.error(f"Error dynamically fetching option tokens: {e}")
            else:
                logger.warning("InstrumentMaster data not available. Proceeding with index-only subscriptions.")
            
            self.ws_handler = WebSocketFeedHandler(session, tokens, on_tick_callback=self._dispatch_tick, instruments=self.instruments)
            self.ws_handler.start()
            self._start_index_bridge(session)
            if self.enable_strategies:
                self._maybe_bootstrap_paper_test_trade(session)

                now = datetime.now()
                if now.weekday() < 5 and dt_time(12, 35) <= now.time() < dt_time(12, 40):
                    self.trigger_time_based_strategy("twelve_thirty_five")
                elif now.weekday() < 5 and dt_time(15, 25) <= now.time() < dt_time(15, 30):
                    self.trigger_time_based_strategy("twelve_thirty_five")
            
            logger.info("System successfully started.")
            
        except Exception as e:
            logger.error(f"Failed to start system: {e}")

    def stop(self):
        logger.info("Stopping system...")
        self._index_bridge_running = False
        if self.ws_handler:
            self.ws_handler.stop()
        if self.paper_trader:
            self.paper_trader.stop()
            
        # Calculate end of day PnL
        self.generate_report()
        logger.info("System stopped.")

    def generate_report(self):
        report = PnlReport()
        summary = report.calculate_daily_pnl()
        if summary:
            console.print(Panel(
                f"Total PnL: {summary.total_pnl}\nTrades: {summary.total_trades}",
                title="Daily PnL Report",
                expand=False,
                style="green" if summary.total_pnl >= 0 else "red"
            ))

def run_dashboard():
    # Run FastAPI app with uvicorn
    # Redirecting uvicorn access logs to standard logging can be tricky, using default here.
    dashboard_host = os.getenv("DASHBOARD_HOST", "::")
    uvicorn.run(fastapi_app, host=dashboard_host, port=8000, log_level="warning")


def _wait_for_dashboard_ready(timeout_seconds=12.0):
    deadline = time.time() + timeout_seconds
    probes = ("::1", "127.0.0.1", "localhost")
    last_error = None

    while time.time() < deadline:
        for probe in probes:
            try:
                with socket.create_connection((probe, 8000), timeout=0.5):
                    return probe
            except OSError as exc:
                last_error = exc
        time.sleep(0.2)

    if last_error:
        logger.debug("Dashboard readiness probe failed: %s", last_error)
    return None

def main():
    try:
        _acquire_single_instance_lock()
    except RuntimeError as exc:
        console.print(Panel.fit(f"[bold red]{exc}[/bold red]"))
        logger.error("%s", exc)
        return 1

    # Print Banner
    console.print(Panel.fit("[bold blue]FYERS Live Market Dashboard[/bold blue]", subtitle="Initializing..."))
    
    # Init DB
    init_db()
    
    # Init Controller
    controller = SystemController()
    
    console.print(f"Mode: [bold {'yellow' if controller.mode == 'paper' else 'red'}]{controller.mode.upper()}[/]")
    if controller.mode == 'paper':
         console.print(f"Virtual Cash: {controller.virtual_cash}")
    console.print(f"Risk Limits: Daily Loss = {controller.max_daily_loss}, Max Pos = {controller.max_positions}")
    
    # Start Dashboard
    dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
    dashboard_thread.start()
    ready_probe = _wait_for_dashboard_ready()
    if ready_probe:
        logger.info("Dashboard started on port 8000 (%s reachable)", ready_probe)
    else:
        logger.warning("Dashboard thread started, but no local probe became ready on port 8000.")
    
    # Start Scheduler
    scheduler = TradingScheduler(controller)
    scheduler.start()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
        scheduler.stop()
        controller.stop()
        return 0

if __name__ == "__main__":
    sys.exit(main())
