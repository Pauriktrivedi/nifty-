from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse
import json
import os
import logging
import math
import re
import requests
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from functools import lru_cache
from datetime import date, datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from database.database import SessionLocal
from database.models import Order, Trade, PnlSummary, MarketData, RiskState, OiSnapshot
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import OperationalError
from core import strategy_runtime
from core.fyers_utils import get_fyers_client, to_fyers_symbol
from core.instruments import InstrumentMaster

app = FastAPI()
logger = logging.getLogger(__name__)

_DEBUG_SESSION_ID = "846581"
_DEBUG_RUN_ID = "initial"
_DEBUG_LOG_PATH = "/Users/paurik/Desktop/Nifty/.cursor/debug-846581.log"
_DEBUG_SERVER_ENDPOINT = "http://127.0.0.1:7627/ingest/cf0fc31c-dde5-4dbe-840a-91b7136d33d4"
_DEBUG_AGENT_ENABLED = str(os.getenv("ENABLE_AGENT_DEBUG_LOG", "")).strip().lower() in {"1", "true", "yes", "on"}

def _agent_debug_log(hypothesis_id: str, location: str, message: str, data: dict | None = None) -> None:
    if not _DEBUG_AGENT_ENABLED:
        return
    # NDJSON append-only logger used for DEBUG MODE runtime evidence.
    try:
        payload = {
            "sessionId": _DEBUG_SESSION_ID,
            "runId": _DEBUG_RUN_ID,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        # Prefer emitting logs to the debug ingest endpoint.
        # This avoids filesystem restrictions under the agent sandbox.
        try:
            requests.post(
                _DEBUG_SERVER_ENDPOINT,
                headers={
                    "Content-Type": "application/json",
                    "X-Debug-Session-Id": _DEBUG_SESSION_ID,
                },
                data=json.dumps(payload, default=str),
                timeout=0.2,
            ).raise_for_status()
            return
        except Exception:
            pass

        # Best-effort fallback to the expected NDJSON file.
        # If this fails (permissions), we still keep the dashboard working.
        try:
            with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, default=str) + "\n")
        except Exception:
            pass
    except Exception:
        # Never break the dashboard due to debug logging issues.
        pass

_option_chain_cache = {}
_option_chain_lock = threading.Lock()
_option_chain_downloading = False
_option_chain_download_lock = threading.Lock()
_nse_session = requests.Session()
_nse_headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.nseindia.com/option-chain",
}
_nse_index_cache = {
    "timestamp": 0.0,
    "rows": [],
}
_oi_dashboard_cache = {}
_oi_dashboard_lock = threading.Lock()
_oi_dashboard_refreshing = set()
_oi_dashboard_refresh_lock = threading.Lock()
_oi_snapshot_capture_lock = threading.Lock()
_oi_snapshot_capture_in_progress = False
_futures_head_cache = {
    "timestamp": 0.0,
    "rows": [],
}
_futures_head_lock = threading.Lock()
_futures_head_refreshing = False
_futures_head_refresh_lock = threading.Lock()
_market_data_refreshing = False
_market_data_refresh_lock = threading.Lock()
_kotak_quote_cache = {}
_kotak_quote_cache_lock = threading.Lock()
_gift_nifty_cache = {
    "timestamp": 0.0,
    "row": None,
}
_gift_nifty_cache_lock = threading.Lock()
_gift_nifty_refreshing = False
_gift_nifty_refresh_lock = threading.Lock()

_TARGET_NSE_INDICES = [
    {"display": "NIFTY 50", "aliases": ["NIFTY 50", "NIFTY"], "group": "key"},
    {"display": "BANKNIFTY", "aliases": ["NIFTY BANK", "BANKNIFTY"], "group": "key"},
    {"display": "INDIA VIX", "aliases": ["INDIA VIX", "INDIAVIX"], "group": "key"},
    {"display": "FINNIFTY", "aliases": ["FINNIFTY"], "group": "key"},
    {"display": "MIDCPNIFTY", "aliases": ["NIFTY MIDCAP SELECT", "MIDCPNIFTY"], "group": "key"},
    {"display": "NIFTY 500", "aliases": ["NIFTY 500"], "group": "key"},
    {"display": "NIFTY SMALLCAP", "aliases": ["NIFTY SMALLCAP 250", "NIFTY SMALLCAP"], "group": "key"},
    {"display": "NIFTY AUTO", "aliases": ["NIFTY AUTO"], "group": "sector"},
    {"display": "NIFTY IT", "aliases": ["NIFTY IT"], "group": "sector"},
    {"display": "NIFTY PHARMA", "aliases": ["NIFTY PHARMA"], "group": "sector"},
    {"display": "NIFTY FMCG", "aliases": ["NIFTY FMCG"], "group": "sector"},
    {"display": "NIFTY METAL", "aliases": ["NIFTY METAL"], "group": "sector"},
    {"display": "NIFTY PSU BANK", "aliases": ["NIFTY PSU BANK"], "group": "sector"},
    {"display": "NIFTY PVT BANK", "aliases": ["NIFTY PRIVATE BANK", "NIFTY PVT BANK"], "group": "sector"},
    {"display": "NIFTY REALTY", "aliases": ["NIFTY REALTY"], "group": "sector"},
    {"display": "NIFTY OIL & GAS", "aliases": ["NIFTY OIL & GAS", "NIFTY OIL AND GAS"], "group": "sector"},
    {"display": "NIFTY MEDIA", "aliases": ["NIFTY MEDIA"], "group": "sector"},
    {"display": "NIFTY ENERGY", "aliases": ["NIFTY ENERGY"], "group": "sector"},
    {"display": "NIFTY MNC", "aliases": ["NIFTY MNC"], "group": "sector"},
    {"display": "NIFTY FINSERV", "aliases": ["NIFTY FINANCIAL SERVICES", "NIFTY FINSERV"], "group": "sector"},
    {"display": "NIFTY INFRA", "aliases": ["NIFTY INFRA"], "group": "sector"},
    {"display": "BSE BANKEX", "aliases": ["BSE BANKEX"], "group": "key"},
]

_OI_LADDER_STRIKES_PER_SIDE = 4
_OI_DASHBOARD_CACHE_TTL_SEC = 1.0
_OPTION_CHAIN_FALLBACK_CACHE_TTL_SEC = 3.0
_MARKETDATA_OPTION_MAX_AGE_SEC = 8.0
_MARKETDATA_INDEX_MAX_AGE_SEC = 5.0
_OPTION_REFRESH_FORCE_SEC = 4.0
_GIFT_NIFTY_CACHE_TTL_SEC = 30.0
_OI_STALE_THRESHOLD_SEC = 5.0
_WS_STORM_WINDOW_SEC = 60.0
_WS_STORM_RECONNECTS = 3


def _load_dashboard_session():
    session_file = "session.json"
    if not os.path.exists(session_file):
        return None

    try:
        with open(session_file, "r") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.debug("Dashboard session cache read failed: %s", exc)
        return None

    if data.get("expiry", 0) <= time.time():
        return None

    if str(data.get("broker", "FYERS")).upper() != "FYERS":
        return None

    if not data.get("client_id"):
        data["client_id"] = os.getenv("FYERS_CLIENT_ID") or os.getenv("FYERS_APP_ID")
    if not data.get("client_id"):
        return None

    return data


def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _safe_quote_value(entry, key, default=0.0):
    return _to_float(entry.get(key, default), default=default)


def _normalize_index_name(value):
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _norm_symbol(value):
    return _normalize_index_name(value)


def _normal_cdf(value):
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_pdf(value):
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _parse_expiry_date(expiry_text):
    if not expiry_text:
        return None
    expiry_text = str(expiry_text).strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", expiry_text):
        try:
            raw = float(expiry_text)
            if raw > 10**12:
                return datetime.fromtimestamp(raw / 1000.0)
            if raw > 10**9:
                return datetime.fromtimestamp(raw)
        except Exception:
            pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d%b%Y",
        "%d%b%y",
    ):
        try:
            return datetime.strptime(expiry_text, fmt)
        except Exception:
            continue
    return None


def _parse_row_timestamp(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _latest_rows_timestamp_iso(rows, default_to_now=False):
    latest = None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        parsed = _parse_row_timestamp(row.get("timestamp"))
        if parsed and (latest is None or parsed > latest):
            latest = parsed
    if latest is None and default_to_now:
        latest = datetime.now()
    return latest.isoformat() if latest else None


def _format_expiry_label(expiry_text):
    expiry_dt = _parse_expiry_date(expiry_text)
    if expiry_dt is None:
        return str(expiry_text or "").strip() or "Live / Auto"
    return expiry_dt.strftime("%d-%b-%Y")


def _json_for_html(value):
    return json.dumps(value, default=str)


def _black_scholes_greeks(spot, strike, iv_percent, expiry_text, side, rate=0.10):
    spot = float(spot or 0.0)
    strike = float(strike or 0.0)
    iv = max(float(iv_percent or 0.0) / 100.0, 0.0001)
    expiry_dt = _parse_expiry_date(expiry_text)
    if spot <= 0 or strike <= 0 or expiry_dt is None:
        return {"delta": None, "gamma": None, "theta": None, "vega": None}

    now = datetime.now()
    t = max((expiry_dt - now).total_seconds(), 0.0) / (365.0 * 24.0 * 3600.0)
    if t <= 0:
        t = 1.0 / (365.0 * 24.0)

    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    pdf_d1 = _normal_pdf(d1)

    if side == "CE":
        delta = _normal_cdf(d1)
        theta = (
            -(spot * pdf_d1 * iv) / (2.0 * sqrt_t)
            - rate * strike * math.exp(-rate * t) * _normal_cdf(d2)
        )
    else:
        delta = _normal_cdf(d1) - 1.0
        theta = (
            -(spot * pdf_d1 * iv) / (2.0 * sqrt_t)
            + rate * strike * math.exp(-rate * t) * _normal_cdf(-d2)
        )

    gamma = pdf_d1 / (spot * iv * sqrt_t)
    vega = spot * pdf_d1 * sqrt_t / 100.0

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta / 365.0, 4),
        "vega": round(vega, 4),
    }


def _select_symmetric_strikes(strikes, atm, each_side=_OI_LADDER_STRIKES_PER_SIDE):
    strikes = sorted({float(strike) for strike in strikes if _to_float(strike, 0.0) > 0})
    if not strikes:
        return []
    if atm is None:
        return strikes[: (each_side * 2) + 1]

    atm = float(atm)
    nearest_idx = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - atm))
    start = max(0, nearest_idx - each_side)
    end = min(len(strikes), nearest_idx + each_side + 1)

    window = strikes[start:end]
    if len(window) < (each_side * 2) + 1:
        deficit = (each_side * 2) + 1 - len(window)
        left_room = start
        right_room = len(strikes) - end
        left_extra = min(left_room, (deficit + 1) // 2)
        right_extra = min(right_room, deficit - left_extra)
        start = max(0, start - left_extra)
        end = min(len(strikes), end + right_extra)
        window = strikes[start:end]

    return window[: (each_side * 2) + 1]


def _sorted_expiry_values(expiry_values):
    cleaned = []
    for expiry in expiry_values or []:
        expiry_text = str(expiry).strip()
        if expiry_text and expiry_text.lower() != "nan":
            cleaned.append(expiry_text)
    return sorted(set(cleaned), key=lambda item: _parse_expiry_date(item) or datetime.max)


_WEEKLY_EXPIRY_WEEKDAY = {
    "NIFTY": 1,      # Tuesday
    "BANKNIFTY": 2,  # Wednesday
}


def _next_weekly_expiry_date(underlying_symbol, reference_dt=None):
    reference_dt = reference_dt or datetime.now()
    underlying_upper = str(underlying_symbol or "").upper()
    target_weekday = _WEEKLY_EXPIRY_WEEKDAY.get(underlying_upper, 3)
    today = reference_dt.date()
    days_ahead = (target_weekday - today.weekday()) % 7
    expiry_date = today + timedelta(days=days_ahead)
    market_close = datetime.strptime("15:30", "%H:%M").time()
    if expiry_date == today and reference_dt.time() > market_close:
        expiry_date += timedelta(days=7)
    return expiry_date


def _generate_weekly_expiry_seed(underlying_symbol, count=4, reference_dt=None):
    start_date = _next_weekly_expiry_date(underlying_symbol, reference_dt=reference_dt)
    return [
        (start_date + timedelta(days=7 * idx)).strftime("%d-%b-%Y")
        for idx in range(max(1, int(count or 1)))
    ]


def _is_future_expiry(expiry_text):
    expiry_dt = _parse_expiry_date(expiry_text)
    if expiry_dt is None:
        return False
    today = datetime.now().date()
    return expiry_dt.date() >= today


def _future_only_expiry_values(expiry_values):
    return [expiry for expiry in _sorted_expiry_values(expiry_values) if _is_future_expiry(expiry)]


@lru_cache(maxsize=32)
def _get_underlying_lot_size(underlying_symbol):
    try:
        instrument_master = InstrumentMaster()
        df = instrument_master.load("nse_fo")
        if df is None or df.empty:
            return None
        df = df.copy()
        df.columns = [str(col).strip().replace(";", "") for col in df.columns]
        col_map = {str(col).lower(): col for col in df.columns}

        def col(name):
            return col_map.get(name.lower())

        symbol_col = col("pSymbolName")
        inst_col = col("pInstType")
        lot_col = col("lLotSize") or col("iLotSize")
        if not all([symbol_col, inst_col, lot_col]):
            return None

        symbol_filter = df[symbol_col].astype(str).str.upper()
        underlying_upper = str(underlying_symbol or "NIFTY").upper()
        if underlying_upper == "BANKNIFTY":
            symbol_mask = symbol_filter.str.contains("BANKNIFTY", na=False) | symbol_filter.str.contains("NIFTY BANK", na=False)
        else:
            symbol_mask = symbol_filter.str.contains("NIFTY", na=False)

        lot_series = pd.to_numeric(
            df[symbol_mask & df[inst_col].astype(str).str.upper().str.contains("OPT", na=False)][lot_col],
            errors="coerce",
        ).dropna()
        lot_series = lot_series[lot_series > 0]
        if lot_series.empty:
            return None
        return int(lot_series.iloc[0])
    except Exception as exc:
        logger.debug("Lot size lookup failed for %s: %s", underlying_symbol, exc)
        return None


def _choose_expiry_value(expiry_values, selected_expiry=None, underlying_symbol=None):
    future_expiries = _future_only_expiry_values(expiry_values)
    if not future_expiries:
        fallback = _generate_weekly_expiry_seed(underlying_symbol) if underlying_symbol else []
        if not fallback:
            return None, []
        return fallback[0], fallback

    selected = str(selected_expiry or "").strip()
    if selected and selected in future_expiries:
        return selected, future_expiries

    return future_expiries[0], future_expiries


def _option_chain_cache_key(symbol, expiry=None):
    expiry_key = str(expiry or "").strip() or "AUTO"
    return f"{symbol}|{expiry_key}"


def _cached_oi_dashboard_sections(symbol=None, expiry=None):
    cache_key = _option_chain_cache_key(symbol or "ALL", expiry)
    with _oi_dashboard_lock:
        bucket = _oi_dashboard_cache.get(cache_key) or {}
        rows = bucket.get("rows")
        if rows is None:
            return None
        if isinstance(rows, dict):
            return [rows]
        return rows if isinstance(rows, list) else None


def _fetch_oi_dashboard_with_timeout(symbol=None, expiry=None, timeout_sec=8.0):
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_fetch_oi_dashboard, symbol=symbol, expiry=expiry)
    timed_out = False
    try:
        return future.result(timeout=max(1.0, float(timeout_sec)))
    except FutureTimeoutError:
        timed_out = True
        future.cancel()
        logger.warning(
            "OI dashboard fetch timed out after %.1fs for symbol=%s expiry=%s",
            timeout_sec,
            symbol,
            expiry,
        )
        return None
    except Exception as exc:
        logger.debug("OI dashboard fetch failed for snapshot capture: %s", exc)
        return None
    finally:
        # Avoid blocking the caller on executor shutdown when the task timed out.
        executor.shutdown(wait=not timed_out, cancel_futures=timed_out)


def _get_available_option_expiries(underlying_symbol):
    try:
        from core.instruments import InstrumentMaster
    except Exception as exc:
        logger.debug("Unable to import InstrumentMaster for expiry lookup: %s", exc)
        return []

    try:
        instrument_master = InstrumentMaster()
        fo_path = instrument_master.fo_file
        if os.path.exists(fo_path) and os.path.getsize(fo_path) == 0:
            return []
        df = instrument_master.load("nse_fo")
        if df is None or df.empty:
            return []
        df = df.copy()
        df.columns = [str(col).strip().replace(";", "") for col in df.columns]
        col_map = {str(col).lower(): col for col in df.columns}

        def col(name):
            return col_map.get(name.lower())

        symbol_col = col("pSymbolName")
        inst_col = col("pInstType")
        expiry_col = col("lExpiryDate") or col("pExpiryDate")
        trd_col = col("pTrdSymbol")
        if not all([symbol_col, inst_col, expiry_col]):
            return []

        symbol_filter = df[symbol_col].astype(str).str.upper()
        underlying_upper = str(underlying_symbol or "NIFTY").upper()
        if underlying_upper == "BANKNIFTY":
            symbol_mask = symbol_filter.str.contains("BANKNIFTY", na=False) | symbol_filter.str.contains("NIFTY BANK", na=False)
        else:
            symbol_mask = symbol_filter.str.contains("NIFTY", na=False)

        option_rows = df[
            symbol_mask &
            df[inst_col].astype(str).str.upper().str.contains("OPT", na=False)
        ].copy()
        if option_rows.empty:
            return _generate_weekly_expiry_seed(underlying_symbol)

        if trd_col:
            expiry_values = option_rows.apply(
                lambda row: _normalize_future_expiry_label(row[expiry_col], row[trd_col]),
                axis=1,
            ).dropna().tolist()
        else:
            expiry_values = option_rows[expiry_col].astype(str).tolist()

        sorted_expiries = _sorted_expiry_values(expiry_values)
        future_expiries = [expiry for expiry in sorted_expiries if _is_future_expiry(expiry)]
        if future_expiries:
            return future_expiries
        return _generate_weekly_expiry_seed(underlying_symbol)
    except Exception as exc:
        logger.debug("Expiry lookup failed for %s: %s", underlying_symbol, exc)
        return _generate_weekly_expiry_seed(underlying_symbol)


def _build_empty_oi_section(symbol, expiry=None, source="FYERS OI Master"):
    available_expiries = _get_available_option_expiries(symbol)
    chosen_expiry, available_expiries = _choose_expiry_value(
        available_expiries,
        selected_expiry=expiry,
        underlying_symbol=symbol,
    )
    return {
        "symbol": symbol,
        "source": source,
        "feed": "expiry_seed",
        "underlying": 0.0,
        "atm": None,
        "expiry": chosen_expiry,
        "selected_expiry": chosen_expiry,
        "last_fetched_at": None,
        "lot_size": _get_underlying_lot_size(symbol),
        "available_expiries": available_expiries,
        "rows": [],
        "summary": {
            "call_oi_sum": 0,
            "put_oi_sum": 0,
            "oi_diff": 0,
            "call_change_oi_sum": 0,
            "put_change_oi_sum": 0,
            "change_oi_diff": 0,
            "pcr": None,
            "change_pcr": None,
        },
        "note": "No live option rows yet. Select an expiry to view OI / IV data as soon as it becomes available.",
    }


def _build_latest_oi_snapshot_section(symbol):
    db = SessionLocal()
    try:
        snapshot = (
            db.query(OiSnapshot)
            .filter(OiSnapshot.symbol == symbol)
            .order_by(OiSnapshot.captured_at.desc())
            .first()
        )
        if not snapshot or not snapshot.row_details_json:
            return None

        try:
            rows = json.loads(snapshot.row_details_json)
        except Exception:
            rows = []
        if not isinstance(rows, list) or not rows:
            return None

        expiry = str(snapshot.expiry or "").strip() or None
        available_expiries = _future_only_expiry_values(
            [expiry] + _get_available_option_expiries(symbol)
        )
        if expiry and expiry not in available_expiries:
            available_expiries = [expiry] + [value for value in available_expiries if value != expiry]

        captured_at_text = snapshot.captured_at.strftime("%d-%b-%Y %H:%M:%S") if snapshot.captured_at else "unknown time"
        return {
            "symbol": symbol,
            "source": snapshot.source or "FYERS MarketData",
            "feed": "snapshot_bootstrap",
            "underlying": snapshot.underlying,
            "atm": snapshot.atm,
            "expiry": expiry,
            "selected_expiry": expiry,
            "last_fetched_at": snapshot.captured_at.isoformat() if snapshot.captured_at else None,
            "lot_size": _get_underlying_lot_size(symbol),
            "available_expiries": available_expiries,
            "rows": rows,
            "summary": {
                "call_oi_sum": int(snapshot.call_oi_sum or 0),
                "put_oi_sum": int(snapshot.put_oi_sum or 0),
                "oi_diff": int(snapshot.oi_diff or 0),
                "call_change_oi_sum": int(snapshot.call_change_oi_sum or 0),
                "put_change_oi_sum": int(snapshot.put_change_oi_sum or 0),
                "change_oi_diff": int(snapshot.change_oi_diff or 0),
                "pcr": snapshot.pcr,
                "change_pcr": snapshot.change_pcr,
            },
            "note": f"Loaded from the latest OI snapshot captured at {captured_at_text}. Live data will refresh shortly.",
        }
    except Exception as exc:
        logger.debug("Latest OI snapshot bootstrap failed for %s: %s", symbol, exc)
        return None
    finally:
        db.close()


def _safe_oi_side_value(option_side):
    if not isinstance(option_side, dict):
        return None
    return {
        "ltp": _to_float(option_side.get("lastPrice") or option_side.get("ltp") or 0.0),
        "iv": _to_float(option_side.get("impliedVolatility") or option_side.get("IV") or 0.0),
        "oi": int(_to_float(option_side.get("openInterest") or option_side.get("OI") or 0.0)),
        "change_oi": int(_to_float(option_side.get("changeinOpenInterest") or option_side.get("changeInOI") or 0.0)),
        "volume": int(_to_float(option_side.get("totalTradedVolume") or option_side.get("volume") or 0.0)),
        "bid": _to_float(option_side.get("bidprice") or option_side.get("bidPrice") or option_side.get("buyPrice1") or 0.0),
        "ask": _to_float(option_side.get("askPrice") or option_side.get("sellPrice1") or 0.0),
    }


def _fetch_nse_option_chain(symbol, selected_expiry=None):
    try:
        _nse_session.get("https://www.nseindia.com", headers=_nse_headers, timeout=8)
        _nse_session.get(f"https://www.nseindia.com/option-chain?symbol={urllib.parse.quote(symbol)}", headers=_nse_headers, timeout=8)
        resp = _nse_session.get(
            f"https://www.nseindia.com/api/option-chain-indices?symbol={urllib.parse.quote(symbol)}",
            headers=_nse_headers,
            timeout=12,
        )
        resp.raise_for_status()
        payload = resp.json()
        records = payload.get("records", {}) if isinstance(payload, dict) else {}
        underlying = _to_float(records.get("underlyingValue") or records.get("underlying") or 0.0)
        expiry_dates = records.get("expiryDates", []) or []
        raw_rows = records.get("data", []) or []
        if not raw_rows:
            return None

        expiry, available_expiries = _choose_expiry_value(
            expiry_dates,
            selected_expiry=selected_expiry,
            underlying_symbol=symbol,
        )
        expiry_rows = [row for row in raw_rows if str(row.get("expiryDate") or "").strip() == expiry] if expiry else list(raw_rows)
        if not expiry_rows:
            expiry_rows = list(raw_rows)

        strikes = sorted({
            _to_float(row.get("strikePrice"), 0.0)
            for row in expiry_rows
            if _to_float(row.get("strikePrice"), 0.0) > 0
        })
        if not strikes:
            return None

        atm = min(strikes, key=lambda strike: abs(strike - underlying)) if underlying > 0 else strikes[len(strikes) // 2]
        window_strikes = _select_symmetric_strikes(strikes, atm, each_side=_OI_LADDER_STRIKES_PER_SIDE)

        rows = []
        call_oi_sum = call_chg_oi_sum = put_oi_sum = put_chg_oi_sum = 0

        for strike in window_strikes:
            row = next((item for item in expiry_rows if _to_float(item.get("strikePrice"), 0.0) == strike), {})
            ce = _safe_oi_side_value(row.get("CE"))
            pe = _safe_oi_side_value(row.get("PE"))

            ce_greeks = _black_scholes_greeks(underlying, strike, ce["iv"] if ce else 0.0, row.get("expiryDate") or expiry, "CE") if ce else {"delta": None, "gamma": None, "theta": None, "vega": None}
            pe_greeks = _black_scholes_greeks(underlying, strike, pe["iv"] if pe else 0.0, row.get("expiryDate") or expiry, "PE") if pe else {"delta": None, "gamma": None, "theta": None, "vega": None}

            call_oi_sum += ce["oi"] if ce else 0
            call_chg_oi_sum += ce["change_oi"] if ce else 0
            put_oi_sum += pe["oi"] if pe else 0
            put_chg_oi_sum += pe["change_oi"] if pe else 0

            rows.append({
                "strike": strike,
                "is_atm": strike == atm,
                "call": {**(ce or {}), **ce_greeks},
                "put": {**(pe or {}), **pe_greeks},
            })

        return {
            "symbol": symbol,
            "source": "NSE",
            "feed": "nse_option_chain",
            "underlying": underlying,
            "atm": atm,
            "expiry": expiry,
            "selected_expiry": expiry,
            "last_fetched_at": datetime.now().isoformat(),
            "lot_size": _get_underlying_lot_size(symbol),
            "available_expiries": _future_only_expiry_values(
                list(available_expiries) + _get_available_option_expiries(symbol)
            ),
            "rows": rows,
            "summary": {
                "call_oi_sum": call_oi_sum,
                "put_oi_sum": put_oi_sum,
                "oi_diff": put_oi_sum - call_oi_sum,
                "call_change_oi_sum": call_chg_oi_sum,
                "put_change_oi_sum": put_chg_oi_sum,
                "change_oi_diff": put_chg_oi_sum - call_chg_oi_sum,
                "pcr": round(put_oi_sum / call_oi_sum, 4) if call_oi_sum else None,
                "change_pcr": round(put_chg_oi_sum / call_chg_oi_sum, 4) if call_chg_oi_sum else None,
            },
            "note": None,
        }
    except Exception as e:
        logger.debug("NSE option-chain fetch failed for %s: %s", symbol, e)
        return None


def _fetch_oi_dashboard(symbol=None, expiry=None):
    def _build_oi_section_from_rows(symbol, rows, underlying_ltp=None, source="FYERS Option Fallback", note=None, db=None, expiry=None):
        if not rows:
            return None

        local_db = db
        close_db = False
        if local_db is None:
            local_db = SessionLocal()
            close_db = True

        strikes = sorted({
            _to_float(row.get("strike_price"), 0.0)
            for row in rows
            if _to_float(row.get("strike_price"), 0.0) > 0
        })
        if not strikes:
            if close_db:
                local_db.close()
            return None

        available_expiries = _future_only_expiry_values([row.get("expiry_date") for row in rows])
        chosen_expiry, available_expiries = _choose_expiry_value(
            available_expiries,
            selected_expiry=expiry,
            underlying_symbol=symbol,
        )
        if chosen_expiry:
            rows = [row for row in rows if str(row.get("expiry_date") or "").strip() == chosen_expiry]
            strikes = sorted({
                _to_float(row.get("strike_price"), 0.0)
                for row in rows
                if _to_float(row.get("strike_price"), 0.0) > 0
            })
            if not strikes:
                if close_db:
                    local_db.close()
                return None

        atm = min(strikes, key=lambda value: abs(value - (underlying_ltp or value))) if underlying_ltp else strikes[len(strikes) // 2]
        selected_strikes = _select_symmetric_strikes(strikes, atm, each_side=_OI_LADDER_STRIKES_PER_SIDE)
        selected_set = set(selected_strikes)
        previous_row_map = _previous_oi_snapshot_row_map(local_db, symbol)

        rows_by_strike = {}
        call_oi_sum = put_oi_sum = 0
        call_chg_oi_sum = put_chg_oi_sum = 0
        for row in rows:
            strike = _to_float(row.get("strike_price"), 0.0)
            if strike not in selected_set:
                continue
            entry = rows_by_strike.setdefault(strike, {"strike": strike, "call": None, "put": None})
            side_key = "call" if str(row.get("instrument_type", "")).upper() == "CE" else "put"
            symbol_key = row.get("symbol")
            change_oi = int(_to_float(row.get("change_oi", 0.0), 0.0))
            if change_oi == 0 and symbol_key:
                change_oi = _resolve_row_change_oi(
                    local_db,
                    symbol,
                    strike,
                    side_key,
                    row.get("oi", 0.0),
                    previous_row_map=previous_row_map,
                    market_symbol=str(symbol_key),
                    current_timestamp=row.get("timestamp"),
                )
            payload = {
                "ltp": (
                    _to_float(row.get("ltp"), None)
                    if row.get("ltp") not in (None, "", "None")
                    else None
                ),
                "iv": _pick_option_iv(underlying_ltp, strike, row.get("expiry_date"), side_key, row),
                "oi": int(_to_float(row.get("oi", 0.0))),
                "change_oi": change_oi,
                "volume": int(_to_float(row.get("volume", 0.0))),
                "bid": (
                    _to_float(row.get("bid"), None)
                    if row.get("bid") not in (None, "", "None")
                    else None
                ),
                "ask": (
                    _to_float(row.get("ask"), None)
                    if row.get("ask") not in (None, "", "None")
                    else None
                ),
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
            }
            entry[side_key] = payload
            if side_key == "call":
                call_oi_sum += payload["oi"]
                call_chg_oi_sum += payload["change_oi"]
            else:
                put_oi_sum += payload["oi"]
                put_chg_oi_sum += payload["change_oi"]

        if not rows_by_strike:
            if close_db:
                local_db.close()
            return None

        section_rows = []
        for strike in sorted(rows_by_strike.keys()):
            section_rows.append({
                "strike": strike,
                "is_atm": strike == atm,
                "call": rows_by_strike[strike]["call"],
                "put": rows_by_strike[strike]["put"],
            })

        latest_ts = _latest_rows_timestamp_iso(rows, default_to_now=True)
        latest_dt = _parse_row_timestamp(latest_ts)
        option_age_sec = int((datetime.now() - latest_dt).total_seconds()) if latest_dt else None

        section = {
            "symbol": symbol,
            "source": source,
            "feed": "option_rows",
            "underlying": underlying_ltp,
            "option_age_sec": option_age_sec,
            "atm": atm,
            "expiry": chosen_expiry,
            "selected_expiry": chosen_expiry,
            "last_fetched_at": latest_ts,
            "lot_size": _get_underlying_lot_size(symbol),
            "available_expiries": _future_only_expiry_values(
                list(available_expiries) + _get_available_option_expiries(symbol)
            ),
            "rows": section_rows,
            "summary": {
                "call_oi_sum": call_oi_sum,
                "put_oi_sum": put_oi_sum,
                "oi_diff": put_oi_sum - call_oi_sum,
                "call_change_oi_sum": call_chg_oi_sum,
                "put_change_oi_sum": put_chg_oi_sum,
                "change_oi_diff": put_chg_oi_sum - call_chg_oi_sum,
                "pcr": round(put_oi_sum / call_oi_sum, 4) if call_oi_sum else None,
                "change_pcr": round(put_chg_oi_sum / call_chg_oi_sum, 4) if call_chg_oi_sum else None,
            },
            "note": note,
        }
        if call_chg_oi_sum == 0 and put_chg_oi_sum == 0:
            snap_call, snap_put, snap_diff, snap_pcr = _derive_snapshot_deltas(
                local_db,
                symbol,
                section["summary"],
            )
            if snap_call or snap_put:
                section["summary"].update({
                    "call_change_oi_sum": snap_call,
                    "put_change_oi_sum": snap_put,
                    "change_oi_diff": snap_diff,
                    "change_pcr": snap_pcr,
                })
        if close_db:
            local_db.close()
        return section

    def _build_from_market_data(underlying_symbol):
        db = SessionLocal()
        try:
            idx_hint = "Nifty 50" if underlying_symbol == "NIFTY" else "Nifty Bank"
            index_row = (
                db.query(MarketData)
                .filter(MarketData.symbol.ilike(f"%{idx_hint}%"))
                .order_by(MarketData.timestamp.desc())
                .first()
            )
            underlying = _to_float(index_row.last_traded_price if index_row else 0.0, 0.0)
            now = datetime.now()
            recent_cutoff = now - timedelta(seconds=max(90, int(_MARKETDATA_OPTION_MAX_AGE_SEC * 6)))

            if underlying_symbol == "NIFTY":
                option_symbol_filter = and_(
                    MarketData.symbol.like("NSE:NIFTY%"),
                    ~MarketData.symbol.like("NSE:BANKNIFTY%"),
                    ~MarketData.symbol.like("NSE:FINNIFTY%"),
                    ~MarketData.symbol.like("NSE:MIDCPNIFTY%"),
                )
            elif underlying_symbol == "BANKNIFTY":
                option_symbol_filter = or_(
                    MarketData.symbol.like("NSE:BANKNIFTY%"),
                    MarketData.symbol.like("NSE:NIFTYBANK%"),
                )
            else:
                option_symbol_filter = MarketData.trading_symbol.ilike(f"%{underlying_symbol}%")

            subquery = (
                db.query(
                    MarketData.symbol,
                    func.max(MarketData.timestamp).label("max_timestamp"),
                )
                .filter(
                    MarketData.exchange_seg == "nse_fo",
                    MarketData.instrument_type.in_(["CE", "PE"]),
                    option_symbol_filter,
                    MarketData.timestamp >= recent_cutoff,
                )
                .group_by(MarketData.symbol)
                .subquery()
            )

            latest_rows = (
                db.query(MarketData)
                .join(
                    subquery,
                    (MarketData.symbol == subquery.c.symbol)
                    & (MarketData.timestamp == subquery.c.max_timestamp),
                )
                .all()
            )
            if not latest_rows:
                return None

            fresh_rows = [
                row
                for row in latest_rows
                if row.timestamp and (now - row.timestamp).total_seconds() <= _MARKETDATA_OPTION_MAX_AGE_SEC
            ]
            if not fresh_rows:
                return None
            latest_option_timestamp = max((row.timestamp for row in fresh_rows if row.timestamp), default=None)
            option_age_sec = int((now - latest_option_timestamp).total_seconds()) if latest_option_timestamp else None

            available_expiries = []
            normalized_rows = []
            for row in fresh_rows:
                row_expiry_label = _normalize_future_expiry_label(row.expiry_date, row.trading_symbol)
                if not row_expiry_label:
                    continue
                normalized_rows.append((row, row_expiry_label))
                if row_expiry_label not in available_expiries:
                    available_expiries.append(row_expiry_label)

            available_expiries = _future_only_expiry_values(available_expiries)
            chosen_expiry, available_expiries = _choose_expiry_value(
                available_expiries,
                selected_expiry=expiry,
                underlying_symbol=underlying_symbol,
            )

            rows_by_strike = {}
            call_oi_sum = put_oi_sum = 0
            strikes = []
            for row, row_expiry_label in normalized_rows:
                if chosen_expiry and row_expiry_label != chosen_expiry:
                    continue
                strike = _to_float(row.strike_price, 0.0)
                if strike <= 0:
                    continue
                entry = rows_by_strike.setdefault(strike, {"strike": strike, "call": None, "put": None})
                side_key = "call" if str(row.instrument_type).upper() == "CE" else "put"
                payload = {
                    "ltp": _to_float(row.last_traded_price, 0.0),
                    "iv": _pick_option_iv(underlying, strike, row_expiry_label, side_key, {
                        "ltp": row.last_traded_price,
                        "iv": None,
                    }),
                    "oi": int(_to_float(row.oi, 0.0)),
                    "change_oi": None,
                    "volume": int(_to_float(row.volume, 0.0)),
                    "bid": _to_float(row.bid_price, 0.0),
                    "ask": _to_float(row.ask_price, 0.0),
                    "delta": None,
                    "gamma": None,
                    "theta": None,
                    "vega": None,
                }
                entry[side_key] = payload
                strikes.append(strike)
                if side_key == "call":
                    call_oi_sum += payload["oi"]
                else:
                    put_oi_sum += payload["oi"]

            if not rows_by_strike:
                return None

            unique_strikes = sorted(set(strikes))
            atm = min(unique_strikes, key=lambda value: abs(value - underlying)) if underlying > 0 and unique_strikes else None
            selected_strikes = _select_symmetric_strikes(unique_strikes, atm, each_side=_OI_LADDER_STRIKES_PER_SIDE) if unique_strikes else []
            selected_rows = []
            for strike in sorted(selected_strikes):
                selected_rows.append({
                    "strike": strike,
                    "is_atm": atm is not None and strike == atm,
                    "call": rows_by_strike.get(strike, {}).get("call"),
                    "put": rows_by_strike.get(strike, {}).get("put"),
                })

            result = {
                "symbol": underlying_symbol,
                "source": "FYERS MarketData",
                "feed": "market_data",
                "underlying": underlying,
                "option_age_sec": option_age_sec,
                "atm": atm,
                "expiry": chosen_expiry,
                "selected_expiry": chosen_expiry,
                "last_fetched_at": latest_option_timestamp.isoformat() if latest_option_timestamp else None,
                "lot_size": _get_underlying_lot_size(underlying_symbol),
                "available_expiries": _future_only_expiry_values(
                    list(available_expiries) + _get_available_option_expiries(underlying_symbol)
                ),
                "rows": selected_rows,
                "summary": {
                    "call_oi_sum": call_oi_sum,
                    "put_oi_sum": put_oi_sum,
                    "oi_diff": put_oi_sum - call_oi_sum,
                    "call_change_oi_sum": None,
                    "put_change_oi_sum": None,
                    "change_oi_diff": None,
                    "pcr": round(put_oi_sum / call_oi_sum, 4) if call_oi_sum else None,
                    "change_pcr": None,
                },
                "note": "Live option rows are coming from FYERS market-data because the external option-chain feed is unavailable.",
            }
            return result
        finally:
            db.close()

    def _latest_index_ltp(symbol_fragment):
        db = SessionLocal()
        try:
            rows = (
                db.query(MarketData)
                .filter(MarketData.symbol.ilike(f"%{symbol_fragment}%"))
                .order_by(MarketData.timestamp.desc())
                .limit(1)
                .all()
            )
            if rows:
                ltp = _to_float(rows[0].last_traded_price, 0.0)
                if ltp > 0:
                    return ltp
        finally:
            db.close()

        session = _load_session()
        if not session:
            return 0.0

        try:
            quote_map = _fetch_fyers_quotes_batch(
                session=session,
                exchange_seg="nse_cm",
                trading_symbols=[symbol_fragment],
                timeout_sec=3.5,
                retries=1,
                cache_ttl_sec=2.0,
            )
            quote = quote_map.get(symbol_fragment) if isinstance(quote_map, dict) else None
            if quote is None and isinstance(quote_map, dict) and quote_map:
                quote = next(iter(quote_map.values()))
            return _to_float((quote or {}).get("ltp"), 0.0)
        except Exception:
            return 0.0

    def _load_session():
        return _load_dashboard_session()

    dashboards = []
    symbols = [(symbol, "Nifty 50")] if symbol else [("NIFTY", "Nifty 50"), ("BANKNIFTY", "Nifty Bank")]
    for symbol_name, db_fragment in symbols:
        section = _build_from_market_data(symbol_name)
        has_rows = bool((section or {}).get("rows"))
        option_age = _to_float((section or {}).get("option_age_sec"), _MARKETDATA_OPTION_MAX_AGE_SEC + 1.0)
        should_refresh_marketdata = (section is None) or not has_rows
        if should_refresh_marketdata:
            session = _load_session()
            if session:
                ltp = _to_float((section or {}).get("underlying"), 0.0)
                if ltp <= 0:
                    ltp = _latest_index_ltp(db_fragment)
                if ltp > 0:
                    fallback_rows = _get_option_chain_fallback(
                        session,
                        ltp,
                        underlying_symbol=symbol_name,
                        selected_expiry=expiry,
                    )
                    if fallback_rows:
                        _persist_market_data_rows(fallback_rows)
                        refreshed = _build_oi_section_from_rows(
                            symbol_name,
                            fallback_rows,
                            underlying_ltp=ltp,
                            source="FYERS Option Fallback",
                            note="Live option rows are coming from FYERS because the NSE option-chain feed is unavailable.",
                            expiry=expiry,
                        )
                        if refreshed:
                            section = refreshed
        if not section:
            session = _load_session()
            if session:
                ltp = _latest_index_ltp(db_fragment)
                if ltp > 0:
                    fallback_rows = _get_option_chain_fallback(
                        session,
                        ltp,
                        underlying_symbol=symbol_name,
                        selected_expiry=expiry,
                    )
                    if fallback_rows:
                        _persist_market_data_rows(fallback_rows)
                    section = _build_oi_section_from_rows(
                        symbol_name,
                        fallback_rows,
                        underlying_ltp=ltp,
                        source="FYERS Option Fallback",
                        note="Live option rows are coming from FYERS because the NSE option-chain feed is unavailable.",
                        expiry=expiry,
                    )
        current_age = _to_float((section or {}).get("option_age_sec"), None)
        if (
            section
            and str((section or {}).get("feed", "")).lower() == "market_data"
            and current_age is not None
            and current_age >= 120.0
        ):
            snapshot_section = _build_latest_oi_snapshot_section(symbol_name)
            if snapshot_section:
                snapshot_section["source"] = "Latest stored snapshot"
                snapshot_section["feed"] = "snapshot_fallback"
                snapshot_section["note"] = (
                    f"Live option refresh is delayed ({int(current_age)}s old). "
                    "Showing the latest stored snapshot."
                )
                section = snapshot_section
            else:
                section["feed"] = "market_data_stale"
                section["note"] = f"Live option refresh delayed. Showing cached rows from {int(current_age)}s ago."
        if not section:
            snapshot_section = _build_latest_oi_snapshot_section(symbol_name)
            if snapshot_section:
                snapshot_section["source"] = "Latest stored snapshot"
                snapshot_section["feed"] = "snapshot_fallback"
                snapshot_section["note"] = "Live option data is temporarily unavailable. Showing the latest stored snapshot."
                section = snapshot_section
        if not section:
            section = _build_empty_oi_section(symbol_name, expiry=expiry)
        if section:
            dashboards.append(section)
    if not dashboards:
        logger.warning("OI dashboard returned no rows from NSE or FYERS fallback; seeding expiry lists from master data.")
        if symbol:
            return _build_empty_oi_section(symbol, expiry=expiry)
        return [_build_empty_oi_section("NIFTY", expiry=expiry), _build_empty_oi_section("BANKNIFTY", expiry=expiry)]
    if symbol:
        return dashboards[0] if dashboards else None
    return dashboards


def _apply_ws_strict_oi_semantics(section):
    if not isinstance(section, dict):
        return section

    rows = section.get("rows") or []
    feed = str(section.get("feed", "")).strip().lower()
    strict_ws_only = str(os.getenv("OI_STRICT_WEBSOCKET_ONLY", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    use_feed_oi = (feed == "market_data") if strict_ws_only else True
    call_oi_sum = 0
    put_oi_sum = 0
    call_change_oi_sum = 0
    put_change_oi_sum = 0
    oi_available = False
    change_oi_available = False

    for row in rows:
        if not isinstance(row, dict):
            continue
        for side_key in ("call", "put"):
            leg = row.get(side_key)
            if not isinstance(leg, dict):
                continue
            leg_oi = int(_to_float(leg.get("oi"), 0.0))
            if use_feed_oi and leg_oi > 0:
                leg["oi"] = leg_oi
                oi_available = True
                if side_key == "call":
                    call_oi_sum += leg_oi
                else:
                    put_oi_sum += leg_oi
            else:
                leg["oi"] = None
            if strict_ws_only:
                # Keep strict mode behavior available behind env flag.
                leg["change_oi"] = None
                continue
            raw_change = leg.get("change_oi")
            if raw_change in (None, "", "None"):
                leg["change_oi"] = None
                continue
            parsed_change = int(_to_float(raw_change, 0.0))
            leg["change_oi"] = parsed_change
            change_oi_available = True
            if side_key == "call":
                call_change_oi_sum += parsed_change
            else:
                put_change_oi_sum += parsed_change

    summary = section.get("summary") or {}
    section["summary"] = summary
    if oi_available:
        summary["call_oi_sum"] = call_oi_sum
        summary["put_oi_sum"] = put_oi_sum
        summary["oi_diff"] = put_oi_sum - call_oi_sum
        summary["pcr"] = round(put_oi_sum / call_oi_sum, 4) if call_oi_sum else None
    else:
        summary["call_oi_sum"] = None
        summary["put_oi_sum"] = None
        summary["oi_diff"] = None
        summary["pcr"] = None
    if strict_ws_only:
        summary["call_change_oi_sum"] = None
        summary["put_change_oi_sum"] = None
        summary["change_oi_diff"] = None
        summary["change_pcr"] = None
    elif change_oi_available:
        summary["call_change_oi_sum"] = call_change_oi_sum
        summary["put_change_oi_sum"] = put_change_oi_sum
        summary["change_oi_diff"] = put_change_oi_sum - call_change_oi_sum
        summary["change_pcr"] = round(put_change_oi_sum / call_change_oi_sum, 4) if call_change_oi_sum else None
    else:
        summary["call_change_oi_sum"] = None
        summary["put_change_oi_sum"] = None
        summary["change_oi_diff"] = None
        summary["change_pcr"] = None

    section["oi_available"] = bool(oi_available)
    if not oi_available:
        extra_note = (
            "OI/Change OI unavailable from websocket stream right now."
            if strict_ws_only
            else "OI/Change OI unavailable in current live feed."
        )
        existing_note = str(section.get("note") or "").strip()
        section["note"] = f"{existing_note} {extra_note}".strip() if existing_note else extra_note
    return section


def _annotate_oi_section_freshness(section, live_snapshot=None):
    if not isinstance(section, dict):
        return section

    live = live_snapshot or _get_live_feed_snapshot()
    now = datetime.now()
    last_fetched_dt = _parse_row_timestamp(section.get("last_fetched_at"))
    option_age_sec = section.get("option_age_sec")
    if option_age_sec is None and last_fetched_dt:
        option_age_sec = int((now - last_fetched_dt).total_seconds())

    # Freshness is websocket-heartbeat driven (not snapshot age).
    stale_age_sec = live.get("stale_age_sec")
    is_stale = bool(live.get("is_stale"))
    last_live_tick_at = live.get("last_live_tick_at")

    section["option_age_sec"] = option_age_sec
    section["stale_age_sec"] = stale_age_sec
    section["is_stale"] = is_stale
    section["last_live_tick_at"] = last_live_tick_at
    if last_live_tick_at:
        section["last_fetched_at"] = last_live_tick_at
    return section


def _prepare_oi_rows_for_ui(rows):
    live = _get_live_feed_snapshot()
    if isinstance(rows, dict):
        return _annotate_oi_section_freshness(_apply_ws_strict_oi_semantics(rows), live_snapshot=live)
    if isinstance(rows, list):
        return [
            _annotate_oi_section_freshness(_apply_ws_strict_oi_semantics(section), live_snapshot=live)
            for section in rows
            if isinstance(section, dict)
        ]
    return rows


def capture_oi_snapshot():
    global _oi_snapshot_capture_in_progress

    def _section_list(raw_sections):
        if isinstance(raw_sections, list):
            return [section for section in raw_sections if isinstance(section, dict)]
        if isinstance(raw_sections, dict):
            return [raw_sections]
        return []

    def _is_capture_candidate(section):
        if not section or not section.get("rows"):
            return False
        feed = str(section.get("feed", "")).strip().lower()
        # Never capture purely seeded/bootstrap rows as live snapshots.
        if feed in {"expiry_seed", "snapshot_fallback", "snapshot_bootstrap"}:
            return False
        age = _to_float(section.get("option_age_sec"), None)
        if feed in {"market_data", "market_data_stale"} and age is not None and age > 180:
            return False
        return True

    with _oi_snapshot_capture_lock:
        if _oi_snapshot_capture_in_progress:
            logger.info("Skipping OI snapshot capture because previous capture is still running.")
            return []
        _oi_snapshot_capture_in_progress = True

    try:
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            db = SessionLocal()
            try:
                captured_at = datetime.now()
                dashboards = _cached_oi_dashboard_sections()
                if not dashboards:
                    dashboards = _fetch_oi_dashboard_with_timeout(timeout_sec=8.0)
                sections = _section_list(dashboards)
                capture_sections = [section for section in sections if _is_capture_candidate(section)]

                if not capture_sections:
                    dashboards = _fetch_oi_dashboard_with_timeout(timeout_sec=10.0)
                    sections = _section_list(dashboards)
                    capture_sections = [section for section in sections if _is_capture_candidate(section)]

                if not capture_sections:
                    # Final fallback: bootstrap from last persisted snapshots to keep history continuity.
                    fallback_sections = []
                    for symbol_name in ("NIFTY", "BANKNIFTY"):
                        section = _build_latest_oi_snapshot_section(symbol_name)
                        if section:
                            section["feed"] = "snapshot_fallback"
                            fallback_sections.append(section)
                    capture_sections = [section for section in fallback_sections if (section.get("rows") or [])]
                    dashboards = fallback_sections

                captured_rows = []

                # #region agent log (H3: COI live missing because snapshots aren't captured)
                try:
                    dash_stats = []
                    if isinstance(dashboards, list):
                        for s in dashboards[:3]:
                            if not isinstance(s, dict):
                                continue
                            rows_len = len(s.get("rows") or [])
                            dash_stats.append({
                                "symbol": s.get("symbol"),
                                "feed": s.get("feed"),
                                "rows_len": rows_len,
                                "summary": {
                                    "call_change_oi_sum": (s.get("summary") or {}).get("call_change_oi_sum"),
                                    "put_change_oi_sum": (s.get("summary") or {}).get("put_change_oi_sum"),
                                }
                            })
                    _agent_debug_log(
                        hypothesis_id="H3",
                        location="dashboard/dashboard.py:capture_oi_snapshot",
                        message="capture_oi_snapshot_dashboards_loaded",
                        data={
                            "dashboards_len": len(dashboards) if isinstance(dashboards, list) else None,
                            "dash_stats": dash_stats,
                            "capture_sections_len": len(capture_sections),
                        },
                    )
                except Exception:
                    pass
                # #endregion

                for section in capture_sections:
                    source = str(section.get("source", "NSE"))

                    summary = section.get("summary", {}) or {}
                    call_change_oi_sum, put_change_oi_sum, change_oi_diff, change_pcr = _derive_snapshot_deltas(
                        db,
                        str(section.get("symbol", "UNKNOWN")),
                        summary,
                    )
                    snapshot = OiSnapshot(
                        captured_at=captured_at,
                        symbol=section.get("symbol", "UNKNOWN"),
                        source=source,
                        expiry=section.get("expiry"),
                        underlying=section.get("underlying"),
                        atm=section.get("atm"),
                        call_oi_sum=int(summary.get("call_oi_sum") or 0),
                        put_oi_sum=int(summary.get("put_oi_sum") or 0),
                        oi_diff=int(summary.get("oi_diff") or 0),
                        call_change_oi_sum=call_change_oi_sum,
                        put_change_oi_sum=put_change_oi_sum,
                        change_oi_diff=change_oi_diff,
                        pcr=summary.get("pcr"),
                        change_pcr=change_pcr,
                        row_details_json=json.dumps(section.get("rows", []), default=str),
                    )
                    db.add(snapshot)
                    captured_rows.append({
                        "symbol": snapshot.symbol,
                        "captured_at": snapshot.captured_at.isoformat(),
                        "oi_diff": snapshot.oi_diff,
                        "change_oi_diff": snapshot.change_oi_diff,
                    })

                if captured_rows:
                    db.commit()
                else:
                    logger.warning("OI snapshot capture produced no rows from live sections.")

                # #region agent log (H3: confirm whether any snapshot rows were persisted)
                try:
                    _agent_debug_log(
                        hypothesis_id="H3",
                        location="dashboard/dashboard.py:capture_oi_snapshot",
                        message="capture_oi_snapshot_persisted",
                        data={
                            "captured_rows_len": len(captured_rows) if isinstance(captured_rows, list) else None,
                            "captured_rows_sample": (captured_rows[0] if captured_rows else None),
                        },
                    )
                except Exception:
                    pass
                # #endregion
                return captured_rows
            except OperationalError as exc:
                db.rollback()
                message = str(exc).lower()
                if "database is locked" in message and attempt < max_attempts:
                    wait_seconds = 0.12 * attempt
                    logger.warning(
                        "OI snapshot write hit SQLite lock (attempt %s/%s). Retrying in %.2fs...",
                        attempt,
                        max_attempts,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue
                logger.error("OI snapshot capture failed: %s", exc)
                return []
            except Exception as exc:
                db.rollback()
                logger.error("OI snapshot capture failed: %s", exc)
                return []
            finally:
                db.close()
        return []
    finally:
        with _oi_snapshot_capture_lock:
            _oi_snapshot_capture_in_progress = False


def _serialize_oi_snapshot_row(row):
    return {
        "symbol": row.symbol,
        "captured_at": row.captured_at.isoformat() if row.captured_at else None,
        "source": row.source,
        "expiry": row.expiry,
        "underlying": row.underlying,
        "atm": row.atm,
        "call_oi_sum": row.call_oi_sum,
        "put_oi_sum": row.put_oi_sum,
        "oi_diff": row.oi_diff,
        "call_change_oi_sum": row.call_change_oi_sum,
        "put_change_oi_sum": row.put_change_oi_sum,
        "change_oi_diff": row.change_oi_diff,
        "pcr": row.pcr,
        "change_pcr": row.change_pcr,
    }


def _normalize_oi_history_date(history_date):
    if history_date is None:
        return None
    if isinstance(history_date, datetime):
        return history_date.date()
    if isinstance(history_date, date):
        return history_date

    value = str(history_date).strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    try:
        return date.fromisoformat(value[:10])
    except Exception:
        return None


def _oi_history_day_bounds(history_date):
    start = datetime.combine(history_date, datetime.min.time())
    return start, start + timedelta(days=1)


def get_oi_history(symbol=None, expiry=None, limit=500, history_date=None):
    db = SessionLocal()
    try:
        target_date = _normalize_oi_history_date(history_date) or datetime.now().date()
        start_dt, end_dt = _oi_history_day_bounds(target_date)
        base_query = db.query(OiSnapshot).order_by(OiSnapshot.captured_at.asc())
        if symbol:
            base_query = base_query.filter(OiSnapshot.symbol == symbol)
        scoped_query = base_query
        if expiry:
            scoped_query = scoped_query.filter(OiSnapshot.expiry == expiry)
        rows = scoped_query.filter(OiSnapshot.captured_at >= start_dt, OiSnapshot.captured_at < end_dt).all()
        if expiry and not rows:
            rows = base_query.filter(OiSnapshot.captured_at >= start_dt, OiSnapshot.captured_at < end_dt).all()
        if limit and len(rows) > limit:
            rows = rows[-limit:]

        return [_serialize_oi_snapshot_row(row) for row in rows]
    finally:
        db.close()


def get_oi_history_dates(symbol=None, expiry=None, limit=365):
    db = SessionLocal()
    try:
        base_query = db.query(OiSnapshot.captured_at).order_by(OiSnapshot.captured_at.asc())
        if symbol:
            base_query = base_query.filter(OiSnapshot.symbol == symbol)

        query = base_query.filter(OiSnapshot.expiry == expiry) if expiry else base_query
        rows = query.all()
        if expiry and not rows:
            rows = base_query.all()
        counts = {}
        for (captured_at,) in rows:
            if not captured_at:
                continue
            history_date = captured_at.date().isoformat()
            counts[history_date] = counts.get(history_date, 0) + 1

        ordered = sorted(counts.items(), key=lambda item: item[0], reverse=True)
        if limit and len(ordered) > limit:
            ordered = ordered[:limit]

        today = datetime.now().date().isoformat()
        return [
            {
                "date": history_date,
                "count": count,
                "is_today": history_date == today,
            }
            for history_date, count in ordered
        ]
    finally:
        db.close()


def _extract_live_indices_from_nse():
    try:
        _nse_session.get("https://www.nseindia.com", headers=_nse_headers, timeout=8)
        resp = _nse_session.get("https://www.nseindia.com/api/allIndices", headers=_nse_headers, timeout=12)
        resp.raise_for_status()
        payload = resp.json()
        raw_rows = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(raw_rows, dict):
            raw_rows = raw_rows.get("data") or raw_rows.get("records") or []
        if not isinstance(raw_rows, list):
            raw_rows = []

        normalized_map = {}
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            index_name = row.get("index") or row.get("name") or row.get("indexName")
            if not index_name:
                continue
            normalized_map[_normalize_index_name(index_name)] = row

        results = []
        for target in _TARGET_NSE_INDICES:
            matched_row = None
            for alias in target["aliases"]:
                matched_row = normalized_map.get(_normalize_index_name(alias))
                if matched_row:
                    break

            if matched_row is None:
                results.append({
                    "symbol": target["display"],
                    "ltp": None,
                    "change": None,
                    "change_pct": None,
                    "status": "unavailable",
                })
                continue

            ltp = _to_float(
                matched_row.get("last")
                or matched_row.get("lastPrice")
                or matched_row.get("close")
                or matched_row.get("value"),
                0.0,
            )
            change = _to_float(
                matched_row.get("variation")
                or matched_row.get("change")
                or matched_row.get("changePrice")
                or 0.0,
                0.0,
            )
            change_pct = _to_float(
                matched_row.get("percentChange")
                or matched_row.get("pChange")
                or matched_row.get("changePercent")
                or 0.0,
                0.0,
            )

            results.append({
                "symbol": target["display"],
                "ltp": ltp,
                "change": change,
                "change_pct": change_pct,
                "status": "live",
                "group": target.get("group", "sector"),
            })

        return results
    except Exception as e:
        logger.debug("NSE live index fetch failed: %s", e)
        return []


def _resolve_option_chain_from_fo(
    session,
    underlying_ltp,
    underlying_symbol="NIFTY",
    selected_expiry=None,
    allow_live_quotes=True,
):
    try:
        from core.instruments import InstrumentMaster
    except Exception as e:
        logger.error(f"Unable to import InstrumentMaster for option fallback: {e}")
        return {"rows": [], "expiry": None, "available_expiries": []}

    instrument_master = InstrumentMaster()
    fo_path = instrument_master.fo_file
    if os.path.exists(fo_path) and os.path.getsize(fo_path) == 0:
        try:
            os.remove(fo_path)
        except Exception:
            pass

    if not os.path.exists(fo_path):
        def _background_download():
            global _option_chain_downloading
            try:
                instrument_master.download(session, segments=("nse_fo",))
            except Exception as inner_e:
                logger.error(f"Background option master download failed: {inner_e}")
            finally:
                with _option_chain_download_lock:
                    _option_chain_downloading = False

        global _option_chain_downloading
        with _option_chain_download_lock:
            if not _option_chain_downloading:
                _option_chain_downloading = True
                threading.Thread(target=_background_download, daemon=True).start()
        return {"rows": [], "expiry": None, "available_expiries": []}

    df = instrument_master.load('nse_fo')
    if df is None or df.empty:
        # Bad/partial file; force refresh in background and return gracefully.
        try:
            if os.path.exists(fo_path):
                os.remove(fo_path)
        except Exception:
            pass
        return {"rows": [], "expiry": None, "available_expiries": []}

    # Normalize incoming file columns defensively in case vendor file format changes.
    df = df.copy()
    df.columns = [str(col).strip().replace(";", "") for col in df.columns]
    col_map = {str(col).lower(): col for col in df.columns}

    def col(name):
        return col_map.get(name.lower())

    symbol_col = col("pSymbolName")
    inst_col = col("pInstType")
    option_col = col("pOptionType")
    strike_col = col("dStrikePrice")
    expiry_col = col("lExpiryDate") or col("pExpiryDate")
    trd_symbol_col = col("pTrdSymbol")

    if not all([symbol_col, inst_col, option_col, strike_col, expiry_col, trd_symbol_col]):
        return {"rows": [], "expiry": None, "available_expiries": []}

    symbol_filter = df[symbol_col].astype(str).str.upper()
    underlying_upper = str(underlying_symbol or "NIFTY").upper()
    if underlying_upper == "BANKNIFTY":
        symbol_mask = symbol_filter.str.contains("BANKNIFTY", na=False) | symbol_filter.str.contains("NIFTY BANK", na=False)
    else:
        symbol_mask = (
            symbol_filter.str.contains(r"(?:^|[^A-Z0-9])NIFTY(?:[^A-Z0-9]|$)", na=False, regex=True)
            & ~symbol_filter.str.contains("BANKNIFTY", na=False)
            & ~symbol_filter.str.contains("FINNIFTY", na=False)
            & ~symbol_filter.str.contains("MIDCPNIFTY", na=False)
            & ~symbol_filter.str.contains("NIFTY BANK", na=False)
        )

    option_df = df[
        symbol_mask &
        df[inst_col].astype(str).str.upper().str.contains("OPT", na=False)
    ].copy()
    if option_df.empty:
        return {"rows": [], "expiry": None, "available_expiries": []}

    option_df[strike_col] = pd.to_numeric(option_df[strike_col], errors='coerce')
    option_df = option_df.dropna(subset=[strike_col])
    if option_df.empty:
        return {"rows": [], "expiry": None, "available_expiries": []}

    max_strike = option_df[strike_col].max()
    strike_divisor = 100.0 if max_strike and max_strike > 100000 else 1.0
    option_df["strike_display"] = option_df[strike_col] / strike_divisor

    option_df["expiry_text"] = option_df[expiry_col].astype(str).str.strip()
    option_df["expiry_label"] = option_df.apply(
        lambda row: _normalize_future_expiry_label(row[expiry_col], row[trd_symbol_col]),
        axis=1,
    )
    valid_expiries = _future_only_expiry_values(option_df["expiry_label"].dropna().unique())
    if not valid_expiries:
        return {"rows": [], "expiry": None, "available_expiries": []}
    nearest_expiry, valid_expiries = _choose_expiry_value(
        valid_expiries,
        selected_expiry=selected_expiry,
        underlying_symbol=underlying_symbol,
    )
    option_df = option_df[option_df["expiry_label"] == nearest_expiry]
    if option_df.empty:
        return {"rows": [], "expiry": nearest_expiry, "available_expiries": valid_expiries}

    atm_step = 100.0 if underlying_upper == "BANKNIFTY" else 50.0
    atm = round(underlying_ltp / atm_step) * atm_step if underlying_ltp > 0 else None
    strikes = sorted(option_df["strike_display"].dropna().unique())
    if not strikes:
        return {"rows": [], "expiry": nearest_expiry, "available_expiries": valid_expiries}

    if atm is None:
        mid_idx = len(strikes) // 2
        left = max(0, mid_idx - _OI_LADDER_STRIKES_PER_SIDE)
        right = min(len(strikes), mid_idx + _OI_LADDER_STRIKES_PER_SIDE + 1)
        selected_strikes = strikes[left:right]
    else:
        selected_strikes = _select_symmetric_strikes(strikes, atm, each_side=_OI_LADDER_STRIKES_PER_SIDE)

    quote_specs = []
    for strike in selected_strikes:
        for option_type in ("CE", "PE"):
            row = option_df[
                (option_df["strike_display"] == strike) &
                (option_df[option_col].astype(str).str.upper() == option_type)
            ]
            if row.empty:
                continue
            trading_symbol = str(row.iloc[0][trd_symbol_col]).strip()
            if not trading_symbol:
                continue
            quote_specs.append({
                "strike": float(strike),
                "option_type": option_type,
                "trading_symbol": trading_symbol,
            })

    quote_map = {}
    if session and allow_live_quotes and quote_specs:
        quote_map = _fetch_fyers_quotes_batch(
            session=session,
            exchange_seg="nse_fo",
            trading_symbols=[spec["trading_symbol"] for spec in quote_specs],
            timeout_sec=3.0,
            retries=1,
            cache_ttl_sec=6.0,
        )

    option_chain_row_map = {}
    if session and allow_live_quotes and quote_specs:
        option_chain_snapshot = _fetch_fyers_optionchain_snapshot(
            session=session,
            underlying_symbol=underlying_symbol,
            selected_expiry=nearest_expiry,
            strikecount=max(6, _OI_LADDER_STRIKES_PER_SIDE + 2),
        )
        for row in option_chain_snapshot.get("rows", []):
            row_strike = _to_float(row.get("strike_price"), 0.0)
            row_side = str(row.get("instrument_type") or "").upper()
            if row_strike <= 0 or row_side not in {"CE", "PE"}:
                continue
            option_chain_row_map[(row_strike, row_side)] = row

    option_rows = []
    for spec in quote_specs:
        quote = quote_map.get(spec["trading_symbol"])
        option_chain_row = option_chain_row_map.get((_to_float(spec["strike"], 0.0), spec["option_type"]))
        depth = (quote.get("depth") or {}) if quote else {}
        buy = (depth.get("buy") or [{}])[0] if depth else {}
        sell = (depth.get("sell") or [{}])[0] if depth else {}
        quote_change_oi = (
            _safe_quote_value(quote, "changeinOpenInterest", 0.0)
            or _safe_quote_value(quote, "change_in_oi", 0.0)
            or _safe_quote_value(quote, "change_oi", 0.0)
        ) if quote else 0.0

        quote_oi = int(_safe_quote_value(quote, "open_int", 0.0)) if quote else 0
        chain_oi = int(_to_float((option_chain_row or {}).get("oi"), 0.0))
        chain_change_oi = int(_to_float((option_chain_row or {}).get("change_oi"), 0.0))
        effective_oi = chain_oi if chain_oi > 0 else quote_oi
        effective_change_oi = chain_change_oi if chain_change_oi != 0 else int(_to_float(quote_change_oi, 0.0))

        chain_ltp = _to_float((option_chain_row or {}).get("ltp"), 0.0)
        chain_bid = _to_float((option_chain_row or {}).get("bid"), 0.0)
        chain_ask = _to_float((option_chain_row or {}).get("ask"), 0.0)
        chain_volume = int(_to_float((option_chain_row or {}).get("volume"), 0.0))
        chain_iv = _to_float((option_chain_row or {}).get("iv"), 0.0)

        effective_ltp = _safe_quote_value(quote, "ltp", 0.0) if quote else 0.0
        if effective_ltp <= 0 and chain_ltp > 0:
            effective_ltp = chain_ltp
        effective_bid = _to_float(buy.get("price", 0), 0.0) if quote else 0.0
        if effective_bid <= 0 and chain_bid > 0:
            effective_bid = chain_bid
        effective_ask = _to_float(sell.get("price", 0), 0.0) if quote else 0.0
        if effective_ask <= 0 and chain_ask > 0:
            effective_ask = chain_ask
        effective_volume = int(_safe_quote_value(quote, "last_volume", 0.0)) if quote else 0
        if effective_volume <= 0 and chain_volume > 0:
            effective_volume = chain_volume

        option_rows.append(
            {
                "symbol": f"nse_fo|{spec['trading_symbol']}",
                "trading_symbol": spec["trading_symbol"],
                "instrument_type": spec["option_type"],
                "strike_price": spec["strike"],
                "expiry_date": nearest_expiry,
                "ltp": effective_ltp if effective_ltp > 0 else None,
                "bid": effective_bid if effective_bid > 0 else None,
                "ask": effective_ask if effective_ask > 0 else None,
                "volume": effective_volume,
                "oi": effective_oi,
                "change_oi": effective_change_oi,
                "iv": chain_iv if chain_iv > 0 else None,
            }
        )

    option_rows.sort(key=lambda row: (float(row.get("strike_price") or 0.0), row.get("instrument_type") or ""))
    return {"rows": option_rows, "expiry": nearest_expiry, "available_expiries": valid_expiries}


def _format_selected_expiry_for_optionchain(selected_expiry):
    raw = str(selected_expiry or "").strip()
    if not raw:
        return None
    dt = _parse_expiry_date(raw)
    if not dt:
        for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except Exception:
                dt = None
        if not dt:
            return None
    return dt.strftime("%d-%m-%Y")


def _fetch_fyers_optionchain_snapshot(session, underlying_symbol="NIFTY", selected_expiry=None, strikecount=8):
    if not session:
        return {"rows": [], "expiry": None, "available_expiries": []}
    symbol_map = {
        "NIFTY": "NSE:NIFTY50-INDEX",
        "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    }
    api_symbol = symbol_map.get(str(underlying_symbol or "").upper())
    if not api_symbol:
        return {"rows": [], "expiry": None, "available_expiries": []}

    try:
        client = get_fyers_client(session.get("client_id"), session.get("access_token"))
    except Exception:
        return {"rows": [], "expiry": None, "available_expiries": []}

    def _run_optionchain(ts=None):
        req = {
            "symbol": api_symbol,
            "strikecount": int(max(1, strikecount)),
            "greeks": "1",
        }
        if ts:
            req["timestamp"] = str(ts)
        return client.optionchain(req)

    try:
        payload = _run_optionchain()
        data = payload.get("data") if isinstance(payload, dict) else {}
        expiry_entries = data.get("expiryData") or []
        expiry_values = []
        expiry_ts_map = {}
        for expiry_item in expiry_entries:
            label = _normalize_future_expiry_label(expiry_item.get("date"))
            if label:
                expiry_values.append(label)
            ts_val = str(expiry_item.get("expiry") or "").strip()
            date_key = str(expiry_item.get("date") or "").strip()
            if ts_val and date_key:
                expiry_ts_map[date_key] = ts_val

        wanted_date = _format_selected_expiry_for_optionchain(selected_expiry)
        if wanted_date and wanted_date in expiry_ts_map:
            payload = _run_optionchain(ts=expiry_ts_map[wanted_date])
            data = payload.get("data") if isinstance(payload, dict) else {}

        rows = []
        for item in (data.get("optionsChain") or []):
            option_type = str(item.get("option_type") or "").upper()
            strike_price = _to_float(item.get("strike_price"), 0.0)
            if option_type not in {"CE", "PE"} or strike_price <= 0:
                continue
            symbol_text = str(item.get("symbol") or "").strip()
            trading_symbol = symbol_text.split(":", 1)[-1] if symbol_text else ""
            rows.append(
                {
                    "symbol": f"nse_fo|{trading_symbol}" if trading_symbol else symbol_text,
                    "trading_symbol": trading_symbol,
                    "instrument_type": option_type,
                    "strike_price": strike_price,
                    "expiry_date": _normalize_future_expiry_label(None, trading_symbol),
                    "ltp": _to_float(item.get("ltp"), 0.0),
                    "bid": _to_float(item.get("bid"), 0.0),
                    "ask": _to_float(item.get("ask"), 0.0),
                    "volume": int(_to_float(item.get("volume"), 0.0)),
                    "oi": int(_to_float(item.get("oi"), 0.0)),
                    "change_oi": int(_to_float(item.get("oich"), 0.0)),
                    "iv": _to_float((item.get("greeks") or {}).get("iv"), 0.0),
                }
            )
        chosen_expiry = selected_expiry if selected_expiry in expiry_values else (expiry_values[0] if expiry_values else selected_expiry)
        return {"rows": rows, "expiry": chosen_expiry, "available_expiries": _future_only_expiry_values(expiry_values)}
    except Exception as exc:
        logger.debug("FYERS optionchain snapshot fetch failed for %s: %s", underlying_symbol, exc)
        return {"rows": [], "expiry": None, "available_expiries": []}


def _resolve_option_chain_from_nse_public(underlying_symbol, underlying_ltp, selected_expiry=None):
    try:
        _nse_session.get("https://www.nseindia.com/option-chain", headers=_nse_headers, timeout=8)

        # Probe endpoint with an invalid expiry to fetch valid expiry dates.
        expiry_probe = "01-Jan-1900"
        probe_url = (
            "https://www.nseindia.com/api/option-chain-v3"
            f"?type=Indices&symbol={urllib.parse.quote(underlying_symbol)}&expiry={urllib.parse.quote(expiry_probe)}"
        )
        probe_resp = _nse_session.get(probe_url, headers=_nse_headers, timeout=12)
        probe_resp.raise_for_status()
        probe_payload = probe_resp.json()

        expiry_dates = probe_payload.get("records", {}).get("expiryDates", []) or []
        if not expiry_dates:
            return {"rows": [], "expiry": None, "available_expiries": []}

        nearest_expiry, expiry_dates = _choose_expiry_value(
            expiry_dates,
            selected_expiry=selected_expiry,
            underlying_symbol=underlying_symbol,
        )
        if not nearest_expiry:
            return {"rows": [], "expiry": None, "available_expiries": expiry_dates}

        option_url = (
            "https://www.nseindia.com/api/option-chain-v3"
            f"?type=Indices&symbol={urllib.parse.quote(underlying_symbol)}&expiry={urllib.parse.quote(nearest_expiry)}"
        )
        option_resp = _nse_session.get(option_url, headers=_nse_headers, timeout=12)
        option_resp.raise_for_status()
        option_payload = option_resp.json()
        option_data = option_payload.get("records", {}).get("data", []) or []
        if not option_data:
            return {"rows": [], "expiry": nearest_expiry, "available_expiries": expiry_dates}

        rows = []
        for option_entry in option_data:
            strike = _to_float(option_entry.get("strikePrice"), 0.0)
            if strike <= 0:
                continue

            for option_type in ("CE", "PE"):
                leg = option_entry.get(option_type) or {}
                if not leg:
                    continue
                rows.append(
                    {
                        "symbol": leg.get("identifier", f"NIFTY_{strike}_{option_type}"),
                        "trading_symbol": leg.get("identifier", f"NIFTY {nearest_expiry} {strike} {option_type}"),
                        "instrument_type": option_type,
                        "strike_price": strike,
                        "expiry_date": nearest_expiry,
                        "ltp": _to_float(leg.get("lastPrice", 0.0)),
                        "bid": _to_float(leg.get("buyPrice1", 0.0)),
                        "ask": _to_float(leg.get("sellPrice1", 0.0)),
                        "volume": int(_to_float(leg.get("totalTradedVolume", 0.0))),
                        "oi": int(_to_float(leg.get("openInterest", 0.0))),
                    }
                )

        if not rows:
            return {"rows": [], "expiry": None, "available_expiries": []}

        strikes = sorted({row["strike_price"] for row in rows})
        if underlying_ltp > 0 and strikes:
            selected = sorted(strikes, key=lambda value: abs(value - underlying_ltp))[: (_OI_LADDER_STRIKES_PER_SIDE * 2) + 1]
            selected_set = set(selected)
            rows = [row for row in rows if row["strike_price"] in selected_set]

        return {"rows": rows, "expiry": nearest_expiry, "available_expiries": expiry_dates}
    except Exception as e:
        logger.debug("NSE option-chain fallback failed: %s", e)
        return {"rows": [], "expiry": None, "available_expiries": []}


def _get_option_chain_fallback(session, underlying_ltp, underlying_symbol="NIFTY", selected_expiry=None):
    cache_key = _option_chain_cache_key(underlying_symbol, selected_expiry)
    market_open = _is_market_window_now()
    allow_after_hours = _after_hours_refresh_enabled()
    with _option_chain_lock:
        cache_bucket = _option_chain_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": [], "expiry": None, "available_expiries": []})
        cache_age = time.time() - cache_bucket["timestamp"]
        if cache_age < _OPTION_CHAIN_FALLBACK_CACHE_TTL_SEC:
            return cache_bucket["rows"]

    result = _resolve_option_chain_from_fo(
        session,
        underlying_ltp,
        underlying_symbol=underlying_symbol,
        selected_expiry=selected_expiry,
        allow_live_quotes=(market_open or allow_after_hours),
    )
    if not result.get("rows") and (market_open or allow_after_hours):
        result = _resolve_option_chain_from_nse_public(underlying_symbol, underlying_ltp, selected_expiry=selected_expiry)

    with _option_chain_lock:
        cache_bucket = _option_chain_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": [], "expiry": None, "available_expiries": []})
        cache_bucket["timestamp"] = time.time()
        cache_bucket["rows"] = result.get("rows", [])
        cache_bucket["expiry"] = result.get("expiry")
        cache_bucket["available_expiries"] = result.get("available_expiries", [])
    return result.get("rows", [])


def _persist_market_data_rows(rows):
    if not rows:
        return

    db = SessionLocal()
    try:
        captured_at = datetime.now()
        for row in rows:
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            trading_symbol = str(row.get("trading_symbol") or symbol).strip()
            exchange_seg = str(row.get("exchange_seg") or (symbol.split("|", 1)[0] if "|" in symbol else "nse_cm")).strip() or "nse_cm"
            instrument_type = str(row.get("instrument_type") or "EQ").strip() or "EQ"
            strike_value = row.get("strike_price")
            expiry_value = row.get("expiry_date")

            md = MarketData(
                symbol=symbol,
                trading_symbol=trading_symbol,
                exchange_seg=exchange_seg,
                instrument_type=instrument_type,
                bid_price=_to_float(row.get("bid", 0.0), 0.0),
                ask_price=_to_float(row.get("ask", 0.0), 0.0),
                last_traded_price=_to_float(row.get("ltp", 0.0), 0.0),
                volume=int(_to_float(row.get("volume", 0.0), 0.0)),
                oi=int(_to_float(row.get("oi", 0.0), 0.0)),
                strike_price=_to_float(strike_value, None) if strike_value not in (None, "", "None") else None,
                expiry_date=str(expiry_value).strip() if expiry_value not in (None, "", "None") else None,
                timestamp=captured_at,
            )
            db.add(md)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.debug("Failed to persist market data rows: %s", exc)
    finally:
        db.close()


def _previous_marketdata_row(db, symbol):
    return (
        db.query(MarketData)
        .filter(MarketData.symbol == symbol)
        .order_by(MarketData.timestamp.desc())
        .offset(1)
        .first()
    )


def _compute_marketdata_change_oi(db, symbol, current_oi, current_timestamp=None):
    query = db.query(MarketData).filter(MarketData.symbol == symbol)
    previous = None
    if current_timestamp is not None:
        previous = (
            query
            .filter(MarketData.timestamp < current_timestamp)
            .order_by(MarketData.timestamp.desc())
            .first()
        )
    else:
        now = datetime.now()
        cutoff = now - timedelta(seconds=55)
        previous = (
            query
            .filter(MarketData.timestamp <= cutoff)
            .order_by(MarketData.timestamp.desc())
            .first()
        )
        if not previous:
            previous = _previous_marketdata_row(db, symbol)
    if previous:
        return int(round(_to_float(current_oi, 0.0) - _to_float(previous.oi, 0.0)))
    return 0


def _previous_oi_snapshot_row_map(db, symbol):
    previous = (
        db.query(OiSnapshot)
        .filter(OiSnapshot.symbol == symbol)
        .order_by(OiSnapshot.captured_at.desc())
        .first()
    )
    if not previous or not previous.row_details_json:
        return {}

    try:
        previous_rows = json.loads(previous.row_details_json)
    except Exception:
        return {}

    row_map = {}
    for row in previous_rows or []:
        strike = round(_to_float((row or {}).get("strike"), 0.0), 2)
        if strike <= 0:
            continue
        for side_key in ("call", "put"):
            side = (row or {}).get(side_key) or {}
            row_map[(strike, side_key)] = int(_to_float(side.get("oi", 0.0), 0.0))
    return row_map


def _resolve_row_change_oi(
    db,
    symbol,
    strike,
    side_key,
    current_oi,
    previous_row_map=None,
    market_symbol=None,
    current_timestamp=None,
):
    current_oi = int(_to_float(current_oi, 0.0))
    if market_symbol:
        market_delta = _compute_marketdata_change_oi(
            db,
            market_symbol,
            current_oi,
            current_timestamp=current_timestamp,
        )
        if market_delta != 0:
            return market_delta

    if previous_row_map:
        previous_oi = previous_row_map.get((round(float(strike), 2), side_key))
        if previous_oi is not None:
            return int(round(current_oi - int(previous_oi)))

    return 0


def _derive_snapshot_deltas(db, symbol, summary):
    previous = (
        db.query(OiSnapshot)
        .filter(OiSnapshot.symbol == symbol)
        .order_by(OiSnapshot.captured_at.desc())
        .first()
    )

    call_oi_sum = int(summary.get("call_oi_sum") or 0)
    put_oi_sum = int(summary.get("put_oi_sum") or 0)

    if previous:
        call_change_oi_sum = call_oi_sum - int(previous.call_oi_sum or 0)
        put_change_oi_sum = put_oi_sum - int(previous.put_oi_sum or 0)
    else:
        call_change_oi_sum = int(summary.get("call_change_oi_sum") or 0)
        put_change_oi_sum = int(summary.get("put_change_oi_sum") or 0)

    change_oi_diff = put_change_oi_sum - call_change_oi_sum
    change_pcr = round(put_change_oi_sum / call_change_oi_sum, 4) if call_change_oi_sum else None

    return call_change_oi_sum, put_change_oi_sum, change_oi_diff, change_pcr


_MONTH_ABBREV_TO_NUMBER = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _last_weekday_of_month(year, month, target_weekday=3):
    next_month = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    delta_days = (last_day.weekday() - target_weekday) % 7
    return last_day - timedelta(days=delta_days)


def _derive_future_expiry_from_symbol(trading_symbol):
    text = str(trading_symbol or "").upper().replace(" ", "")
    match = re.search(r"(\d{2})([A-Z]{3})", text)
    if not match:
        return None

    year_suffix = int(match.group(1))
    month_abbrev = match.group(2)
    month = _MONTH_ABBREV_TO_NUMBER.get(month_abbrev)
    if not month:
        return None

    year = 2000 + year_suffix
    try:
        return _last_weekday_of_month(year, month, target_weekday=3)
    except Exception:
        return None


def _normalize_future_expiry_label(expiry_text, trading_symbol=None):
    expiry_dt = _parse_expiry_date(expiry_text)
    today = datetime.now().date()
    if expiry_dt and expiry_dt.date() >= today:
        return expiry_dt.strftime("%d-%b-%Y")

    derived_dt = _derive_future_expiry_from_symbol(trading_symbol)
    if derived_dt and derived_dt.date() >= today:
        return derived_dt.strftime("%d-%b-%Y")

    return None


def _detect_future_underlying(row):
    text = " ".join([
        str(row.trading_symbol or ""),
        str(row.symbol or ""),
        str(row.expiry_date or ""),
    ]).upper()
    if "BANKNIFTY" in text or "NIFTY BANK" in text:
        return "BANKNIFTY"
    if "FINNIFTY" in text or "MIDCPNIFTY" in text:
        return None
    if "NIFTY" in text:
        return "NIFTY"
    return None


def _extract_quote_symbol(entry):
    if not isinstance(entry, dict):
        return None
    direct = str(entry.get("n") or "").strip()
    if direct:
        return direct
    values = entry.get("v") or {}
    inferred = str(values.get("n") or values.get("symbol") or "").strip()
    return inferred or None


def _normalize_fyers_quote_entry(entry, fallback_symbol=None):
    if not isinstance(entry, dict):
        return None

    values = entry.get("v") or {}
    if isinstance(values, dict) and str(values.get("s", "")).lower() == "error":
        return None

    resolved_symbol = _extract_quote_symbol(entry) or str(fallback_symbol or "").strip()
    if not resolved_symbol:
        return None

    ltp = _to_float(values.get("lp") or values.get("ltp") or 0.0, 0.0)
    bid = _to_float(values.get("bid") or values.get("bp") or values.get("bid_price") or ltp, ltp)
    ask = _to_float(values.get("ask") or values.get("ap") or values.get("ask_price") or ltp, ltp)
    volume = _to_float(
        values.get("vol_traded_today")
        or values.get("volume")
        or values.get("vtt")
        or 0.0,
        0.0,
    )
    # Quotes API may not always include OI, so default to 0 when absent.
    open_int = _to_float(values.get("oi") or values.get("OI") or 0.0, 0.0)
    return {
        "_fyers_symbol": resolved_symbol,
        "display_symbol": resolved_symbol,
        "ltp": ltp,
        "open_int": open_int,
        "last_volume": volume,
        "depth": {
            "buy": [{"price": bid}],
            "sell": [{"price": ask}],
        },
    }


def _fetch_fyers_quotes_batch(session, exchange_seg, trading_symbols, timeout_sec=8.0, retries=2, cache_ttl_sec=30.0):
    if not session:
        return {}

    requested = []
    for raw in trading_symbols or []:
        value = str(raw or "").strip()
        if value and value not in requested:
            requested.append(value)
    if not requested:
        return {}

    raw_to_fyers = {}
    for raw in requested:
        fyers_symbol = to_fyers_symbol(exchange_seg, raw)
        if fyers_symbol:
            raw_to_fyers[raw] = fyers_symbol
    if not raw_to_fyers:
        return {}

    fyers_symbols = []
    seen = set()
    for value in raw_to_fyers.values():
        if value in seen:
            continue
        seen.add(value)
        fyers_symbols.append(value)

    per_symbol = {}
    last_exc = None
    retries = max(1, int(retries or 1))
    for attempt in range(1, retries + 1):
        try:
            client = get_fyers_client(session.get("client_id"), session.get("access_token"))
            payload = client.quotes({"symbols": ",".join(fyers_symbols)})
            data = payload.get("d") if isinstance(payload, dict) else None
            if payload.get("s") == "ok" and isinstance(data, list):
                for entry in data:
                    quote = _normalize_fyers_quote_entry(entry)
                    if not quote:
                        continue
                    symbol_key = str(quote.get("_fyers_symbol") or "").strip()
                    if symbol_key:
                        per_symbol[symbol_key] = quote
                if per_symbol:
                    break
        except Exception as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(0.2 * attempt)

    now_ts = time.time()
    results = {}
    with _kotak_quote_cache_lock:
        for raw_symbol, fyers_symbol in raw_to_fyers.items():
            cache_key = f"{exchange_seg}|{raw_symbol}"
            quote = per_symbol.get(fyers_symbol)
            if quote:
                cached_quote = dict(quote)
                cached_quote.pop("_fyers_symbol", None)
                _kotak_quote_cache[cache_key] = {
                    "timestamp": now_ts,
                    "quote": cached_quote,
                }
                results[raw_symbol] = cached_quote
                continue

            cached = _kotak_quote_cache.get(cache_key)
            if cached and now_ts - float(cached.get("timestamp", 0.0)) <= float(cache_ttl_sec):
                results[raw_symbol] = cached.get("quote")

    if not results and last_exc:
        logger.debug("FYERS batch quote fetch failed for %s symbols: %s", len(raw_to_fyers), last_exc)
    return results


def _fetch_kotak_quote(session, exchange_seg, trading_symbol, timeout_sec=8.0, retries=3, cache_ttl_sec=30.0):
    symbol = str(trading_symbol or "").strip()
    if not symbol:
        return None
    rows = _fetch_fyers_quotes_batch(
        session=session,
        exchange_seg=exchange_seg,
        trading_symbols=[symbol],
        timeout_sec=timeout_sec,
        retries=retries,
        cache_ttl_sec=cache_ttl_sec,
    )
    return rows.get(symbol)


def _build_futures_section_from_master(underlying_symbol, allow_live_quotes=True):
    try:
        from core.instruments import InstrumentMaster
    except Exception as exc:
        logger.debug("Unable to import InstrumentMaster for futures lookup: %s", exc)
        return None

    try:
        instrument_master = InstrumentMaster()
        future_df = instrument_master.load("nse_fo")
        if future_df is None or future_df.empty:
            return None

        future_df = future_df.copy()
        future_df.columns = [str(col).strip().replace(";", "") for col in future_df.columns]
        col_map = {str(col).lower(): col for col in future_df.columns}

        def col(name):
            return col_map.get(name.lower())

        symbol_col = col("pSymbolName")
        token_col = col("pSymbol")
        trd_col = col("pTrdSymbol")
        inst_col = col("pInstType")
        expiry_col = col("lExpiryDate") or col("pExpiryDate")
        if not all([symbol_col, token_col, trd_col, inst_col, expiry_col]):
            return None

        symbol_filter = future_df[symbol_col].astype(str).str.upper()
        underlying_upper = str(underlying_symbol or "").upper()
        if underlying_upper == "BANKNIFTY":
            symbol_mask = symbol_filter.str.contains("BANKNIFTY", na=False) | symbol_filter.str.contains("NIFTY BANK", na=False)
        else:
            symbol_mask = (
                symbol_filter.str.contains(r"(?:^|[^A-Z0-9])NIFTY(?:[^A-Z0-9]|$)", na=False, regex=True)
                & ~symbol_filter.str.contains("BANKNIFTY", na=False)
                & ~symbol_filter.str.contains("FINNIFTY", na=False)
                & ~symbol_filter.str.contains("MIDCPNIFTY", na=False)
                & ~symbol_filter.str.contains("NIFTY BANK", na=False)
            )

        future_rows = future_df[
            symbol_mask &
            future_df[inst_col].astype(str).str.upper().str.contains("FUT", na=False)
        ].copy()
        if future_rows.empty:
            return None

        future_rows[expiry_col] = future_rows[expiry_col].astype(str).str.strip()
        future_rows = future_rows[future_rows[expiry_col] != ""]
        if future_rows.empty:
            return None

        future_rows["normalized_expiry"] = future_rows.apply(
            lambda row: _normalize_future_expiry_label(row[expiry_col], row[trd_col]),
            axis=1,
        )
        future_rows = future_rows[future_rows["normalized_expiry"].notna()].copy()
        if future_rows.empty:
            return None

        future_rows["_expiry_rank"] = future_rows["normalized_expiry"].map(
            lambda value: _parse_expiry_date(value) or datetime.max
        )
        future_rows = future_rows.sort_values(["_expiry_rank", "normalized_expiry"], na_position="last")

        available_expiries = []
        for expiry_value in future_rows["normalized_expiry"].tolist():
            expiry_value = str(expiry_value).strip()
            if expiry_value and expiry_value not in available_expiries:
                available_expiries.append(expiry_value)
            if len(available_expiries) >= 3:
                break

        if not available_expiries:
            return None

        session = _load_dashboard_session() if (
            allow_live_quotes and (_is_market_window_now() or _after_hours_refresh_enabled())
        ) else None
        month_labels = ["Current Month", "Next Month", "3rd Month"]
        contract_specs = []
        db = SessionLocal()
        try:
            for idx, expiry_value in enumerate(available_expiries[:3]):
                contract = future_rows[future_rows["normalized_expiry"] == expiry_value].head(1)
                if contract.empty:
                    continue
                row = contract.iloc[0]
                trading_symbol = str(row[trd_col]).strip()
                token_value = str(row[token_col]).strip()
                db_row = None
                if trading_symbol:
                    db_row = (
                        db.query(MarketData)
                        .filter(MarketData.trading_symbol == trading_symbol)
                        .order_by(MarketData.timestamp.desc())
                        .first()
                    )
                contract_specs.append({
                    "idx": idx,
                    "row": row,
                    "trading_symbol": trading_symbol,
                    "token_value": token_value,
                    "expiry": expiry_value,
                    "db_row": db_row,
                })
        finally:
            db.close()

        quote_results = {}
        if session and allow_live_quotes:
            quote_specs = [spec for spec in contract_specs if spec["db_row"] is None]
            if quote_specs:
                query_symbols = []
                for spec in quote_specs:
                    token_value = str(spec.get("token_value") or "").strip()
                    trading_symbol = str(spec.get("trading_symbol") or "").strip()
                    if token_value:
                        query_symbols.append(token_value)
                    if trading_symbol and trading_symbol != token_value:
                        query_symbols.append(trading_symbol)

                batched = _fetch_fyers_quotes_batch(
                    session=session,
                    exchange_seg="nse_fo",
                    trading_symbols=query_symbols,
                    timeout_sec=4.0,
                    retries=2,
                    cache_ttl_sec=20.0,
                )
                for spec in quote_specs:
                    quote = None
                    token_value = str(spec.get("token_value") or "").strip()
                    trading_symbol = str(spec.get("trading_symbol") or "").strip()
                    if token_value:
                        quote = batched.get(token_value)
                    if quote is None and trading_symbol:
                        quote = batched.get(trading_symbol)
                    quote_results[spec["expiry"]] = quote

        contracts = []
        for spec in contract_specs:
            row = spec["row"]
            db_row = spec["db_row"]
            trading_symbol = spec["trading_symbol"]
            token_value = spec["token_value"]
            expiry_value = spec["expiry"]
            quote = quote_results.get(expiry_value)
            depth = (quote.get("depth") or {}) if quote else {}
            buy_side = (depth.get("buy") or [{}])[0] if depth else {}
            sell_side = (depth.get("sell") or [{}])[0] if depth else {}
            if db_row is not None:
                ltp_value = _to_float(db_row.last_traded_price, 0.0)
                bid_value = _to_float(db_row.bid_price, 0.0)
                ask_value = _to_float(db_row.ask_price, 0.0)
                oi_value = int(_to_float(db_row.oi, 0.0))
                volume_value = int(_to_float(db_row.volume, 0.0))
            elif quote:
                ltp_value = _to_float(quote.get("ltp"), 0.0)
                bid_value = _to_float(buy_side.get("price", 0.0), 0.0)
                ask_value = _to_float(sell_side.get("price", 0.0), 0.0)
                oi_value = int(_to_float(quote.get("open_int"), 0.0))
                volume_value = int(_to_float(quote.get("last_volume"), 0.0))
            else:
                ltp_value = None
                bid_value = None
                ask_value = None
                oi_value = None
                volume_value = None
            contracts.append({
                "symbol": f"nse_fo|{token_value}",
                "trading_symbol": trading_symbol,
                "month_label": month_labels[spec["idx"]] if spec["idx"] < len(month_labels) else f"Month {spec['idx'] + 1}",
                "expiry": expiry_value,
                "ltp": ltp_value,
                "bid": bid_value,
                "ask": ask_value,
                "oi": oi_value,
                "volume": volume_value,
                "timestamp": db_row.timestamp.isoformat() if db_row and db_row.timestamp else None,
            })

        if not contracts:
            return None

        return {
            "underlying": underlying_upper,
            "label": f"{underlying_upper} Futures",
            "contracts": contracts,
            "source": "FYERS MarketData" if any(c.get("timestamp") for c in contracts) else "FYERS Quote Fallback",
        }
    except Exception as exc:
        logger.debug("Futures master fallback failed for %s: %s", underlying_symbol, exc)
        return None


def _build_ui_bootstrap_data():
    initial_oi_dashboard = []
    for symbol in ("NIFTY", "BANKNIFTY"):
        section = _build_latest_oi_snapshot_section(symbol)
        if not section:
            section = _build_empty_oi_section(symbol)
            section["source"] = "Bootstrap"
            section["feed"] = "bootstrap"
            section["note"] = "Loading live option-chain data..."
        initial_oi_dashboard.append(section)

    initial_futures_head = []
    for underlying in ("NIFTY", "BANKNIFTY"):
        bootstrap_section = _build_futures_section_from_master(underlying, allow_live_quotes=False)
        if bootstrap_section:
            bootstrap_section["source"] = "Bootstrap"
            initial_futures_head.append(bootstrap_section)
        else:
            initial_futures_head.append({
                "underlying": underlying,
                "label": f"{underlying} Futures",
                "contracts": [],
                "source": "Bootstrap",
            })

    initial_futures_head.append({
        "underlying": "GIFT NIFTY",
        "label": "GIFT Nifty",
        "contracts": [],
        "source": "Bootstrap",
    })

    return initial_oi_dashboard, initial_futures_head


def _build_empty_gift_nifty_section(source="NSE India"):
    return {
        "underlying": "GIFT NIFTY",
        "label": "GIFT Nifty",
        "contracts": [{
            "month_label": "Live",
            "expiry": None,
            "trading_symbol": "GIFT Nifty",
            "ltp": None,
            "bid": None,
            "ask": None,
            "oi": None,
            "volume": None,
            "timestamp": None,
            "change": None,
            "change_pct": None,
        }],
        "source": source,
    }


def _fetch_gift_nifty_head_network():
    def _build_section(ltp_value, change_value, change_pct_value, expiry_value=None, timestamp_value=None, source="NSE India"):
        expiry_label = None
        if expiry_value and _parse_expiry_date(expiry_value):
            expiry_label = _format_expiry_label(expiry_value)
        return {
            "underlying": "GIFT NIFTY",
            "label": "GIFT Nifty",
            "contracts": [{
                "month_label": "Live",
                "expiry": expiry_label,
                "trading_symbol": "GIFT Nifty",
                "ltp": _to_float(ltp_value, 0.0),
                "bid": None,
                "ask": None,
                "oi": None,
                "volume": None,
                "timestamp": timestamp_value,
                "change": _to_float(change_value, 0.0),
                "change_pct": _to_float(change_pct_value, 0.0),
            }],
            "source": source,
        }

    html_url = "https://www.nseindia.com/nse-indices"
    html_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
    }
    json_headers = {
        **_nse_headers,
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    }
    try:
        _nse_session.get(
            "https://www.nseindia.com",
            headers={**_nse_headers, "Accept": html_headers["Accept"]},
            timeout=3,
        )

        # Preferred source: NSE all-indices JSON.
        try:
            json_resp = _nse_session.get(
                "https://www.nseindia.com/api/allIndices",
                headers=json_headers,
                timeout=3,
            )
            json_resp.raise_for_status()
            payload = json_resp.json() if "application/json" in str(json_resp.headers.get("Content-Type", "")).lower() else {}
            rows = payload.get("data") if isinstance(payload, dict) else []
            if isinstance(rows, list):
                candidate = None
                for row in rows:
                    name = str(
                        row.get("index")
                        or row.get("indexSymbol")
                        or row.get("symbol")
                        or row.get("key")
                        or ""
                    ).strip()
                    normalized = _normalize_index_name(name)
                    if "GIFTNIFTY" in normalized or normalized == "SGXNIFTY":
                        candidate = row
                        break
                if candidate:
                    ltp = _to_float(
                        candidate.get("last")
                        or candidate.get("lastPrice")
                        or candidate.get("indexLast")
                        or candidate.get("ltp"),
                        0.0,
                    )
                    if ltp > 0:
                        change = candidate.get("variation") or candidate.get("change") or candidate.get("absoluteChange")
                        change_pct = candidate.get("percentChange") or candidate.get("pChange") or candidate.get("percChange")
                        expiry_raw = candidate.get("expiryDate") or candidate.get("nearWkExpDate")
                        timestamp_raw = candidate.get("lastUpdateTime") or candidate.get("timeVal")
                        return _build_section(
                            ltp_value=ltp,
                            change_value=change,
                            change_pct_value=change_pct,
                            expiry_value=expiry_raw,
                            timestamp_value=timestamp_raw,
                            source="NSE All Indices",
                        )
        except Exception as exc:
            logger.debug("Gift Nifty JSON fetch failed: %s", exc)

        # Fallback source: NSE indices HTML parsing.
        resp = _nse_session.get(html_url, headers={**_nse_headers, **html_headers}, timeout=3)
        resp.raise_for_status()
        html = resp.text
        match = None
        for pattern in (
            r"GiftNiftyFutures.*?(?P<expiry>[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}).*?(?P<ltp>[0-9,]+(?:\.[0-9]+)?).*?(?P<chg>[+-]?[0-9,]+(?:\.[0-9]+)?)\s*\((?P<pct>[+-]?[0-9,]+(?:\.[0-9]+)?)%\)",
            r"Gift\s*Nifty\s*Futures.*?(?P<expiry>[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}).*?(?P<ltp>[0-9,]+(?:\.[0-9]+)?).*?(?P<chg>[+-]?[0-9,]+(?:\.[0-9]+)?)\s*\((?P<pct>[+-]?[0-9,]+(?:\.[0-9]+)?)%\)",
            r"GIFTNIFTY.*?(?P<expiry>[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}).*?(?P<ltp>[0-9,]+(?:\.[0-9]+)?).*?(?P<chg>[+-]?[0-9,]+(?:\.[0-9]+)?)\s*\((?P<pct>[+-]?[0-9,]+(?:\.[0-9]+)?)%\)",
        ):
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                break
        if not match:
            return None
        return _build_section(
            ltp_value=match.group("ltp"),
            change_value=match.group("chg"),
            change_pct_value=match.group("pct"),
            expiry_value=match.group("expiry"),
            source="NSE India",
        )
    except Exception as exc:
        logger.debug("Gift Nifty fetch failed: %s", exc)
        return None


def _refresh_gift_nifty_cache_async():
    global _gift_nifty_refreshing

    def worker():
        global _gift_nifty_refreshing
        try:
            section = _fetch_gift_nifty_head_network()
            if section:
                with _gift_nifty_cache_lock:
                    _gift_nifty_cache["timestamp"] = time.time()
                    _gift_nifty_cache["row"] = section
        finally:
            with _gift_nifty_refresh_lock:
                _gift_nifty_refreshing = False

    with _gift_nifty_refresh_lock:
        if _gift_nifty_refreshing:
            return
        _gift_nifty_refreshing = True

    threading.Thread(target=worker, daemon=True).start()


def _get_gift_nifty_head():
    now = time.time()
    with _gift_nifty_cache_lock:
        cached_row = _gift_nifty_cache.get("row")
        cache_ts = float(_gift_nifty_cache.get("timestamp") or 0.0)
    cache_age = now - cache_ts if cache_ts else None

    if cached_row and cache_age is not None and cache_age <= _GIFT_NIFTY_CACHE_TTL_SEC:
        return cached_row

    # Never block request-paths on external NSE calls; refresh in background.
    _refresh_gift_nifty_cache_async()

    if cached_row:
        stale_copy = dict(cached_row)
        stale_copy["source"] = f"{cached_row.get('source') or 'NSE India'} (cached)"
        return stale_copy

    return _build_empty_gift_nifty_section(source="NSE India (pending)")


def _fetch_futures_head():
    db = SessionLocal()
    try:
        latest_subquery = (
            db.query(
                MarketData.symbol,
                func.max(MarketData.timestamp).label("max_timestamp"),
            )
            .filter(MarketData.instrument_type.ilike("%FUT%"))
            .group_by(MarketData.symbol)
            .subquery()
        )
        latest_rows = (
            db.query(MarketData)
            .join(
                latest_subquery,
                (MarketData.symbol == latest_subquery.c.symbol)
                & (MarketData.timestamp == latest_subquery.c.max_timestamp),
            )
            .order_by(MarketData.timestamp.desc())
            .all()
        )

        grouped = {"NIFTY": [], "BANKNIFTY": []}
        for row in latest_rows:
            underlying = _detect_future_underlying(row)
            if underlying not in grouped:
                continue
            expiry_label = _normalize_future_expiry_label(row.expiry_date, row.trading_symbol)
            if not expiry_label:
                continue
            grouped[underlying].append({
                "symbol": row.symbol,
                "trading_symbol": row.trading_symbol,
                "expiry": expiry_label,
                "ltp": _to_float(row.last_traded_price, 0.0),
                "bid": _to_float(row.bid_price, 0.0),
                "ask": _to_float(row.ask_price, 0.0),
                "oi": int(_to_float(row.oi, 0.0)),
                "volume": int(_to_float(row.volume, 0.0)),
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            })

        month_labels = ["Current Month", "Next Month", "3rd Month"]
        for underlying in grouped:
            grouped[underlying].sort(key=lambda item: (
                _parse_expiry_date(item.get("expiry")) or datetime.max,
                item.get("timestamp") or "",
            ))
            selected = []
            seen_expiries = set()
            for item in grouped[underlying]:
                expiry_key = str(item.get("expiry") or "").strip()
                if not expiry_key or expiry_key in seen_expiries:
                    continue
                seen_expiries.add(expiry_key)
                selected.append(item)
                if len(selected) >= 3:
                    break
            grouped[underlying] = [
                {
                    **item,
                    "month_label": month_labels[idx] if idx < len(month_labels) else f"Month {idx + 1}",
                }
                for idx, item in enumerate(selected)
            ]

        if len(grouped["NIFTY"]) < 3:
            fallback_section = _build_futures_section_from_master("NIFTY")
            if fallback_section:
                grouped["NIFTY"] = fallback_section["contracts"]
        if len(grouped["BANKNIFTY"]) < 3:
            fallback_section = _build_futures_section_from_master("BANKNIFTY")
            if fallback_section:
                grouped["BANKNIFTY"] = fallback_section["contracts"]

        sections = []
        if grouped["NIFTY"]:
            sections.append({
                "underlying": "NIFTY",
                "label": "NIFTY Futures",
                "contracts": grouped["NIFTY"],
                "source": "FYERS MarketData" if any(item.get("timestamp") for item in grouped["NIFTY"]) else "FYERS Quote Fallback",
            })
        if grouped["BANKNIFTY"]:
            sections.append({
                "underlying": "BANKNIFTY",
                "label": "BANKNIFTY Futures",
                "contracts": grouped["BANKNIFTY"],
                "source": "FYERS MarketData" if any(item.get("timestamp") for item in grouped["BANKNIFTY"]) else "FYERS Quote Fallback",
            })

        gift_nifty = _get_gift_nifty_head()
        if gift_nifty:
            sections.append(gift_nifty)
        else:
            sections.append(_build_empty_gift_nifty_section(source="NSE India"))

        if not sections:
            return []

        # #region agent log (H1/H2: check whether DB-derived futures head has live ltp/bid/ask/oi)
        try:
            sample_section = sections[0] if sections else None
            first_contract = None
            if isinstance(sample_section, dict):
                contracts = sample_section.get("contracts") or []
                first_contract = contracts[0] if contracts else None
            _agent_debug_log(
                hypothesis_id="H2",
                location="dashboard/dashboard.py:_fetch_futures_head",
                message="futures_head_fetched_from_db_and_fallback",
                data={
                    "latest_rows_len": len(latest_rows),
                    "sections_len": len(sections),
                    "nifty_contracts_len": len(grouped.get("NIFTY") or []),
                    "banknifty_contracts_len": len(grouped.get("BANKNIFTY") or []),
                    "first_contract_sample": (
                        {
                            "ltp": (first_contract or {}).get("ltp"),
                            "bid": (first_contract or {}).get("bid"),
                            "ask": (first_contract or {}).get("ask"),
                            "oi": (first_contract or {}).get("oi"),
                            "volume": (first_contract or {}).get("volume"),
                        }
                        if isinstance(first_contract, dict)
                        else None
                    ),
                },
            )
        except Exception:
            pass
        # #endregion
        return sections
    finally:
        db.close()


def _black_scholes_price(spot, strike, iv_percent, expiry_text, side, rate=0.10):
    spot = float(spot or 0.0)
    strike = float(strike or 0.0)
    iv = max(float(iv_percent or 0.0) / 100.0, 0.0001)
    expiry_dt = _parse_expiry_date(expiry_text)
    if spot <= 0 or strike <= 0 or expiry_dt is None:
        return None

    now = datetime.now()
    t = max((expiry_dt - now).total_seconds(), 0.0) / (365.0 * 24.0 * 3600.0)
    if t <= 0:
        t = 1.0 / (365.0 * 24.0)

    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t

    if side == "CE":
        return spot * _normal_cdf(d1) - strike * math.exp(-rate * t) * _normal_cdf(d2)
    return strike * math.exp(-rate * t) * _normal_cdf(-d2) - spot * _normal_cdf(-d1)


def _estimate_iv_from_price(spot, strike, price, expiry_text, side, rate=0.10):
    spot = float(spot or 0.0)
    strike = float(strike or 0.0)
    price = float(price or 0.0)
    if spot <= 0 or strike <= 0 or price <= 0:
        return None
    expiry_dt = _parse_expiry_date(expiry_text)
    if expiry_dt is None:
        return None

    low = 0.01
    high = 500.0
    best = None
    for _ in range(48):
        mid = (low + high) / 2.0
        model_price = _black_scholes_price(spot, strike, mid, expiry_text, side, rate=rate)
        if model_price is None:
            return None
        best = mid
        if abs(model_price - price) < 0.01:
            break
        if model_price > price:
            high = mid
        else:
            low = mid

    return round(best, 2) if best is not None else None


def _pick_option_iv(spot, strike, expiry_text, side, payload):
    iv_value = _to_float((payload or {}).get("iv"), 0.0)
    if iv_value > 0:
        return iv_value
    ltp = _to_float((payload or {}).get("ltp"), 0.0)
    if spot and strike and expiry_text and ltp > 0:
        estimated = _estimate_iv_from_price(spot, strike, ltp, expiry_text, "CE" if side == "call" else "PE")
        if estimated:
            return estimated
    return None


def _signal_label_from_diff(value, bullish_label="BUY CALL", bearish_label="BUY PUT"):
    value = _to_float(value, 0.0)
    if value > 0:
        return bullish_label
    if value < 0:
        return bearish_label
    return "WAIT"


def _compute_ws_quality_pct(ws_metrics, last_tick_age_sec=None, last_message_age_sec=None):
    if not ws_metrics:
        return 0

    score = 100.0
    if not ws_metrics.get("connected"):
        score -= 25.0
    if last_tick_age_sec is not None:
        score -= min(60.0, float(last_tick_age_sec) * 12.0)
    if last_message_age_sec is not None:
        score -= min(20.0, float(last_message_age_sec) * 5.0)

    reconnect_count = int(_to_float(ws_metrics.get("reconnect_count"), 0.0))
    consecutive_failures = int(_to_float(ws_metrics.get("consecutive_failures"), 0.0))
    score -= min(25.0, reconnect_count * 3.0)
    score -= min(20.0, consecutive_failures * 6.0)

    return int(max(0.0, min(100.0, score)))


def _get_live_feed_snapshot():
    now = datetime.now()
    ws_metrics = {}
    get_ws_status = lambda: False

    try:
        from core.status import get_ws_metrics, get_ws_status

        ws_metrics = dict(get_ws_metrics() or {})
    except Exception:
        ws_metrics = {}

    last_tick_at_raw = ws_metrics.get("last_tick_at")
    last_message_at_raw = ws_metrics.get("last_message_at")
    ws_last_tick_age_sec = None
    ws_last_message_age_sec = None
    last_live_tick_at = None

    last_tick_dt = _parse_row_timestamp(last_tick_at_raw)
    if last_tick_dt:
        ws_last_tick_age_sec = int((now - last_tick_dt).total_seconds())
        last_live_tick_at = last_tick_dt.isoformat()
    elif last_tick_at_raw:
        last_live_tick_at = str(last_tick_at_raw)

    last_message_dt = _parse_row_timestamp(last_message_at_raw)
    if last_message_dt:
        ws_last_message_age_sec = int((now - last_message_dt).total_seconds())

    reconnect_events = [
        _to_float(value, 0.0)
        for value in (ws_metrics.get("reconnect_events") or [])
        if _to_float(value, 0.0) > 0
    ]
    now_epoch = time.time()
    reconnects_last_min = len([value for value in reconnect_events if (now_epoch - value) <= _WS_STORM_WINDOW_SEC])
    reconnect_storm = reconnects_last_min >= _WS_STORM_RECONNECTS

    is_market_window = _is_market_window_now()
    stale_age_sec = ws_last_tick_age_sec
    is_stale = bool(is_market_window and (stale_age_sec is None or stale_age_sec > _OI_STALE_THRESHOLD_SEC))
    ws_connected = bool(get_ws_status()) and not is_stale
    if ws_connected:
        ws_metrics["connected"] = True
    ws_quality_pct = _compute_ws_quality_pct(ws_metrics, ws_last_tick_age_sec, ws_last_message_age_sec)
    degraded = bool(reconnect_storm or int(_to_float(ws_metrics.get("consecutive_failures"), 0.0)) >= 2)
    if ws_connected and ws_quality_pct < 55:
        degraded = True

    feed_state = "live" if ws_connected else ("idle" if not is_market_window else "disconnected")
    if ws_connected and degraded:
        feed_state = "degraded"

    return {
        "ws_metrics": ws_metrics,
        "ws_connected": ws_connected,
        "ws_quality_pct": ws_quality_pct,
        "ws_last_tick_age_sec": ws_last_tick_age_sec,
        "ws_last_message_age_sec": ws_last_message_age_sec,
        "last_live_tick_at": last_live_tick_at,
        "is_stale": is_stale,
        "stale_age_sec": stale_age_sec,
        "reconnects_last_min": reconnects_last_min,
        "reconnect_storm": reconnect_storm,
        "feed_state": feed_state,
        "market_window": is_market_window,
    }


def _is_market_window_now():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    current_time = now.time()
    return current_time >= datetime.strptime("09:15", "%H:%M").time() and current_time <= datetime.strptime("15:30", "%H:%M").time()


def _after_hours_refresh_enabled():
    return str(os.getenv("AFTER_HOURS_REFRESH", "true")).strip().lower() in {"1", "true", "yes", "y", "on"}

html_content = r"""
<!DOCTYPE html>
<html>
<head>
    <title>Paurik Trivedi's Live Market Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f4f4f9; }
        h1 { color: #333; }
        .header { display: flex; justify-content: space-between; align-items: center; background: #fff; padding: 10px 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .badge { padding: 5px 10px; border-radius: 5px; color: white; font-weight: bold; }
        .paper { background-color: #ff9800; }
        .live { background-color: #f44336; }
        .status-dot { height: 15px; width: 15px; background-color: #ccc; border-radius: 50%; display: inline-block; margin-right: 10px; }
        .status-dot.connected { background-color: #4caf50; box-shadow: 0 0 8px #4caf50; }
        .status-dot.idle { background-color: #f59e0b; box-shadow: 0 0 8px #f59e0b; }
        .status-dot.disconnected { background-color: #f44336; box-shadow: 0 0 8px #f44336; }
        .header-controls { display: flex; align-items: center; }
        .connection-banner { margin-top: 14px; padding: 14px 16px; border-radius: 18px; background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%); border: 1px solid #e6e8ef; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
        .connection-top { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
        .connection-title { font-size: 0.92em; font-weight: 900; color: #0f172a; text-transform: uppercase; letter-spacing: 0.06em; }
        .connection-sub { color: #64748b; font-size: 0.82em; margin-top: 2px; }
        .connection-pill { padding: 6px 10px; border-radius: 999px; font-size: 0.78em; font-weight: 900; letter-spacing: 0.04em; }
        .connection-pill.good { background: #e9f9ef; color: #137a3a; }
        .connection-pill.warn { background: #fff6df; color: #a16207; }
        .connection-pill.bad { background: #fdecec; color: #c93737; }
        .connection-meter { margin-top: 10px; height: 12px; border-radius: 999px; background: #e5e7eb; overflow: hidden; }
        .connection-meter-fill { height: 100%; width: 0%; border-radius: inherit; background: linear-gradient(90deg, #16a34a, #84cc16); transition: width 0.25s ease, background 0.25s ease; }
        .connection-meter-fill.warn { background: linear-gradient(90deg, #f59e0b, #f97316); }
        .connection-meter-fill.bad { background: linear-gradient(90deg, #ef4444, #fb7185); }
        .connection-meta { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 10px; color: #475569; font-size: 0.82em; }
        .connection-meta span { padding: 5px 8px; border-radius: 999px; background: #f8fafc; border: 1px solid #e8edf5; }
        .card { background: #fff; padding: 20px; border-radius: 8px; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .pnl { font-size: 2em; font-weight: bold; }
        .positive { color: #4caf50; }
        .negative { color: #f44336; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f8f9fa; }
        .strategy-container { display: flex; flex-direction: column; gap: 15px; }
        .strategy-summary { display: flex; gap: 20px; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 5px; flex: 1; text-align: center; border: 1px solid #ddd; }
        .stat-box .num { font-size: 1.5em; font-weight: bold; }
        .stat-box.live-box .num { color: #4caf50; }
        .stat-box.paused-box .num { color: #ff9800; }
        .stat-box.stopped-box .num { color: #f44336; }
        .strategy-action { display: flex; gap: 8px; }
        .action-btn { border: none; padding: 6px 10px; border-radius: 5px; color: white; cursor: pointer; font-size: 0.9em; }
        .btn-start { background: #4caf50; }
        .btn-pause { background: #ff9800; }
        .btn-resume { background: #2196f3; }
        .btn-stop { background: #f44336; }
        .btn-neutral { background: #607d8b; }
        .status-live { color: #4caf50; font-weight: bold; }
        .status-paused { color: #ff9800; font-weight: bold; }
        .status-stopped { color: #f44336; font-weight: bold; }
        .control-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-top: 12px; }
        .metric-card { background: #f8f9fa; border: 1px solid #ddd; border-radius: 6px; padding: 12px; }
        .metric-label { color: #555; font-size: 0.85em; }
        .metric-value { font-size: 1.2em; font-weight: bold; margin-top: 4px; }
        .control-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 16px; }
        .index-toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 12px; margin-bottom: 10px; }
        .index-search { flex: 1; min-width: 220px; padding: 10px 12px; border: 1px solid #d9d9e3; border-radius: 10px; font-size: 0.95em; }
        .index-tabs { display: flex; gap: 8px; flex-wrap: wrap; }
        .index-tab { border: 1px solid #163d67; background: #fff; color: #163d67; padding: 8px 12px; border-radius: 999px; cursor: pointer; font-weight: 600; }
        .index-tab.active { background: #163d67; color: #fff; }
        .index-list { display: grid; gap: 10px; margin-top: 10px; }
        .index-card { display: flex; justify-content: space-between; align-items: center; padding: 14px 16px; border-radius: 14px; border: 1px solid #ececf5; background: linear-gradient(180deg, #ffffff 0%, #fbfbfe 100%); }
        .index-name { font-size: 0.98em; font-weight: 700; color: #1d2433; letter-spacing: 0.2px; }
        .index-change { font-size: 0.92em; font-weight: 700; }
        .index-ltp { font-size: 1.22em; font-weight: 800; color: #111827; }
        .index-meta { display: flex; flex-direction: column; align-items: flex-end; gap: 2px; }
        .index-muted { color: #6b7280; font-size: 0.82em; }
        .strategy-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; margin-top: 14px; }
        .strategy-card { border: 1px solid #e6e8ef; border-radius: 18px; background: linear-gradient(180deg, #ffffff 0%, #fafbff 100%); padding: 16px; box-shadow: 0 8px 28px rgba(15, 23, 42, 0.05); display: flex; flex-direction: column; gap: 12px; }
        .strategy-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; }
        .strategy-title { font-size: 1.05em; font-weight: 800; color: #111827; }
        .strategy-desc { font-size: 0.88em; color: #4b5563; line-height: 1.4; }
        .status-pill { display: inline-flex; align-items: center; padding: 6px 10px; border-radius: 999px; font-size: 0.78em; font-weight: 800; letter-spacing: 0.2px; }
        .status-pill.live { background: #e9f9ef; color: #137a3a; }
        .status-pill.paused { background: #fff4e5; color: #b45b00; }
        .status-pill.stopped { background: #fdecec; color: #c93737; }
        .strategy-metrics { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
        .strategy-metric { border-radius: 12px; background: #f8fafc; border: 1px solid #e8edf5; padding: 10px 12px; }
        .strategy-metric-label { font-size: 0.75em; color: #64748b; text-transform: uppercase; letter-spacing: 0.06em; }
        .strategy-metric-value { font-size: 1.02em; font-weight: 800; color: #0f172a; margin-top: 4px; }
        .strategy-footer { display: flex; flex-direction: column; gap: 8px; }
        .strategy-positions { display: flex; flex-wrap: wrap; gap: 8px; }
        .position-chip { padding: 7px 10px; border-radius: 999px; background: #eef2ff; color: #334155; font-size: 0.8em; font-weight: 700; }
        .strategy-action-row { display: flex; flex-wrap: wrap; gap: 8px; }
        .strategy-note { color: #64748b; font-size: 0.82em; }
        .overview-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
        .overview-card { background: linear-gradient(180deg, #ffffff 0%, #fbfbfe 100%); border: 1px solid #e6e8ef; border-radius: 16px; padding: 14px; }
        .overview-label { color: #64748b; font-size: 0.82em; text-transform: uppercase; letter-spacing: 0.05em; }
        .overview-value { font-size: 1.6em; font-weight: 900; margin-top: 4px; }
        .oi-wrap { display: grid; gap: 18px; }
        .oi-panel { border: 1px solid #e6e8ef; border-radius: 18px; background: linear-gradient(180deg, #ffffff 0%, #fafbff 100%); padding: 16px; box-shadow: 0 8px 28px rgba(15, 23, 42, 0.05); }
        .oi-panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }
        .oi-panel-title { font-size: 1.05em; font-weight: 900; color: #0f172a; }
        .oi-panel-sub { color: #64748b; font-size: 0.86em; margin-top: 2px; }
        .oi-alert { margin: 8px 0 10px; padding: 8px 12px; border-radius: 10px; font-size: 0.82em; font-weight: 700; }
        .oi-alert.stale { background: #fff7ed; color: #c2410c; border: 1px solid #fed7aa; }
        .oi-badge { display: inline-flex; align-items: center; gap: 6px; margin-left: 8px; font-size: 0.72em; padding: 3px 8px; border-radius: 999px; }
        .oi-badge.unavailable { background: #fff1f2; color: #be123c; border: 1px solid #fecdd3; }
        .oi-chip-value.muted { color: #94a3b8; }
        .oi-summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 10px; margin-bottom: 14px; }
        .oi-chip { border-radius: 14px; background: #f8fafc; border: 1px solid #e8edf5; padding: 10px 12px; }
        .oi-chip-label { font-size: 0.72em; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
        .oi-chip-value { font-size: 1.06em; font-weight: 900; color: #0f172a; margin-top: 3px; }
        .table-scroll { overflow-x: auto; }
        .oi-table { min-width: 1450px; width: 100%; border-collapse: collapse; }
        .oi-table th, .oi-table td { padding: 8px 10px; font-size: 0.85em; white-space: nowrap; }
        .oi-table th { position: sticky; top: 0; z-index: 1; }
        .row-atm { background: #fff4d9; font-weight: 700; }
        .oi-note { margin-top: 10px; color: #b45309; font-size: 0.84em; }
        .oi-history-grid { display: grid; gap: 16px; margin-top: 16px; }
        .oi-history-card { border: 1px solid #e6e8ef; border-radius: 18px; background: #fff; padding: 14px; }
        .oi-history-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }
        .oi-history-title { font-size: 1em; font-weight: 900; color: #0f172a; }
        .oi-history-sub { color: #64748b; font-size: 0.84em; margin-top: 2px; }
        .oi-history-controls { display: flex; flex-direction: column; gap: 6px; min-width: 220px; }
        .oi-history-date-select { min-width: 220px; padding: 9px 12px; border: 1px solid #cfd8e3; border-radius: 10px; background: #fff; color: #0f172a; font-weight: 700; }
        .oi-svg { width: 100%; height: 220px; display: block; }
        .oi-chart-svg { width: 100%; height: 255px; display: block; overflow: visible; shape-rendering: geometricPrecision; text-rendering: geometricPrecision; }
        .oi-chart-axis { fill: #64748b; font-size: 10px; }
        .oi-chart-major-line { stroke: #e5e7eb; stroke-width: 1; }
        .oi-chart-minor-line { stroke: #f1f5f9; stroke-width: 1; }
        .oi-chart-zero { stroke: #cbd5e1; stroke-width: 1.2; stroke-dasharray: 4 4; }
        .oi-chart-point { stroke: #ffffff; stroke-width: 1; }
        .oi-chart-stack { display: grid; gap: 16px; }
        .oi-legend { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 8px; font-size: 0.82em; color: #475569; }
        .legend-item { display: inline-flex; align-items: center; gap: 6px; }
        .legend-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
        .legend-call { background: #2563eb; }
        .legend-change { background: #f97316; }
        .legend-diff { background: #9333ea; }
        .legend-nifty { background: #2563eb; }
        .legend-bank { background: #f97316; }
        .oi-history-table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        .oi-history-table th, .oi-history-table td { padding: 8px 10px; border-bottom: 1px solid #edf2f7; font-size: 0.84em; white-space: nowrap; }
        .oi-side-grid { display: grid; grid-template-columns: 1fr 260px 1fr; gap: 12px; align-items: start; }
        .oi-side-card { border: 1px solid #e8edf5; border-radius: 16px; background: #fff; overflow: hidden; align-self: start; }
        .oi-side-head { padding: 10px 12px; font-weight: 900; color: #0f172a; border-bottom: 1px solid #edf2f7; }
        .oi-side-head.call { background: #eef7e7; }
        .oi-side-head.put { background: #fdeae6; }
        .oi-side-table { width: 100%; border-collapse: collapse; }
        .oi-side-table th, .oi-side-table td { padding: 7px 8px; font-size: 0.8em; white-space: nowrap; border-bottom: 1px solid #f1f5f9; }
        .oi-side-table th { background: #f8fafc; position: sticky; top: 0; z-index: 1; }
        .oi-max-row { background: #eaf2ff !important; box-shadow: inset 4px 0 0 #2563eb; font-weight: 800; }
        .oi-center-card { border: 1px solid #e8edf5; border-radius: 16px; background: linear-gradient(180deg, #fff7f2, #fff); padding: 12px; }
        .oi-center-card .oi-chip { margin-bottom: 8px; }
        .oi-center-card .oi-split-line { margin: 10px 0 12px; }
        .oi-diff-list { display: grid; gap: 8px; margin-top: 8px; }
        .oi-diff-row { border-radius: 12px; padding: 10px 12px; background: #f8fafc; border: 1px solid #edf2f7; }
        .oi-diff-row .label { font-size: 0.74em; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
        .oi-diff-row .value { font-size: 1.2em; font-weight: 900; margin-top: 4px; }
        .oi-diff-row.negative { background: #fff1f1; }
        .oi-diff-row.positive { background: #effaf1; }
        .oi-symbol-tabs { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
        .oi-symbol-tab { border: 1px solid #163d67; background: #fff; color: #163d67; padding: 8px 12px; border-radius: 999px; cursor: pointer; font-weight: 700; }
        .oi-symbol-tab.active { background: #163d67; color: #fff; }
        .oi-filter-row { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 12px; }
        .oi-filter-label { font-size: 0.82em; font-weight: 700; color: #334155; }
        .oi-expiry-select { min-width: 220px; padding: 9px 12px; border: 1px solid #cfd8e3; border-radius: 10px; background: #fff; color: #0f172a; font-weight: 700; }
        .oi-last-fetched { margin-left: auto; color: #64748b; font-size: 0.84em; font-weight: 700; }
        .action-btn:disabled { opacity: 0.7; cursor: not-allowed; }
        .card-subtle { color: #64748b; font-size: 0.84em; margin: -4px 0 12px; }
        .index-sort { display: flex; gap: 8px; flex-wrap: wrap; margin-left: auto; }
        .index-sort-btn { border: 1px solid #163d67; background: #fff; color: #163d67; padding: 8px 12px; border-radius: 999px; cursor: pointer; font-weight: 700; }
        .index-sort-btn.active { background: #163d67; color: #fff; }
        .option-chain-live-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 14px; }
        .option-chain-card { border: 1px solid #e8edf5; border-radius: 16px; background: linear-gradient(180deg, #ffffff, #fafbff); overflow: hidden; }
        .option-chain-card-head { display: flex; justify-content: space-between; gap: 10px; align-items: flex-start; padding: 12px 14px; border-bottom: 1px solid #edf2f7; }
        .option-chain-card-title { font-size: 1em; font-weight: 900; color: #0f172a; }
        .option-chain-card-sub { color: #64748b; font-size: 0.82em; margin-top: 2px; }
        .option-chain-card .table-scroll { max-height: 420px; }
        .option-chain-table { width: 100%; border-collapse: collapse; }
        .option-chain-table th, .option-chain-table td { padding: 7px 8px; font-size: 0.82em; white-space: nowrap; border-bottom: 1px solid #f1f5f9; }
        .option-chain-table th { background: #f8fafc; position: sticky; top: 0; z-index: 1; }
        .signal-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-bottom: 12px; }
        .signal-card { border-radius: 14px; border: 1px solid #e8edf5; background: #f8fafc; padding: 10px 12px; }
        .signal-label { font-size: 0.72em; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
        .signal-value { font-size: 1.02em; font-weight: 900; color: #0f172a; margin-top: 4px; }
        .signal-buy { background: #effaf1; border-color: #cde9d3; }
        .signal-sell { background: #fff1f1; border-color: #f3c0c0; }
        .signal-wait { background: #f8fafc; border-color: #e8edf5; }
        .signal-best { box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.08) inset; }
        .iv-high-head { position: relative; }
        .iv-high-head::after { content: "High IV"; position: absolute; right: 10px; top: 10px; font-size: 0.68em; font-weight: 900; padding: 4px 8px; border-radius: 999px; background: #0f172a; color: #fff; }
        .futures-head-grid { display: grid; gap: 14px; }
        .futures-card { border: 1px solid #e8edf5; border-radius: 16px; background: linear-gradient(180deg, #ffffff, #fafbff); overflow: hidden; }
        .futures-card-head { display: flex; justify-content: space-between; gap: 10px; align-items: flex-start; padding: 12px 14px; border-bottom: 1px solid #edf2f7; }
        .futures-card-title { font-size: 1em; font-weight: 900; color: #0f172a; }
        .futures-card-sub { color: #64748b; font-size: 0.82em; margin-top: 2px; }
        .futures-table { width: 100%; border-collapse: collapse; }
        .futures-table th, .futures-table td { padding: 7px 8px; font-size: 0.84em; white-space: nowrap; border-bottom: 1px solid #f1f5f9; }
        .futures-table th { background: #f8fafc; position: sticky; top: 0; z-index: 1; }
        .dashboard-hidden { display: none !important; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Paurik Trivedi's Live Market Dashboard</h1>
        <div class="header-controls">
            <span id="ws-status-dot" class="status-dot disconnected" title="Disconnected"></span>
            <span id="mode-badge" class="badge">Loading...</span>
        </div>
    </div>

    <div class="connection-banner">
        <div class="connection-top">
            <div>
                <div class="connection-title">Live Connection Quality</div>
                <div class="connection-sub">Broker feed health, tick freshness, and reconnect pressure.</div>
            </div>
            <div id="connection-pill" class="connection-pill bad">0%</div>
        </div>
        <div class="connection-meter">
            <div id="connection-meter-fill" class="connection-meter-fill bad" style="width: 0%;"></div>
        </div>
        <div class="connection-meta">
            <span id="connection-state">Disconnected</span>
            <span id="connection-freshness">Tick age: -</span>
            <span id="connection-message-age">Message age: -</span>
            <span id="connection-reconnects">Reconnects: 0</span>
        </div>
    </div>

    <div class="card dashboard-hidden">
        <h2>Today's P&L</h2>
        <div id="pnl-amount" class="pnl">₹0.00</div>
    </div>

    <div class="card dashboard-hidden">
        <h2>Strategy Management</h2>
        <div class="strategy-container">
            <div class="strategy-summary">
                <div class="stat-box live-box">
                    <div>Live</div>
                    <div id="stat-live" class="num">0</div>
                </div>
                <div class="stat-box paused-box">
                    <div>Paused</div>
                    <div id="stat-paused" class="num">0</div>
                </div>
                <div class="stat-box stopped-box">
                    <div>Stopped</div>
                    <div id="stat-stopped" class="num">0</div>
                </div>
            </div>
            <div class="overview-grid">
                <div class="overview-card">
                    <div class="overview-label">Live Strategies</div>
                    <div id="overview-live-count" class="overview-value">0</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">Total Strategy P&amp;L</div>
                    <div id="overview-total-pnl" class="overview-value">₹0.00</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">Open Strategy Positions</div>
                    <div id="overview-open-positions" class="overview-value">0</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">Today&apos;s Rejections</div>
                    <div id="overview-rejected" class="overview-value">0</div>
                </div>
            </div>
            <div id="strategies-body" class="strategy-grid"></div>
        </div>
    </div>

    <div class="card dashboard-hidden">
        <h2>Personal Algo Controls</h2>
        <div class="control-grid">
            <div class="metric-card">
                <div class="metric-label">Active Strategy</div>
                <div id="metric-active-strategy" class="metric-value">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Tick Freshness</div>
                <div id="metric-tick-age" class="metric-value">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Fill Rate (Today)</div>
                <div id="metric-fill-rate" class="metric-value">0%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Avg Slippage</div>
                <div id="metric-slippage" class="metric-value">0.00</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Open Positions</div>
                <div id="metric-open-pos" class="metric-value">0/0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Daily Loss Used</div>
                <div id="metric-loss-used" class="metric-value">0%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Rejected Orders</div>
                <div id="metric-rejected" class="metric-value">0</div>
            </div>
                <div class="metric-card">
                    <div class="metric-label">Option Rows</div>
                    <div id="metric-option-rows" class="metric-value">0</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">OI Snapshot Age</div>
                    <div id="metric-oi-age" class="metric-value">-</div>
                </div>
            </div>
        <div class="control-actions">
            <button class="action-btn btn-pause" onclick="runControlAction('pause_active')">Pause Live</button>
            <button class="action-btn btn-resume" onclick="runControlAction('resume_active')">Resume Paused</button>
            <button class="action-btn btn-stop" onclick="runControlAction('emergency_stop')">Emergency Stop All</button>
            <button class="action-btn btn-neutral" onclick="runControlAction('refresh_option_chain')">Refresh Option Chain</button>
        </div>
    </div>

    <div class="card">
        <h2>Live Indices</h2>
        <div class="index-toolbar">
            <input id="index-search" class="index-search" type="text" placeholder="Search index">
            <div class="index-tabs">
                <button class="index-tab active" data-index-tab="all" onclick="setIndexTab('all')">All</button>
                <button class="index-tab" data-index-tab="key" onclick="setIndexTab('key')">Key indices</button>
                <button class="index-tab" data-index-tab="sector" onclick="setIndexTab('sector')">Sector-based indices</button>
            </div>
            <div class="index-sort">
                <button class="index-sort-btn active" data-index-sort="desc" onclick="setIndexSort('desc')">High to low %</button>
                <button class="index-sort-btn" data-index-sort="asc" onclick="setIndexSort('asc')">Low to high %</button>
            </div>
        </div>
        <div id="indices-body" class="index-list"></div>
    </div>

    <div class="card">
        <h2>Futures Head</h2>
        <div class="card-subtle">Next 3 live futures contracts for NIFTY and BANKNIFTY, updated every second.</div>
        <div id="futures-head" class="futures-head-grid"></div>
    </div>

    <div class="card">
        <h2>OI / IV Dashboard</h2>
        <div class="card-subtle">Derived OI summary and session history. Use the live option ladder below for strike-level detail.</div>
        <div class="oi-symbol-tabs">
            <button class="oi-symbol-tab active" data-oi-symbol="NIFTY" onclick="setOiSymbol('NIFTY')">NIFTY</button>
            <button class="oi-symbol-tab" data-oi-symbol="BANKNIFTY" onclick="setOiSymbol('BANKNIFTY')">BANKNIFTY</button>
        </div>
        <div class="oi-filter-row">
            <div class="oi-filter-label">Expiry</div>
            <select id="oi-expiry-select" class="oi-expiry-select" onchange="setOiExpiry(this.value)"></select>
            <button id="oi-refresh-btn" class="action-btn btn-neutral" onclick="refreshOiNow()">Refresh OI Data</button>
            <div id="oi-last-fetched" class="oi-last-fetched">Last fetched: -</div>
        </div>
        <div id="oi-dashboard" class="oi-wrap"></div>
    </div>

    <div class="card">
        <h2>OI Difference History (5 min)</h2>
        <div id="oi-history" class="oi-history-grid"></div>
    </div>

    <div class="card dashboard-hidden">
        <h2>Positions by Strategy</h2>
        <table>
            <thead>
                <tr>
                    <th>Strategy</th>
                    <th>Product</th>
                    <th>Instrument</th>
                    <th>Qty</th>
                    <th>Avg</th>
                    <th>LTP</th>
                    <th>P&amp;L</th>
                    <th>Chg</th>
                </tr>
            </thead>
            <tbody id="positions-body">
            </tbody>
        </table>
    </div>

    <div class="card dashboard-hidden">
        <h2>Recent Trades</h2>
        <table>
            <thead>
                <tr>
                    <th>Strategy</th>
                    <th>Time</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Price</th>
                </tr>
            </thead>
            <tbody id="trades-body">
            </tbody>
        </table>
    </div>

    <script>
        let strategiesData = [];
        let strategyOverviewData = [];
        let indicesDataCache = [];
        let futuresHeadData = __INITIAL_FUTURES_HEAD__;
        let oiDashboardData = __INITIAL_OI_DASHBOARD__;
        let oiDashboardDataBySymbol = {};
        let oiHistoryStateByKey = {};
        let oiHistoryFetchInFlightKeys = new Set();
        let oiHistoryDatesFetchInFlightKeys = new Set();
        let oiHistoryArchiveFetchInFlightKeys = new Set();
        let dashboardFetchInFlight = false;
        let oiDashboardFetchInFlight = false;
        let oiDashboardRequestSeq = 0;
        let activeIndexTab = 'all';
        let activeIndexSort = 'desc';
        let selectedOiSymbol = 'NIFTY';
        let selectedOiExpiryBySymbol = { NIFTY: '', BANKNIFTY: '' };
        let futuresHeadFetchInFlight = false;
        let marketDataRefreshInFlight = false;
        let lastMarketDataRefreshAt = 0;
        const OI_SESSION_START_MINUTES = 9 * 60 + 15;
        const OI_SESSION_END_MINUTES = 15 * 60 + 30;
        const OI_SESSION_STEP_MINUTES = 5;
        const OI_HISTORY_REFRESH_MS = 15000;
        const OI_HISTORY_DATES_REFRESH_MS = 60000;
        const OI_HISTORY_ARCHIVE_REFRESH_MS = 300000;
        const MARKET_DATA_REFRESH_MS = 5000;

        function statusClass(status) {
            const value = (status || '').toLowerCase();
            if (value === 'live') return 'status-live';
            if (value === 'paused') return 'status-paused';
            return 'status-stopped';
        }

        function statusPillClass(status) {
            const value = (status || '').toLowerCase();
            if (value === 'live') return 'live';
            if (value === 'paused') return 'paused';
            return 'stopped';
        }

        function normalizeOiSymbol(symbol) {
            const value = String(symbol || 'NIFTY').trim().toUpperCase();
            return value || 'NIFTY';
        }

        function getOiHistoryKey(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol)) {
            const activeSymbol = normalizeOiSymbol(symbol);
            const activeExpiry = String(expiry || '').trim() || 'AUTO';
            return `${activeSymbol}::${activeExpiry}`;
        }

        function ensureOiHistoryState(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol)) {
            const key = getOiHistoryKey(symbol, expiry);
            if (!oiHistoryStateByKey[key]) {
                oiHistoryStateByKey[key] = {
                    todayRows: [],
                    todayHistoryDate: '',
                    todayFetchedAt: 0,
                    availableDates: [],
                    datesFetchedAt: 0,
                    selectedDate: '',
                    archiveRowsByDate: {},
                    archiveFetchedAtByDate: {},
                };
            }
            return { key, state: oiHistoryStateByKey[key] };
        }

        function toLocalIsoDate(dateObj = new Date()) {
            const year = dateObj.getFullYear();
            const month = String(dateObj.getMonth() + 1).padStart(2, '0');
            const day = String(dateObj.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }

        function isTodayHistoryDate(dateValue) {
            return String(dateValue || '').trim() === toLocalIsoDate();
        }

        function formatHistoryDateLabel(dateValue) {
            const value = String(dateValue || '').trim();
            if (!value) return '-';
            const label = formatExpiryLabel(value);
            return isTodayHistoryDate(value) ? `${label} (Today)` : label;
        }

        function chooseDefaultHistoryDate(dates) {
            const cleanDates = Array.isArray(dates)
                ? dates.map(item => String(item?.date || item || '').trim()).filter(Boolean)
                : [];
            if (!cleanDates.length) return '';
            const today = toLocalIsoDate();
            const recentPast = cleanDates.find(value => value !== today);
            return recentPast || cleanDates[0] || '';
        }

        async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 9000) {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), Math.max(1000, Number(timeoutMs) || 9000));
            try {
                const response = await fetch(url, {
                    cache: 'no-store',
                    ...options,
                    signal: controller.signal,
                });
                const raw = await response.text();
                let payload = null;
                try {
                    payload = raw ? JSON.parse(raw) : {};
                } catch (_) {
                    payload = { message: raw || '' };
                }
                if (!response.ok) {
                    throw new Error(payload?.message || `HTTP ${response.status}`);
                }
                return payload;
            } catch (err) {
                if (err && err.name === 'AbortError') {
                    throw new Error('Request timed out');
                }
                throw err;
            } finally {
                clearTimeout(timer);
            }
        }

        function seedOiDashboardCache(payload) {
            if (Array.isArray(payload)) {
                payload.forEach(section => {
                    const key = normalizeOiSymbol(section?.symbol);
                    if (key && section) {
                        oiDashboardDataBySymbol[key] = section;
                    }
                });
                return;
            }
            if (payload && typeof payload === 'object') {
                const key = normalizeOiSymbol(payload.symbol);
                if (key) {
                    oiDashboardDataBySymbol[key] = payload;
                }
            }
        }

        seedOiDashboardCache(__INITIAL_OI_DASHBOARD__);

        function getActiveOiExpiry(symbol = selectedOiSymbol) {
            return selectedOiExpiryBySymbol[normalizeOiSymbol(symbol)] || '';
        }

        async function setOiSymbol(symbol) {
            selectedOiSymbol = normalizeOiSymbol(symbol);
            document.querySelectorAll('.oi-symbol-tab').forEach(btn => {
                btn.classList.toggle('active', normalizeOiSymbol(btn.dataset.oiSymbol) === selectedOiSymbol);
            });
            renderOiExpiryOptions(oiDashboardDataBySymbol[selectedOiSymbol] || oiDashboardData, selectedOiSymbol);
            renderOiDashboard(selectedOiSymbol);
            renderOiHistoryCharts(selectedOiSymbol);
            await fetchOiDashboard(selectedOiSymbol, getActiveOiExpiry(selectedOiSymbol), true);
        }

        async function setOiExpiry(expiry) {
            const key = normalizeOiSymbol(selectedOiSymbol);
            selectedOiExpiryBySymbol[key] = expiry || '';
            renderOiExpiryOptions(oiDashboardDataBySymbol[key] || oiDashboardData, key);
            renderOiDashboard(key);
            renderOiHistoryCharts(key);
            await fetchOiDashboard(key, expiry || '', true);
        }

        async function refreshOiNow() {
            const button = document.getElementById('oi-refresh-btn');
            const originalLabel = button ? button.textContent : 'Refresh OI Data';
            if (button) {
                button.disabled = true;
                button.textContent = 'Refreshing...';
            }
            try {
                const payload = await fetchJsonWithTimeout('/api/control/refresh_oi', { method: 'POST' }, 7000);
                if (payload?.ok === false) {
                    console.warn(payload.message || 'OI refresh failed');
                }
                const activeSymbol = normalizeOiSymbol(selectedOiSymbol);
                const activeExpiry = getActiveOiExpiry(activeSymbol);

                const runUiRefreshPass = () => {
                    fetchOiDashboard(activeSymbol, activeExpiry, false).catch(e => console.error('OI dashboard refresh pass failed', e));
                    Promise.allSettled([
                        fetchOiHistory(activeSymbol, activeExpiry, false),
                        fetchOiHistoryDates(activeSymbol, activeExpiry, false),
                        fetchFuturesHead(false),
                        fetchHealth(),
                    ]).then(() => {
                        renderOiHistoryCharts(activeSymbol);
                    }).catch(e => console.error('OI refresh pass post-render failed', e));
                };

                // Non-blocking refresh passes so UI never freezes on slow upstream data providers.
                runUiRefreshPass();
                setTimeout(runUiRefreshPass, 1500);
                setTimeout(runUiRefreshPass, 3500);
            } catch (e) {
                console.error('Error refreshing OI data', e);
                const activeSymbol = normalizeOiSymbol(selectedOiSymbol);
                const activeExpiry = getActiveOiExpiry(activeSymbol);
                const runUiRefreshPass = () => {
                    fetchOiDashboard(activeSymbol, activeExpiry, false).catch(err => console.error('OI dashboard refresh fallback failed', err));
                    Promise.allSettled([
                        fetchOiHistory(activeSymbol, activeExpiry, false),
                        fetchOiHistoryDates(activeSymbol, activeExpiry, false),
                        fetchFuturesHead(false),
                        fetchHealth(),
                    ]).then(() => {
                        renderOiHistoryCharts(activeSymbol);
                    }).catch(err => console.error('OI refresh fallback post-render failed', err));
                };
                runUiRefreshPass();
                setTimeout(runUiRefreshPass, 1500);
                setTimeout(runUiRefreshPass, 3500);
            } finally {
                if (button) {
                    button.disabled = false;
                    button.textContent = originalLabel;
                }
            }
        }

        function formatSessionMinute(minuteOfDay) {
            const hours = Math.floor(minuteOfDay / 60);
            const minutes = minuteOfDay % 60;
            const date = new Date();
            date.setHours(hours, minutes, 0, 0);
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }

        function parseExpiryDateValue(expiry) {
            const value = String(expiry ?? '').trim();
            if (!value) return null;

            const direct = new Date(value);
            if (!Number.isNaN(direct.getTime())) {
                return direct;
            }

            if (/^\d+(?:\.\d+)?$/.test(value)) {
                const numeric = Number(value);
                const millis = numeric > 1e12 ? numeric : numeric * 1000;
                const dt = new Date(millis);
                if (!Number.isNaN(dt.getTime())) {
                    return dt;
                }
            }

            const dateMatch = value.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
            if (dateMatch) {
                const [, day, month, year] = dateMatch;
                const monthMap = {
                    JAN: 0,
                    FEB: 1,
                    MAR: 2,
                    APR: 3,
                    MAY: 4,
                    JUN: 5,
                    JUL: 6,
                    AUG: 7,
                    SEP: 8,
                    OCT: 9,
                    NOV: 10,
                    DEC: 11,
                };
                const monthIdx = monthMap[month.toUpperCase()];
                if (monthIdx != null) {
                    const dt = new Date(Number(year), monthIdx, Number(day));
                    if (!Number.isNaN(dt.getTime())) {
                        return dt;
                    }
                }
            }

            return null;
        }

        function isFutureOrTodayExpiry(expiry) {
            const dt = parseExpiryDateValue(expiry);
            if (!dt) return false;
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            const normalized = new Date(dt);
            normalized.setHours(0, 0, 0, 0);
            return normalized >= today;
        }

        function formatExpiryLabel(expiry) {
            const value = String(expiry ?? '').trim();
            if (!value) return 'Live / Auto';

            const parsed = new Date(value);
            if (!Number.isNaN(parsed.getTime())) {
                return parsed.toLocaleDateString('en-GB', {
                    day: '2-digit',
                    month: 'short',
                    year: 'numeric',
                }).replace(/\s+/g, '-');
            }

            if (/^\d+(?:\.\d+)?$/.test(value)) {
                const numeric = Number(value);
                const millis = numeric > 1e12 ? numeric : numeric * 1000;
                const dt = new Date(millis);
                if (!Number.isNaN(dt.getTime())) {
                    return dt.toLocaleDateString('en-GB', {
                        day: '2-digit',
                        month: 'short',
                        year: 'numeric',
                    }).replace(/\s+/g, '-');
                }
            }

            const dateMatch = value.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
            if (dateMatch) {
                const [, day, month, year] = dateMatch;
                return `${day.padStart(2, '0')}-${month}-${year}`;
            }

            return value;
        }

        function formatFetchTimestamp(timestampValue) {
            const raw = String(timestampValue || '').trim();
            if (!raw) return '-';
            const dt = new Date(raw);
            if (!Number.isNaN(dt.getTime())) {
                return dt.toLocaleString('en-GB', {
                    day: '2-digit',
                    month: 'short',
                    year: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                    hour12: false,
                }).replace(',', '');
            }
            return raw;
        }

        function displayNumber(value, decimals = 0) {
            if (value == null || value === '' || Number.isNaN(Number(value))) return '-';
            const numeric = Number(value);
            if (!Number.isFinite(numeric)) return '-';
            return decimals > 0 ? numeric.toFixed(decimals) : String(Math.trunc(numeric));
        }

        function updateOiLastFetched(section) {
            const target = document.getElementById('oi-last-fetched');
            if (!target) return;
            target.textContent = `Last fetched: ${formatFetchTimestamp(section?.last_live_tick_at || section?.last_fetched_at)}`;
        }

        function buildOiTimeline(points) {
            const slots = [];
            const slotMap = new Map();

            for (let minute = OI_SESSION_START_MINUTES; minute <= OI_SESSION_END_MINUTES; minute += OI_SESSION_STEP_MINUTES) {
                const slot = {
                    minute,
                    label: formatSessionMinute(minute),
                    captured_at: null,
                    call_oi_sum: null,
                    put_oi_sum: null,
                    call_change_oi_sum: null,
                    put_change_oi_sum: null,
                    oi_diff: null,
                    change_oi_diff: null,
                    pcr: null,
                    change_pcr: null,
                };
                slots.push(slot);
                slotMap.set(minute, slot);
            }

            (points || []).forEach(point => {
                const stamp = new Date(point.captured_at);
                if (Number.isNaN(stamp.getTime())) return;
                const minute = stamp.getHours() * 60 + stamp.getMinutes();
                if (minute < OI_SESSION_START_MINUTES || minute > OI_SESSION_END_MINUTES) return;
                const rounded = Math.round(minute / OI_SESSION_STEP_MINUTES) * OI_SESSION_STEP_MINUTES;
                const slotMinute = Math.min(
                    OI_SESSION_END_MINUTES,
                    Math.max(OI_SESSION_START_MINUTES, rounded),
                );
                const slot = slotMap.get(slotMinute);
                if (!slot) return;
                slot.captured_at = point.captured_at;
                slot.call_oi_sum = Number(point.call_oi_sum ?? 0);
                slot.put_oi_sum = Number(point.put_oi_sum ?? 0);
                slot.call_change_oi_sum = Number(point.call_change_oi_sum ?? 0);
                slot.put_change_oi_sum = Number(point.put_change_oi_sum ?? 0);
                slot.oi_diff = Number(point.oi_diff ?? 0);
                slot.change_oi_diff = Number(point.change_oi_diff ?? 0);
                slot.pcr = point.pcr;
                slot.change_pcr = point.change_pcr;
            });

            return slots;
        }

        function renderTrendChartCard({ title, subtitle, slots, series, height = 255 }) {
            const width = 1200;
            const leftPad = 52;
            const topPad = 22;
            const bottomPad = 34;
            const plotWidth = width - leftPad - 20;
            const plotHeight = height - topPad - bottomPad;
            const labelEveryMinutes = 30;
            const majorGridEveryMinutes = 60;

            const values = [];
            series.forEach(def => {
                slots.forEach(slot => {
                    const value = Number(def.value(slot));
                    if (Number.isFinite(value) && !(def.skipZero !== false && value === 0)) {
                        values.push(value);
                    }
                });
            });

            if (!values.length) {
                return `
                    <div class="oi-history-card">
                        <div class="oi-history-head">
                            <div>
                                <div class="oi-history-title">${title}</div>
                                <div class="oi-history-sub">${subtitle}</div>
                            </div>
                        </div>
                        <div class="oi-chart-caption">Waiting for live captures during market hours.</div>
                    </div>
                `;
            }

            const minValue = Math.min(0, ...values);
            const maxValue = Math.max(0, ...values);
            const padding = Math.max((maxValue - minValue) * 0.12, 1);
            const plotMin = minValue - padding;
            const plotMax = maxValue + padding;
            const valueRange = (plotMax - plotMin) || 1;
            const xFor = idx => leftPad + (idx * plotWidth) / Math.max(slots.length - 1, 1);
            const yFor = value => topPad + ((plotMax - value) / valueRange) * plotHeight;

            const zeroLine = (plotMin <= 0 && plotMax >= 0)
                ? `<line x1="${leftPad}" y1="${yFor(0)}" x2="${width - 18}" y2="${yFor(0)}" class="oi-chart-zero" />`
                : '';
            const yGrid = Array.from({ length: 5 }, (_, idx) => {
                const y = topPad + (plotHeight * idx) / 4;
                return `<line x1="${leftPad}" y1="${y}" x2="${width - 18}" y2="${y}" class="${idx === 2 ? 'oi-chart-major-line' : 'oi-chart-minor-line'}" />`;
            }).join('');

            const xGrid = slots.map((slot, idx) => {
                const minuteOffset = slot.minute - OI_SESSION_START_MINUTES;
                const isMajor = minuteOffset % majorGridEveryMinutes === 0 || idx === 0 || idx === slots.length - 1;
                const isVisible = minuteOffset % labelEveryMinutes === 0 || idx === 0 || idx === slots.length - 1;
                if (!isVisible) return '';
                const x = xFor(idx);
                return `<line x1="${x}" y1="${topPad}" x2="${x}" y2="${topPad + plotHeight}" class="${isMajor ? 'oi-chart-major-line' : 'oi-chart-minor-line'}" opacity="${isMajor ? 0.75 : 0.3}" />`;
            }).join('');

            const xLabels = slots.map((slot, idx) => {
                const minuteOffset = slot.minute - OI_SESSION_START_MINUTES;
                const shouldLabel = minuteOffset % labelEveryMinutes === 0 || idx === 0 || idx === slots.length - 1;
                if (!shouldLabel) return '';
                const x = xFor(idx);
                return `<text x="${x}" y="${height - 8}" text-anchor="middle" class="oi-chart-axis">${slot.label}</text>`;
            }).join('');

            const lineSegments = series.map(def => {
                const lineParts = [];
                const pointParts = [];
                const points = [];

                slots.forEach((slot, idx) => {
                    const value = Number(def.value(slot));
                    if (!Number.isFinite(value) || (def.skipZero !== false && value === 0)) return;
                    const x = xFor(idx);
                    const y = yFor(value);
                    points.push([x, y]);
                    if (def.showPoints !== false) {
                        pointParts.push(`<circle cx="${x}" cy="${y}" r="2.7" fill="${def.color}" opacity="${def.pointOpacity ?? 0.9}" class="oi-chart-point" />`);
                    }
                });

                if (points.length) {
                    lineParts.push(`
                        <polyline
                            fill="none"
                            stroke="${def.color}"
                            stroke-width="${def.strokeWidth || 2.8}"
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            ${def.dasharray ? `stroke-dasharray="${def.dasharray}"` : ''}
                            points="${points.map(([x, y]) => `${x},${y}`).join(' ')}"
                        ></polyline>
                    `);
                }

                return lineParts.join('') + pointParts.join('');
            }).join('');

            const latestSlot = [...slots].reverse().find(slot => slot.captured_at);
            const latestLabel = latestSlot ? latestSlot.label : '-';
            const latestValues = latestSlot ? series.map(def => {
                const value = Number(def.value(latestSlot));
                return Number.isFinite(value) ? value : null;
            }) : [];
            const summaryBits = latestSlot
                ? latestValues.map((value, idx) => `${series[idx].label}: ${value == null ? '-' : value}`).join(' | ')
                : 'Waiting for live captures during market hours.';

            return `
                <div class="oi-history-card">
                    <div class="oi-history-head">
                        <div>
                            <div class="oi-history-title">${title}</div>
                            <div class="oi-history-sub">Latest: ${latestLabel} | ${summaryBits}</div>
                        </div>
                    </div>
                    <svg class="oi-chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
                        ${yGrid}
                        ${xGrid}
                        ${zeroLine}
                        ${lineSegments}
                        ${xLabels}
                    </svg>
                    <div class="oi-legend">
                        ${series.map(def => `<span class="legend-item"><span class="legend-dot" style="background:${def.color};${def.legendStyle || ''}"></span>${def.label}</span>`).join('')}
                    </div>
                </div>
            `;
        }

        function renderOiHistoryTableCard({ title, subtitle, slots, emptyMessage }) {
            const populatedSlots = (slots || []).filter(slot => slot && slot.captured_at);
            if (!populatedSlots.length) {
                return `
                    <div class="oi-history-card">
                        <div class="oi-history-head">
                            <div>
                                <div class="oi-history-title">${title}</div>
                                <div class="oi-history-sub">${emptyMessage}</div>
                            </div>
                        </div>
                    </div>
                `;
            }

            const tableRows = populatedSlots.slice(-12);
            return `
                <div class="oi-history-card">
                    <div class="oi-history-head">
                        <div>
                            <div class="oi-history-title">${title}</div>
                            <div class="oi-history-sub">${subtitle}</div>
                        </div>
                    </div>
                    <div class="table-scroll">
                        <table class="oi-history-table">
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Call OI</th>
                                    <th>Put OI</th>
                                    <th>Call COI</th>
                                    <th>Put COI</th>
                                    <th>Put - Call OI</th>
                                    <th>Put - Call COI</th>
                                    <th>PCR</th>
                                    <th>Change PCR</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${tableRows.map(point => `
                                    <tr>
                                        <td>${point.label}</td>
                                        <td>${point.call_oi_sum ?? '-'}</td>
                                        <td>${point.put_oi_sum ?? '-'}</td>
                                        <td>${point.call_change_oi_sum ?? '-'}</td>
                                        <td>${point.put_change_oi_sum ?? '-'}</td>
                                        <td>${point.oi_diff}</td>
                                        <td>${point.change_oi_diff}</td>
                                        <td>${point.pcr == null ? '-' : Number(point.pcr).toFixed(4)}</td>
                                        <td>${point.change_pcr == null ? '-' : Number(point.change_pcr).toFixed(4)}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        function setIndexTab(tab) {
            activeIndexTab = tab;
            document.querySelectorAll('.index-tab').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.indexTab === tab);
            });
            renderIndices();
        }

        function setIndexSort(sortOrder) {
            activeIndexSort = sortOrder;
            document.querySelectorAll('.index-sort-btn').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.indexSort === sortOrder);
            });
            renderIndices();
        }

        function renderIndices() {
            const query = (document.getElementById('index-search').value || '').trim().toLowerCase();
            const indicesBody = document.getElementById('indices-body');
            const filtered = indicesDataCache.filter(item => {
                const matchesTab =
                    activeIndexTab === 'all' ||
                    (activeIndexTab === 'key' && item.group === 'key') ||
                    (activeIndexTab === 'sector' && item.group === 'sector');
                const matchesSearch = !query || (item.symbol || '').toLowerCase().includes(query);
                return matchesTab && matchesSearch;
            }).sort((a, b) => {
                const aPct = Number(a.change_pct);
                const bPct = Number(b.change_pct);
                const aValid = Number.isFinite(aPct);
                const bValid = Number.isFinite(bPct);
                if (!aValid && !bValid) return 0;
                if (!aValid) return 1;
                if (!bValid) return -1;
                return activeIndexSort === 'asc' ? aPct - bPct : bPct - aPct;
            });

            indicesBody.innerHTML = '';
            if (!filtered.length) {
                indicesBody.innerHTML = `<div class="index-card"><div class="index-name">No matching indices</div><div class="index-muted">Try another search</div></div>`;
                return;
            }

            filtered.forEach(item => {
                const change = item.change == null ? '-' : item.change.toFixed(2);
                const changePct = item.change_pct == null ? '-' : `${item.change_pct.toFixed(2)}%`;
                const changeClass = item.status === 'unavailable' ? '' : ((item.change || 0) >= 0 ? 'positive' : 'negative');
                indicesBody.innerHTML += `
                    <div class="index-card">
                        <div>
                            <div class="index-name">${item.symbol}</div>
                            <div class="index-muted">${item.group === 'key' ? 'Key index' : 'Sector index'}</div>
                        </div>
                        <div class="index-meta">
                            <div class="index-ltp">${item.ltp == null ? '-' : item.ltp.toFixed(2)}</div>
                            <div class="index-change ${changeClass}">${change} (${changePct})</div>
                        </div>
                    </div>
                `;
            });
        }

        function averagePositive(values) {
            const filtered = (values || []).map(Number).filter(value => Number.isFinite(value) && value > 0);
            if (!filtered.length) return null;
            return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
        }

        function signalClassFor(action) {
            const value = (action || '').toUpperCase();
            if (value.includes('BUY CALL')) return 'signal-buy';
            if (value.includes('BUY PUT')) return 'signal-sell';
            return 'signal-wait';
        }

        function computeOiSignals(section) {
            const summary = section?.summary || {};
            const rows = section?.rows || [];
            const oiDiff = Number(summary.oi_diff || 0);
            const changeOiDiff = Number(summary.change_oi_diff || 0);
            const oiSignal = oiDiff > 0 ? 'BUY CALL' : oiDiff < 0 ? 'BUY PUT' : 'WAIT';
            const changeSignal = changeOiDiff > 0 ? 'BUY CALL' : changeOiDiff < 0 ? 'BUY PUT' : 'WAIT';
            const callAvgIv = averagePositive(rows.map(row => row?.call?.iv));
            const putAvgIv = averagePositive(rows.map(row => row?.put?.iv));
            let ivSignal = 'WAIT';
            if (callAvgIv != null || putAvgIv != null) {
                if (callAvgIv != null && putAvgIv != null) {
                    if (callAvgIv > putAvgIv) ivSignal = 'BUY CALL';
                    else if (putAvgIv > callAvgIv) ivSignal = 'BUY PUT';
                } else if (callAvgIv != null) {
                    ivSignal = 'BUY CALL';
                } else if (putAvgIv != null) {
                    ivSignal = 'BUY PUT';
                }
            }
            const bestTrade = (oiSignal !== 'WAIT' && oiSignal === changeSignal && oiSignal === ivSignal) ? oiSignal : 'WAIT';
            return { oiSignal, changeSignal, ivSignal, bestTrade, callAvgIv, putAvgIv };
        }

        function renderFuturesHead() {
            const container = document.getElementById('futures-head');
            if (!container) return;

            if (!futuresHeadData.length) {
                container.innerHTML = `
                    <div class="futures-card">
                        <div class="futures-card-head">
                            <div>
                                <div class="futures-card-title">Futures data waiting</div>
                                <div class="futures-card-sub">Live futures rows will appear once the websocket feed captures them.</div>
                            </div>
                        </div>
                    </div>
                `;
                return;
            }

            container.innerHTML = futuresHeadData.map(section => {
                const rows = (section.contracts || []).filter(row => !row.expiry || isFutureOrTodayExpiry(row.expiry));
                const rowsHtml = rows.length ? rows.map(row => `
                    <tr>
                        <td>${row.month_label || row.expiry || '-'}</td>
                        <td>${row.expiry ? formatExpiryLabel(row.expiry) : '-'}</td>
                        <td>${row.trading_symbol || row.symbol || '-'}</td>
                        <td class="positive" style="font-weight:900;">${row.ltp == null ? '-' : Number(row.ltp).toFixed(2)}</td>
                        <td>${row.bid == null ? '-' : Number(row.bid).toFixed(2)}</td>
                        <td>${row.ask == null ? '-' : Number(row.ask).toFixed(2)}</td>
                        <td>${row.oi == null ? '-' : row.oi}</td>
                        <td>${row.volume == null ? '-' : row.volume}</td>
                    </tr>
                `).join('') : `<tr><td colspan="8" style="text-align:center;color:#64748b;">No live contracts yet.</td></tr>`;
                return `
                    <div class="futures-card">
                        <div class="futures-card-head">
                            <div>
                                <div class="futures-card-title">${section.label || section.underlying}</div>
                                <div class="futures-card-sub">Current month, next month, and 3rd month contracts from the live feed, plus GIFT Nifty</div>
                            </div>
                        </div>
                        <div class="table-scroll">
                            <table class="futures-table">
                                <thead>
                                    <tr>
                                        <th>Month</th>
                                        <th>Expiry</th>
                                        <th>Symbol</th>
                                        <th>LTP</th>
                                        <th>Bid</th>
                                        <th>Ask</th>
                                        <th>OI</th>
                                        <th>Vol</th>
                                    </tr>
                                </thead>
                                <tbody>${rowsHtml}</tbody>
                            </table>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function strategyActionButtons(strategy) {
            const status = (strategy.status || '').toLowerCase();
            if (status === 'live') {
                return `
                    <div class="strategy-action-row">
                        <button class="action-btn btn-pause" onclick="updateStrategyState('${strategy.id}','pause')">Pause</button>
                        <button class="action-btn btn-stop" onclick="updateStrategyState('${strategy.id}','stop')">Stop</button>
                    </div>
                `;
            }
            if (status === 'paused') {
                return `
                    <div class="strategy-action-row">
                        <button class="action-btn btn-resume" onclick="updateStrategyState('${strategy.id}','resume')">Resume</button>
                        <button class="action-btn btn-stop" onclick="updateStrategyState('${strategy.id}','stop')">Stop</button>
                    </div>
                `;
            }
            return `
                <div class="strategy-action-row">
                    <button class="action-btn btn-start" onclick="updateStrategyState('${strategy.id}','start')">Start</button>
                </div>
            `;
        }

        function formatMoney(value) {
            const num = Number(value || 0);
            return `₹${num.toFixed(2)}`;
        }

        function renderStrategyCards() {
            const container = document.getElementById('strategies-body');
            container.innerHTML = '';

            let liveCount = 0;
            let pausedCount = 0;
            let stoppedCount = 0;
            let combinedPnl = 0;
            let combinedOpen = 0;

            strategyOverviewData.forEach(strategy => {
                const status = (strategy.status || '').toLowerCase();
                if (status === 'live') liveCount += 1;
                else if (status === 'paused') pausedCount += 1;
                else stoppedCount += 1;

                combinedPnl += Number(strategy.total_pnl || 0);
                combinedOpen += Number(strategy.open_positions_count || 0);

                const positions = Object.entries(strategy.positions || {});
                const positionsHtml = positions.length
                    ? positions.map(([symbol, qty]) => `<span class="position-chip">${symbol}: ${qty}</span>`).join('')
                    : '<span class="strategy-note">No open positions</span>';

                const card = document.createElement('div');
                card.className = 'strategy-card';
                card.innerHTML = `
                    <div class="strategy-top">
                        <div>
                            <div class="strategy-title">${strategy.name}</div>
                            <div class="strategy-desc">${strategy.description}</div>
                        </div>
                        <span class="status-pill ${statusPillClass(strategy.status)}">${strategy.status}</span>
                    </div>
                    <div class="strategy-desc">${strategy.schedule}</div>
                    <div class="strategy-metrics">
                        <div class="strategy-metric">
                            <div class="strategy-metric-label">Total P&amp;L</div>
                            <div class="strategy-metric-value ${Number(strategy.total_pnl || 0) >= 0 ? 'positive' : 'negative'}">${formatMoney(strategy.total_pnl)}</div>
                        </div>
                        <div class="strategy-metric">
                            <div class="strategy-metric-label">Open Positions</div>
                            <div class="strategy-metric-value">${strategy.open_positions_count || 0}</div>
                        </div>
                        <div class="strategy-metric">
                            <div class="strategy-metric-label">Realized P&amp;L</div>
                            <div class="strategy-metric-value ${Number(strategy.realized_pnl || 0) >= 0 ? 'positive' : 'negative'}">${formatMoney(strategy.realized_pnl)}</div>
                        </div>
                        <div class="strategy-metric">
                            <div class="strategy-metric-label">Win Rate</div>
                            <div class="strategy-metric-value">${Number(strategy.win_rate || 0).toFixed(2)}%</div>
                        </div>
                    </div>
                    <div class="strategy-footer">
                        <div class="strategy-positions">${positionsHtml}</div>
                        <div class="strategy-note">Trades: ${strategy.total_trades || 0} | Winners: ${strategy.winning_trades || 0}</div>
                        ${strategyActionButtons(strategy)}
                    </div>
                `;
                container.appendChild(card);
            });

            document.getElementById('stat-live').textContent = liveCount;
            document.getElementById('stat-paused').textContent = pausedCount;
            document.getElementById('stat-stopped').textContent = stoppedCount;
            document.getElementById('overview-live-count').textContent = liveCount;
            document.getElementById('overview-total-pnl').textContent = formatMoney(combinedPnl);
            document.getElementById('overview-open-positions').textContent = combinedOpen;
            document.getElementById('overview-rejected').textContent = document.getElementById('metric-rejected').textContent || '0';
        }

        function renderStrategyPositionsTable() {
            const posBody = document.getElementById('positions-body');
            posBody.innerHTML = '';
            const rows = [];
            strategyOverviewData.forEach(strategy => {
                const positionRows = Array.isArray(strategy.position_rows) && strategy.position_rows.length
                    ? strategy.position_rows
                    : Object.entries(strategy.positions || {}).map(([symbol, qty]) => ({
                        symbol,
                        display_symbol: symbol,
                        product: 'PAPER',
                        qty,
                        avg_price: null,
                        ltp: null,
                        pnl: 0,
                        chg_pct: null,
                    }));
                positionRows.forEach(row => {
                    rows.push({
                        strategy: strategy.name,
                        product: row.product || '-',
                        instrument: row.display_symbol || row.symbol || '-',
                        qty: row.qty,
                        avg_price: row.avg_price,
                        ltp: row.ltp,
                        pnl: row.pnl,
                        chg_pct: row.chg_pct,
                    });
                });
            });

            if (!rows.length) {
                posBody.innerHTML = `<tr><td colspan="8" style="text-align:center;color:#777;">No open positions yet.</td></tr>`;
                return;
            }

            let currentStrategy = '';
            rows.forEach(row => {
                if (row.strategy !== currentStrategy) {
                    currentStrategy = row.strategy;
                    posBody.innerHTML += `
                        <tr class="strategy-divider">
                            <td colspan="8" style="font-weight:800;color:#1e293b;background:#f8fafc;">${currentStrategy}</td>
                        </tr>
                    `;
                }
                posBody.innerHTML += `<tr>
                    <td>${row.strategy}</td>
                    <td>${row.product}</td>
                    <td>${row.instrument}</td>
                    <td class="${Number(row.qty || 0) >= 0 ? 'positive' : 'negative'}">${row.qty}</td>
                    <td>${row.avg_price == null ? '-' : Number(row.avg_price).toFixed(2)}</td>
                    <td>${row.ltp == null ? '-' : Number(row.ltp).toFixed(2)}</td>
                    <td class="${Number(row.pnl || 0) >= 0 ? 'positive' : 'negative'}">${formatMoney(row.pnl)}</td>
                    <td class="${row.chg_pct == null ? '' : (Number(row.chg_pct) >= 0 ? 'positive' : 'negative')}">${row.chg_pct == null ? '-' : `${Number(row.chg_pct).toFixed(2)}%`}</td>
                </tr>`;
            });
        }

        function normalizeOiSection(payload, symbol = selectedOiSymbol) {
            const activeSymbol = normalizeOiSymbol(symbol);
            if (Array.isArray(payload)) {
                const exact = payload.find(item => normalizeOiSymbol(item?.symbol) === activeSymbol);
                if (exact) return exact;
                return oiDashboardDataBySymbol[activeSymbol] || (payload.length === 1 ? payload[0] : null);
            }
            if (!payload || typeof payload !== 'object') {
                return oiDashboardDataBySymbol[activeSymbol] || null;
            }
            if (payload.symbol && normalizeOiSymbol(payload.symbol) !== activeSymbol) {
                return oiDashboardDataBySymbol[activeSymbol] || null;
            }
            return payload;
        }

        function renderOiExpiryOptions(section, symbol = selectedOiSymbol) {
            const activeSymbol = normalizeOiSymbol(symbol);
            const select = document.getElementById('oi-expiry-select');
            if (!select) return;

            const expiries = [...new Set([
                section?.expiry || '',
                ...(Array.isArray(section?.available_expiries) ? section.available_expiries : []),
            ].map(value => String(value || '').trim()).filter(Boolean))].filter(isFutureOrTodayExpiry);
            const preferred = String(section?.selected_expiry || section?.expiry || '').trim();
            const current = String(selectedOiExpiryBySymbol[activeSymbol] || '').trim();
            let nextValue = '';
            if (preferred && expiries.includes(preferred)) {
                nextValue = preferred;
            } else if (current && expiries.includes(current)) {
                nextValue = current;
            } else {
                nextValue = expiries[0] || '';
            }
            if (nextValue && selectedOiExpiryBySymbol[activeSymbol] !== nextValue) {
                selectedOiExpiryBySymbol[activeSymbol] = nextValue;
            }

            if (!expiries.length) {
                select.innerHTML = `<option value="">Live</option>`;
                select.value = '';
                select.disabled = true;
                selectedOiExpiryBySymbol[activeSymbol] = '';
                return;
            }

            select.disabled = false;
            select.innerHTML = [`<option value="">Live / Auto</option>`].concat(
                expiries.map(expiry => `<option value="${expiry}" ${expiry === nextValue ? 'selected' : ''}>${formatExpiryLabel(expiry)}</option>`)
            ).join('');
            select.value = nextValue || '';
            if (nextValue) {
                selectedOiExpiryBySymbol[activeSymbol] = nextValue;
            }
        }

        async function fetchOiDashboard(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol), force = false) {
            const requestSymbol = normalizeOiSymbol(symbol);
            const requestExpiry = expiry || '';
            if (oiDashboardFetchInFlight && !force) {
                return;
            }
            const requestSeq = ++oiDashboardRequestSeq;
            oiDashboardFetchInFlight = true;
            try {
                const params = new URLSearchParams({ symbol: requestSymbol });
                if (requestExpiry) params.set('expiry', requestExpiry);
                if (force) params.set('force', '1');
                params.set('_ts', Date.now().toString());
                const payload = await fetchJsonWithTimeout(`/api/oi_dashboard?${params.toString()}`, {}, 8500);
                if (requestSeq !== oiDashboardRequestSeq) {
                    return;
                }
                const nextSection = normalizeOiSection(payload, requestSymbol);
                const resolvedExpiry = String(nextSection?.selected_expiry || nextSection?.expiry || requestExpiry || '').trim();
                if (resolvedExpiry) {
                    selectedOiExpiryBySymbol[requestSymbol] = resolvedExpiry;
                }
                if (nextSection) {
                    oiDashboardData = nextSection;
                    oiDashboardDataBySymbol[requestSymbol] = nextSection;
                }
                renderOiExpiryOptions(nextSection || oiDashboardData, requestSymbol);
                renderOiDashboard(requestSymbol);
                const activeExpiry = getActiveOiExpiry(requestSymbol);
                await Promise.all([
                    fetchOiHistory(requestSymbol, activeExpiry),
                    fetchOiHistoryDates(requestSymbol, activeExpiry),
                ]);
                renderOiHistoryCharts(requestSymbol);
            } catch (e) {
                console.error('Error fetching OI dashboard', e);
            } finally {
                oiDashboardFetchInFlight = false;
            }
        }

        async function fetchOiHistory(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol), force = false) {
            const requestSymbol = normalizeOiSymbol(symbol);
            const requestExpiry = String(expiry || '').trim();
            const { key, state } = ensureOiHistoryState(requestSymbol, requestExpiry);
            const now = Date.now();
            const todayIso = toLocalIsoDate();
            if (!force && state.todayHistoryDate === todayIso && state.todayFetchedAt && (now - state.todayFetchedAt) < OI_HISTORY_REFRESH_MS) {
                return;
            }
            if (oiHistoryFetchInFlightKeys.has(key) && !force) {
                return;
            }
            oiHistoryFetchInFlightKeys.add(key);
            try {
                const params = new URLSearchParams({ symbol: requestSymbol, limit: '500' });
                if (requestExpiry) {
                    params.set('expiry', requestExpiry);
                }
                params.set('history_date', todayIso);
                params.set('_ts', Date.now().toString());
                const history = await fetchJsonWithTimeout(`/api/oi_history?${params.toString()}`, {}, 8500);
                state.todayRows = Array.isArray(history) ? history : [];
                state.todayHistoryDate = todayIso;
                state.todayFetchedAt = Date.now();
                if (state.todayRows.length && !state.availableDates.some(item => item.date === todayIso)) {
                    state.datesFetchedAt = 0;
                }
                renderOiHistoryCharts(requestSymbol);
            } catch (e) {
                console.error('Error fetching OI history', e);
            } finally {
                oiHistoryFetchInFlightKeys.delete(key);
            }
        }

        async function fetchOiHistoryDates(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol), force = false) {
            const requestSymbol = normalizeOiSymbol(symbol);
            const requestExpiry = String(expiry || '').trim();
            const { key, state } = ensureOiHistoryState(requestSymbol, requestExpiry);
            const now = Date.now();
            if (!force && state.datesFetchedAt && (now - state.datesFetchedAt) < OI_HISTORY_DATES_REFRESH_MS) {
                if (state.availableDates.length) {
                    const selectedDate = state.selectedDate && state.availableDates.some(item => item.date === state.selectedDate)
                        ? state.selectedDate
                        : chooseDefaultHistoryDate(state.availableDates);
                    state.selectedDate = selectedDate;
                    if (selectedDate && !state.archiveRowsByDate[selectedDate]) {
                        await fetchOiHistoryArchive(requestSymbol, requestExpiry, selectedDate);
                    } else {
                        renderOiHistoryCharts(requestSymbol);
                    }
                }
                return;
            }
            if (oiHistoryDatesFetchInFlightKeys.has(key) && !force) {
                return;
            }
            oiHistoryDatesFetchInFlightKeys.add(key);
            try {
                const params = new URLSearchParams({ symbol: requestSymbol, limit: '365' });
                if (requestExpiry) {
                    params.set('expiry', requestExpiry);
                }
                params.set('_ts', Date.now().toString());
                const dates = await fetchJsonWithTimeout(`/api/oi_history_dates?${params.toString()}`, {}, 8500);
                state.availableDates = Array.isArray(dates) ? dates : [];
                state.datesFetchedAt = Date.now();
                const selectedDate = state.selectedDate && state.availableDates.some(item => item.date === state.selectedDate)
                    ? state.selectedDate
                    : chooseDefaultHistoryDate(state.availableDates);
                state.selectedDate = selectedDate;
                if (selectedDate && !state.archiveRowsByDate[selectedDate]) {
                    await fetchOiHistoryArchive(requestSymbol, requestExpiry, selectedDate);
                } else {
                    renderOiHistoryCharts(requestSymbol);
                }
            } catch (e) {
                console.error('Error fetching OI history dates', e);
            } finally {
                oiHistoryDatesFetchInFlightKeys.delete(key);
            }
        }

        async function fetchOiHistoryArchive(symbol = selectedOiSymbol, expiry = getActiveOiExpiry(symbol), historyDate = '', force = false) {
            const requestSymbol = normalizeOiSymbol(symbol);
            const requestExpiry = String(expiry || '').trim();
            const requestDate = String(historyDate || '').trim();
            if (!requestDate) {
                return;
            }
            const { key, state } = ensureOiHistoryState(requestSymbol, requestExpiry);
            const cacheKey = `${key}::${requestDate}`;
            const now = Date.now();
            const lastFetchedAt = state.archiveFetchedAtByDate[requestDate] || 0;
            if (!force && lastFetchedAt && (now - lastFetchedAt) < OI_HISTORY_ARCHIVE_REFRESH_MS && state.archiveRowsByDate[requestDate]) {
                state.selectedDate = requestDate;
                renderOiHistoryCharts(requestSymbol);
                return;
            }
            if (oiHistoryArchiveFetchInFlightKeys.has(cacheKey) && !force) {
                return;
            }
            if (state.archiveRowsByDate[requestDate] && !force) {
                state.selectedDate = requestDate;
                renderOiHistoryCharts(requestSymbol);
                return;
            }
            oiHistoryArchiveFetchInFlightKeys.add(cacheKey);
            try {
                const params = new URLSearchParams({ symbol: requestSymbol, limit: '500', history_date: requestDate });
                if (requestExpiry) {
                    params.set('expiry', requestExpiry);
                }
                params.set('_ts', Date.now().toString());
                const archive = await fetchJsonWithTimeout(`/api/oi_history?${params.toString()}`, {}, 8500);
                state.archiveRowsByDate[requestDate] = Array.isArray(archive) ? archive : [];
                state.archiveFetchedAtByDate[requestDate] = Date.now();
                state.selectedDate = requestDate;
                renderOiHistoryCharts(requestSymbol);
            } catch (e) {
                console.error('Error fetching archived OI history', e);
            } finally {
                oiHistoryArchiveFetchInFlightKeys.delete(cacheKey);
            }
        }

        async function setOiHistoryDate(historyDate) {
            const requestSymbol = normalizeOiSymbol(selectedOiSymbol);
            const requestExpiry = getActiveOiExpiry(requestSymbol);
            const { state } = ensureOiHistoryState(requestSymbol, requestExpiry);
            state.selectedDate = String(historyDate || '').trim();
            renderOiHistoryCharts(requestSymbol);
            if (state.selectedDate) {
                await fetchOiHistoryArchive(requestSymbol, requestExpiry, state.selectedDate);
            }
        }

        async function fetchFuturesHead(force = false) {
            if (futuresHeadFetchInFlight && !force) {
                return;
            }
            futuresHeadFetchInFlight = true;
            try {
                const nextFutures = await fetchJsonWithTimeout('/api/futures_head', {}, 8500);
                if (Array.isArray(nextFutures) && nextFutures.length) {
                    futuresHeadData = nextFutures;
                }
                renderFuturesHead();
            } catch (e) {
                console.error('Error fetching futures head', e);
            } finally {
                futuresHeadFetchInFlight = false;
            }
        }

        async function refreshMarketData(force = false) {
            if (marketDataRefreshInFlight && !force) {
                return;
            }
            marketDataRefreshInFlight = true;
            try {
                await fetch(`/api/market_data?_ts=${Date.now()}`, { cache: 'no-store' });
            } catch (e) {
                console.error('Error refreshing market data', e);
            } finally {
                marketDataRefreshInFlight = false;
            }
        }

        function renderOiDashboard(symbol = selectedOiSymbol) {
            const container = document.getElementById('oi-dashboard');
            container.innerHTML = '';

            const activeSymbol = normalizeOiSymbol(symbol);
            const section = oiDashboardDataBySymbol[activeSymbol] || normalizeOiSection(oiDashboardData, activeSymbol);
            const lotSize = section?.lot_size ? Number(section.lot_size) : null;

            if (!section) {
                updateOiLastFetched(null);
                container.innerHTML = `<div class="oi-panel"><div class="oi-panel-title">No OI data yet</div><div class="oi-panel-sub">The dashboard will populate once live option-chain data is available.</div></div>`;
                return;
            }
            updateOiLastFetched(section);
            const summary = section.summary || {};
            const rows = section.rows || [];
            const signals = computeOiSignals(section);
            const oiAvailable = section.oi_available !== false;
            const callIvHigh = signals.callAvgIv != null && (signals.putAvgIv == null || signals.callAvgIv >= signals.putAvgIv);
            const putIvHigh = signals.putAvgIv != null && (signals.callAvgIv == null || signals.putAvgIv > signals.callAvgIv);
            const callMaxOi = Math.max(...rows.map(row => Number(row.call?.oi)).filter(value => Number.isFinite(value) && value > 0), 0);
            const putMaxOi = Math.max(...rows.map(row => Number(row.put?.oi)).filter(value => Number.isFinite(value) && value > 0), 0);
            const ivNote = signals.callAvgIv == null && signals.putAvgIv == null
                ? 'IV unavailable'
                : `Call IV avg: ${signals.callAvgIv == null ? '-' : signals.callAvgIv.toFixed(2)} | Put IV avg: ${signals.putAvgIv == null ? '-' : signals.putAvgIv.toFixed(2)}`;
            const panel = document.createElement('div');
            panel.className = 'oi-panel';
            const expiryLabel = section.expiry ? `Expiry: ${formatExpiryLabel(section.expiry)}` : 'Expiry: live';
            const underlyingLabel = section.underlying == null ? 'Underlying: -' : `Underlying: ${Number(section.underlying).toFixed(2)}`;
            const lotSizeLabel = Number.isFinite(lotSize) && lotSize > 0 ? `Lot Size: ${lotSize}` : 'Lot Size: -';
            const lastFetchedLabel = formatFetchTimestamp(section.last_live_tick_at || section.last_fetched_at);
            const staleBanner = section.is_stale
                ? `<div class="oi-alert stale">Live option feed is stale (${section.stale_age_sec == null ? '-' : `${section.stale_age_sec}s`} old). Last live tick: ${formatFetchTimestamp(section.last_live_tick_at)}</div>`
                : '';
            const callRowsHtml = rows.length ? rows.map(row => `
                <tr class="${[row.is_atm ? 'row-atm' : '', Number(row.call?.oi) === callMaxOi && callMaxOi > 0 ? 'oi-max-row' : ''].filter(Boolean).join(' ')}">
                    <td>${row.strike}</td>
                    <td>${displayNumber(row.call?.ltp, 2)}</td>
                    <td>${displayNumber(row.call?.iv, 2)}</td>
                    <td>${displayNumber(row.call?.oi)}</td>
                    <td>${displayNumber(row.call?.change_oi)}</td>
                </tr>
            `).join('') : `<tr><td colspan="5" style="text-align:center;color:#64748b;">No live call rows yet.</td></tr>`;
            const putRowsHtml = rows.length ? rows.map(row => `
                <tr class="${[row.is_atm ? 'row-atm' : '', Number(row.put?.oi) === putMaxOi && putMaxOi > 0 ? 'oi-max-row' : ''].filter(Boolean).join(' ')}">
                    <td>${row.strike}</td>
                    <td>${displayNumber(row.put?.ltp, 2)}</td>
                    <td>${displayNumber(row.put?.iv, 2)}</td>
                    <td>${displayNumber(row.put?.oi)}</td>
                    <td>${displayNumber(row.put?.change_oi)}</td>
                </tr>
            `).join('') : `<tr><td colspan="5" style="text-align:center;color:#64748b;">No live put rows yet.</td></tr>`;
            panel.innerHTML = `
                <div class="oi-panel-head">
                    <div>
                        <div class="oi-panel-title">${section.symbol} OI Table ${oiAvailable ? '' : '<span class="oi-badge unavailable">OI unavailable from websocket</span>'}</div>
                        <div class="oi-panel-sub">${underlyingLabel} | ATM: ${section.atm == null ? '-' : section.atm} | ${expiryLabel} | ${lotSizeLabel}</div>
                    </div>
                    <div class="oi-panel-sub">Source: ${section.source || 'NSE'} | Feed: ${section.feed || 'option_chain'} | Last fetched: ${lastFetchedLabel}</div>
                </div>
                ${staleBanner}
                <div class="oi-summary">
                    <div class="oi-chip"><div class="oi-chip-label">Call OI</div><div class="oi-chip-value ${summary.call_oi_sum == null ? 'muted' : ''}">${displayNumber(summary.call_oi_sum)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Put OI</div><div class="oi-chip-value ${summary.put_oi_sum == null ? 'muted' : ''}">${displayNumber(summary.put_oi_sum)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Put - Call OI</div><div class="oi-chip-value ${summary.oi_diff == null ? 'muted' : (Number(summary.oi_diff || 0) >= 0 ? 'positive' : 'negative')}">${displayNumber(summary.oi_diff)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Call Chg OI</div><div class="oi-chip-value muted">${displayNumber(summary.call_change_oi_sum)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Put Chg OI</div><div class="oi-chip-value muted">${displayNumber(summary.put_change_oi_sum)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Put - Call Chg OI</div><div class="oi-chip-value muted">${displayNumber(summary.change_oi_diff)}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">PCR</div><div class="oi-chip-value">${summary.pcr == null ? '-' : summary.pcr}</div></div>
                    <div class="oi-chip"><div class="oi-chip-label">Change PCR</div><div class="oi-chip-value">${summary.change_pcr == null ? '-' : summary.change_pcr}</div></div>
                </div>
                <div class="signal-grid">
                    <div class="signal-card ${signalClassFor(signals.oiSignal)}">
                        <div class="signal-label">OI Signal</div>
                        <div class="signal-value">${signals.oiSignal}</div>
                    </div>
                    <div class="signal-card ${signalClassFor(signals.changeSignal)}">
                        <div class="signal-label">Change OI Signal</div>
                        <div class="signal-value">${signals.changeSignal}</div>
                    </div>
                    <div class="signal-card ${signalClassFor(signals.ivSignal)}">
                        <div class="signal-label">IV Confirmation</div>
                        <div class="signal-value">${signals.ivSignal}</div>
                    </div>
                    <div class="signal-card ${signalClassFor(signals.bestTrade)} signal-best">
                        <div class="signal-label">Best Trade</div>
                        <div class="signal-value">${signals.bestTrade}</div>
                    </div>
                </div>
                <div class="card-subtle">${ivNote} | OI shown in contracts, notional depends on lot size.</div>
                <div class="oi-side-grid">
                    <div class="oi-side-card">
                        <div class="oi-side-head call ${callIvHigh ? 'iv-high-head' : ''}">Call Side</div>
                        <div class="table-scroll">
                            <table class="oi-side-table">
                                <thead>
                                    <tr>
                                        <th>Strike</th>
                                        <th>LTP</th>
                                        <th>IV</th>
                                        <th>OI</th>
                                        <th>Chg OI</th>
                                    </tr>
                                </thead>
                                <tbody>${callRowsHtml}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="oi-center-card">
                        <div class="oi-chip"><div class="oi-chip-label">ATM</div><div class="oi-chip-value">${section.atm == null ? '-' : section.atm}</div></div>
                        <div class="oi-chip"><div class="oi-chip-label">Status</div><div class="oi-chip-value">${rows.length ? 'Live' : 'Waiting'}</div></div>
                        <div class="oi-split-line"></div>
                        <div class="oi-diff-list">
                            <div class="oi-diff-row ${Number(summary.oi_diff || 0) >= 0 ? 'positive' : 'negative'}">
                                <div class="label">Put - Call OI</div>
                                <div class="value">${displayNumber(summary.oi_diff)}</div>
                            </div>
                            <div class="oi-diff-row">
                                <div class="label">Put - Call Change in OI</div>
                                <div class="value">${displayNumber(summary.change_oi_diff)}</div>
                            </div>
                            <div class="oi-diff-row">
                                <div class="label">Call / Put Totals</div>
                                <div class="value">${displayNumber(summary.call_oi_sum)} / ${displayNumber(summary.put_oi_sum)}</div>
                            </div>
                            <div class="oi-diff-row">
                                <div class="label">Call / Put Change OI</div>
                                <div class="value">${displayNumber(summary.call_change_oi_sum)} / ${displayNumber(summary.put_change_oi_sum)}</div>
                            </div>
                        </div>
                    </div>
                    <div class="oi-side-card">
                        <div class="oi-side-head put ${putIvHigh ? 'iv-high-head' : ''}">Put Side</div>
                        <div class="table-scroll">
                            <table class="oi-side-table">
                                <thead>
                                    <tr>
                                        <th>Strike</th>
                                        <th>LTP</th>
                                        <th>IV</th>
                                        <th>OI</th>
                                        <th>Chg OI</th>
                                    </tr>
                                </thead>
                                <tbody>${putRowsHtml}</tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
            if (section.note) {
                const note = document.createElement('div');
                note.className = 'oi-note';
                note.textContent = section.note;
                panel.appendChild(note);
            }
            container.appendChild(panel);
        }

        function renderOiHistoryCharts(symbol = selectedOiSymbol) {
            const container = document.getElementById('oi-history');
            container.innerHTML = '';
            const activeSymbol = normalizeOiSymbol(symbol);
            const activeExpiry = String(getActiveOiExpiry(activeSymbol) || '').trim();
            const { state } = ensureOiHistoryState(activeSymbol, activeExpiry);
            const todayRows = Array.isArray(state.todayRows) ? state.todayRows : [];
            const todaySlots = buildOiTimeline(todayRows);
            const todayExpiryLabel = formatExpiryLabel(activeExpiry);
            const todayDateLabel = formatHistoryDateLabel(state.todayHistoryDate || toLocalIsoDate());
            const todayTable = renderOiHistoryTableCard({
                title: `${OI_SESSION_STEP_MINUTES} Minute Snapshot Table`,
                subtitle: 'Aligned to the market session from 09:15 to 15:30. Resets each market day.',
                slots: todaySlots,
                emptyMessage: 'Waiting for live captures during market hours.',
            });

            const liveChart = renderTrendChartCard({
                title: `OI Difference History (${OI_SESSION_STEP_MINUTES} min) - ${activeSymbol}`,
                subtitle: `Date: ${todayDateLabel} | Expiry: ${todayExpiryLabel} | Blue = Put - Call OI | Orange = Put - Call Change in OI | Session: 09:15 to 15:30`,
                slots: todaySlots,
                height: 255,
                series: [
                    {
                        label: 'Put - Call OI',
                        color: '#2563eb',
                        value: slot => slot.oi_diff,
                        strokeWidth: 3,
                        skipZero: true,
                    },
                    {
                        label: 'Put - Call Change in OI',
                        color: '#f97316',
                        value: slot => slot.change_oi_diff,
                        strokeWidth: 3,
                        skipZero: true,
                    },
                ],
            });

            const changeFocusChart = renderTrendChartCard({
                title: `Change OI Focus (${OI_SESSION_STEP_MINUTES} min) - ${activeSymbol}`,
                subtitle: `Date: ${todayDateLabel} | Expiry: ${todayExpiryLabel} | Orange = Change OI Diff only | Session: 09:15 to 15:30`,
                slots: todaySlots,
                height: 235,
                series: [
                    {
                        label: 'Change OI Diff',
                        color: '#f97316',
                        value: slot => slot.change_oi_diff,
                        strokeWidth: 3.2,
                        skipZero: true,
                    },
                ],
            });

            const availableDates = Array.isArray(state.availableDates) ? state.availableDates : [];
            const selectedDate = String(state.selectedDate || chooseDefaultHistoryDate(availableDates)).trim();
            if (selectedDate && state.selectedDate !== selectedDate) {
                state.selectedDate = selectedDate;
            }
            const archiveRows = selectedDate ? (state.archiveRowsByDate[selectedDate] || []) : [];
            const archiveSlots = buildOiTimeline(archiveRows);
            const archiveDateLabel = selectedDate ? formatHistoryDateLabel(selectedDate) : 'Select a stored date';
            const historyOptionsHtml = availableDates.length
                ? availableDates.map(item => {
                    const optionDate = String(item?.date || '').trim();
                    const optionLabel = formatHistoryDateLabel(optionDate);
                    const countLabel = item?.count != null ? ` · ${item.count} snaps` : '';
                    const selectedAttr = optionDate === selectedDate ? 'selected' : '';
                    return `<option value="${optionDate}" ${selectedAttr}>${optionLabel}${countLabel}</option>`;
                }).join('')
                : `<option value="">No saved dates yet</option>`;

            const historyPicker = `
                <div class="oi-history-card">
                    <div class="oi-history-head">
                        <div>
                            <div class="oi-history-title">Historical OI Replay</div>
                            <div class="oi-history-sub">Choose a stored market date to replay the line and snapshot table.</div>
                        </div>
                        <div class="oi-history-controls">
                            <div class="oi-filter-label">Date</div>
                            <select id="oi-history-date-select" class="oi-history-date-select" onchange="setOiHistoryDate(this.value)" ${availableDates.length ? '' : 'disabled'}>
                                ${historyOptionsHtml}
                            </select>
                        </div>
                    </div>
                </div>
            `;

            const archiveChart = renderTrendChartCard({
                title: `Historical OI Replay (${OI_SESSION_STEP_MINUTES} min) - ${activeSymbol}`,
                subtitle: `Date: ${archiveDateLabel} | Expiry: ${todayExpiryLabel} | Blue = Put - Call OI | Orange = Put - Call Change in OI | Stored in your system`,
                slots: archiveSlots,
                height: 255,
                series: [
                    {
                        label: 'Put - Call OI',
                        color: '#2563eb',
                        value: slot => slot.oi_diff,
                        strokeWidth: 3,
                        skipZero: true,
                    },
                    {
                        label: 'Put - Call Change in OI',
                        color: '#f97316',
                        value: slot => slot.change_oi_diff,
                        strokeWidth: 3,
                        skipZero: true,
                    },
                ],
            });

            const archiveTable = renderOiHistoryTableCard({
                title: 'Historical Session Table',
                subtitle: `Stored session for ${archiveDateLabel}.`,
                slots: archiveSlots,
                emptyMessage: selectedDate ? `No stored rows found for ${archiveDateLabel}.` : 'Choose a stored date to view archived rows.',
            });

            container.innerHTML = `
                <div class="oi-chart-stack">
                    ${liveChart}
                    ${changeFocusChart}
                    ${historyPicker}
                    ${archiveChart}
                    ${archiveTable}
                    ${todayTable}
                </div>
            `;
        }

        function renderLiveOptionChains() {
            const container = document.getElementById('option-chain-live');
            if (!container) return;

            const symbols = ['NIFTY', 'BANKNIFTY'];
            const currentSection = normalizeOiSection(oiDashboardData, selectedOiSymbol);
            const sections = currentSection && symbols.includes(currentSection.symbol) ? [currentSection] : [];

            if (!sections.length) {
                container.innerHTML = `
                    <div class="oi-panel">
                        <div class="oi-panel-title">Option chain loading...</div>
                        <div class="oi-panel-sub">Waiting for live NIFTY and BANKNIFTY rows.</div>
                    </div>
                `;
                return;
            }

            container.innerHTML = sections.map(section => {
                const rows = section.rows || [];
                const summary = section.summary || {};
                const signals = computeOiSignals(section);
                const callIvHigh = signals.callAvgIv != null && (signals.putAvgIv == null || signals.callAvgIv >= signals.putAvgIv);
                const putIvHigh = signals.putAvgIv != null && (signals.callAvgIv == null || signals.putAvgIv > signals.callAvgIv);
                const expiryLabel = section.expiry ? `Expiry: ${formatExpiryLabel(section.expiry)}` : 'Expiry: live';
                const lotSizeValue = Number(section.lot_size || 0);
                const lotSizeLabel = lotSizeValue > 0 ? `${lotSizeValue}` : '-';
                const underlyingLabel = section.underlying == null ? 'Underlying: -' : `Underlying: ${Number(section.underlying).toFixed(2)}`;
                const callRowsHtml = rows.length ? rows.map(row => `
                    <tr class="${row.is_atm ? 'row-atm' : ''}">
                        <td>${row.strike}</td>
                        <td>${row.call?.ltp ?? '-'}</td>
                        <td>${row.call?.iv ?? '-'}</td>
                        <td>${row.call?.oi ?? '-'}</td>
                    </tr>
                `).join('') : `<tr><td colspan="4" style="text-align:center;color:#64748b;">No live call rows yet.</td></tr>`;
                const putRowsHtml = rows.length ? rows.map(row => `
                    <tr class="${row.is_atm ? 'row-atm' : ''}">
                        <td>${row.strike}</td>
                        <td>${row.put?.ltp ?? '-'}</td>
                        <td>${row.put?.iv ?? '-'}</td>
                        <td>${row.put?.oi ?? '-'}</td>
                    </tr>
                `).join('') : `<tr><td colspan="4" style="text-align:center;color:#64748b;">No live put rows yet.</td></tr>`;
                return `
                    <div class="option-chain-card">
                        <div class="option-chain-card-head">
                            <div>
                                <div class="option-chain-card-title">${section.symbol}</div>
                                <div class="option-chain-card-sub">${underlyingLabel} | ${expiryLabel}</div>
                            </div>
                            <div class="option-chain-card-sub">ATM: ${section.atm == null ? '-' : section.atm} | OI Diff: ${summary.oi_diff || 0} | PCR: ${summary.pcr == null ? '-' : summary.pcr}</div>
                        </div>
                        <div class="signal-grid" style="padding: 12px 14px 0;">
                            <div class="signal-card ${signalClassFor(signals.oiSignal)}"><div class="signal-label">OI</div><div class="signal-value">${signals.oiSignal}</div></div>
                            <div class="signal-card ${signalClassFor(signals.changeSignal)}"><div class="signal-label">Change OI</div><div class="signal-value">${signals.changeSignal}</div></div>
                            <div class="signal-card ${signalClassFor(signals.ivSignal)}"><div class="signal-label">IV</div><div class="signal-value">${signals.ivSignal}</div></div>
                            <div class="signal-card ${signalClassFor(signals.bestTrade)} signal-best"><div class="signal-label">Best Trade</div><div class="signal-value">${signals.bestTrade}</div></div>
                        </div>
                        <div class="oi-side-grid">
                            <div class="oi-side-card">
                                <div class="oi-side-head call ${callIvHigh ? 'iv-high-head' : ''}">Call Side</div>
                                <div class="table-scroll">
                                    <table class="oi-side-table">
                                        <thead>
                                            <tr>
                                                <th>Strike</th>
                                                <th>LTP</th>
                                                <th>IV</th>
                                                <th>OI</th>
                                            </tr>
                                        </thead>
                                        <tbody>${callRowsHtml}</tbody>
                                    </table>
                                </div>
                            </div>
                            <div class="oi-center-card">
                                <div class="oi-chip"><div class="oi-chip-label">Expiry</div><div class="oi-chip-value">${section.expiry == null ? 'live' : formatExpiryLabel(section.expiry)}</div></div>
                                <div class="oi-chip"><div class="oi-chip-label">ATM</div><div class="oi-chip-value">${section.atm == null ? '-' : section.atm}</div></div>
                                <div class="oi-chip"><div class="oi-chip-label">IV Bias</div><div class="oi-chip-value">${signals.ivSignal}</div></div>
                                <div class="oi-chip"><div class="oi-chip-label">Lot Size</div><div class="oi-chip-value">${lotSizeLabel}</div></div>
                                <div class="oi-diff-list">
                                    <div class="oi-diff-row ${Number(summary.oi_diff || 0) >= 0 ? 'positive' : 'negative'}">
                                        <div class="label">Put - Call OI</div>
                                        <div class="value">${summary.oi_diff || 0}</div>
                                    </div>
                                    <div class="oi-diff-row ${Number(summary.change_oi_diff || 0) >= 0 ? 'positive' : 'negative'}">
                                        <div class="label">Put - Call Change in OI</div>
                                        <div class="value">${summary.change_oi_diff || 0}</div>
                                    </div>
                                    <div class="oi-diff-row">
                                        <div class="label">PCR</div>
                                        <div class="value">${summary.pcr == null ? '-' : summary.pcr}</div>
                                    </div>
                                </div>
                            </div>
                            <div class="oi-side-card">
                                <div class="oi-side-head put ${putIvHigh ? 'iv-high-head' : ''}">Put Side</div>
                                <div class="table-scroll">
                                    <table class="oi-side-table">
                                        <thead>
                                            <tr>
                                                <th>Strike</th>
                                                <th>LTP</th>
                                                <th>IV</th>
                                                <th>OI</th>
                                            </tr>
                                        </thead>
                                        <tbody>${putRowsHtml}</tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        async function updateStrategyState(strategyId, action) {
            try {
                const res = await fetch(`/api/strategies/${strategyId}/${action}`, {
                    method: 'POST'
                });
                const payload = await res.json();
                if (!res.ok || payload.ok === false) {
                    alert(payload.message || 'Strategy action failed');
                }
            } catch (e) {
                console.error('Strategy update failed', e);
            }
        }

        async function runControlAction(action) {
            try {
                const endpointMap = {
                    pause_active: '/api/control/pause_all_live',
                    resume_active: '/api/control/resume_all_paused',
                    emergency_stop: '/api/control/emergency_stop_all',
                    refresh_option_chain: '/api/control/refresh_option_chain',
                };
                const res = await fetch(endpointMap[action] || `/api/control/${action}`, { method: 'POST' });
                const payload = await res.json();
                if (!res.ok || payload.ok === false) {
                    alert(payload.message || 'Control action failed');
                }
                await fetchHealth();
                await fetchData();
            } catch (e) {
                console.error('Control action failed', e);
            }
        }

        async function fetchHealth() {
            try {
                const data = await fetchJsonWithTimeout('/api/health', {}, 7000);
                const tickAge = data.tick_age_sec == null ? '-' : `${data.tick_age_sec}s`;
                const activeStrategy = (data.active_strategy_ids || []).length ? data.active_strategy_ids.join(', ') : 'None';
                const quality = Number(data.ws_quality_pct || 0);
                const pill = document.getElementById('connection-pill');
                const meter = document.getElementById('connection-meter-fill');
                const stateEl = document.getElementById('connection-state');
                const freshnessEl = document.getElementById('connection-freshness');
                const messageAgeEl = document.getElementById('connection-message-age');
                const reconnectEl = document.getElementById('connection-reconnects');

                let meterClass = data.feed_state === 'idle' ? 'warn' : 'bad';
                let stateText = data.feed_state === 'idle' ? 'Market closed / feed idle' : 'Disconnected';
                if (data.ws_connected) {
                    if (data.feed_state === 'degraded') {
                        meterClass = 'warn';
                        stateText = 'Live feed degraded';
                    } else
                    if (quality >= 80) {
                        meterClass = 'good';
                        stateText = 'Strong live feed';
                    } else if (quality >= 55) {
                        meterClass = 'warn';
                        stateText = 'Live feed stable';
                    } else {
                        meterClass = 'bad';
                        stateText = 'Live feed weak';
                    }
                }

                if (pill) {
                    pill.textContent = `${quality}%`;
                    pill.className = `connection-pill ${meterClass}`;
                }
                if (meter) {
                    meter.style.width = `${Math.max(0, Math.min(100, quality))}%`;
                    meter.className = `connection-meter-fill ${meterClass}`;
                }
                if (stateEl) stateEl.textContent = stateText;
                if (freshnessEl) freshnessEl.textContent = `Tick age: ${tickAge}`;
                if (messageAgeEl) messageAgeEl.textContent = `Message age: ${data.ws_last_message_age_sec == null ? '-' : `${data.ws_last_message_age_sec}s`}`;
                if (reconnectEl) reconnectEl.textContent = `Reconnects: ${data.ws_reconnect_count || 0}`;

                document.getElementById('metric-active-strategy').textContent = activeStrategy;
                document.getElementById('metric-tick-age').textContent = tickAge;
                document.getElementById('metric-fill-rate').textContent = `${(data.fill_rate_pct || 0).toFixed(2)}%`;
                document.getElementById('metric-slippage').textContent = (data.avg_slippage || 0).toFixed(2);
                document.getElementById('metric-open-pos').textContent = `${data.open_positions_count || 0}/${data.max_open_positions || 0}`;
                document.getElementById('metric-loss-used').textContent = `${(data.loss_used_pct || 0).toFixed(2)}%`;
                document.getElementById('metric-rejected').textContent = `${data.rejected_orders_today || 0}`;
                document.getElementById('metric-option-rows').textContent = `${data.option_rows_count || 0}`;
                document.getElementById('metric-oi-age').textContent = data.oi_snapshot_age_sec == null ? '-' : `${data.oi_snapshot_age_sec}s`;
                document.getElementById('overview-rejected').textContent = `${data.rejected_orders_today || 0}`;
            } catch (e) {
                console.error('Error fetching health metrics', e);
            }
        }

        async function fetchStrategies() {
            try {
                const res = await fetch('/api/strategy_overview');
                strategyOverviewData = await res.json();
                strategiesData = strategyOverviewData;
                renderStrategyCards();
                renderStrategyPositionsTable();
            } catch (e) {
                console.error("Error fetching strategies", e);
            }
        }

        async function fetchData() {
            if (dashboardFetchInFlight) return;
            dashboardFetchInFlight = true;
            try {
                fetchOiDashboard().catch(e => console.error('Error fetching OI dashboard', e));
                fetchFuturesHead().catch(e => console.error('Error fetching futures head', e));
                const nowTs = Date.now();
                if (!lastMarketDataRefreshAt || (nowTs - lastMarketDataRefreshAt) >= MARKET_DATA_REFRESH_MS) {
                    lastMarketDataRefreshAt = nowTs;
                    refreshMarketData().catch(e => console.error('Error refreshing market data', e));
                }
                const [statusResult, indicesResult] = await Promise.allSettled([
                    fetch('/api/status', { cache: 'no-store' }).then(res => res.json()),
                    fetch('/api/nse_indices', { cache: 'no-store' }).then(res => res.json()),
                ]);

                if (statusResult.status === 'fulfilled') {
                    const statusData = statusResult.value;
                    const badge = document.getElementById('mode-badge');
                    if (statusData.mode === 'paper') {
                        badge.textContent = 'PAPER MODE';
                        badge.className = 'badge paper';
                    } else {
                        badge.textContent = 'LIVE MODE';
                        badge.className = 'badge live';
                    }

                    const dot = document.getElementById('ws-status-dot');
                    if (statusData.ws_connected) {
                        if (statusData.feed_state === 'degraded') {
                            dot.className = 'status-dot idle';
                            dot.title = `Degraded | Tick age ${statusData.ws_last_tick_age_sec ?? '-'}s | Reconnects/min ${statusData.ws_reconnects_last_min ?? 0}`;
                        } else {
                            dot.className = 'status-dot connected';
                            dot.title = `Connected | Quality ${statusData.ws_quality_pct || 0}%`;
                        }
                    } else if (statusData.feed_state === 'idle' || statusData.market_window === false) {
                        dot.className = 'status-dot idle';
                        dot.title = 'Market closed / feed idle';
                    } else {
                        dot.className = 'status-dot disconnected';
                        dot.title = 'Disconnected | Quality 0%';
                    }
                } else {
                    console.error("Error fetching status", statusResult.reason);
                }

                if (indicesResult.status === 'fulfilled') {
                    indicesDataCache = indicesResult.value;
                    renderIndices();
                } else {
                    console.error("Error fetching indices", indicesResult.reason);
                }

            } catch (e) {
                console.error("Error updating dashboard", e);
            } finally {
                dashboardFetchInFlight = false;
            }
        }

        const LIVE_DASHBOARD_REFRESH_MS = 1000;
        const HEALTH_REFRESH_MS = 3000;
        setInterval(() => {
            fetchData();
        }, LIVE_DASHBOARD_REFRESH_MS);
        setInterval(() => {
            fetchHealth();
        }, HEALTH_REFRESH_MS);

        document.getElementById('index-search').addEventListener('input', renderIndices);
        renderFuturesHead();
        renderOiExpiryOptions(normalizeOiSection(oiDashboardData, selectedOiSymbol), selectedOiSymbol);
        renderOiDashboard(selectedOiSymbol);
        renderOiHistoryCharts(selectedOiSymbol);
        fetchData();
        fetchHealth();
        setIndexSort(activeIndexSort);
    </script>
</body>
</html>
"""

@app.get("/")
def read_root():
    return RedirectResponse(url="/ui")

@app.get("/ui")
def serve_ui():
    initial_oi_dashboard, initial_futures_head = _build_ui_bootstrap_data()
    initial_oi_dashboard = _prepare_oi_rows_for_ui(initial_oi_dashboard)
    bootstrap_timestamp = time.time()
    with _oi_dashboard_lock:
        for section in initial_oi_dashboard:
            symbol = section.get("symbol")
            if not symbol:
                continue
            cache_expiries = {None, section.get("selected_expiry"), section.get("expiry")}
            for cache_expiry in cache_expiries:
                cache_key = _option_chain_cache_key(symbol, cache_expiry)
                _oi_dashboard_cache[cache_key] = {
                    "timestamp": bootstrap_timestamp,
                    "rows": section,
                }
    with _futures_head_lock:
        _futures_head_cache["timestamp"] = bootstrap_timestamp
        _futures_head_cache["rows"] = initial_futures_head
    rendered = (
        html_content
        .replace("__INITIAL_OI_DASHBOARD__", _json_for_html(initial_oi_dashboard))
        .replace("__INITIAL_FUTURES_HEAD__", _json_for_html(initial_futures_head))
    )
    return HTMLResponse(content=rendered)

@app.get("/api/status")
def get_status():
    load_dotenv()
    is_paper = os.getenv("PAPER_MODE", "true").lower() == "true"
    live = _get_live_feed_snapshot()
    ws_metrics = live.get("ws_metrics") or {}

    return {
        "mode": "paper" if is_paper else "live",
        "ws_connected": bool(live.get("ws_connected")),
        "ws_quality_pct": int(_to_float(live.get("ws_quality_pct"), 0.0)),
        "ws_last_tick_age_sec": live.get("ws_last_tick_age_sec"),
        "ws_last_message_age_sec": live.get("ws_last_message_age_sec"),
        "market_window": bool(live.get("market_window")),
        "feed_state": live.get("feed_state"),
        "last_live_tick_at": live.get("last_live_tick_at"),
        "is_stale": bool(live.get("is_stale")),
        "stale_age_sec": live.get("stale_age_sec"),
        "ws_reconnect_count": int(_to_float(ws_metrics.get("reconnect_count"), 0.0)),
        "ws_consecutive_failures": int(_to_float(ws_metrics.get("consecutive_failures"), 0.0)),
        "ws_reconnects_last_min": int(_to_float(live.get("reconnects_last_min"), 0.0)),
        "ws_reconnect_storm": bool(live.get("reconnect_storm")),
        "ws_last_error": ws_metrics.get("last_error"),
    }

@app.get("/api/pnl")
def get_pnl():
    db = SessionLocal()
    try:
        summary = db.query(PnlSummary).filter(PnlSummary.date == date.today()).first()
        pnl = summary.total_pnl if summary else 0.0
        snapshots = _strategy_pnl_snapshot(db)
        return {
            "pnl": pnl,
            "strategy_pnl": {
                strategy_id: {
                    "realized_pnl": round(snapshot["realized_pnl"], 2),
                    "unrealized_pnl": round(snapshot["unrealized_pnl"], 2),
                    "total_pnl": round(snapshot["total_pnl"], 2),
                }
                for strategy_id, snapshot in snapshots.items()
            },
        }
    finally:
        db.close()

@app.get("/api/trades")
def get_trades(strategy_id: str | None = None):
    db = SessionLocal()
    try:
        query = db.query(Trade).order_by(Trade.timestamp.desc())
        if strategy_id:
            query = query.filter(Trade.strategy_id == strategy_id)
        trades = query.limit(50).all()
        return [{
            "symbol": t.symbol,
            "side": t.side,
            "quantity": t.quantity,
            "price": t.price,
            "strategy_id": t.strategy_id,
            "timestamp": t.timestamp.isoformat()
        } for t in trades]
    finally:
        db.close()

@app.get("/api/orders")
def get_orders(strategy_id: str | None = None):
    db = SessionLocal()
    try:
        query = db.query(Order).order_by(Order.timestamp.desc())
        if strategy_id:
            query = query.filter(Order.strategy_id == strategy_id)
        orders = query.limit(50).all()
        return [{
            "symbol": o.symbol,
            "side": o.side,
            "quantity": o.quantity,
            "status": o.status,
            "strategy_id": o.strategy_id,
            "timestamp": o.timestamp.isoformat()
        } for o in orders]
    finally:
        db.close()


def _calculate_positions_from_trades(db, strategy_id=None):
    query = db.query(Trade)
    if strategy_id:
        query = query.filter(Trade.strategy_id == strategy_id)
    trades = query.all()
    positions = {}
    for trade in trades:
        if trade.symbol not in positions:
            positions[trade.symbol] = 0
        if trade.side == "BUY":
            positions[trade.symbol] += trade.quantity
        else:
            positions[trade.symbol] -= trade.quantity
    return {symbol: qty for symbol, qty in positions.items() if qty != 0}


def _get_active_strategy_id():
    active = strategy_runtime.get_active_strategy_ids()
    return active[0] if active else None


def _latest_prices_by_symbol(db):
    latest = {}
    rows = (
        db.query(MarketData.symbol, func.max(MarketData.timestamp))
        .group_by(MarketData.symbol)
        .all()
    )
    for symbol, ts in rows:
        if not symbol or not ts:
            continue
        md = (
            db.query(MarketData)
            .filter(MarketData.symbol == symbol, MarketData.timestamp == ts)
            .first()
        )
        if md:
            latest[symbol] = md.last_traded_price
    return latest


def _latest_marketdata_by_symbol(db):
    latest = {}
    rows = (
        db.query(MarketData.symbol, func.max(MarketData.timestamp))
        .group_by(MarketData.symbol)
        .all()
    )
    for symbol, ts in rows:
        if not symbol or not ts:
            continue
        md = (
            db.query(MarketData)
            .filter(MarketData.symbol == symbol, MarketData.timestamp == ts)
            .first()
        )
        if md:
            latest[symbol] = md
    return latest


def _strategy_pnl_snapshot(db):
    trades = db.query(Trade).order_by(Trade.timestamp.asc()).all()
    latest_marketdata = _latest_marketdata_by_symbol(db)
    latest_prices = {symbol: md.last_traded_price for symbol, md in latest_marketdata.items()}
    snapshots = {}
    paper_mode = os.getenv("PAPER_MODE", "true").lower() == "true"

    for trade in trades:
        strategy_id = trade.strategy_id or "legacy"
        snapshot = snapshots.setdefault(strategy_id, {
            "strategy_id": strategy_id,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "total_pnl": 0.0,
            "total_trades": 0,
            "winning_trades": 0,
            "positions": {},
            "last_trade_at": None,
        })
        snapshot["total_trades"] += 1
        snapshot["last_trade_at"] = trade.timestamp
        symbol_book = snapshot["positions"].setdefault(trade.symbol, {
            "buy_lots": [],
            "sell_lots": [],
        })

        qty = int(trade.quantity)
        price = float(trade.price)

        if trade.side == "BUY":
            while qty > 0 and symbol_book["sell_lots"]:
                lot = symbol_book["sell_lots"][0]
                close_qty = min(qty, lot["qty"])
                pnl = (lot["price"] - price) * close_qty
                snapshot["realized_pnl"] += pnl
                if pnl > 0:
                    snapshot["winning_trades"] += 1
                qty -= close_qty
                lot["qty"] -= close_qty
                if lot["qty"] == 0:
                    symbol_book["sell_lots"].pop(0)
            if qty > 0:
                symbol_book["buy_lots"].append({"qty": qty, "price": price})
        else:
            while qty > 0 and symbol_book["buy_lots"]:
                lot = symbol_book["buy_lots"][0]
                close_qty = min(qty, lot["qty"])
                pnl = (price - lot["price"]) * close_qty
                snapshot["realized_pnl"] += pnl
                if pnl > 0:
                    snapshot["winning_trades"] += 1
                qty -= close_qty
                lot["qty"] -= close_qty
                if lot["qty"] == 0:
                    symbol_book["buy_lots"].pop(0)
            if qty > 0:
                symbol_book["sell_lots"].append({"qty": qty, "price": price})

    for snapshot in snapshots.values():
        positions = {}
        position_rows = []
        unrealized = 0.0
        for symbol, lot_book in snapshot["positions"].items():
            buy_qty = sum(lot["qty"] for lot in lot_book["buy_lots"])
            sell_qty = sum(lot["qty"] for lot in lot_book["sell_lots"])
            net_qty = buy_qty - sell_qty
            if net_qty != 0:
                positions[symbol] = net_qty

            ltp = latest_prices.get(symbol)
            md = latest_marketdata.get(symbol)
            display_symbol = (
                str(md.trading_symbol).strip()
                if md and md.trading_symbol
                else str(symbol.split("|", 1)[-1]).strip()
            )
            product_label = "PAPER" if paper_mode else ((md.instrument_type or "NRML") if md else "NRML")
            avg_price = None
            row_pnl = 0.0
            change_pct = None
            direction = "LONG" if net_qty > 0 else ("SHORT" if net_qty < 0 else None)

            if ltp is None:
                ltp = None
            if buy_qty:
                avg_buy = sum(lot["qty"] * lot["price"] for lot in lot_book["buy_lots"]) / buy_qty
                if net_qty > 0:
                    avg_price = avg_buy
                    if ltp is not None:
                        row_pnl = (ltp - avg_buy) * buy_qty
                        unrealized += row_pnl
                        change_pct = ((ltp - avg_buy) / avg_buy) * 100 if avg_buy else None
            if sell_qty:
                avg_sell = sum(lot["qty"] * lot["price"] for lot in lot_book["sell_lots"]) / sell_qty
                if net_qty < 0:
                    avg_price = avg_sell
                    if ltp is not None:
                        row_pnl = (avg_sell - ltp) * sell_qty
                        unrealized += row_pnl
                        change_pct = ((avg_sell - ltp) / avg_sell) * 100 if avg_sell else None

            if net_qty != 0:
                position_rows.append({
                    "symbol": symbol,
                    "display_symbol": display_symbol,
                    "product": product_label,
                    "direction": direction,
                    "qty": net_qty,
                    "avg_price": round(avg_price, 2) if avg_price is not None else None,
                    "ltp": round(ltp, 2) if ltp is not None else None,
                    "pnl": round(row_pnl, 2),
                    "chg_pct": round(change_pct, 2) if change_pct is not None else None,
                })

        snapshot["positions"] = positions
        snapshot["position_rows"] = position_rows
        snapshot["open_positions_count"] = len(positions)
        snapshot["unrealized_pnl"] = unrealized
        snapshot["total_pnl"] = snapshot["realized_pnl"] + unrealized

    return snapshots


def _strategy_positions_table(db, strategy_id=None):
    snapshots = _strategy_pnl_snapshot(db)
    if strategy_id:
        return snapshots.get(strategy_id, {})
    return snapshots


def _reset_option_chain_cache():
    with _option_chain_lock:
        _option_chain_cache.clear()


@app.get("/api/positions")
def get_positions(strategy_id: str | None = None):
    db = SessionLocal()
    try:
        return _calculate_positions_from_trades(db, strategy_id=strategy_id)
    finally:
        db.close()


@app.get("/api/health")
def get_health():
    db = SessionLocal()
    try:
        now = datetime.now()
        today_start = datetime.combine(date.today(), datetime.min.time())
        is_market_window = _is_market_window_now()
        ws_metrics = {}
        ws_connected = False
        ws_last_tick_age_sec = None
        ws_last_message_age_sec = None
        ws_quality_pct = 0
        get_ws_status = lambda: False
        try:
            from core.status import get_ws_metrics, get_ws_status
            ws_metrics = get_ws_metrics()
            last_tick_at = ws_metrics.get("last_tick_at")
            last_message_at = ws_metrics.get("last_message_at")
            if last_tick_at:
                try:
                    ws_last_tick_age_sec = int((now - datetime.fromisoformat(last_tick_at)).total_seconds())
                except Exception:
                    ws_last_tick_age_sec = None
            if last_message_at:
                try:
                    ws_last_message_age_sec = int((now - datetime.fromisoformat(last_message_at)).total_seconds())
                except Exception:
                    ws_last_message_age_sec = None
        except Exception:
            pass

        last_tick = db.query(func.max(MarketData.timestamp)).scalar()
        tick_age_sec = int((now - last_tick).total_seconds()) if last_tick else None
        live_feed_connected = tick_age_sec is not None and tick_age_sec <= 10
        ws_connected = bool(get_ws_status() or live_feed_connected)
        ws_metrics = dict(ws_metrics or {})
        if ws_connected:
            ws_metrics["connected"] = True
        ws_quality_pct = _compute_ws_quality_pct(ws_metrics, ws_last_tick_age_sec, ws_last_message_age_sec)
        feed_state = "live" if ws_connected else ("idle" if not is_market_window else "disconnected")

        total_orders = db.query(func.count(Order.id)).filter(Order.timestamp >= today_start).scalar() or 0
        filled_orders = db.query(func.count(Order.id)).filter(
            Order.timestamp >= today_start,
            Order.status == "FILLED",
        ).scalar() or 0
        rejected_orders = db.query(func.count(Order.id)).filter(
            Order.timestamp >= today_start,
            Order.status == "REJECTED",
        ).scalar() or 0
        avg_slippage = db.query(func.avg(Trade.slippage)).filter(Trade.timestamp >= today_start).scalar() or 0.0

        risk_state = db.query(RiskState).filter(RiskState.date == date.today()).first()
        daily_pnl = _to_float(risk_state.daily_pnl if risk_state else 0.0)
        max_daily_loss = _to_float(os.getenv("MAX_DAILY_LOSS", "10000"), 10000.0)
        max_open_positions = int(_to_float(os.getenv("MAX_OPEN_POSITIONS", "5"), 5))

        open_positions = _calculate_positions_from_trades(db)
        open_positions_count = len(open_positions)
        live_strategies = strategy_runtime.get_active_strategy_ids()
        strategy_snapshots = _strategy_pnl_snapshot(db)
        last_oi_snapshot = db.query(func.max(OiSnapshot.captured_at)).scalar()
        oi_snapshot_age_sec = int((now - last_oi_snapshot).total_seconds()) if last_oi_snapshot else None

        with _option_chain_lock:
            option_rows_count = sum(len(bucket["rows"]) for bucket in _option_chain_cache.values())
            recent_timestamps = [bucket["timestamp"] for bucket in _option_chain_cache.values() if bucket["timestamp"]]
            option_cache_age = int(time.time() - max(recent_timestamps)) if recent_timestamps else None

        return {
            "last_tick_at": last_tick.isoformat() if last_tick else None,
            "tick_age_sec": tick_age_sec,
            "ws_connected": ws_connected,
            "ws_quality_pct": ws_quality_pct,
            "ws_last_tick_age_sec": ws_last_tick_age_sec,
            "ws_last_message_age_sec": ws_last_message_age_sec,
            "market_window": is_market_window,
            "feed_state": feed_state,
            "ws_reconnect_count": int(_to_float(ws_metrics.get("reconnect_count"), 0.0)) if ws_metrics else 0,
            "ws_consecutive_failures": int(_to_float(ws_metrics.get("consecutive_failures"), 0.0)) if ws_metrics else 0,
            "ws_last_error": ws_metrics.get("last_error") if ws_metrics else None,
            "total_orders_today": int(total_orders),
            "filled_orders_today": int(filled_orders),
            "rejected_orders_today": int(rejected_orders),
            "fill_rate_pct": round((filled_orders / total_orders) * 100, 2) if total_orders else 0.0,
            "avg_slippage": round(float(avg_slippage), 4),
            "daily_pnl": round(float(daily_pnl), 2),
            "max_daily_loss": max_daily_loss,
            "loss_used_pct": round((abs(daily_pnl) / max_daily_loss) * 100, 2) if (max_daily_loss and daily_pnl < 0) else 0.0,
            "open_positions_count": open_positions_count,
            "max_open_positions": max_open_positions,
            "active_strategy_id": live_strategies[0] if live_strategies else None,
            "active_strategy_ids": live_strategies,
            "live_strategy_count": len(live_strategies),
            "option_rows_count": option_rows_count,
            "option_cache_age_sec": option_cache_age,
            "last_oi_snapshot_at": last_oi_snapshot.isoformat() if last_oi_snapshot else None,
            "oi_snapshot_age_sec": oi_snapshot_age_sec,
            "strategy_overview": {
                strategy_id: {
                    "realized_pnl": round(snapshot["realized_pnl"], 2),
                    "unrealized_pnl": round(snapshot["unrealized_pnl"], 2),
                    "total_pnl": round(snapshot["total_pnl"], 2),
                    "open_positions_count": snapshot["open_positions_count"],
                    "positions": snapshot["positions"],
                    "total_trades": snapshot["total_trades"],
                    "winning_trades": snapshot["winning_trades"],
                }
                for strategy_id, snapshot in strategy_snapshots.items()
            },
        }
    finally:
        db.close()


@app.get("/api/strategy_overview")
def get_strategy_overview():
    db = SessionLocal()
    try:
        strategies = strategy_runtime.get_strategies()
        snapshots = _strategy_pnl_snapshot(db)
        overview = []
        for strategy in strategies:
            snapshot = snapshots.get(strategy["id"], {
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "open_positions_count": 0,
                "positions": {},
                "total_trades": 0,
                "winning_trades": 0,
            })
            overview.append({
                **strategy,
                "realized_pnl": round(snapshot["realized_pnl"], 2),
                "unrealized_pnl": round(snapshot["unrealized_pnl"], 2),
                "total_pnl": round(snapshot["total_pnl"], 2),
                "open_positions_count": snapshot["open_positions_count"],
                "positions": snapshot["positions"],
                "position_rows": snapshot.get("position_rows", []),
                "total_trades": snapshot["total_trades"],
                "winning_trades": snapshot["winning_trades"],
                "win_rate": round((snapshot["winning_trades"] / snapshot["total_trades"]) * 100, 2) if snapshot["total_trades"] else 0.0,
            })
        return overview
    finally:
        db.close()


@app.get("/api/nse_indices")
def get_nse_indices():
    with _option_chain_lock:
        cache_age = time.time() - _nse_index_cache["timestamp"]
        if cache_age < 5:
            return _nse_index_cache["rows"]

    rows = _extract_live_indices_from_nse()

    with _option_chain_lock:
        if rows:
            _nse_index_cache["timestamp"] = time.time()
            _nse_index_cache["rows"] = rows
        elif _nse_index_cache["rows"]:
            rows = _nse_index_cache["rows"]

    return rows


@app.get("/api/oi_dashboard")
def get_oi_dashboard(symbol: str | None = None, expiry: str | None = None, force: bool = False):
    cache_key = _option_chain_cache_key(symbol or "ALL", expiry)
    cached_rows = None
    if force:
        rows = _fetch_oi_dashboard_with_timeout(symbol=symbol, expiry=expiry, timeout_sec=3.0)
        if not rows:
            rows = _build_empty_oi_section(symbol or "NIFTY", expiry=expiry) if symbol else [
                _build_empty_oi_section("NIFTY", expiry=expiry),
                _build_empty_oi_section("BANKNIFTY", expiry=expiry),
            ]
        rows = _prepare_oi_rows_for_ui(rows)
        with _oi_dashboard_lock:
            cache_bucket = _oi_dashboard_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": None})
            cache_bucket["timestamp"] = time.time()
            cache_bucket["rows"] = rows if rows else None
        return rows

    with _oi_dashboard_lock:
        cache_bucket = _oi_dashboard_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": None})
        cache_age = time.time() - cache_bucket["timestamp"]
        cached_rows = cache_bucket["rows"]
        if cached_rows is not None and cache_age <= _OI_DASHBOARD_CACHE_TTL_SEC and not _oi_dashboard_needs_refresh(cached_rows):
            # #region agent log (H4: oi_dashboard cache hit returning empty/stale rows)
            try:
                first_section = cached_rows[0] if isinstance(cached_rows, list) and cached_rows else None
                _agent_debug_log(
                    hypothesis_id="H4",
                    location="dashboard/dashboard.py:get_oi_dashboard",
                    message="oi_dashboard_cache_hit",
                    data={
                        "symbol": symbol,
                        "expiry": expiry,
                        "cache_age_sec": cache_age,
                        "sections_len": len(cached_rows) if isinstance(cached_rows, list) else None,
                        "first_rows_len": len((first_section or {}).get("rows") or []) if isinstance(first_section, dict) else None,
                        "first_call_change_oi_sum": (first_section or {}).get("summary", {}).get("call_change_oi_sum") if isinstance(first_section, dict) else None,
                        "first_put_change_oi_sum": (first_section or {}).get("summary", {}).get("put_change_oi_sum") if isinstance(first_section, dict) else None,
                    },
                )
            except Exception:
                pass
            # #endregion
            return _prepare_oi_rows_for_ui(cached_rows)

    # Keep UI responsive: serve cached rows immediately and refresh in background.
    if cached_rows is not None:
        # #region agent log (H4: serving cached rows; async refresh may update later)
        try:
            first_section = cached_rows[0] if isinstance(cached_rows, list) and cached_rows else None
            _agent_debug_log(
                hypothesis_id="H4",
                location="dashboard/dashboard.py:get_oi_dashboard",
                message="oi_dashboard_serving_cached_refresh_bg",
                data={
                    "symbol": symbol,
                    "expiry": expiry,
                    "cache_age_sec": cache_age,
                    "sections_len": len(cached_rows) if isinstance(cached_rows, list) else None,
                    "first_rows_len": len((first_section or {}).get("rows") or []) if isinstance(first_section, dict) else None,
                    "first_call_change_oi_sum": (first_section or {}).get("summary", {}).get("call_change_oi_sum") if isinstance(first_section, dict) else None,
                    "first_put_change_oi_sum": (first_section or {}).get("summary", {}).get("put_change_oi_sum") if isinstance(first_section, dict) else None,
                },
            )
        except Exception:
            pass
        # #endregion
        _refresh_oi_dashboard_cache_async(cache_key, symbol, expiry)
        return _prepare_oi_rows_for_ui(cached_rows)

    rows = _fetch_oi_dashboard_with_timeout(symbol=symbol, expiry=expiry, timeout_sec=1.8)
    if rows is None:
        if symbol:
            rows = _build_latest_oi_snapshot_section(symbol) or _build_empty_oi_section(symbol, expiry=expiry)
        else:
            rows = []
            for symbol_name in ("NIFTY", "BANKNIFTY"):
                rows.append(_build_latest_oi_snapshot_section(symbol_name) or _build_empty_oi_section(symbol_name, expiry=expiry))
        _refresh_oi_dashboard_cache_async(cache_key, symbol, expiry)
    if not rows:
        rows = _build_empty_oi_section(symbol or "NIFTY", expiry=expiry) if symbol else [
            _build_empty_oi_section("NIFTY", expiry=expiry),
            _build_empty_oi_section("BANKNIFTY", expiry=expiry),
        ]
    rows = _prepare_oi_rows_for_ui(rows)

    with _oi_dashboard_lock:
        cache_bucket = _oi_dashboard_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": None})
        cache_bucket["timestamp"] = time.time()
        cache_bucket["rows"] = rows if rows else None
    # #region agent log (H4: returning fresh or fallback oi_dashboard rows)
    try:
        first_section = rows[0] if isinstance(rows, list) and rows else None
        _agent_debug_log(
            hypothesis_id="H4",
            location="dashboard/dashboard.py:get_oi_dashboard",
            message="oi_dashboard_returning_fresh_or_fallback",
            data={
                "symbol": symbol,
                "expiry": expiry,
                "sections_len": len(rows) if isinstance(rows, list) else None,
                "first_rows_len": len((first_section or {}).get("rows") or []) if isinstance(first_section, dict) else None,
                "first_call_change_oi_sum": (first_section or {}).get("summary", {}).get("call_change_oi_sum") if isinstance(first_section, dict) else None,
                "first_put_change_oi_sum": (first_section or {}).get("summary", {}).get("put_change_oi_sum") if isinstance(first_section, dict) else None,
            },
        )
    except Exception:
        pass
    # #endregion
    return rows


@app.get("/api/oi_history")
def get_oi_history_api(
    symbol: str | None = None,
    expiry: str | None = None,
    limit: int = 500,
    history_date: str | None = None,
):
    return get_oi_history(symbol=symbol, expiry=expiry, limit=limit, history_date=history_date)


@app.get("/api/oi_history_dates")
def get_oi_history_dates_api(symbol: str | None = None, expiry: str | None = None, limit: int = 365):
    return get_oi_history_dates(symbol=symbol, expiry=expiry, limit=limit)


def _futures_head_has_live_values(rows):
    if not rows:
        return False
    for section in rows:
        for contract in section.get("contracts", []):
            for field in ("ltp", "bid", "ask", "oi", "volume"):
                value = contract.get(field)
                if value not in (None, "", "-"):
                    return True
    return False


def _futures_head_has_contracts(rows):
    if not rows:
        return False
    for section in rows:
        contracts = section.get("contracts", []) if isinstance(section, dict) else []
        if contracts:
            return True
    return False


def _futures_head_needs_refresh(rows):
    if not _is_market_window_now() and not _after_hours_refresh_enabled():
        return not _futures_head_has_contracts(rows)
    return not _futures_head_has_live_values(rows)


def _refresh_futures_head_cache_async():
    global _futures_head_refreshing

    def worker():
        global _futures_head_refreshing
        try:
            rows = _fetch_futures_head()
            if rows and _futures_head_has_contracts(rows):
                with _futures_head_lock:
                    existing_rows = _futures_head_cache.get("rows") or []
                    if _futures_head_has_live_values(rows) or not _futures_head_has_contracts(existing_rows):
                        _futures_head_cache["timestamp"] = time.time()
                        _futures_head_cache["rows"] = rows
        finally:
            with _futures_head_refresh_lock:
                _futures_head_refreshing = False

    with _futures_head_refresh_lock:
        if _futures_head_refreshing:
            return
        _futures_head_refreshing = True
    threading.Thread(target=worker, daemon=True).start()


def _refresh_oi_dashboard_cache_async(cache_key, symbol, expiry):
    def worker():
        try:
            # Use timeout-guarded fetch so one blocked provider call doesn't pin this cache key forever.
            rows = _fetch_oi_dashboard_with_timeout(symbol=symbol, expiry=expiry, timeout_sec=3.0)
            if not rows:
                rows = _build_empty_oi_section(symbol or "NIFTY", expiry=expiry) if symbol else [
                    _build_empty_oi_section("NIFTY", expiry=expiry),
                    _build_empty_oi_section("BANKNIFTY", expiry=expiry),
                ]
            rows = _prepare_oi_rows_for_ui(rows)
            with _oi_dashboard_lock:
                cache_bucket = _oi_dashboard_cache.setdefault(cache_key, {"timestamp": 0.0, "rows": None})
                cache_bucket["timestamp"] = time.time()
                cache_bucket["rows"] = rows if rows else None
        finally:
            with _oi_dashboard_refresh_lock:
                _oi_dashboard_refreshing.discard(cache_key)

    with _oi_dashboard_refresh_lock:
        if cache_key in _oi_dashboard_refreshing:
            return
        _oi_dashboard_refreshing.add(cache_key)
    threading.Thread(target=worker, daemon=True).start()


def _oi_dashboard_needs_refresh(rows):
    if not _is_market_window_now() and not _after_hours_refresh_enabled():
        if isinstance(rows, list):
            return not any(section.get("rows") for section in rows if isinstance(section, dict))
        if isinstance(rows, dict):
            return not rows.get("rows")
        return True
    if not rows:
        return True
    if isinstance(rows, list):
        for section in rows:
            if not isinstance(section, dict):
                continue
            if section.get("oi_available") is False:
                return True
            if section.get("feed") in {"bootstrap", "snapshot_bootstrap", "snapshot_fallback", "market_data_stale"}:
                return True
            age = _to_float(section.get("option_age_sec"), None)
            if age is not None and age > _OPTION_REFRESH_FORCE_SEC:
                return True
        return not any(section.get("rows") for section in rows if isinstance(section, dict))
    if isinstance(rows, dict):
        if rows.get("oi_available") is False:
            return True
        if rows.get("feed") in {"bootstrap", "snapshot_bootstrap", "snapshot_fallback", "market_data_stale"}:
            return True
        age = _to_float(rows.get("option_age_sec"), None)
        if age is not None and age > _OPTION_REFRESH_FORCE_SEC:
            return True
        return not rows.get("rows")
    return True


@app.get("/api/futures_head")
def get_futures_head():
    with _futures_head_lock:
        cached_rows = _futures_head_cache["rows"]
        cache_age = time.time() - _futures_head_cache["timestamp"]
        if cached_rows:
            if cache_age > 1.0 or _futures_head_needs_refresh(cached_rows):
                _refresh_futures_head_cache_async()
            # #region agent log (H1/H2: futures_head serving cached data; might be stuck on nulls)
            try:
                sample_section = cached_rows[0] if isinstance(cached_rows, list) and cached_rows else None
                first_contract = None
                if isinstance(sample_section, dict):
                    contracts = sample_section.get("contracts") or []
                    first_contract = contracts[0] if contracts else None
                _agent_debug_log(
                    hypothesis_id="H1",
                    location="dashboard/dashboard.py:get_futures_head",
                    message="futures_head_cache_hit",
                    data={
                        "cache_age_sec": cache_age,
                        "sections_len": len(cached_rows) if isinstance(cached_rows, list) else None,
                        "has_live_values": _futures_head_has_live_values(cached_rows),
                        "first_contract_sample": (
                            {
                                "ltp": (first_contract or {}).get("ltp"),
                                "bid": (first_contract or {}).get("bid"),
                                "ask": (first_contract or {}).get("ask"),
                                "oi": (first_contract or {}).get("oi"),
                                "volume": (first_contract or {}).get("volume"),
                            }
                            if isinstance(first_contract, dict)
                            else None
                        ),
                    },
                )
            except Exception:
                pass
            # #endregion
            return cached_rows

    rows = _fetch_futures_head()
    if rows and _futures_head_has_contracts(rows):
        with _futures_head_lock:
            cached_rows = _futures_head_cache["rows"]
            if _futures_head_has_live_values(rows) or not _futures_head_has_contracts(cached_rows):
                _futures_head_cache["timestamp"] = time.time()
                _futures_head_cache["rows"] = rows
                return rows
        if cached_rows:
            return cached_rows

    with _futures_head_lock:
        cached_rows = _futures_head_cache["rows"]
        if rows:
            _futures_head_cache["timestamp"] = time.time()
            _futures_head_cache["rows"] = rows
            return rows
        if cached_rows:
            return cached_rows
    return rows


@app.get("/api/strategies")
def get_strategies():
    return strategy_runtime.get_strategies()


@app.post("/api/strategies/{strategy_id}/start")
def start_strategy(strategy_id: str):
    ok, message = strategy_runtime.start_strategy(strategy_id)
    return {"ok": ok, "message": message}


@app.post("/api/strategies/{strategy_id}/pause")
def pause_strategy(strategy_id: str):
    ok, message = strategy_runtime.pause_strategy(strategy_id)
    return {"ok": ok, "message": message}


@app.post("/api/strategies/{strategy_id}/resume")
def resume_strategy(strategy_id: str):
    ok, message = strategy_runtime.resume_strategy(strategy_id)
    return {"ok": ok, "message": message}


@app.post("/api/strategies/{strategy_id}/stop")
def stop_strategy(strategy_id: str):
    ok, message = strategy_runtime.stop_strategy(strategy_id)
    return {"ok": ok, "message": message}


@app.post("/api/control/pause_all_live")
def pause_all_live_strategies():
    active_strategies = strategy_runtime.get_active_strategy_ids()
    if not active_strategies:
        return {"ok": False, "message": "No live strategies to pause."}
    results = [strategy_runtime.pause_strategy(strategy_id) for strategy_id in active_strategies]
    ok = all(result[0] for result in results)
    return {"ok": ok, "message": f"Paused {len(active_strategies)} live strategy(s)."}


@app.post("/api/control/resume_all_paused")
def resume_all_paused_strategies():
    paused_strategies = [
        strategy["id"]
        for strategy in strategy_runtime.get_strategies()
        if str(strategy.get("status", "")).lower() == "paused"
    ]
    if not paused_strategies:
        return {"ok": False, "message": "No paused strategies to resume."}
    results = [strategy_runtime.resume_strategy(strategy_id) for strategy_id in paused_strategies]
    ok = all(result[0] for result in results)
    return {"ok": ok, "message": f"Resumed {len(paused_strategies)} paused strategy(s)."}


@app.post("/api/control/emergency_stop_all")
def emergency_stop_all_strategies():
    running_strategies = [
        strategy["id"]
        for strategy in strategy_runtime.get_strategies()
        if str(strategy.get("status", "")).lower() != "stopped"
    ]
    if not running_strategies:
        return {"ok": True, "message": "No active strategy. System is already stopped."}
    results = [strategy_runtime.stop_strategy(strategy_id) for strategy_id in running_strategies]
    ok = all(result[0] for result in results)
    return {"ok": ok, "message": f"Stopped {len(running_strategies)} strategy(s)."}


@app.post("/api/control/refresh_option_chain")
def refresh_option_chain():
    _reset_option_chain_cache()
    with _oi_dashboard_lock:
        _oi_dashboard_cache.clear()
    return {"ok": True, "message": "Option-chain cache cleared. Refresh in a few seconds."}


@app.post("/api/control/refresh_oi")
def refresh_oi():
    latest_snapshot_at = None
    errors = []
    try:
        _reset_option_chain_cache()
    except Exception as exc:
        logger.warning("Manual OI refresh cache reset failed: %s", exc)
        errors.append("cache_reset")

    try:
        with _oi_dashboard_lock:
            _oi_dashboard_cache.clear()
    except Exception as exc:
        logger.warning("Manual OI refresh dashboard cache clear failed: %s", exc)
        errors.append("dashboard_cache_clear")

    try:
        _refresh_market_data_async()
    except Exception as exc:
        logger.warning("Manual OI refresh market-data async trigger failed: %s", exc)
        errors.append("market_async")

    try:
        threading.Thread(target=capture_oi_snapshot, daemon=True).start()
    except Exception as exc:
        logger.warning("Manual OI refresh snapshot thread trigger failed: %s", exc)
        errors.append("snapshot_thread")

    try:
        db = SessionLocal()
        try:
            latest_snapshot_at = db.query(func.max(OiSnapshot.captured_at)).scalar()
        finally:
            db.close()
    except Exception as exc:
        logger.warning("Manual OI refresh latest snapshot lookup failed: %s", exc)
        errors.append("snapshot_lookup")

    return {
        "ok": True,
        "message": "OI refresh started.",
        "market_data_refreshed": None,
        "captured_rows": None,
        "last_oi_snapshot_at": latest_snapshot_at.isoformat() if latest_snapshot_at else None,
        "warnings": errors,
    }


def _fetch_latest_market_rows(local_db):
    subquery = (
        local_db.query(
            MarketData.symbol,
            func.max(MarketData.timestamp).label("max_timestamp")
        ).group_by(MarketData.symbol).subquery()
    )

    latest_data = local_db.query(MarketData).join(
        subquery,
        (MarketData.symbol == subquery.c.symbol) &
        (MarketData.timestamp == subquery.c.max_timestamp)
    ).all()

    return [
        {
            "symbol": md.symbol,
            "trading_symbol": md.trading_symbol,
            "instrument_type": md.instrument_type,
            "strike_price": md.strike_price,
            "expiry_date": md.expiry_date,
            "ltp": md.last_traded_price,
            "bid": md.bid_price,
            "ask": md.ask_price,
            "volume": md.volume,
            "oi": md.oi,
            "timestamp": md.timestamp.isoformat() if md.timestamp else None,
        }
        for md in latest_data
    ]


def _latest_market_age_seconds(local_db, instrument_types):
    latest_ts = (
        local_db.query(func.max(MarketData.timestamp))
        .filter(MarketData.instrument_type.in_(instrument_types))
        .scalar()
    )
    if not latest_ts:
        return None
    return (datetime.now() - latest_ts).total_seconds()


def _fetch_index_fallback_rows(session):
    if not session:
        return []

    symbols = [("nse_cm", "Nifty 50"), ("nse_cm", "Nifty Bank")]
    query_symbols = [f"{exchange_seg}|{symbol_name}" for exchange_seg, symbol_name in symbols]
    batched = _fetch_fyers_quotes_batch(
        session=session,
        exchange_seg=None,
        trading_symbols=query_symbols,
        timeout_sec=4.0,
        retries=2,
        cache_ttl_sec=12.0,
    )

    rows = []
    for exchange_seg, symbol_name in symbols:
        try:
            row = batched.get(f"{exchange_seg}|{symbol_name}") or batched.get(symbol_name)
            if not row:
                continue
            depth = row.get("depth", {}) or {}
            buy = (depth.get("buy") or [{}])[0] or {}
            sell = (depth.get("sell") or [{}])[0] or {}
            rows.append({
                "symbol": f"{exchange_seg}|{symbol_name}",
                "trading_symbol": row.get("display_symbol", symbol_name),
                "exchange_seg": exchange_seg,
                "instrument_type": "INDEX",
                "strike_price": None,
                "ltp": _safe_quote_value(row, "ltp", 0.0),
                "bid": _to_float(buy.get("price", 0), 0.0),
                "ask": _to_float(sell.get("price", 0), 0.0),
                "volume": int(_safe_quote_value(row, "last_volume", 0.0)),
                "oi": int(_safe_quote_value(row, "open_int", 0.0)),
            })
        except Exception:
            continue
    return rows


def _find_underlying_ltp(rows, symbol_hint):
    target_symbol = f"nse_cm|{symbol_hint}"
    for row in rows:
        if row.get("symbol") == target_symbol:
            return _to_float(row.get("ltp", 0.0), 0.0)
    for row in rows:
        trading_symbol = str(row.get("trading_symbol", ""))
        if symbol_hint in trading_symbol:
            return _to_float(row.get("ltp", 0.0), 0.0)
    return 0.0


def _refresh_market_data_now():
    db = SessionLocal()
    try:
        current_rows = _fetch_latest_market_rows(db)
        index_age_sec = _latest_market_age_seconds(db, ["INDEX"])
        option_age_sec = _latest_market_age_seconds(db, ["CE", "PE"])
    finally:
        db.close()

    index_is_fresh = index_age_sec is not None and index_age_sec <= _MARKETDATA_INDEX_MAX_AGE_SEC
    option_is_fresh = option_age_sec is not None and option_age_sec <= _OPTION_REFRESH_FORCE_SEC
    if current_rows and index_is_fresh and option_is_fresh:
        return False

    # Avoid aggressive fallback polling when markets are closed unless explicitly enabled.
    if not _is_market_window_now():
        if not _after_hours_refresh_enabled():
            return False
        has_index_rows = any(str((row or {}).get("instrument_type", "")).upper() == "INDEX" for row in (current_rows or []))
        has_option_rows = any(str((row or {}).get("instrument_type", "")).upper() in {"CE", "PE"} for row in (current_rows or []))
        if current_rows and has_index_rows and has_option_rows:
            return False

    session = _load_dashboard_session()
    if not session:
        return False

    refreshed_rows = []
    if index_is_fresh:
        refreshed_rows.extend([row for row in current_rows if str(row.get("instrument_type", "")).upper() == "INDEX"])
    else:
        refreshed_rows.extend(_fetch_index_fallback_rows(session))

    if option_is_fresh:
        refreshed_rows.extend([row for row in current_rows if str(row.get("instrument_type", "")).upper() in ("CE", "PE")])
    else:
        index_targets = [
            ("NIFTY", "Nifty 50"),
            ("BANKNIFTY", "Nifty Bank"),
        ]
        option_rows = []
        for symbol_name, symbol_hint in index_targets:
            underlying_ltp = _find_underlying_ltp(refreshed_rows, symbol_hint)
            if underlying_ltp <= 0:
                underlying_ltp = _find_underlying_ltp(current_rows, symbol_hint)
            if underlying_ltp <= 0:
                continue
            fetched = _get_option_chain_fallback(session, underlying_ltp, underlying_symbol=symbol_name)
            if fetched:
                option_rows.extend(fetched)
        refreshed_rows.extend(option_rows)

    if not refreshed_rows:
        return False
    _persist_market_data_rows(refreshed_rows)
    return True


def _refresh_market_data_async():
    global _market_data_refreshing

    def worker():
        global _market_data_refreshing
        try:
            _refresh_market_data_now()
        except Exception as exc:
            logger.debug("Background market-data refresh failed: %s", exc)
        finally:
            with _market_data_refresh_lock:
                _market_data_refreshing = False

    with _market_data_refresh_lock:
        if _market_data_refreshing:
            return
        _market_data_refreshing = True

    threading.Thread(target=worker, daemon=True).start()


@app.get("/api/market_data")
def get_market_data():
    db = SessionLocal()
    try:
        current_rows = _fetch_latest_market_rows(db)
        index_age_sec = _latest_market_age_seconds(db, ["INDEX"])
        option_age_sec = _latest_market_age_seconds(db, ["CE", "PE"])
    finally:
        db.close()

    index_is_fresh = index_age_sec is not None and index_age_sec <= _MARKETDATA_INDEX_MAX_AGE_SEC
    option_is_fresh = option_age_sec is not None and option_age_sec <= _OPTION_REFRESH_FORCE_SEC

    if not _is_market_window_now() and not _after_hours_refresh_enabled():
        return current_rows

    if not current_rows:
        _refresh_market_data_now()
        db = SessionLocal()
        try:
            return _fetch_latest_market_rows(db)
        finally:
            db.close()

    if not index_is_fresh or not option_is_fresh:
        _refresh_market_data_async()

    return current_rows


@app.get("/api/ticks")
def get_ticks(limit: int = 120, symbol: str | None = None, exchange_seg: str | None = None, instrument_type: str | None = None):
    db = SessionLocal()
    try:
        limit = max(1, min(int(limit or 120), 300))
        query = db.query(MarketData).order_by(MarketData.timestamp.desc())

        if symbol:
            pattern = f"%{symbol.strip()}%"
            query = query.filter(or_(MarketData.symbol.ilike(pattern), MarketData.trading_symbol.ilike(pattern)))
        if exchange_seg:
            query = query.filter(MarketData.exchange_seg == exchange_seg)
        if instrument_type:
            query = query.filter(MarketData.instrument_type == instrument_type)

        rows = query.limit(limit).all()
        return [
            {
                "symbol": row.symbol,
                "trading_symbol": row.trading_symbol,
                "exchange_seg": row.exchange_seg,
                "instrument_type": row.instrument_type,
                "strike_price": row.strike_price,
                "expiry_date": row.expiry_date,
                "ltp": row.last_traded_price,
                "bid": row.bid_price,
                "ask": row.ask_price,
                "oi": row.oi,
                "volume": row.volume,
                "timestamp": row.timestamp.isoformat(),
            }
            for row in rows
        ]
    finally:
        db.close()
