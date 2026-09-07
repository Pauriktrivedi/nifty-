import re
from functools import lru_cache

from fyers_apiv3.fyersModel import FyersModel

_INDEX_INTERNAL_TO_FYERS = {
    "nse_cm|nifty50": "NSE:NIFTY50-INDEX",
    "nse_cm|nifty 50": "NSE:NIFTY50-INDEX",
    "nse_cm|nifty": "NSE:NIFTY50-INDEX",
    # Websocket subscription expects NIFTYBANK-INDEX.
    "nse_cm|niftybank": "NSE:NIFTYBANK-INDEX",
    "nse_cm|nifty bank": "NSE:NIFTYBANK-INDEX",
    "nse_cm|banknifty": "NSE:NIFTYBANK-INDEX",
}

_INDEX_FYERS_TO_INTERNAL = {
    "NSE:NIFTY50-INDEX": "nse_cm|Nifty 50",
    "NSE:NIFTYBANK-INDEX": "nse_cm|Nifty Bank",
    # Backward-compatible alias seen in some REST payloads.
    "NSE:BANKNIFTY-INDEX": "nse_cm|Nifty Bank",
}


@lru_cache(maxsize=8)
def get_fyers_client(client_id: str, access_token: str) -> FyersModel:
    if not client_id or not access_token:
        raise ValueError("FYERS client_id/access_token missing.")
    return FyersModel(
        is_async=False,
        log_path="",
        client_id=client_id,
        token=access_token,
    )


def to_ws_access_token(client_id: str, access_token: str) -> str:
    return f"{client_id}:{access_token}"


def _clean_exchange(exchange_seg: str | None) -> str:
    return str(exchange_seg or "").strip().lower()


def to_fyers_symbol(exchange_seg: str | None, symbol: str | None) -> str | None:
    raw = str(symbol or "").strip()
    if not raw:
        return None

    # Normalize known BANKNIFTY aliases before passthrough checks.
    upper_raw = raw.upper()
    if upper_raw in {"NSE:NIFTYBANK-INDEX", "NSE:BANKNIFTY-INDEX"}:
        return "NSE:NIFTYBANK-INDEX"

    if ":" in raw and raw.split(":", 1)[0].isalpha():
        return raw

    if "|" in raw:
        inferred_exchange, raw_symbol = raw.split("|", 1)
        exchange_seg = exchange_seg or inferred_exchange
        raw = raw_symbol.strip()
        upper_raw = raw.upper()
        if upper_raw in {"NSE:NIFTYBANK-INDEX", "NSE:BANKNIFTY-INDEX"}:
            return "NSE:NIFTYBANK-INDEX"

    if ":" in raw and raw.split(":", 1)[0].isalpha():
        return raw

    exchange = _clean_exchange(exchange_seg)

    internal_key = f"{exchange}|{raw.lower()}"
    if internal_key in _INDEX_INTERNAL_TO_FYERS:
        return _INDEX_INTERNAL_TO_FYERS[internal_key]

    if exchange in {"nse_cm", "nse", "cm"}:
        if raw.upper().endswith("-INDEX"):
            return f"NSE:{raw}"
        if raw.upper().endswith("-EQ"):
            return f"NSE:{raw}"
        return f"NSE:{raw}-EQ"

    if exchange in {"nse_fo", "nfo", "fo"}:
        return f"NSE:{raw}"

    return raw


def to_internal_symbol(fyers_symbol: str | None) -> tuple[str, str]:
    symbol = str(fyers_symbol or "").strip()
    if not symbol:
        return "", ""

    if symbol in _INDEX_FYERS_TO_INTERNAL:
        internal = _INDEX_FYERS_TO_INTERNAL[symbol]
        return internal, "nse_cm"

    if symbol.upper().endswith("-EQ"):
        return f"nse_cm|{symbol}", "nse_cm"

    if symbol.upper().endswith("-INDEX"):
        return f"nse_cm|{symbol}", "nse_cm"

    return f"nse_fo|{symbol}", "nse_fo"


def infer_instrument_type(fyers_symbol: str | None) -> str:
    symbol = str(fyers_symbol or "").upper()
    if symbol.endswith("-INDEX"):
        return "INDEX"
    if symbol.endswith("-EQ"):
        return "EQ"
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    if symbol.endswith("FUT"):
        return "FUT"
    return "EQ"


def infer_strike(fyers_symbol: str | None) -> float | None:
    symbol = str(fyers_symbol or "").upper()
    match = re.search(r"(\d+(?:\.\d+)?)(CE|PE)$", symbol)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None
