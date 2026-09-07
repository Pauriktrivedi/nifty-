import logging
import os
import threading
import time
from datetime import datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_FYERS_MASTER_URLS = {
    "nse_cm": "https://public.fyers.in/sym_details/NSE_CM.csv",
    "nse_fo": "https://public.fyers.in/sym_details/NSE_FO.csv",
}

_FYERS_COLUMNS = [
    "fytoken",
    "display_name",
    "exchange_instrument_type",
    "lot_size",
    "tick_size",
    "isin",
    "trading_session",
    "last_update",
    "expiry_epoch",
    "symbol",
    "exchange",
    "segment",
    "scrip_code",
    "underlying_symbol",
    "underlying_scrip_code",
    "strike_price",
    "option_type",
    "underlying_fytoken",
    "reserved_1",
    "reserved_2",
    "reserved_3",
]


class InstrumentMaster:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.cm_file = os.path.join(self.data_dir, "fyers_nse_cm.csv")
        self.fo_file = os.path.join(self.data_dir, "fyers_nse_fo.csv")
        self.cm_df = None
        self.fo_df = None
        self._download_lock = threading.Lock()

    def _segment_destination(self, segment):
        return self.cm_file if segment == "nse_cm" else self.fo_file

    def _is_stale(self, filepath, max_age_seconds=6 * 3600):
        if not os.path.exists(filepath):
            return True
        return (time.time() - os.path.getmtime(filepath)) > max_age_seconds

    def _download_segment(self, segment):
        url = _FYERS_MASTER_URLS.get(segment)
        destination = self._segment_destination(segment)
        if not url:
            return

        logger.info("Downloading FYERS %s master from %s", segment, url)
        response = requests.get(url, timeout=(20, 180))
        response.raise_for_status()
        content = response.text
        if len(content) < 1024:
            raise RuntimeError(f"Downloaded master file too small for {segment}")

        with open(destination, "w", encoding="utf-8") as handle:
            handle.write(content)

    @staticmethod
    def _format_expiry(epoch_value):
        if pd.isna(epoch_value):
            return None
        try:
            raw = float(epoch_value)
        except Exception:
            return None
        if raw <= 0:
            return None
        if raw > 10**12:
            raw = raw / 1000.0
        try:
            return datetime.fromtimestamp(raw).strftime("%d-%b-%Y")
        except Exception:
            return None

    @staticmethod
    def _instrument_type(row):
        opt = str(row.get("option_type") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        if opt in {"CE", "PE"}:
            return "OPT"
        if symbol.endswith("FUT"):
            return "FUT"
        if symbol.endswith("-INDEX"):
            return "INDEX"
        return "EQ"

    def _to_legacy_shape(self, raw_df):
        df = raw_df.copy()

        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["pSymbol"] = df["symbol"]
        df["pTrdSymbol"] = df["symbol"]
        df["pSymbolName"] = (
            df["underlying_symbol"].fillna(df["display_name"]).astype(str).str.strip()
        )
        df["pOptionType"] = df["option_type"].fillna("XX").astype(str).str.upper().str.strip()
        df["dStrikePrice"] = pd.to_numeric(df["strike_price"], errors="coerce")
        df["pInstType"] = df.apply(self._instrument_type, axis=1)
        df["lExpiryDate"] = df["expiry_epoch"].apply(self._format_expiry)
        df["pExpiryDate"] = df["lExpiryDate"]

        return df

    def download(self, auth_session=None, segments=None):
        requested = set(segments or ("nse_cm", "nse_fo"))
        with self._download_lock:
            for segment in requested:
                destination = self._segment_destination(segment)
                if self._is_stale(destination):
                    try:
                        self._download_segment(segment)
                    except Exception as exc:
                        logger.error("Failed to download FYERS %s master: %s", segment, exc)

    def load(self, segment='nse_fo'):
        file_path = self._segment_destination(segment)
        if not os.path.exists(file_path):
            logger.warning("Master file missing for %s. Triggering download.", segment)
            self.download(segments=[segment])
        if not os.path.exists(file_path):
            logger.error("File %s still missing after download.", file_path)
            return None

        try:
            raw = pd.read_csv(file_path, header=None, names=_FYERS_COLUMNS, low_memory=False)
            shaped = self._to_legacy_shape(raw)
            if segment == "nse_fo":
                self.fo_df = shaped
            else:
                self.cm_df = shaped
            return shaped
        except Exception as exc:
            logger.error("Failed loading %s master: %s", segment, exc)
            return None

    def _ensure_fo(self):
        if self.fo_df is None:
            self.load("nse_fo")
        return self.fo_df is not None

    def find_option(self, underlying, expiry, strike, opt_type):
        if not self._ensure_fo():
            return None

        df = self.fo_df
        try:
            strike_target = float(strike)
            expiry_label = str(expiry or "").strip()
            mask = (
                df["pInstType"].astype(str).str.contains("OPT", na=False)
                & df["pSymbolName"].astype(str).str.upper().str.contains(str(underlying).upper(), na=False)
                & (df["pOptionType"].astype(str).str.upper() == str(opt_type).upper())
                & (pd.to_numeric(df["dStrikePrice"], errors="coerce") == strike_target)
            )
            if expiry_label:
                mask = mask & (df["lExpiryDate"].astype(str) == expiry_label)

            result = df[mask]
            if result.empty:
                return None
            row = result.iloc[0]
            return {
                "pTrdSymbol": row["pTrdSymbol"],
                "pSymbol": row["pSymbol"],
            }
        except Exception as exc:
            logger.error("Error finding option for %s %s %s %s: %s", underlying, expiry, strike, opt_type, exc)
            return None

    def find_future(self, underlying, expiry):
        if not self._ensure_fo():
            return None

        df = self.fo_df
        try:
            expiry_label = str(expiry or "").strip()
            mask = (
                df["pInstType"].astype(str).str.contains("FUT", na=False)
                & df["pSymbolName"].astype(str).str.upper().str.contains(str(underlying).upper(), na=False)
            )
            if expiry_label:
                mask = mask & (df["lExpiryDate"].astype(str) == expiry_label)

            result = df[mask]
            if result.empty:
                return None
            row = result.iloc[0]
            return {
                "pTrdSymbol": row["pTrdSymbol"],
                "pSymbol": row["pSymbol"],
            }
        except Exception as exc:
            logger.error("Error finding future for %s %s: %s", underlying, expiry, exc)
            return None
