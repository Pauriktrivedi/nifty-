import json
import os
import tempfile
import threading
import time
from datetime import datetime, timezone

STATUS_FILE = "/tmp/ws_status.json"
_STATUS_LOCK = threading.RLock()
_STATUS_CACHE = None
_LAST_FLUSH_AT = 0.0
_FLUSH_INTERVAL_SEC = 1.0


def _load_status():
    if not os.path.exists(STATUS_FILE):
        return {}
    try:
        with open(STATUS_FILE, "r") as handle:
            payload = json.load(handle)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_status(payload):
    status_dir = os.path.dirname(STATUS_FILE) or "."
    os.makedirs(status_dir, exist_ok=True)

    # Serialize writes to avoid race conditions across websocket callbacks.
    with _STATUS_LOCK:
        fd, tmp_file = tempfile.mkstemp(prefix="ws_status_", suffix=".tmp", dir=status_dir)
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle)
            try:
                os.replace(tmp_file, STATUS_FILE)
            except FileNotFoundError:
                # Rarely observed on some systems under heavy callback churn;
                # degrade gracefully instead of breaking websocket callbacks.
                with open(STATUS_FILE, "w") as handle:
                    json.dump(payload, handle)
        finally:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except Exception:
                    pass


def _ensure_cache_loaded():
    global _STATUS_CACHE
    if _STATUS_CACHE is None:
        _STATUS_CACHE = _load_status()
        if not isinstance(_STATUS_CACHE, dict):
            _STATUS_CACHE = {}
        _STATUS_CACHE.setdefault("connected", False)
    return _STATUS_CACHE


def set_ws_metrics(**metrics):
    global _LAST_FLUSH_AT
    try:
        with _STATUS_LOCK:
            payload = _ensure_cache_loaded()
            payload.update(metrics)
            payload.setdefault("connected", False)
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()

            # Persist sparingly to avoid I/O pressure under high tick throughput.
            now = time.time()
            important_update = any(key in metrics for key in ("connected", "last_error", "last_connect_at"))
            if important_update or (now - _LAST_FLUSH_AT) >= _FLUSH_INTERVAL_SEC:
                _write_status(dict(payload))
                _LAST_FLUSH_AT = now
    except Exception:
        # Status tracking must never interrupt market data processing.
        return


def set_ws_status(connected: bool):
    set_ws_metrics(connected=bool(connected))


def get_ws_metrics():
    with _STATUS_LOCK:
        payload = dict(_ensure_cache_loaded())
    payload.setdefault("connected", False)
    return payload


def get_ws_status() -> bool:
    return bool(get_ws_metrics().get("connected", False))
