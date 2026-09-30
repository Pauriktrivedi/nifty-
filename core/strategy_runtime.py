import threading


STRATEGY_CATALOG = {
    "breakout_range": {
        "id": "breakout_range",
        "name": "Breakout Range Strategy",
        "description": "Executes trades when NIFTY breaks a predefined range.",
        "schedule": "Continuous during market hours.",
        "details": "Goes LONG above range-high and SHORT below range-low.",
    },
    "twelve_thirty_five": {
        "id": "twelve_thirty_five",
        "name": "12:35 Options Selling",
        "description": "Shorts ATM CE and 50-point ITM PE at 12:35 PM.",
        "schedule": "Entry 12:35 PM, exit 03:25 PM.",
        "details": "Each leg has independent 25-point stop-loss from entry.",
    },
    "paper_test": {
        "id": "paper_test",
        "name": "Paper Test Strategy",
        "description": "Places one known paper trade to verify order and fill plumbing.",
        "schedule": "Runs once on the first live NIFTY tick.",
        "details": "Used to prove the end-to-end strategy, order, and fill path.",
    },
    "sample_strategy": {
        "id": "sample_strategy",
        "name": "Sample Dummy Strategy",
        "description": "Basic placeholder strategy for framework checks.",
        "schedule": "Continuous.",
        "details": "Useful for end-to-end testing of ticks and order flow.",
    },
}

_lock = threading.Lock()
_strategy_states = {
    strategy_id: {"status": "Stopped"}
    for strategy_id in STRATEGY_CATALOG
}
_callbacks = {
    "start": None,
    "pause": None,
    "resume": None,
    "stop": None,
}


def register_callbacks(start_cb=None, pause_cb=None, resume_cb=None, stop_cb=None):
    with _lock:
        _callbacks["start"] = start_cb
        _callbacks["pause"] = pause_cb
        _callbacks["resume"] = resume_cb
        _callbacks["stop"] = stop_cb


def set_runtime_state(strategy_id, status):
    with _lock:
        if strategy_id not in _strategy_states:
            _strategy_states[strategy_id] = {"status": "Stopped"}
        _strategy_states[strategy_id]["status"] = status


def _status_for(strategy_id):
    state = _strategy_states.get(strategy_id) or {}
    return state.get("status", "Stopped")


def get_strategies():
    with _lock:
        return [
            {
                **meta,
                "status": _status_for(strategy_id),
            }
            for strategy_id, meta in STRATEGY_CATALOG.items()
        ]


def get_strategy_state(strategy_id):
    with _lock:
        return dict(_strategy_states.get(strategy_id, {"status": "Stopped"}))


def get_active_strategy_ids():
    with _lock:
        return [
            strategy_id
            for strategy_id, state in _strategy_states.items()
            if str(state.get("status", "")).lower() == "live"
        ]


def _call_callback(action, strategy_id):
    with _lock:
        cb = _callbacks.get(action)
    if cb is None:
        return False, "Strategy control callback not registered."
    return cb(strategy_id)


def start_strategy(strategy_id):
    if strategy_id not in STRATEGY_CATALOG:
        return False, "Unknown strategy id."
    ok, message = _call_callback("start", strategy_id)
    if ok:
        set_runtime_state(strategy_id, "Live")
    return ok, message


def pause_strategy(strategy_id):
    if strategy_id not in STRATEGY_CATALOG:
        return False, "Unknown strategy id."
    ok, message = _call_callback("pause", strategy_id)
    if ok:
        set_runtime_state(strategy_id, "Paused")
    return ok, message


def resume_strategy(strategy_id):
    if strategy_id not in STRATEGY_CATALOG:
        return False, "Unknown strategy id."
    ok, message = _call_callback("resume", strategy_id)
    if ok:
        set_runtime_state(strategy_id, "Live")
    return ok, message


def stop_strategy(strategy_id):
    if strategy_id not in STRATEGY_CATALOG:
        return False, "Unknown strategy id."
    ok, message = _call_callback("stop", strategy_id)
    if ok:
        set_runtime_state(strategy_id, "Stopped")
    return ok, message
