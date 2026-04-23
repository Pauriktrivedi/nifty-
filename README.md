# FYERS Algorithmic Trading System

This repository is now migrated from Kotak to FYERS for authentication, live quotes, option discovery, and order placement.

## Setup

1. Create and activate virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create `.env` from template:
   ```bash
   cp .env.example .env
   ```

4. Fill FYERS credentials in `.env`:
   - `FYERS_CLIENT_ID`
   - `FYERS_SECRET_KEY`
   - `FYERS_REDIRECT_URI` (default works for most users)

5. Generate access token:
   ```bash
   python fyers_auth.py
   ```
   This saves token to `.access_token` (or `FYERS_ACCESS_TOKEN_FILE`).

## Run

```bash
python main.py
```

Or macOS launcher:
```bash
./start.command
```

## Modes

- `PAPER_MODE=true`: simulated order fills and virtual cash.
- `PAPER_MODE=false`: live FYERS order placement.
- `ENABLE_STRATEGIES=false`: dashboard-only mode (no strategy execution, no order engine startup).

## FYERS Migration Notes

- `core/auth.py` now uses FYERS token/session model.
- `core/websocket_feed.py` now uses FYERS DataSocket.
- `core/order_manager.py` now places FYERS orders.
- `core/instruments.py` now downloads FYERS symbol masters:
  - `https://public.fyers.in/sym_details/NSE_CM.csv`
  - `https://public.fyers.in/sym_details/NSE_FO.csv`
- Existing strategy/risk/dashboard flow is preserved with internal compatibility symbols.

## Live Data Notes

- NSE market live stream is expected only during market hours (roughly 09:15-15:30 IST).
- Dashboard fallback quote polling is throttled and skips aggressive refresh when markets are closed.
- FYERS quotes API does not reliably provide OI for option contracts; OI is primarily expected from live feed / market depth paths.
- If you launch after market close, scheduler keeps the live engine idle by default and logs this clearly.
- Optional knobs:
  - `FORCE_START_OUTSIDE_MARKET=true`: start engine even after hours (diagnostics).
  - `AFTER_HOURS_REFRESH=true`: allow dashboard quote refresh after hours (enabled by default for data visibility).
  - `OI_SNAPSHOT_INTERVAL_MINUTES=5`: scheduler capture interval for OI history charts.

## Tests

```bash
python -m pytest tests/
```
