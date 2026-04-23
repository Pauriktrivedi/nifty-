#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"
LOCK_FILE="/tmp/nifty_main.lock"

# Fail fast when another backend instance already owns the lock.
if [ -f "${LOCK_FILE}" ]; then
  lock_pid=$(cat "${LOCK_FILE}" 2>/dev/null | tr -d '[:space:]' || true)
  if [ -n "${lock_pid}" ] && ps -p "${lock_pid}" -o command= 2>/dev/null | grep -q "python.*main.py"; then
    echo "Another backend instance is already running (pid ${lock_pid})."
    echo "Stop it first, then rerun start.command."
    exit 1
  fi
  # Stale lock file.
  rm -f "${LOCK_FILE}" 2>/dev/null || true
fi

# Ensure stale dashboard servers do not keep old code/routes alive.
if command -v lsof >/dev/null 2>&1; then
  pids=$(lsof -ti tcp:8000 || true)
  if [ -n "${pids}" ]; then
    kill_list=""
    for pid in ${pids}; do
      cmd=$(ps -p "${pid}" -o comm= 2>/dev/null || true)
      case "${cmd}" in
        *python*|*Python*)
          kill_list="${kill_list} ${pid}"
          ;;
      esac
    done
    if [ -n "${kill_list}" ]; then
      echo "Stopping existing Python process on port 8000:${kill_list}"
      kill -9 ${kill_list} 2>/dev/null || true
    fi
    sleep 0.5
  fi
fi

if [ ! -d "venv" ]; then
  echo "Virtual environment not found. Run: python -m venv venv"
  exit 1
fi

source venv/bin/activate
rm -f session.json

if [ ! -s ".access_token" ]; then
  echo "FYERS access token not found. Starting auth flow..."
  python fyers_auth.py
fi

if ! python -c "
from core.auth import FyersAuth
auth = FyersAuth()
s = auth.get_session()
print('Broker:', s.get('broker'))
print('Client ID:', s.get('client_id'))
print('Session cached:', bool(s.get('access_token')))
"; then
  echo "Existing FYERS token is invalid/expired. Starting auth flow..."
  python fyers_auth.py

  python -c "
from core.auth import FyersAuth
auth = FyersAuth()
s = auth.get_session()
print('Broker:', s.get('broker'))
print('Client ID:', s.get('client_id'))
print('Session cached:', bool(s.get('access_token')))
"
fi

python main.py
