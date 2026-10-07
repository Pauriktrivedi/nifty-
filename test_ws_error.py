import logging
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket
from core.websocket_feed import _patched_response_output

ws = FyersDataSocket('', write_to_file=False)

def mock_on_error(self, e):
    print("ON ERROR:", e)

ws.On_error = mock_on_error.__get__(ws, FyersDataSocket)

ws.FyersDataSocket = FyersDataSocket
ws.FyersDataSocket._FyersDataSocket__response_output = _patched_response_output

# Send a tick that has missing precision or multiplier?
# wait, what if precision or multiplier is missing?
mock_data = {
    'ltp': 10000,
    # 'precision': 2, # missing!
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

try:
    ws._FyersDataSocket__response_output(mock_data, "scrips")
except Exception as e:
    print("Caught:", e)
