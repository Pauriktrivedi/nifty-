import logging
logging.basicConfig(level=logging.DEBUG)
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket
from core.websocket_feed import _patched_response_output

ws = FyersDataSocket('', write_to_file=False)

# Let's call the patched method with some mock data
mock_data = {
    'ltp': 10000,
    'precision': 2,
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

print(ws._FyersDataSocket__response_output)

# Inject data
try:
    ws._FyersDataSocket__response_output(mock_data, "scrips")
except Exception as e:
    print("Error:", e)
