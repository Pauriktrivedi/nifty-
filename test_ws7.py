import logging
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket
from core.websocket_feed import _patched_response_output

ws = FyersDataSocket('', write_to_file=False)
ws.FyersDataSocket = FyersDataSocket
ws.FyersDataSocket._FyersDataSocket__response_output = _patched_response_output

# Mock data based on fyers_apiv3 3.1.19
mock_data = {
    'ltp': 10000,
    'precision': 2,
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

# The __response_output gets called with (data_resp, data_type)
ws._FyersDataSocket__response_output(mock_data, "scrips")
