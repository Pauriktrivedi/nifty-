import logging
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket
from core.websocket_feed import _patched_response_output
data_ws = __import__('fyers_apiv3.FyersWebsocket.data_ws', fromlist=['FyersDataSocket'])

ws = FyersDataSocket('', write_to_file=False)
data_ws.FyersDataSocket._FyersDataSocket__response_output = _patched_response_output

ws._FyersDataSocket__response_output({"ltp": 100, "multiplier": 1, "precision": 2}, "scrips")
