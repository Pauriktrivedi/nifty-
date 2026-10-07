import logging
logging.basicConfig(level=logging.DEBUG)
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket
from core.websocket_feed import _patched_response_output

# Mock a WebSocket message processing
class MySocket(FyersDataSocket):
    def On_message(self, response):
        print("Message received:", response)

    def __init__(self):
        super().__init__('', write_to_file=False)

ws = MySocket()

# Let's call the patched method directly, but bound to our instance.
import types
patched = types.MethodType(_patched_response_output, ws)

mock_data = {
    'ltp': 10000,
    'precision': 2,
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

patched(mock_data, "scrips")
