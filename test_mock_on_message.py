from core.websocket_feed import WebSocketFeedHandler
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

import logging
logging.basicConfig(level=logging.DEBUG)

handler = WebSocketFeedHandler({'client_id': 'DUMMY', 'access_token': 'DUMMY'}, ['NSE:NIFTY50-EQ'])
handler._build_subscription_symbols()

# Let's mock fyers tick exactly as it arrives to FyersDataSocket
ws = FyersDataSocket('', write_to_file=False)

def mock_on_tick(tick):
    print("FINAL NORMALIZED TICK:", tick)

handler.on_tick_callback = mock_on_tick

# Tie ws OnMessage to handler
ws.On_message = handler._on_message

mock_data = {
    'ltp': 10000,
    'precision': 2,
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

ws._FyersDataSocket__response_output(mock_data, "scrips")
