from core.websocket_feed import data_ws, _patched_response_output

# Monkey patch is already applied in `core.websocket_feed` upon import
# Let's verify it actually applied successfully

ws = data_ws.FyersDataSocket('', write_to_file=False)

def mock_on_message(self, resp):
    print("MOCK:", resp)

ws.On_message = mock_on_message.__get__(ws, data_ws.FyersDataSocket)

mock_data = {
    'ltp': 10000,
    'precision': 2,
    'multiplier': 1,
    'symbol': 'NSE:NIFTY50-EQ',
    'OI': 5000,
}

ws._FyersDataSocket__response_output(mock_data, "scrips")
