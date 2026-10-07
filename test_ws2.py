import logging
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

class Dummy:
    pass

data_ws = Dummy()
data_ws.FyersDataSocket = FyersDataSocket

# Our patched response output
def _patched_response_output(self, data: str, data_type: str) -> object:
    self.On_message({"some": "data"})

data_ws.FyersDataSocket._FyersDataSocket__response_output = _patched_response_output

ws = FyersDataSocket('', write_to_file=False)
ws._FyersDataSocket__response_output({}, "scrips")
