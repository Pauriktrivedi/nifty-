import logging
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

def patched(self, data: str, data_type: str) -> object:
    print(f"Patched method called! OI in data: {data.get('OI')}")
    # Call original method just in case? No, we replace it.
    pass

FyersDataSocket._FyersDataSocket__response_output = patched

ws = FyersDataSocket('', write_to_file=False)

ws._FyersDataSocket__response_output({'OI': 100}, 'scrips')
