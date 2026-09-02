import sys
import fyers_apiv3.FyersWebsocket.data_ws as data_ws

original_response_output = data_ws.FyersDataSocket._FyersDataSocket__response_output

def patched_response_output(self, data, data_type):
    original_on_message = self.On_message

    def fake_on_message(response):
        if isinstance(data, dict) and "OI" in data:
            response["OI"] = data["OI"]
        original_on_message(response)

    self.On_message = fake_on_message
    try:
        original_response_output(self, data, data_type)
    finally:
        self.On_message = original_on_message

data_ws.FyersDataSocket._FyersDataSocket__response_output = patched_response_output
print("Patched successfully")
