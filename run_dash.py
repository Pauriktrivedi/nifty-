import uvicorn
from main import app
import threading
import time

def run():
    uvicorn.run(app, host="127.0.0.1", port=8000)

t = threading.Thread(target=run, daemon=True)
t.start()
time.sleep(5)
