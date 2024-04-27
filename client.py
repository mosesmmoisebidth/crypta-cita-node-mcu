import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Server, Config
import asyncio
import threading

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
sio = socketio.AsyncClient(
    logger=True,
    engineio_logger=True
)

@sio.event
async def connect():
    print("CONNECTED")

@sio.event
async def disconnect():
    print("DISCONNECTED")

@sio.event
async def data(data):
    print("the received data is: {}".format(data))

async def handle_connection():
    try:
        await sio.connect("http://127.0.0.1:8001", namespaces=["/"])
        await sio.wait()
    except Exception as e:
        print("An Exception occured: {}".format(e))

@app.on_event("startup")
async def handle_startup():
    print("SERVER UP AND RUNNING")
    def asyncio_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_connection())
        loop.close()
    loop_thread = threading.Thread(target=asyncio_loop, args=())
    loop_thread.daemon = True
    loop_thread.start()

@app.on_event("shutdown")
async def handle_shutdown():
    print("SERVER STOPPED RUNNING")


if __name__ == '__main__':
    config = Config(app, host="127.0.0.1", port=8002, reload=True)
    server = Server(config=config)
    server.run()

