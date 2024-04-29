from fastapi import FastAPI
from fastapi.responses import JSONResponse
from uvicorn import Config, Server
import socketio
import paho.mqtt.client as mqtt
from fastapi.middleware.cors import CORSMiddleware
import datetime
import asyncio
import threading
import json

start_time = datetime.datetime.now()
global_event_loop = None
app = FastAPI()
sio = socketio.AsyncServer(
    async_mode='asgi',
    logger=True,
    cors_allowed_origins=[],
    engineio_logger=True
)

app = FastAPI(
    title="Crypta-Vita",
    version="2.15",
    description="Data Streaming",
    openapi_url="/openapi.json",
    docs_url="/swagger",
    redoc_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
sio_app = socketio.ASGIApp(
    socketio_server=sio,
    socketio_path='/socket.io'
)
app.mount("/socket.io", app=sio_app)
@sio.event
async def connect(sid, environ, auth):
    print("Client connected with id: {}".format(sid))
@sio.event
async def disconnect(sid):
    print("Client diconnected with id: {}".format(sid))
async def async_emit(event, data):
    await sio.emit(event, data)
def save_data_to_file(file_path, formatted_data):
    with open(file_path, "a") as file:
        file.write(json.dumps(formatted_data) + "\n")

def asyncio_loop():
    global global_event_loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    global_event_loop = loop
    loop.run_until_complete(initiate_connection())
    loop.run_forever()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("water_sensor")

def on_message(client, userdata, msg):
    global start_time
    data = msg.payload.decode()
    parts = data.split()
    data_points = []
    try:
        if len(parts) >= 2:
            location = parts[0]
            value = parts[1]
            data_points.append(value)
            saving_data = {"location":location, "timestamp":datetime.datetime.now().isoformat(), "water_level":value}
            print("the data that is going to be saved is: {}".format(saving_data))
            asyncio.run_coroutine_threadsafe(async_emit('data', saving_data), global_event_loop)
            save_data_to_file("received.txt", saving_data)
        if int((datetime.datetime.now() - start_time).total_seconds()) >= 360:
            if len(parts) >= 2:
                current_location = parts[0]
                current_value = parts[1]
                average_water_level = sum(int(value) for value in data_points) / len(data_points)
                data_points = []
                data_points.append(current_value)
                total_duration = int((datetime.datetime.now() - start_time).total_seconds())
                start_time = datetime.datetime.now()
                data_to_send = {"location": current_location, "timestamp":datetime.datetime.now().isoformat(), "average_water_level": average_water_level, "current_water_level": current_value, "total_submission_duration": total_duration}
                print("the data after 15 seconds is: {}".format(data_to_send))
                asyncio.run_coroutine_threadsafe(async_emit('data', data_to_send), global_event_loop)
                save_data_to_file("received.txt", data_to_send)
    except Exception as e:
        print("An Exception occured as: {}".format(e))

async def initiate_connection():
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect("82.165.97.169", 1883, 60)
        client.loop_start()
    except Exception as e:
        print(f"An Error occured {e}")
@app.get("/", tags=["Default"])
async def handle_default():
    return JSONResponse(status_code=200, content={"message": "Real Time Sensor Data"})

@app.on_event("startup")
async def handle_startup():
    print("SERVER STARTED RUNNING")
    loop_thread = threading.Thread(target=asyncio_loop, args=())
    loop_thread.daemon = True
    loop_thread.start()
@app.on_event("shutdown")
async def handle_shutdown():
    print("SERVER STOPPED RUNNING")

if __name__ == '__main__':
    config = Config(app, host="127.0.0.1", port=8001, reload=True)
    server = Server(config=config)
    server.run()