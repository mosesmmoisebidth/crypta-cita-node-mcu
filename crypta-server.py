from fastapi import FastAPI, BackgroundTasks
from datetime import datetime
import csv
import asyncio
import threading
from uvicorn import Config, Server
import paho.mqtt.client as mqtt

app = FastAPI()
csv_file_name = 'record.csv'
mqtt_data = []

# Function to format the data
def format_data(location, value):
    now = datetime.now()
    datetime_str = now.strftime("%Y-%m-%d %H:%M:%S")
    parts = datetime_str.split()
    return {"location": location, "date": parts[0], "time": parts[1], "value": value}

# Function to save data to CSV
def save_to_csv(data):
    with open(csv_file_name, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)

# MQTT connection setup
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("water_sensor")

def on_message(client, userdata, msg):
    data = msg.payload.decode()
    parts = data.split()
    if len(parts) >= 2:
        formatted_data = format_data(parts[0], float(parts[1]))
        print("Formatted data is: {}".format(formatted_data))
        save_to_csv([formatted_data["location"], formatted_data["value"]])
        print(f"Data saved to CSV: {formatted_data}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Asynchronous function to initiate MQTT connection
async def initiate():
    client.connect("82.165.97.169", 1883, 60)
    client.loop_start()

@app.on_event("startup")
async def startup_event():
    def asyncio_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(initiate())
        loop.close()
    thread = threading.Thread(target=asyncio_loop, args=())
    thread.daemon = True
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    print("FastAPI server and MQTT client are shutting down")

# Background task to continuously send data to the /data endpoint
def send_data(background_tasks: BackgroundTasks):
    global mqtt_data
    batch_size = 15
    while True:
        with open(csv_file_name, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                mqtt_data.append(float(row[1]))  # Assuming value is at index 1
                if len(mqtt_data) == batch_size:
                    average_value = sum(mqtt_data) / len(mqtt_data)
                    location = row[0]  # Assuming location is at index 0
                    mqtt_data = []
                    data = {"location": location, "average_water_level": average_value}
                    background_tasks.add_task(send_data_task, data)
            if mqtt_data:  # If there's remaining data less than batch size
                average_value = sum(mqtt_data) / len(mqtt_data)
                location = row[0]  # Assuming location is at index 0
                mqtt_data = []
                data = {"location": location, "average_water_level": average_value}
                background_tasks.add_task(send_data_task, data)

# Background task to send data to the /data endpoint
async def send_data_task(data):
    await asyncio.sleep(10)  # Wait for 10 seconds before sending data
    print("Sending data:", data)  # Replace this with your code to send data to the /data endpoint

# Root endpoint
@app.get("/")
async def root():
    return {"message": "FastAPI server is running"}

# Run the FastAPI server
if __name__ == "__main__":
    config = Config(app, host="127.0.0.1", port=8001, reload=True)
    server = Server(config=config)
    server.run()
