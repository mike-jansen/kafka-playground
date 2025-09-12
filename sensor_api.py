from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import threading
import random
import time


sensor_data = {
    "temperature": 25.0,  # celsius
    "humidity": 50.0,     # percent
    "pressure": 1013.25   # hPa
}

def update_sensor_data():
    while True:
        sensor_data["temperature"] += random.uniform(-0.25, 0.25)
        sensor_data["humidity"] += random.uniform(-0.5, 0.5)
        sensor_data["pressure"] += random.uniform(-0.1, 0.1)

        sensor_data["temperature"] = max(min(sensor_data["temperature"], 50), -10)
        sensor_data["humidity"] = max(min(sensor_data["humidity"], 100), 0)
        sensor_data["pressure"] = max(min(sensor_data["pressure"], 1050), 950)

        time.sleep(0.3)

app = FastAPI()

@app.get("/sensor")  # decorator tells API to call this function if user makes GET request to /sensor
def get_sensor_data(sensor: str):
    if sensor:
        sensor = sensor.lower()
        if sensor in sensor_data:
            return JSONResponse(content={sensor: sensor_data[sensor]})
        else:
            return JSONResponse(content={"error": "Invalid sensor name"}, status_code=400)
    return JSONResponse(content=sensor_data)  # return all data if no type was specified
    
if __name__ == "__main__":
    thread = threading.Thread(target=update_sensor_data, daemon=True)
    thread.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)  # run server to host the API application