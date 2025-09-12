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

        time.sleep(0.5)
