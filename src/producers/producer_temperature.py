from src.kafka_utils import create_producer, delivery_report
import json
import time
import random

producer = create_producer()
topic = 'temperature'
callback = delivery_report(topic.capitalize())

# simulate temperature readings
current_value = random.uniform(15, 35)
def generate_temperature():
    global current_value

    step = random.uniform(-0.25, 0.25)
    current_value += step
    current_value = max(15, min(735, current_value))  # keep within bounds

    # simulate bad data
    if random.random() < 0.05:
        outlier = random.choice([random.uniform(-5, 5), random.uniform(50, 60)])
        return round(outlier, 2)

    return round(current_value, 2)

while True:
    value = json.dumps({'value': generate_temperature()})
    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
