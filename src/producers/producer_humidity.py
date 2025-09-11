from src.kafka_utils import create_producer, delivery_report
import json
import time
import random

producer = create_producer()
topic = 'humidity'
callback = delivery_report(topic.capitalize())

# simulate humidity readings
current_value = random.uniform(30, 70)
def generate_humidity():
    global current_value

    step = random.uniform(-0.5, 0.5)
    current_value += step
    current_value = max(30, min(70, current_value))  # keep within bounds

    # simulate bad data
    if random.random() < 0.05:
        outlier = random.choice([random.uniform(0, 10), random.uniform(90, 100)])
        return round(outlier, 2)

    return round(current_value, 2)

while True:
    value = json.dumps({'value': generate_humidity()})
    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
