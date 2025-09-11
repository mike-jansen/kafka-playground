from src.kafka_utils import create_producer, delivery_report
import json
import time
import random

producer = create_producer()
topic = 'pressure'
callback = delivery_report(topic.capitalize())

# simulate pressure readings
current_value = random.uniform(1000, 1030)
def generate_pressure():
    global current_value

    step = random.uniform(-0.1, 0.1)
    current_value += step
    current_value = max(1000, min(1030, current_value))  # keep within bounds

    # simulate bad data
    if random.random() < 0.05:
        outlier = random.choice([random.uniform(800, 850), random.uniform(1100, 1200)])
        return round(outlier, 2)

    return round(current_value, 2)

while True:
    value = json.dumps({'value': generate_pressure()})
    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
