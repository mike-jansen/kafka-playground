from kafka_utils import create_producer, delivery_report
import json
import time
import random

producer = create_producer()
topic = 'pressure'
callback = delivery_report(topic.capitalize())

def generate_pressure():
    temp = random.uniform(1000, 1030)
    return round(temp, 2)

while True:
    value = json.dumps({'value': generate_pressure()})
    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
