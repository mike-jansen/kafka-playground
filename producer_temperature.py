from kafka_utils import create_producer, delivery_report
import json
import time
import random

producer = create_producer()
topic = 'temperature'
callback = delivery_report(topic.capitalize())

def generate_temperature():
    temp = random.uniform(15, 35)
    return round(temp, 2)

while True:
    value = json.dumps({'value': generate_temperature()})
    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
