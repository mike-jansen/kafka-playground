from src.kafka_utils import create_producer, delivery_report
import time
import requests

producer = create_producer()
topic = 'humidity'
callback = delivery_report(topic.capitalize())

while True:
    response = requests.get(f"http://127.0.0.1:8000/sensor?sensor={topic}")
    value = str(response.json().get(topic, None))

    producer.produce(topic, value=value, callback=callback)
    producer.poll(0)
    time.sleep(1)
