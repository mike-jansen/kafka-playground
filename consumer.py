from confluent_kafka import Consumer
import json
import time

conf = {
    'bootstrap.servers': 'localhost:9092',  # specify which kafka broker to connect to
    'group.id': 'sensor_group',  # consumers in same group share the same partition (single)
    'auto.offset.reset': 'earliest'  # read from beginning if not offset is given
}
topics = ['temperature', 'humidity', 'pressure']

consumer = Consumer(conf)
consumer.subscribe(topics)
print("Consumer is running, subscribed to topics:", topics)

try:
    while True:
        msg = consumer.poll(1.0)  # wait up to 1 second
        if msg is None:
            continue
        if msg.error():
            print('Consumer error:', msg.error())
            continue
        
        print(f"Received message from topic {msg.topic()}: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("Consumer stopped by user") 
finally:
    consumer.close()
