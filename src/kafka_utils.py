from confluent_kafka import Producer
import time

def create_producer(bootstrap_servers='localhost:9092'):
    return Producer({'bootstrap.servers': bootstrap_servers})

def delivery_report(subject):  # subject basically topic (temp, humidity, etc)
    start_time = time.time()
    message_count = 0

    def callback(err, msg):
        nonlocal message_count, start_time
        message_count += 1
        if err:
            print(f"[{subject}] Message failed delivery: {err}")
        if time.time() - start_time >= 5:
            print(f"[{subject}] Delivered {message_count} messages in last 5 seconds")
            message_count = 0
            start_time = time.time()
            
    return callback