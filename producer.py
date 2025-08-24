from confluent_kafka import Producer
import requests
import json
import time

conf = {'bootstrap.servers': 'localhost:9092'}  # connect to kafka broker
producer = Producer(conf)

message_count = 0
start_time = time.time()

def delivery_report(err, msg):
    global message_count, start_time
    message_count += 1
    if err:
        print(f"Message failed delivery: {err}")
    # Every 5 seconds, print how many messages delivered
    if time.time() - start_time >= 5:
        print(f"Delivered {message_count} messages in last 5 seconds")
        message_count = 0
        start_time = time.time()


KAFKA_TOPIC = 'crypto-tickers'
URL = "https://api.binance.us/api/v3/ticker/price"

def fetch_tickers():  # tickers == BTC, ETH, etc
    try:
        response = requests.get(URL, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []
    
def product_tickers():
    # continuously fetch and produce tickers
    while True: 
        tickers = fetch_tickers()
        for ticker in tickers:
            message = json.dumps(ticker)
            producer.produce(
                KAFKA_TOPIC,
                key=ticker['symbol'],
                value=message,
                callback=delivery_report
            )
        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    product_tickers()
    producer.flush()