from confluent_kafka import Consumer
import json
import time

conf = {
    'bootstrap.servers': 'localhost:9092',# specify which kafka broker to connect to
    'group.id': 'my-group',  # consumers in same group share the same partition (single)
    'auto.offset.reset': 'earliest'  # read from beginning if not offset is given
}

consumer = Consumer(conf)
consumer.subscribe(['crypto-tickers'])  # producer produces to topic

message_count = 0
start_time = time.time()


def report_price(data):
    print(f"Received message: {data['symbol']} - Price: {data['price']}")

try:
    while True:
        msg = consumer.poll(1.0)  # wait up to 1 second
        if msg is None:
            continue
        if msg.error():
            print('Error:', msg.error())
            continue

        data = json.loads(msg.value().decode('utf-8'))
        message_count += 1


        if data['symbol'] == 'BTCUSDT' or data['symbol'] == 'ETHUSDT' or data['symbol'] == 'XRPUSDT':
            report_price(data)

        # if message_count % 50 == 0:
        #     print(f"{data['symbol']}: {data['price']}")
        #     print()

        # Optional: print throughput every 5 seconds
        if time.time() - start_time >= 5:
            print(f"Consumed {message_count} messages in last 5 seconds")
            message_count = 0
            start_time = time.time()

finally:
    consumer.close()
