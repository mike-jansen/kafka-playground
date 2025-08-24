from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',# specify which kafka broker to connect to
    'group.id': 'my-group',  # consumers in same group share the same partition (single)
    'auto.offset.reset': 'earliest'  # read from beginning if not offset is given
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])  # producer produces to 'test-topic'

try:
    while True:
        msg = consumer.poll(0.0)  # wait up to 1 second
        if msg is None:
            continue
        if msg.error():
            print('Error:', msg.error())
            continue
        print('Received:', msg.value().decode('utf-8'))
finally:
    consumer.close()
