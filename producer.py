from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}  # specify which kafka broker to connect to
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print('Message failed delivery:', err)
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# produce 5 messages to 'test-topic'
for i in range(5):
    producer.produce('test-topic', key=str(i), value=f'Message {i}', callback=delivery_report)

producer.flush()
