from confluent_kafka import Producer
import requests
import json

conf = {'bootstrap.servers': 'localhost:9092'}  # connect to kafka broker
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print('Message failed delivery:', err)
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

SYMBOL = 'BTCUSDT'  # bitcoin
URL = f'https://api.binance.us/api/v3/ticker/price?symbol={SYMBOL}'  # binance us public api endpoint

response = requests.get(URL)
print(response.status_code, response.text)
data = response.json()

price = data['price']
message = json.dumps({
    'symbol': SYMBOL,
    'price': price
})

producer.produce('test-topic', key='binance', value=message, callback=delivery_report)
producer.flush()
