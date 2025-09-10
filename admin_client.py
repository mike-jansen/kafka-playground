from confluent_kafka.admin import AdminClient, NewTopic
import time

admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
topics = ['temperature', 'humidity', 'pressure']
new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]

for _ in range(10):  # retry 10 times
    try:
        fs = admin_client.create_topics(new_topics, request_timeout=15)
        for topic, f in fs.items():
            try:
                f.result() 
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create {topic}: {e}")
        break
    except Exception as e:
        print(f"AdminClient error, retrying in 2s: {e}")
        time.sleep(2)
