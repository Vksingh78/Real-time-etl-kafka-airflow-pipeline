from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending data...\n")

for i in range(10):
    data = {
        "id": i,
        "number": random.randint(1000, 9999)
    }
    producer.send('cdr-topic', data)
    print("Sent:", data)

producer.flush()
print("\n✅ Done")