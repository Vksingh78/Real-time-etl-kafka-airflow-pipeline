# quick_latency.py (fixed)
from kafka import KafkaProducer, KafkaConsumer
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send message
send_time = time.time()
producer.send('cdr-events', {'sent': send_time})
producer.flush()
print(f"Sent at: {send_time}")

# Wait a moment for Kafka to process
time.sleep(0.5)

# Consumer - read from earliest offset
consumer = KafkaConsumer(
    'cdr-events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000
)

print("Waiting for message...")
for msg in consumer:
    if 'sent' in msg.value:
        latency = (time.time() - msg.value['sent']) * 1000
        print(f"✅ Latency: {latency:.2f} ms")
        break
else:
    print("No message received")
    