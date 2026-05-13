# producers/measure_latency_fixed.py
from kafka import KafkaProducer, KafkaConsumer
import time
import json
import statistics

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send fresh messages
print("Sending 100 messages with timestamps...")
send_times = []
for i in range(100):
    send_times.append(time.time())
    producer.send('cdr-events', {'id': i, 'sent': send_times[-1]})
producer.flush()
print("Messages sent. Waiting for consumer...")

# Consumer - read from latest offset only
consumer = KafkaConsumer(
    'cdr-events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    consumer_timeout_ms=10000
)

latencies = []
for msg in consumer:
    if 'sent' in msg.value:
        latency = (time.time() - msg.value['sent']) * 1000
        latencies.append(latency)
        print(f"Received {len(latencies)}: {latency:.2f} ms")
        if len(latencies) >= 100:
            break

if latencies:
    print(f"\n✅ P50 latency: {statistics.median(latencies):.2f} ms")
    print(f"✅ P95 latency: {sorted(latencies)[int(0.95*len(latencies))]:.2f} ms")
    print(f"✅ P99 latency: {sorted(latencies)[int(0.99*len(latencies))]:.2f} ms")
else:
    print("❌ No messages received. Check Kafka connection.")