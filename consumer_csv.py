from kafka import KafkaConsumer
import json
import time
from collections import deque

# Create consumer with explicit api_version
consumer = KafkaConsumer(
    'telecom-transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    max_poll_records=500,
    api_version=(7, 5, 0)  # Add this line - matches your Kafka version
)

latencies = deque(maxlen=10000)
received_count = 0
churn_true_count = 0
churn_false_count = 0
start_time = time.time()

print("Starting CSV consumer...")
print("Listening to topic: telecom-transactions")
print("-" * 50)

try:
    for msg in consumer:
        current_time = int(time.time() * 1000)
        sent_time = msg.value.get('timestamp')
        churn_status = msg.value.get('churn')
        
        if sent_time:
            latency = current_time - sent_time
            latencies.append(latency)
        
        if churn_status is True:
            churn_true_count += 1
        elif churn_status is False:
            churn_false_count += 1
        
        received_count += 1
        
        if received_count % 1000 == 0:
            elapsed = time.time() - start_time
            rate = received_count / elapsed
            
            if latencies:
                sorted_lat = sorted(latencies)
                length = len(sorted_lat)
                p50 = sorted_lat[int(length * 0.5)]
                p95 = sorted_lat[int(length * 0.95)]
                p99 = sorted_lat[int(length * 0.99)]
                
                print(f"\nReceived {received_count} records")
                print(f"Rate: {rate:.0f} RPS")
                print(f"P50: {p50} ms | P95: {p95} ms | P99: {p99} ms")
                print(f"Churn (True): {churn_true_count} | Churn (False): {churn_false_count}")
                print("-" * 40)

except KeyboardInterrupt:
    print(f"\nTotal received: {received_count}")
    elapsed = time.time() - start_time
    print(f"Average rate: {received_count/elapsed:.0f} RPS")
    print(f"Churn (True): {churn_true_count} | Churn (False): {churn_false_count}")
    if latencies:
        sorted_lat = sorted(latencies)
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        print(f"Final P99 latency: {p99} ms")
    consumer.close()