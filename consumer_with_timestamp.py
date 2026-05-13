#!/usr/bin/env python3

from confluent_kafka import Consumer, KafkaError
import json
import time
from collections import deque

# Kafka configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'etl-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'max.poll.records': 500
}

consumer = Consumer(config)
consumer.subscribe(['etl-transactions'])

latencies = deque(maxlen=10000)
received_count = 0
start_time = time.time()

def calculate_percentiles(latencies_list):
    if not latencies_list:
        return 0, 0, 0
    sorted_list = sorted(latencies_list)
    length = len(sorted_list)
    p50 = sorted_list[int(length * 0.5)]
    p95 = sorted_list[int(length * 0.95)]
    p99 = sorted_list[int(length * 0.99)]
    return p50, p95, p99

print("Consumer started. Waiting for messages...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break
        
        # Parse message
        try:
            record = json.loads(msg.value().decode('utf-8'))
            
            # Get sent time from producer
            sent_time = record.get('timestamp')
            current_time = int(time.time() * 1000)
            
            # Calculate latency
            if sent_time:
                latency = current_time - sent_time
                latencies.append(latency)
            
            received_count += 1
            
            # Print every 1000 records
            if received_count % 1000 == 0:
                elapsed = time.time() - start_time
                rate = received_count / elapsed
                p50, p95, p99 = calculate_percentiles(latencies)
                
                print(f"\n[STATS] Records: {received_count}")
                print(f"[STATS] Rate: {rate:.0f} RPS")
                print(f"[STATS] P50: {p50} ms | P95: {p95} ms | P99: {p99} ms")
                
        except Exception as e:
            print(f"Error parsing message: {e}")

except KeyboardInterrupt:
    print(f"\nConsumer stopped.")
    print(f"Total received: {received_count}")
    
    if latencies:
        p50, p95, p99 = calculate_percentiles(latencies)
        print(f"\nFINAL LATENCY STATISTICS:")
        print(f"P50: {p50} ms")
        print(f"P95: {p95} ms")
        print(f"P99: {p99} ms")

finally:
    consumer.close()   