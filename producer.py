from kafka import KafkaProducer
import json
import time
import random
import string

# Create producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=65536,
    linger_ms=10
)

topic = 'etl-transactions'
sent_count = 0
start_time = time.time()

def generate_transaction():
    transaction = {
        'transaction_id': ''.join(random.choices(string.digits, k=10)),
        'customer_id': ''.join(random.choices(string.digits, k=8)),
        'amount': round(random.uniform(10, 5000), 2),
        'merchant': random.choice(['Amazon', 'Flipkart', 'Swiggy', 'Zomato', 'Uber']),
        'timestamp': int(time.time() * 1000)
    }
    return transaction

print("Starting producer...")
print(f"Sending to topic: {topic}")
print("-" * 50)

try:
    while True:
        transaction = generate_transaction()
        producer.send(topic, transaction)
        sent_count += 1
        
        if sent_count % 1000 == 0:
            elapsed = time.time() - start_time
            rate = sent_count / elapsed
            print(f"Sent {sent_count} records in {elapsed:.2f} seconds")
            print(f"Current rate: {rate:.0f} RPS")
            print("-" * 30)
        
        # kafka-python doesn't need poll(), it sends automatically
        
except KeyboardInterrupt:
    print(f"\nProducer stopped.")
    print(f"Total sent: {sent_count}")
    elapsed = time.time() - start_time
    print(f"Average rate: {sent_count/elapsed:.0f} RPS")

finally:
    producer.flush()
    producer.close()