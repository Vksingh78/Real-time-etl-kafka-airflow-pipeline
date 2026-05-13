from kafka import KafkaProducer
import json
import time
import csv

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=65536,
    linger_ms=10,
    api_version=(7, 5, 0)
)

topic = 'telecom-transactions'
csv_file = 'telecom_churn.csv'

sent_count = 0
start_time = time.time()

print("Starting CSV producer...")
print(f"Reading from: {csv_file}")
print(f"Sending to topic: {topic}")
print("-" * 50)

try:
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            try:
                row['account length'] = int(row['account length'])
                row['area code'] = int(row['area code'])
                row['number vmail messages'] = int(row['number vmail messages'])
                row['total day minutes'] = float(row['total day minutes'])
                row['total day calls'] = int(row['total day calls'])
                row['total day charge'] = float(row['total day charge'])
                row['total eve minutes'] = float(row['total eve minutes'])
                row['total eve calls'] = int(row['total eve calls'])
                row['total eve charge'] = float(row['total eve charge'])
                row['total night minutes'] = float(row['total night minutes'])
                row['total night calls'] = int(row['total night calls'])
                row['total night charge'] = float(row['total night charge'])
                row['total intl minutes'] = float(row['total intl minutes'])
                row['total intl calls'] = int(row['total intl calls'])
                row['total intl charge'] = float(row['total intl charge'])
                row['customer service calls'] = int(row['customer service calls'])
                row['churn'] = row['churn'] == 'True'
            except:
                pass
            
            row['timestamp'] = int(time.time() * 1000)
            
            producer.send(topic, row)
            sent_count += 1
            
            if sent_count % 1000 == 0:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed
                print(f"Sent {sent_count} records in {elapsed:.2f} seconds")
                print(f"Current rate: {rate:.0f} RPS")
                print("-" * 30)
    
    print(f"\nCSV file completed!")
    print(f"Total records sent: {sent_count}")

except FileNotFoundError:
    print(f"Error: {csv_file} not found!")
    print("Make sure telecom_churn.csv is in the same folder")

finally:
    producer.flush()
    producer.close()