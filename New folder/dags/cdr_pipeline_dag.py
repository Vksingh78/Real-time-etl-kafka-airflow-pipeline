from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

def check_kafka_health():
    try:
        admin = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='airflow_health_check'
        )
        topics = admin.list_topics()
        if 'cdr-events' in topics:
            print("✅ Kafka is healthy. Topic 'cdr-events' exists.")
        else:
            print("⚠️ Topic 'cdr-events' not found. Creating...")
            try:
                admin.create_topics([NewTopic(name='cdr-events', num_partitions=3, replication_factor=1)])
                print("✅ Topic created.")
            except Exception as e:
                print(f"Error creating topic: {e}")
        admin.close()
        return True
    except NoBrokersAvailable:
        print("❌ No Kafka brokers available. Check connectivity.")
        return False
    except Exception as e:
        print(f"❌ Kafka health check failed: {e}")
        return False

default_args = {
    'owner': 'me',
    'start_date': datetime(2024, 1, 1),
}

with DAG('cdr_health',
         schedule_interval='*/5 * * * *',
         catchup=False,
         default_args=default_args) as dag:

    check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka_health
    )