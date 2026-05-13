from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import json

def extract_from_kafka():
    consumer = KafkaConsumer(
        'cdr-topic',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []
    for i, msg in enumerate(consumer):
        messages.append(msg.value)
        if i >= 20:   # limit for test
            break

    print("Extracted messages:", messages)
    return messages


def transform(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    transformed = [{"id": d["id"], "number": d["number"], "processed": True} for d in data]
    print("Transformed:", transformed)
    return transformed


def load(**context):
    data = context['ti'].xcom_pull(task_ids='transform')
    print("Final Load Data:", data)
    # later: insert into postgres


with DAG(
    dag_id='cdr_etl_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_from_kafka
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract >> transform_task >> load_task