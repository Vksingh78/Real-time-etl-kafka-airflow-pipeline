from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import socket
import requests

default_args = {
    'owner': 'vivek',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def check_kafka():
    """Check if Kafka broker is reachable"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result == 0:
            print("Kafka is running")
            return True
        else:
            print("Kafka is not responding")
            raise Exception("Kafka connection failed")
    except Exception as e:
        print(f"Kafka check failed: {e}")
        raise

def check_consumer():
    """Check if consumer is running"""
    try:
        response = requests.get('http://localhost:8080/api/cluster', timeout=5)
        if response.status_code == 200:
            print("Kafka UI is accessible")
            return True
        else:
            print(f"Kafka UI returned {response.status_code}")
            raise Exception("Kafka UI check failed")
    except Exception as e:
        print(f"Consumer check failed: {e}")
        raise

def send_alert():
    """Send alert (print for demo, can be Slack/email)"""
    print("ALERT: Pipeline issue detected!")
    print("Please check Kafka and consumer status.")
    # In production, add Slack webhook or email here

with DAG(
    'etl_health_check',
    default_args=default_args,
    description='Health check for ETL pipeline',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False
) as dag:
    
    task_check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka
    )
    
    task_check_consumer = PythonOperator(
        task_id='check_consumer',
        python_callable=check_consumer
    )
    
    task_send_alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
        trigger_rule='one_failed'
    )
    
    task_check_kafka >> task_check_consumer
    task_check_kafka >> task_send_alert
    task_check_consumer >> task_send_alert