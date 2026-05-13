from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

# Simple function jo run hoga jab data aayega
def process_data():
    print("=" * 50)
    print("NEW DATA DETECTED! ETL Pipeline Triggered")
    print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
    print("Processing transaction records...")
    print("ETL Pipeline Completed Successfully")
    print("=" * 50)
    return "Success"

with DAG(
    'event_driven_etl_demo',
    default_args={
        'owner': 'vivek',
        'start_date': datetime(2024, 1, 1),
    },
    schedule=None,  # IMPORTANT: No cron schedule!
    catchup=False,
    description='Event-driven ETL - triggers when data arrives'
) as dag:
    
    # Task 1: Process data
    process_task = PythonOperator(
        task_id='process_etl_data',
        python_callable=process_data
    )
    
    # Task 2: Complete
    complete_task = BashOperator(
        task_id='complete',
        bash_command='echo "ETL Pipeline Completed at $(date)"'
    )
    
    process_task >> complete_task 