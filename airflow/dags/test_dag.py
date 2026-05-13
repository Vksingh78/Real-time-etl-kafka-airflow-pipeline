from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.assets import Asset
from datetime import datetime

# Asset definition - ye Kafka topic ko detect karega
kafka_asset = Asset(
    "kafka://localhost:9092/etl-transactions"
)

with DAG(
    'event_driven_etl',
    schedule=None,  # No cron schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    assets=[kafka_asset]  # Ye asset trigger karega DAG ko
) as dag:
    
    process_task = BashOperator(
        task_id='process_data',
        bash_command='echo "New data arrived! Processing ETL pipeline..."'
    )
    
    complete_task = BashOperator(
        task_id='complete',
        bash_command='echo "ETL pipeline completed successfully"'
    )
    
    process_task >> complete_task