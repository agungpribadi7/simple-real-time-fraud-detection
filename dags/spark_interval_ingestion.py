from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'agungpribadi',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), 
    'email': ['bjk18261910@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instantiate the DAG
with DAG(
    'fraud_medallion_pipeline',
    default_args=default_args,
    description='Orchestrates the Bronze, Silver, and Gold PySpark layers',
    schedule_interval='*/5 * * * *', 
    catchup=False,
    tags=['fraud', 'pyspark', 'medallion'],
) as dag:

    ingest_bronze = BashOperator(
        task_id='run_bronze_layer',
        bash_command='cd /app && python bronze_ingestion.py',
    )

    process_silver = BashOperator(
        task_id='run_silver_layer',
        bash_command='cd /app && python silver_processing.py',
    )

    aggregate_gold = BashOperator(
        task_id='run_gold_layer',
        bash_command='cd /app && python gold_aggregation.py',
    )

    ingest_bronze >> process_silver >> aggregate_gold