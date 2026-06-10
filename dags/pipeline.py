from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'DustiniaDelixia Analyst',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'DustiniaDelixia_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_dataset = BashOperator(
        task_id='extract_dataset',
        bash_command='python /opt/airflow/dags/scripts/extract_dataset.py'
    )

    process_dataset = BashOperator(
        task_id='process_dataset',
        bash_command='python /opt/airflow/dags/scripts/process_dataset.py'
    )

    extract_dataset >> process_dataset