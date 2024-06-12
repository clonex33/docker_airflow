from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import os

default_args = {
    'owner': 'mukmin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with DAG(
    'first_dag',
    default_args=default_args,
    description='my first dag',
    start_date=datetime(2024, 6, 11, 6),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='print_current_directory',
        bash_command=f'echo Current directory is: {CUR_DIR}'
    )

    task2 = BashOperator(
        task_id='list_directory_contents',
        bash_command=f'ls {CUR_DIR}'
    )

    task1 >> task2
