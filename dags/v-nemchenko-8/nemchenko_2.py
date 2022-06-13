from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG("2_nemchenko",
    schedule_interval='@daily',
    default_args=default_args,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1),
    tags=['2_nemchenko']) as dag:

    dummy = DummyOperator(task_id="dummy")

    bash_pwd = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd',
        dag=dag
)
    def print_date(ds,**kwargs):
        logging.info(f'{ds}')

    python_print_date = PythonOperator(
        task_id='python_print_date',
        python_callable=print_date,
        dag=dag,
        op_kwargs={'ds': '{{ds}}'}
    )

    dummy >> bash_pwd >> python_print_date