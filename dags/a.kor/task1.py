from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds):
    print(ds)


with DAG(
    'task1_a.kor',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 1 - print ds by Python operator and pwd by Bash operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a.kor_1'],
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )
    t1 >> t2
