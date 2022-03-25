from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_DON',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 24),
        catchup=False,
        tags=['task 1'],
) as dag:
    t1 = BashOperator(
        task_id='print_our_directory',
        bash_command='pwd'
    )

    def func(ds, **kwargs):
        print(ds)
        print('Great!')
        return

    t2 = PythonOperator(
        task_id='strange_print_function',
        python_callable=func
    )

    t1 >> t2 
