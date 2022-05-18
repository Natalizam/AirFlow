from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'e-suzdaleva-6_task1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='e-suzdaleva-6_DAG_task1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['e-suzdaleva-6-task1'],
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    def print_ds(ds, **kwarg):
        print(ds)
        return None

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2
