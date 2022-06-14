from textwrap import dedent
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator

def print_context(ds):
    print ds

with DAG(
    'second task',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description = 'my second task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 14),
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )
    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
    )
    t1 >> t2