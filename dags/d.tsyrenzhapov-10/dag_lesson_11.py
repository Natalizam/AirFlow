from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.model import Variable

def val():
    is_startml = Variable.get('is_startml')
    print(is_startml)
with DAG(
        'task_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Task 9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 28),
        catchup=False,
        tags=['Dugar']

) as dag:
    t1 = PythonOperator(
        task_id='variables',
        python_callable=val
    )