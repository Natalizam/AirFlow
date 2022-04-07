from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_1_k.alekseev-9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='T1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['T1'],
) as dag:

    def print_ds(ds):
        print(ds)

    t1 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t2 = BashOperator(
        task_id='show_pwd',
        bash_command='pwd',
    )

    t2 >> t1