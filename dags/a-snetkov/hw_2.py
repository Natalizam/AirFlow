from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_ds(ds):
    print(f'This is ds: {ds}')


with DAG(
    'hw_2_a-snetkov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['At']
) as dag:
    t1 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )

    t2 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )
    t2 >> t1
