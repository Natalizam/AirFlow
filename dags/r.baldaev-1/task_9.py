"""
Test documentation
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


def set_xcom(ti):
    return "Airflow tracks everything"


def print_xcom(ti):
    xcom_sample = ti.xcom_pull(
        key='return_value',
        task_ids='task_1_return_xcom',
    )
    print(xcom_sample)


with DAG(
    'task_9_r_baldaev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 9 - XCom by two Python operators (by return)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['r.baldaev-1'],
) as dag:
        task1 = PythonOperator(
            task_id='task_1_return_xcom',
            python_callable=set_xcom,
        )
        task2 = PythonOperator(
            task_id='print_xcom',
            python_callable=print_xcom,
        )
        task1 >> task2