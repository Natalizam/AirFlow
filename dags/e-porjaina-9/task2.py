from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hw_2_e-porjaina',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw2'],
) as dag:
    date = "{{ ds }}"
    t1 = BashOperator(
    task_id="test_pwd",
    bash_command="pwd"
    )

    def print_ds(ds):
        print(ds)
        return ds

    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_ds
    )
t1 >> t2