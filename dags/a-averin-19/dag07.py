from datetime import datetime, timedelta #6
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag07',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='VAR in Python operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 20),
    catchup=False,
    tags=['a-averin-19'],
) as dag:

    def print_2(task_number, ts, run_id):
        print(f"task number is: {task_number}. ts = {ts}. run_id = {run_id}")

    for i in range(11,31):
        t2 = PythonOperator(task_id=f'PythonOperator_{i}', python_callable=print_2, op_kwargs={'task_number': i})
    
    t2