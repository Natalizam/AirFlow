from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag01',
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
    start_date=datetime(2022, 4, 19),
    catchup=False,
    tags=['tags'],
) as dag:

    for i in range(10):
        t1 = BashOperator(task_id=f'BashOperator_{i+1}', bash_command=f"echo {i}")

    def print_2(task_number):
        print(f"task number is: {task_number}")

    for i in range(11,31):
        t2 = PythonOperator(task_id=f'PythonOperator_{i}', python_callable=print_2, op_kwargs={'task_number': i})
    
    t1 >> t2
