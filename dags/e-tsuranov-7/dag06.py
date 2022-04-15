from datetime import datetime, timedelta #6
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_6_e-tsuranov-7',
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
    start_date=datetime(2022, 4, 13),
    catchup=False,
    tags=['tsuranov'],
) as dag:

    for i in range(10):
        t1 = BashOperator(task_id=f'BashOperator_{i+1}', bash_command=f"echo {i}")

    def print_2(task_number, ts, run_id):
        print(f"task number is: {task_number}. ts = {ts}. run_id = {run_id}")

    for i in range(11,31):
        t2 = PythonOperator(task_id=f'PythonOperator_{i}', python_callable=print_2, op_kwargs={'task_number': i})
    
    t1 >> t2

    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3