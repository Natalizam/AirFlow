
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_6_v_mit',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_6_v_mit',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 26),
    catchup=False,
    tags=['hw_6_v_mit'],
    ) as dag:
    
    def print_task_info(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(f"ts: {ts}, run_id: {run_id}")
    
    for i in range(1, 31):
        task = PythonOperator(task_id='hw_6_v_mit_task_' + str(i), 
                              python_callable=print_task_info,
                              op_kwargs={'task_number': int(i)})