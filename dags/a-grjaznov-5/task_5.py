import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_5_grjaznov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='task_5_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['hw_5_a-grjaznov-5'],
) as dag:
    for i in range(30):
        if i < 10:
            os.environ['NUMBER'] = str(i)
            t1 = BashOperator(
                task_id='task_number' +str(i),
                bash_command= bash_command="echo $NUMBER"
            )
        else:
            def func(w):
                print(f"task # {w}")
            t2 = PythonOperator(
                task_id='task_number' + str(i),
                python_callable=func,
                op_kwargs = {'w' : i}
            )
    t1>>t2