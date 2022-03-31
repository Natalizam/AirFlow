"""
 Airflow trials

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

    

with DAG(
's-filonov-6_hw6',
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
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:

    def print_args(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print("task nr: {kwargs['task_number']}") 

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='echo_' + str(i),
                bash_command="echo $NUMBER",
                env={"NUMBER": str(i)},
            )
        else:
            t2 = PythonOperator(
            python_callable=print_args,
            op_kwargs={'task_number': i}
          )