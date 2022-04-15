"""
Данный DAG выполняет 30 команд
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


#DAG
with  DAG(
    'hw_2_j-sapriko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'HW-step-6',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 21),
    catchup = False,
    tags = ['hw_6'],
) as dag:

    dag.doc_md = __doc__


    def print_func(ts, run_id, **kwargs):
        print(f"task number is: {kwargs}")
        print(ts)
        print(run_id)


    for i in range(1,30+1):
        if i <= 10:
            task_bash = BashOperator(
                task_id="echo_" + str(i),
                bash_command=f"echo {i}",
            )
        else:
            task_pyth = PythonOperator(
                task_id='task_' + str(i),
                python_callable=print_func,
                op_kwargs={'task_number': i},
            )
