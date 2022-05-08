from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number):
    print(f"task number is: {task_number}")


with DAG(
    'task_3_a.kor',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 1 - print ds by Python operator and pwd by Bash operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a.kor_3'],
) as dag:
    curr_task = None
    for i in range(10):
        task = BashOperator(
            task_id=f'echo_task_{i}',
            bash_command=f'echo {i}',
        )
        task.doc_md = dedent(
            f"""
                # Task {i} Documentation
                **This task print own number:**
                `echo {i}`
                *by echo bash command*
            """
        )
        if curr_task:
            curr_task >> task
        curr_task = task
    for i in range(10, 30):
        task = PythonOperator(
            task_id=f'print_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        task.doc_md = dedent(
            f"""
                # Task {i} Documentation
                **This task print own number:**
                `def print_task_number(task_number):`
                    `print(f"task number is: {i}")`
                *by python print command*
            """
        )
        curr_task >> task
        curr_task = task
