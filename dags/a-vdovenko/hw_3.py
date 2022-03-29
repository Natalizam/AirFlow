from datetime import timedelta, datetime
from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'hw_3_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
},
    description="Lesson 11 home work 3",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022,1,1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    for i in range(31):
        if i <= 10:
            task = BashOperator(
                task_id = f'print_{i}',
                bash_command = f'echo {i}',
            )
            task.doc_md = dedent("""
            # Task Documentation 
            **выполнит код** `"echo {i}"`, где `i` *номер итерации* 
            используя BashOperator
            """)

        else:
            task = PythonOperator(
                task_id = f'print_task_{i}',
                python_callable = print_task_number,
                op_kwargs = {'task_number': i}
            )
            task.doc_md = dedent("""
            # Task_Documentation 
            **напечатает** `"task number is: {task_number}"`, где `i` *номер итерации*
            используя  PythonOperator
            """)
    task