from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'G-Ivanov-task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='First task in the lesson on Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=f"echo {i}",
            dag=dag,
        )

    def print_task_num(task_number, **kwargs):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'task_num_{i}',
            python_callable=print_task_num,
            op_kwargs={"task_number": i},
        )

    t1 >> t2