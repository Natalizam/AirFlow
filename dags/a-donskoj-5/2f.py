rom datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_a-donskoj-5',
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
        start_date=datetime(2022, 3, 24),
        catchup=False,
        tags=['task 2'],
) as dag:

    def func(num):
        print(f"task number is: {num}")

    for i in range(31):
        if i<=10:
            t = BashOperator(
                task_id='print_'+str(i),
                bash_command=f"echo {i}"
            )

        else:
            t = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=func,
                op_kwargs={'num': i}
            )