from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_6_n-shishmakova-7',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='DAG for 6 task in lesson 11',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 1),
        # Запустить за старые даты относительно сегодня
        catchup=False,
        # теги, способ помечать даги
        tags=['hw6'],
) as dag:
    def print_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)


    for i in range(1, 10):
        t2 = PythonOperator(
            task_id='its_py_task_' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )