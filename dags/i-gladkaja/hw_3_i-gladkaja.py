"""
### a simple tutorial dag for printing
"""
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'gladkaja_hw_2',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='t_1_echo_the_' + str(i),
            bash_command=f'echo {i}')
        t1.doc_md = dedent(
            """
            ### BashOperator
            В **таске_1** распечатывается подряд *10* чисел
            при помощи команды `echo {i}`
            """
            )


    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='t_2_python_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent(
            """
            ### PythonOperator
            В **таске_2** распечатывается подряд *20* чисел
            при помощи функции `print_task_number`,
            которая задействует переменную из цикла.
            """
            )
    dag.doc_md = __doc__

    t1 >> t2
