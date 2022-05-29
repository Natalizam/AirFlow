"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'k_le_8_dag_3',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag_3, adding description',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(1,11):
        task = BashOperator(
        task_id='bash_command_' + str(i),  # id, будет отображаться в интерфейсе
        bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )
        task.doc_md = dedent(
            """\
        # **some bashoperator**
        *'task = BashOperator(
        task_id='bash_command_' + str(i),  # id, будет отображаться в интерфейсе
        bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )'*
        """
        )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку


    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(11,31):
        task = PythonOperator(
            task_id='print_task_number_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_task_number,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )
        task.doc_md = dedent(
            """\
        # **some pythonoperator**
        *'task = PythonOperator(
            task_id='print_task_number_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_task_number,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )'*
        """


