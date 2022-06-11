"""
Test documentation
"""

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

with DAG(
    'tutorial',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:

    date = "{{ ds }}"

#Пошли циклы. Сначала Bash, потом python
    for i in range(10):

        task_2_generic = BashOperator(
        task_id="generic_test_" + str(i),
        bash_command=f"echo {i}"
        )

        task_2_generic.doc_md = dedent(
            """\
        #### Task Documentation
        You can *document* **your** task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

        """
        )


    def print_context(task_number):
        print(f'task number is: {task_number}')

    for x in range(20):
        t_py = PythonOperator(
            task_id='generic_test_' + str(x + 10),
            python_callable=print_context,
            op_kwargs={'task_number': int(x)},
        )