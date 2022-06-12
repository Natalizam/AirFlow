"""
Test documentation
"""

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

with DAG(
    'hw_5_a-tkachenko',
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
    ts = "{{ ts }}"
    run_id = "{{ run_id }}"

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )

    task_5 = BashOperator(
    task_id="test_pwd",
    bash_command=templated_command
    )
