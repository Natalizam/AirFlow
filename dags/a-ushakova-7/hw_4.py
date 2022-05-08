from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG('AUshakova_HW_4',
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
            # Описание DAG (не тасок, а самого DAG)
    description='4 homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 6),
    catchup=False,
    tags=['hw_4'], ) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ task_id }}"
    {% endfor %}
    """

    t1 = BashOperator(task_id='templated',  # id, будет отображаться в интерфейсе
                        depends_on_past = False,
                        bash_command = templated_command,  # какую bash команду выполнить в этом таске
                                )
    t1

    
