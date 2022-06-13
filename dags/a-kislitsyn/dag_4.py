from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'dag_4_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Создайте новый DAG, состоящий из одного BashOperator. Этот оператор должен  использовать '
                  'шаблонизированную команду следующего вида: "Для каждого i в диапазоне от 0 до 5 не включительно '
                  'распечатать значение ts и затем распечатать значение run_id". '
                  'Здесь ts и run_id - это шаблонные переменные (вспомните, как в лекции подставляли шаблонные переменные).',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-4'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
        """
    )
    task = BashOperator(
        task_id = 'templated_task',
        bash_command = templated_command,
        )
    task
