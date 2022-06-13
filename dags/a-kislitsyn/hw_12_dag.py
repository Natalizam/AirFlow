from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_12_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Напишите DAG, состоящий из одного PythonOperator. '
                  'Этот оператор должен печатать значение Variable с названием is_startml.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-12'],
) as dag:
    def print_variable():
        from airflow.models import Variable
        print(Variable.get("is_startml"))

    task = PythonOperator(
        task_id = 'variable_startml',
        python_callable = print_variable,
    )

    task