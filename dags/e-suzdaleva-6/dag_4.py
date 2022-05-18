from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'e-suzdaleva-6-task4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='e-suzdaleva-6-task4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['e-suzdaleva-6-task4'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            {% endfor %}
        echo "{{ run_id }}"
        """
    )
    t1 = BashOperator(
        task_id='bash_operator',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1