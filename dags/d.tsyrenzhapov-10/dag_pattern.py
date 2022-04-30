from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'tutorial',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='Task 2',

        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 28),
        catchup=False,
        tags=['example']

) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )
        # Шаблонизация через Jinja
    t1 = BashOperator(
        task_id='templated',
        bash_command=templated_command
    )
#%%
