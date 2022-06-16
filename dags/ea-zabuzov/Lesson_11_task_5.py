from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'Lesson_11_step_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My second training DAG',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_5']
) as dag:
    templated_command = dedent(
        '''
        {% for i in range (5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        '''
    )
    t1 = BashOperator(
        task_id='ts_run_id_printing',
        bash_command=templated_command
    )
