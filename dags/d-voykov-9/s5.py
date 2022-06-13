from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

from datetime import datetime, timedelta

with DAG(
        'dv_dag_m2_l11_s2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='m2 l11 s2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    cmd = dedent(
        """
            {% for i in range(5) %}
                echo "{{ ts }}"
            {% endfor %}
            echo "{{ run_id }}"

        """
    )

    task = BashOperator(
        task_id='show_ts_run_id',
        bash_command=cmd
    )

    task
