from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator


with DAG(
    'task-4-zamyatina',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task-1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['4'],
) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id )}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='task-4',
        depends_on_past=False,
        bash_command=templated_command,
    )
