from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'a-malahov_task4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='a-malahov task 4',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 18),
        catchup=False,
        tags=['malahov'],
) as dag:

    bash_templated = dedent(
        """
    {% for i in range(5) %}
        echo "{{ts}}"
    {% endfor %}
    echo "{{run_id}}"
    """
    )

    bash_op = BashOperator(
        task_id='task4_a-malahov',
        depends_on_past=False,
        bash_command=bash_templated ,
    )

    bash_op