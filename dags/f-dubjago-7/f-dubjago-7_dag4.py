"""
Airflow templates
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from textwrap import dedent

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'f-dubjago-7_dag4',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 16),
        tags=['df']
) as dag:
    dag.doc_md = __doc__

    templated_command = dedent(
        """
        {% for i in range(5) %}
           echo "{{ts}}"
           echo "{{run_id}}"        
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='print-smth',
        bash_command=templated_command,
    )

    t1
