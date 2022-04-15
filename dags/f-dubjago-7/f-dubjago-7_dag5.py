from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Параметры по умолчанию для тасок
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'f-dubjago-7_dag5',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 16),
        tags=['df']
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id='BashOperator' + str(i),
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i}
        )

    t
