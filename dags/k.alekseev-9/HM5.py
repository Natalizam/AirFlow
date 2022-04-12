from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_v-k.alekseev-9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='Fifth DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['T5'],
) as dag:

        for i in range(10):
                t1 = BashOperator(
                        task_id='bash_operator_plus' + str(i),
                        bash_command="echo $NUMBER",
                        env={"NUMBER": str(i)}
                )