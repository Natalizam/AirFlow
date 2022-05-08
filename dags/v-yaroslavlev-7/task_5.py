from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_5_v-yaroslavlev-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Lesson_11_task_5_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_5_v-yaroslavlev-7'],
) as dag:
    
    for i in range(10):
        t1 = BashOperator(
            task_id="echo_" + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )

    t1