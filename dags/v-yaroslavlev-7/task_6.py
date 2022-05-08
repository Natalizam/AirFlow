from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_6_v-yaroslavlev-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Lesson_11_task_6_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_6_v-yaroslavlev-7'],
) as dag:
    
    def print_task_number(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(run_id)
        print(ts)

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number' : i}
        )
    t2