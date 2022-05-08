from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'task_11_v-yaroslavlev-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Lesson_11_task_11_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_11_v-yaroslavlev-7'],
) as dag:

    def echo():
        return print(Variable.get("is_startml"))

    t1 = PythonOperator(
        task_id='echo',
        python_callable=echo,
    )

    t1