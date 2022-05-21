from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def get_var():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'DAG_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 11),
        catchup=False,
        tags=['v-mashir-8'],
) as dag:
    t1 = PythonOperator(
        task_id='is_startml',
        python_callable=get_var,
    )

    t1
