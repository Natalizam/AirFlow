from airflow.hooks.base import BaseHook
import psycopg2
from airflow import DAG
from datetime import timedelta, datetime

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from psycopg2.extras import RealDictCursor

with DAG(
        'murad_satabaev_eleventh_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_sixth_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:
    def get_variable():
        print(Variable.get('is_startml'))


    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
    )

    t1
