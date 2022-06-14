from airflow.hooks.base import BaseHook
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

creds = BaseHook.get_connection(id соединения)
with psycopg2.connect(
  f"postgresql://{creds.login}:{creds.password}"
  f"@{creds.host}:{creds.port}/{creds.schema}"
) as conn:
  with conn.cursor() as cursor:
  
  
  
  
ith DAG(
    's-sehova-17-11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['s-sehova-17'],
) as dag:
    t111 = PythonOperator(
        task_id = 'put',
        python_callable=put,
        )