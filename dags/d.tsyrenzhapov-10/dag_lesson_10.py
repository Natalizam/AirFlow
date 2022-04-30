from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook


def cnn():
    postgres = PostgresHook(postgres_conn_id='start_ml')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select 
                    user_id
                    , count(*) cnt
                from feed_action
                where 1=1 
                and action='like'
                group by user_id
                order by cnt desc
                limit 1
                """
            )
            return cursor.fetchone()


with DAG(
        'task_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Task 9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 28),
        catchup=False,
        tags=['example']
) as dag:
    t1 = PythonOperator(
        task_id='user_id',
        python_callable=cnn

    )