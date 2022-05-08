from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    'task_10_v-yaroslavlev-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Lesson_11_task_10_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_10_v-yaroslavlev-7'],
) as dag:

    def query():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id='query',
        python_callable=query,
    )

    t1