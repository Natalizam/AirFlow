from airflow.hooks.base import BaseHook
import psycopg2
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from psycopg2.extras import RealDictCursor

with DAG(
        'murad_satabaev_tenth_dag',
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
    def download_sql():
        conn = BaseHook.get_connection('startml_feed')
        connection = psycopg2.connect(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
        SELECT user_id, COUNT(action)
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id
        ORDER BY COUNT(action) DESC
        LIMIT 1
        """)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result


    t1 = PythonOperator(
        task_id='download_sql',
        python_callable=download_sql,
    )

    t1
