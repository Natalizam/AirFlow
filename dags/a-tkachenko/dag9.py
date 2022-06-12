from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import json



with DAG(
    'xcom_dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    catchup=False
) as dag:

    def postgres_func(ti):
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory = RealDictCursor) as cursor:
                cursor.execute("""                   
                SELECT user_id, COUNT(action) 
                FROM "feed_action" 
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                """)
                result = dict(cursor.fetchone())
                return result


    t_py1 = PythonOperator(
        task_id='postgres_querry',
        python_callable=postgres_func
    )