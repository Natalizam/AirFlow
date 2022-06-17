from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor

from airflow.providers.postgres.operators.postgres import PostgresHook

def postgres():
  postgres = PostgresHook(postgres_conn_id="startml_feed")
  with postgres.get_conn(
    dbname="feed_action",
    host="postgres.lab.karpov.courses",
    user="robot-startml-ro",
    password="pheiph0hahj1Vaif",
    port=6432,
    cursor_factory=RealDictCursor,) as conn:
      with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
          """
          SELECT user_id, COUNT(action) AS count
          FROM feed_action
          WHERE action = 'like'
          GROUP BY user_id
          ORDER BY count(action) DESC
          LIMIT 1
          """
        )
        res = cursor.fetchone()
        return res

with DAG(
    'hw_10_n-dmitrieva_postgress',
    default_args = { # Default settings applied to all tasks
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:

    def postgres():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn(
                dbname="feed_action",
                host="postgres.lab.karpov.courses",
                user="robot-startml-ro",
                password="pheiph0hahj1Vaif",
                port=6432,
                cursor_factory=RealDictCursor, ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action) AS count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count(action) DESC
                    LIMIT 1
                    """
                )
                res = cursor.fetchone()
                return res

    task1 = PythonOperator(
        task_id = 'postgres', #task ID
        python_callable = postgres,
        )