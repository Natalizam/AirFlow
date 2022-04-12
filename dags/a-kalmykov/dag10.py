from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def pull_xcom(task_id, ti):
  value = ti.xcom_pull(
    key='return_value',
    task_ids=task_id
  )
  print(f'{task_id}[return_value] = {value}')


def user_top_like():
  postgres = PostgresHook(postgres_conn_id="startml_feed")
  with postgres.get_conn() as conn:
    with conn.cursor() as cursor:
      cursor.execute("""                   
    SELECT user_id, COUNT(user_id) as c
    FROM feed_action 
    WHERE action = 'like'
    GROUP BY user_id 
    ORDER BY c DESC LIMIT 1
      """)
      result = cursor.fetchone()
    return result

with DAG(
        dag_id='a-kalmykov-dag-10',
        default_args=default_args,
        description='Dag 10 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
  t1 = PythonOperator(
    task_id='get_user_top_like',
    python_callable=user_top_like,
  )

  t2 = PythonOperator(
      task_id='pull_data_from_xcom',
      python_callable=pull_xcom,
      op_kwargs={'task_id': 'get_user_top_like'}
  )

  t1 >> t2