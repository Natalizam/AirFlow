from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_11_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен, используя подключение '
                  'с conn_id="startml_feed", найти пользователя, который поставил больше всего лайков, '
                  'и вернуть словарь {`user_id`: <идентификатор>, `count`: <количество лайков>}. ' 
                  'Эти значения, кстати, сохранятся в XCom.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-10'],
) as dag:
    def return_most_like():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT user_id, count(*) FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count(*) DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchall()

    task = PythonOperator(
        task_id = 'sql_user_most_like',
        python_callable = return_most_like,
    )

    task