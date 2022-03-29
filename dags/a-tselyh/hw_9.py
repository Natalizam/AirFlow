from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def return_phrase():
        phrase="Airflow tracks everything"
        return phrase


def get_phrase_f(ti):
    value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='get_value'
    )



with DAG(
        'a-tselyh_xcom_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple dag XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='set_xcom_9',
        python_callable=return_phrase,
    )

    t2 = PythonOperator(
        task_id='get_xcom_9',
        python_callable=get_phrase_f,
    )

    t1 >> t2