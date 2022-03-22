from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def return_something():
    return "Airflow tracks everything"


def pull_result(**kwargs):
    ti = kwargs['ti']
    print(ti.xcom_pull(task_ids='xcom_push_test', key='return_value'))


with DAG(
    'e_bogomolova_step_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_10'],
) as dag:

    push_test_task = PythonOperator(
        task_id='xcom_push_test',
        python_callable=return_something,
    )

    pull_result_task = PythonOperator(
        task_id='xcom_pull_test',
        python_callable=pull_result,
    )

    push_test_task >> pull_result_task