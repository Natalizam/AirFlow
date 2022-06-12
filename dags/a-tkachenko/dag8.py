from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG


def xcom_test_push(ti):
    return "Airflow tracks everything"

def xcom_test_pull(ti):
    pull_res = ti.xcom_pull(
        key='return_value',
        task_ids='push_result'
    )
    print(pull_res)

with DAG(
    'hw_9_a-tkachenko',
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

    t_py1 = PythonOperator(
        task_id='push_result',
        python_callable=xcom_test_push
    )
    t_py2 = PythonOperator(
        task_id='pull_result',
        python_callable=xcom_test_pull,
    )

    t_py1 >> t_py2