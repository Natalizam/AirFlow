from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'

def push_xcom(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def retrieve_xcom(ti):
    print(ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='push_xcom'
    ))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'xcom_dag_8',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'push_xcom',
        python_callable=push_xcom,
    )
    t2 = PythonOperator(
        task_id = 'retrieve_xcom',
        python_callable=retrieve_xcom,
    )

    t1 >> t2