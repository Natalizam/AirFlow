from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'


def xcom_tracks_everything():
    return "Airflow tracks everything"

def retrieve_xcom(ti):
    value_xcom = ti.xcom_pull(
        key="return value",
        task_ids='push_xcom'
    )
    print(value_xcom)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'xcom_dag_9',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'text',
        python_callable=xcom_tracks_everything,
    )
    t2 = PythonOperator(
        task_id = 'retrieve_xcom',
        python_callable=retrieve_xcom,
    )

    t1 >> t2