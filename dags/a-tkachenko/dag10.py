from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable


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

    def var_test(**kwargs):
        print(Variable.get("is_startml"))

    t_py = PythonOperator(
        task_id='var_test_py',
        python_callable=var_test
    )