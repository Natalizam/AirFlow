from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


default_args = {
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  }

with DAG(
    'first_dag_krylov',

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 10),
    default_args=default_args,
    catchup=False,
    tags=['example']
) as dag:


    bash_task = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    def print_ds(ds, **kwargs):
        print(ds)

    python_task =PythonOperator(
        task_id = 'print_ds',
        python_callable=print_ds,
    )



    bash_task >> python_task


