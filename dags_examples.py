from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

with DAG(
    'homework',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    date = "{{ ds }}"

    t1 = BashOperator(
        task_id="print_working_directory",
        bash_command="pwd ",
        env={"DATA_INTERVAL_START": date},
    )

    def print_context(ds):
        print(ds)
        return 'Printed logical date'

    t2 = PythonOperator(
        task_id='print_logical_date',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2