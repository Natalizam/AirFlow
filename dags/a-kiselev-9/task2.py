from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



with DAG(
    'task_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), # timedelta из пакета datetime
         },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='pwd',
    )

    def printds(ds, **kwargs):
        print(ds)
        return ds

    t2 = PythonOperator(
        task_id='python1',
        python_callable=printds
    )

t1>>t2
