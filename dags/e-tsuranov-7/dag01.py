from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 13),
    catchup=False,
    tags=['tags'],
) as dag:

    t1 = BashOperator(task_id='BashOperator', bash_command='pwd')

    def print_ds(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(task_id='PythonOperator', python_callable=print_ds)
    
    t1 >> t2

    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3