from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def ds_context(ds, **kwargs):
    print(kwargs)
    print (ds)
    return 'OK'

"""
Test documentation
"""

with DAG(
    'task-1-zamyatina',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task-1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['1'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='pwd',
    )


    def ds_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'OK'

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=ds_context,
        retries=3,
    )

    t1 >> t2