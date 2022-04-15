"""
DAG VARIABLES
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


def print_variable():
    from airflow.models import Variable
    is_startml= Variable.get("is_startml")
    print(is_startml)



#DAG
with  DAG(
    'hw_8_j-sapriko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'HW-step-6',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 21),
    catchup = False,
    tags = ['hw_8'],
) as dag:

    dag.doc_md = __doc__

    t1 = PythonOperator(
        task_id="print_variable",
        python_callable=print_variable,
    )

    t1