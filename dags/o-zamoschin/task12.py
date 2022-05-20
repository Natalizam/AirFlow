"""
Start-ml Airflow Task 12
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

def choose_task():
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"

def print_startml():
    print("StartML is a starter course for ambitious people")

def print_not_startml():
    print("Not a startML course, sorry")

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_12_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    t1 = DummyOperator(
        task_id = 'before_branching',
    )

    t2 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = choose_task,
    )

    t3 = PythonOperator(
        task_id = "startml_desc",
        python_callable = print_startml,
    )

    t4 = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = print_not_startml,
    )

    t5 = DummyOperator(
        task_id = 'after_branching',
    )

    t1 >> t2 >> [t3, t4] >> t5