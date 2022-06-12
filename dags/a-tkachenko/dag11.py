from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable


with DAG(
    'hw_13_a-tkachenko',
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

    start_dummy = DummyOperator(
        task_id='start'
    )

    def branch_direct(**kwargs):
        if Variable.get("is_startml") == True:
            task_id = "startml_desc"
        else:
            task_id = "not_startml_desc"

    brach_py = BranchPythonOperator(
        task_id='course_subdirections',
        python_callable=branch_direct
    )

    def py1_print(**kwargs):
        print("StartML is a starter course for ambitious people")
    t_py1 = PythonOperator(
        task_id="startml_desc",
        python_callable = py1_print
    )

    def py2_print(**kwargs):
        print("Not a startML course, sorry")
    t_py2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable = py2_print
    )

    end_dummy = DummyOperator(
        task_id='end'
    )
