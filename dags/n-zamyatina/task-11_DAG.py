from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task-11-zamyatina',
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
    tags=['11'],
) as dag:

    def startml():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t0 = BranchPythonOperator(
        if is_strtml == True:
            t1 = PythonOperator(
                task_id="startml_desc",
                python_callable = branch_task,
            )
        else:
            t2 = PythonOperator(
                task_id="not_startml_desc",
                python_callable = branch_task,
            )

    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=branch_task,
    )

    t2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=branch_task,
    )