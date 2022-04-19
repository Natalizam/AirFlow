from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'krylov_task_3',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 10),
    catchup=False
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='bash_operator_' + str(i),
            bash_command=f"echo {i}",
        )


    def task_number(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        t2 = PythonOperator(
            task_id='python_operator_' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
