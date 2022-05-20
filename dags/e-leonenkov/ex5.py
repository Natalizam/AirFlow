"""
Dynamic Tasks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_e-leonenkov',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='The DAG with dynamic tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ex5'],
) as dag:
    dag.doc_md = __doc__

    def print_task_num(task_number):
        return f"task number is: {task_number}"


    for i in range(1, 31):
        if i <= 10:
            t1 = BashOperator(
                task_id=f'echo_{i}',
                bash_command='echo $NUMBER',
                env={"NUMBER": i},
            )

        else:
            t2 = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=print_task_num,
            op_kwargs={'task_number': i}
            )


    t1 >> t2