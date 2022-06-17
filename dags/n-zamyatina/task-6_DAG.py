from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'task-6-zamyatina',
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
    tags=['6'],
) as dag:

    def print_task_number(ts, task_number, run_id, **kwargs):
        print(f'task number is: {task_number}')
        print(task_number)
        print(kwargs)
        print(ts)
        print(run_id)
        return 'Whatever you return gets printed in the logs'

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'print_date_{i}',
                depends_on_past=False,
                bash_command="echo $NUMBER",
                env={"NUMBER": str(i)}
            )
            t1.doc_md = dedent(
                f"""\
            #### Task Documentation
            f"echo {i}"
            *iteration* of _first 10_ tasks of **BashOperator** type
            """
            )

        elif i >= 10:
            t2 = PythonOperator(
                task_id=f'print_task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )
            t1.doc_md = dedent(
                f"""\
            #### Task Documentation
            **print** `"task number is: {task_number}"`, where `i` *iteration number*
            """
            )