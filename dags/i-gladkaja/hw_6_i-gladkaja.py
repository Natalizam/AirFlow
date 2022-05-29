from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('gladkaja_hw_6_2',
         default_args=default_args,
         description='A simple tutorial DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False) \
        as dag:

    def print_task_info(**kwargs):
        print(f"task number is: {task_number}")
        print(f"current date is: {ts}")
        print(f"run_id is: {run_id}")

    for i in range(20):
        t1 = PythonOperator(
            task_id=f't1_iter_task_number {i}',
            python_callable=print_task_info,
            op_kwargs={'task_number': i
                       }
        )
    t1.doc_md = dedent(
        '''
    #### print dynamic argument `task_number`
    and two positional arguments 'ts' and 'run_id'
    ''')

    t1