
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'dag2mashir',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.ru'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='first task',
        schedule_interval=timedelta(days=1),
        start_date=datetime(22, 1, 1),
        catchup=False,
        tags=['v-mashir-8'],
) as dag:

    for i in range(10):
        t0 = BashOperator(
            env={'NUMBER': i},
            task_id=f'cycle_{i}',
            bash_command="echo $NUMBER"
        )
        if i == 0:
            t1 = t0
        else:
            t1 >> t0
            t1 = t0

    def print_tasks(task_number):
        print(f'task number is: {task_number}')


    for j in range(10, 20):
        t2 = PythonOperator(
        task_id=f'cycle_{j}',
        python_callable=print_tasks,
        op_kwargs={'task_number': j}
        )
        if j == 0:
            t1 = t0
        else:
            t1 >> t2
            t1 = t2