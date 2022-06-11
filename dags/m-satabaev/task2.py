from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'murad_satabaev_first_task',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_first_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print i from 0 to 9',
            bash_command=f'echo {i}'
        )

    def all_tasks(cycle):
        for i in cycle:
            print(f'task number is: {i}')

    t2 = PythonOperator(
        task_id='print task numbers from cycle',
        python_callable=all_tasks,
        op_kwargs={'cycle': range(20)}
    )
    t1 >> t2