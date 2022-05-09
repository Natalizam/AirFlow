from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task1',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='hw_6_b-boguslavskij',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 17),
        catchup=False,
        tags=['b-b']

) as dag:
    def get_task_num(ts, run_id, **kwargs):
        i = kwargs['i']
        print(f'task number is: {i}')
        print(ts)
        print(run_id)

    for i in range(20):
        run_python = PythonOperator(
            task_id='get_task_num' + str(i),  # id, будет отображаться в интерфейсе
            python_callable=get_task_num,
            op_kwargs={'i': i},

        )
        run_python.doc_md = dedent("""\
                #### Running bash commands
                **Start** *python func* `print_task(**op_kwargs)`
                """)

    run_python
