from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'tutorial',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='Task 2',

        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 28),
        catchup=False,
        tags=['example']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='tasks' + str(i),
            bash_command=f"echo {i}"
        )
    def context(task_number, ts, run_id):
        print(ts)
        print(run_id)
        return f"task number is: {task_number}"


    t1.doc_md = dedent(
        """
    #### Task Documentation
    something should be here idk what to write
    `from airflow import DAG`
    *let it be*
    **where is your love**
        """
    )

    for i in range(20):
        t2 = PythonOperator(
            task_id='python' + str(i),
            python_callable=context,
            op_kwargs={'task_number': i},
        )
    t1 >> t2
#%%
