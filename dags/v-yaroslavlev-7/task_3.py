from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG\
    (
    "task_3_v-yaroslavlev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #3",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 5, 8),
    catchup = False,
    tags = ["task_3"]
    ) as dag:
        
    for i in range(10):
        t1 = BashOperator(
        task_id="echo_" + str(i),
        bash_command=f"echo {i}"
        )

    t1.doc_md = dedent(f"""\
        # Task Documentation
        This **code** *code* `bash_operator_{i}`
        """
        )


    def print_task_number(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number' : i}
        )

    t2.doc_md = dedent(f"""\
        # Task Documentation
        This **task** *execute* `python_operator_{i}`
        """
        )

    t1 >> t2