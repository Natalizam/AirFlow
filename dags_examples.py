from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_2_f-turchenko-5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    date = "{{ ds }}"

    t1 = BashOperator(
        task_id="print_working_directory",
        bash_command="pwd ",
        env={"DATA_INTERVAL_START": date},
    )

    def print_context(ds):
        print(ds)
        return 'Printed logical date'

    t2 = PythonOperator(
        task_id='print_logical_date',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2

with DAG(
    'hw_2_f-turchenko-5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
            task_id="print_task_n_console",
            bash_command=f"echo {i} ",
            env={"DATA_INTERVAL_START": date},
            )

        else:
            def print_task_n(op_kwargs=i):
                print(f"task number is {op_kwargs}")

            t2 = PythonOperator(
                task_id = "print_task_n_python",
                python_callable = print_task_n
            )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    `print_task_n` function **prints** the *number* of task

    """
    )