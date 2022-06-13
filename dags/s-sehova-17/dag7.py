from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        's-sehova-17-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG n-anufrieva_task7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['s-sehova-17'],
) as dag:
    def task(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f"task number is: {task_number}")


    for i in range(30):
        if i < 10:
            task71 = BashOperator(
                task_id='bash_task_' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            task72 = PythonOperator(
                task_id='python_task_' + str(i),
                python_callable=task,
                op_kwargs={'task_number': i},
            )