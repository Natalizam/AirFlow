from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('HW_3_a-telesh',
         default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

    },
    description='HW_3_a-telesh',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 6),
    catchup=False,
    tags=['At'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )


    def get_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )

t1 >> t2