from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent
from datetime import datetime, timedelta


with DAG(
    'hw_4_e-porjaina',
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
    tags=['hw3'],
) as dag:
    for i in range(1, 31):
        if i <= 10:
            task_bash = BashOperator(
            task_id = "task_number" + str(i),
            bash_command = f"echo {i}" 
            )
            
            task_bash.doc_md = dedent( 
                """ \
            #### Task 1 Documentation
                **current** _bash_ command loops `echo {i}`                          
                """

            )   

        else:
            def python_task_number(task_number):
                print(f'task number is: {task_number}')
                     
            task_python = PythonOperator(
                task_id = "task_number" + str(i),
                python_callable = python_task_number,
                op_kwargs = {'task number is': i}
            )
            task_python.doc_md = dedent( 
                    """ \
            #### Task 1 Documentation
            **current** _bash_ command loops `echo {i}`
                    """

            )          
        
    task_bash >> task_python