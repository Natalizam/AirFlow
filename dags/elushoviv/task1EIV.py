"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'Task1ElushovIV',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['T1'],
) as dag:


    t1 = BashOperator(
        task_id = 'pwd',
        bash_command='pwd'
        
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
    task_id='print_the_context',  
    python_callable=print_context,  
    )
    

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    dummy text
    """
    )  

    dag.doc_md = __doc__  
    dag.doc_md = """
    This is a documentation placed anywhere
    """  

    t1 >> t2

