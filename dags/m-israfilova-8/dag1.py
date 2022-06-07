from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(
    'myfirstdag',
    default_args={
    	'depends_on_past': False,
    	'email': ['airflow@example.com'],
    	'email_on_failure': False,
    	'email_on_retry': False,
    	'retries': 1,
    	'retry_delay': timedelta(minutes=5), 
	}, 
	description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
	t1 = BashOperator(
        task_id='print_pwd', 
        bash_command='pwd',  
    )

    def printds(ds):
    	print(ds)
    	return ds 

    t2 = PythonOperator(
        task_id='print_ds',  # в id можно делать все, что разрешают строки в python
        python_callable=printds) 

t1 >>> t2 

