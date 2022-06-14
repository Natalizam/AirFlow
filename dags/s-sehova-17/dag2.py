from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
<<<<<<< Updated upstream:dags/s-sehova-17/dag2.py
from textwrap import dedent
=======
>>>>>>> Stashed changes:dags/s-sehova-17/dag9.py

with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
<<<<<<< Updated upstream:dags/s-sehova-17/dag2.py
    tags=['example'],
) as dag:
    date = "{{ ds }}"
    t1 = BashOperator(
        task_id = 'pwd',
        bash_command='pwd'
	)
    
    def print_context(ds):
        print(ds)
        return ds
        
    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

t1>>t2
=======
    tags=['s-sehova-17'],
) as dag:
    def put(ti):
        value = ti.xcom_push(
            key='sample_xcom_key',
            value = 'xcom test'
        )
    def take(ti):
        value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='put'
        )
        print(value)
    
    t91 = PythonOperator(
        task_id = 'put',
        python_callable=put,
        )
    t92 = PythonOperator(
        task_id = 'take',
        python_callable=take,
        )
        
t91 >> t92
>>>>>>> Stashed changes:dags/s-sehova-17/dag9.py
