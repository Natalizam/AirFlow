from datetime import datetime, timedelta #9
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_8_e-tsuranov-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 13),
    catchup=False,
    tags=['tsuranov'],
) as dag:
    
    t1 = PythonOperator(task_id='PythonOperator_t1', python_callable=lambda: "Airflow tracks everything")
    
    t2 = PythonOperator(task_id='PythonOperator_t2', python_callable=lambda ti: ti.xcom_pull(key='return_value', task_ids='PythonOperator_t1'))
    
    t1 >> t2