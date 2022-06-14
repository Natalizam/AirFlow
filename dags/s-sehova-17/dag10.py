from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

with DAG(
    's-sehova-17-10',
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
    tags=['s-sehova-17'],
) as dag:
    def put():
        return "Airflow tracks everything"
        
    def take(ti):
        value = ti.xcom_pull(
            key='return_value',
            task_ids='put'
        )
        print(value)
    
    t101 = PythonOperator(
        task_id = 'put',
        python_callable=put,
        )
    t102 = PythonOperator(
        task_id = 'take',
        python_callable=take,
        )
        
t101 >> t102