from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import variable
from datetime import datetime, timedelta


def print_var():
    print(variable.get('is_startml'))


with DAG(
    'hw_10_d-zhalsaraev-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['zhalsaraev']
) as dag:
    t1 = PythonOperator(
        task_id='t_return',
        python_callable=print_var
    )

    t1