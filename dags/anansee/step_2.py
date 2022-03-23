from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Default settings applied to all tasks
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    default_args=default_args,
    description='DAG for task_1 anansee',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 19),
    catchup=False,
    tags=['task1']
) as dag:
    t1 = BashOperator(
        task_id='print_current_folder',  # id, который будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )
    def print_date(ds):
        print(ds)
        print("This function prints ds")

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    t1>>t2





    opr_get_covid_data >> opr_analyze_testing_data