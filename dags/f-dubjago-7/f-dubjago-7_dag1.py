# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Параметры по умолчанию для тасок
default_args= {
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'f-dubjago-7_dag1',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df']
) as dag:
    t = BashOperator(
        task_id="curr_dir",
        bash_command="pwd",  # обратите внимание на пробел в конце!
    )


    def print_context(ds, **kwargs):
        print(ds)


    run_this = PythonOperator(
        task_id='print_smth',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t >> run_this