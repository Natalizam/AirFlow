from datetime import datetime, timedelta

#чтоб работать с DAG импортируем класс
from airflow import DAG

# DAG состоит из операторов -кирпичиков(task)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a.gordin_task_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
# Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 10),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='pwd_command',  # id в интерфейсе
        bash_command='pwd',  # выполнение команды
    )
    def print_context(ds, **kwargs):
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'сложнааа'

    t2 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2