from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'lesson3',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='Lesson 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    # теги, способ помечать даги
    tags=['les3'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id=f'loop{i}',  # id, будет отображаться в интерфейсе
        bash_command = f"echo {i}",  # какую bash команду выполнить в этом таске
        )

    def print_tn(task_number):
    	return f"task number is: {task_number}"
    for i in range(20):
        t2 = PythonOperator(
    	task_id=f'print_tn{i}',  # нужен task_id, как и всем операторам
    	python_callable=print_tn,  # свойственен только для PythonOperator - передаем саму функцию
        op_kwargs={'task_number': i},
        )

    t1 >> t2
