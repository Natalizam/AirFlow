from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import timedelta, datetime


'''
Second DAG documentation

'''

with DAG(
    "hw_2_n-dmitrieva", 
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
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='Second exersize',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['NDmitrieva'],
) as dag:

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(30):

        if i<10: 
            Btask1 = BashOperator(
                task_id = 'echo' + str(i), # id, будет отображаться в интерфейсе
                bash_command = f"echo {i}",
            )# какую bash команду выполнить в этом таске
            Btask1.doc_md = dedent(
            f"""\
            #Task Documentation
            'f"echo {i}"' 
            you will see 
            *iteration* of _first 10_ tasks of **BashOperator** type""")
    
        else: 
            Ptask2 = PythonOperator(
                task_id = f'print_task_number_{i}', # в id можно делать все, что разрешают строки в python
                python_callable = print_task_number,  # свойственен только для PythonOperator - передаем саму функцию
                op_kwargs={'task_number': i},  # передаем в аргумент с названием task_number и задействуем переменную из цикла
            )
            Ptask2.doc_md = dedent("""
            # Task_Documentation 
            **print** `"task number is: {task_number}"`, where `i` *iteration number*
            using  PythonOperator
            """)
    Btask1 >> Ptask2