from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_6_a.sidorenko-4',
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
    # # Описание DAG (не тасок, а самого DAG)
    description='a.sidorov HW 6 DAG kwargs',
    # # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # # С какой даты начать запускать DAG
    # # Каждый DAG "видит" свою "дату запуска"
    # # это когда он предположительно должен был
    # # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 23),
    # # Запустить за старые даты относительно сегодня
    # # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # # теги, способ помечать даги
    # tags=['example'],
) as dag:



    def print_task(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(f'{ts}')
        print(f'{run_id}')
        print(f"task number is: {task_number}")
    
    for task_number in range(10):
        run_bash = BashOperator(
            task_id=f'run_bash_number_{task_number}',
            bash_command=f'echo $NUMBER',
            env={"NUMBER": str(task_number)}
        )
        run_bash.doc_md = dedent("""\
        #### Running bash commands
        **Start** *script* `echo {i}`
        """)
    for task_number in range(20):
        run_python = PythonOperator(
            task_id=f'run_python_{task_number}',
            op_kwargs={'task_number':task_number},
            python_callable=print_task,
            
        )
        run_python.doc_md = dedent("""\
        #### Running bash commands
        **Start** *python func* `print_task(**op_kwargs)`
        """)
    
    run_bash >> run_python