from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'shuteev_my_dag_5',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='My Hyper DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['shuteev'],
) as dag:

    for i in range(0,10):
        taskx = BashOperator(
            task_id='loop_bash_' + str(i),
            depends_on_past=False,
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )
        # tloop0 >> taskx
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3