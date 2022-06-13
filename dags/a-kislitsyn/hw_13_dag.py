from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy  import DummyOperator
from airflow.models import Variable

with DAG(
    'hw_12_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'BranchingOperator - это оператор, который по некоторому условию определяет, в какое ответвление '
                  'пойдет выполнение DAG. Один из способов определить это "некоторое условие" - это задать python функцию, '
                  'которая будет возвращать task_id, куда надо перейти после ветвления. '
                  'Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая: '
                  'если значение Variable is_startml равно "True", то перейти в таску с task_id="startml_desc", '
                  'иначе перейти в таску с task_id="not_startml_desc". '
                  'Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc". '
                  'NB: класс Variable возвращает строку! '
                  'В первой таске распечатайте "StartML is a starter course for ambitious people", '
                  'во второй "Not a startML course, sorry". '
                  'Перед BranchPythonOperator можете поставить DummyOperator - он ничего не делает, '
                  'но зато задает красивую "стартовую точку" на графе. '
                  'Точно так же можете поставить DummyOperator в конце DAG.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-12'],
) as dag:
    def who_is_start_ml():
        if Variable.get("is_startml") == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")

    task1 = DummyOperator(
        task_id='before_branching'
    )
    task2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=who_is_start_ml
    )
    task3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )
    task4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )
    task5 = DummyOperator(
        task_id='after_branching'
    )

    task1 >> task2 >> [task3, task4] >> task5
