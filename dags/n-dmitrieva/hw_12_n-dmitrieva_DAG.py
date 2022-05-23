from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

'''BranchingOperator - это оператор, который по некоторому 
условию определяет, в какое ответвление пойдет выполнение DAG.
Один из способов определить это "некоторое условие" - это задать 
python функцию, которая будет возвращать task_id, куда надо 
перейти после ветвления.

Создайте DAG, имеющий BranchPythonOperator. Логика ветвления 
должна быть следующая: если значение Variable is_startml равно 
"True", то перейти в таску с task_id="startml_desc", иначе 
перейти в таску с task_id="not_startml_desc". Затем объявите 
две задачи с task_id="startml_desc" и task_id="not_startml_desc".

NB: класс Variable возвращает строку!

В первой таске распечатайте "StartML is a starter course for 
ambitious people", во второй "Not a startML course, sorry".

Перед BranchPythonOperator можете поставить DummyOperator - 
он ничего не делает, но зато задает красивую "стартовую точку" 
на графе. Точно так же можете поставить DummyOperator в конце 
DAG.

По итогу у вас получится следующий граф (с DummyOperator):

'''
from airflow.models import Variable

def chuse_branch():
      # необходимо передать имя, заданное при создании Variable
    if Variable.get("is_startml"):
        print("StartML is a starter course for ambitious people")
        return "startml_desc"
        
    else:
        print("Not a startML course, sorry")
        return "not_startml_desc"
      

with DAG(
    'hw_12_n-dmitrieva',
    default_args = { # Default settings applied to all tasks
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:

    t1 = DummyOperator(task_id = 'before_branch')

    task1 = BranchPythonOperator(
        task_id = 'startml_desc', #task ID
        python_callable = chuse_branch,
        )
    
    task2 = BranchPythonOperator(
        task_id = 'not_startml_desc', #task ID 
        python_callable = chuse_branch,
    )

    t2 = DummyOperator(task_id = 'after_branch')

    
    task2
