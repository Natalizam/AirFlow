from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'dag_2_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Добавьте к вашим задачам из прошлого задания документацию. В документации обязательно должны быть элементы кода '
                  '(заключены в кавычки `code`), полужирный текст и текст курсивом, а также абзац (объявляется через решетку).',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-2'],
) as dag:
    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    """
    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i <= 9:
            task = BashOperator(
                task_id = 'print_bash_' + str(i),
                bash_command = f"echo {i}",
            )
            task.doc_md = dedent(
                """\
                #### Task Documentation
                You can document your _task_ using **the attributes** `doc_md` (markdown),
                `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
                rendered in the UI's Task Instance Details page.
                # first task
                ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
                """
            )
        else:
            task = PythonOperator(
            task_id = 'print_task_number_' + str(i),
            python_callable = print_task_number,
            op_kwargs = {'task_number' : i},
            )
            task.doc_md = dedent(
                """\
                #### Task Documentation
                PythonOperator Task _task_ using **the attributes** `doc_md` (markdown),
                `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
                rendered in the UI's Task Instance Details page.
                # first
                ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
                """
            )
        task
