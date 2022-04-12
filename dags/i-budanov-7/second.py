from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

def my_sleeping_function(task_number):
    strb = 'task number is: %s' % str(task_number)
    print(strb)

with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Каждый таск будет спать некое количество секунд
    task = BashOperator(
        task_id='sleep_for_',  # в id можно делать все, что разрешают строки в python
        bash_command="""
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id }}"
            {% endfor %}
        """
    )
    # настраиваем зависимости между задачами
    # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
    task.doc_md = dedent(
        """\
    #### Task Documentation
    **You can document** your __task using__ the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )
    task