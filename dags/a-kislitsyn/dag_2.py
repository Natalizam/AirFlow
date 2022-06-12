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
    description = 'Создайте новый DAG и объявите в нем 30 задач. Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду, '
                  'так или иначе использующую переменную цикла (например, можете указать f"echo {i}"). Оставшиеся 20 задач должны быть PythonOperator, '
                  'при этом функция должна задействовать переменную из цикла. Вы можете добиться этого, если передадите переменную через op_kwargs и '
                  'примете ее на стороне функции. Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. ',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-2'],
) as dag:
    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i <= 9:
            task = BashOperator(
                task_id = 'print_bash_' + str(i),
                bash_command = f"echo {i}",
            )
        else:
            task = PythonOperator(
            task_id = 'print_task_number_' + str(i),
            python_callable = print_task_number,
            op_kwargs = {'task_number' : i},
            )
        task
