from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'dag_6_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs и '
                  'передайте в этот kwargs task_number со значением переменной цикла. '
                  'Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, '
                  'и распечатайте эти значения.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-6'],
) as dag:
    def print_task_number(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number} ts: {ts} run_id: {run_id}")

    for i in range(30):
        if i <= 9:
            task = BashOperator(
                task_id = 'print_bash_' + str(i),
                bash_command = f"echo $NUMBER",
                dag = dag,
                env = {'NUMBER' : i}
            )
        else:
            task = PythonOperator(
            task_id = 'print_task_number_' + str(i),
            python_callable = print_task_number,
            op_kwargs = {'task_number' : i},
            )
        task
