from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
"""
Создайте новый DAG и объявите в нем 30 задач. Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду, 
так или иначе использующую переменную цикла (например, можете указать f"echo {i}").

Оставшиеся 20 задач должны быть PythonOperator, при этом функция должна задействовать переменную из цикла. 
Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. 
Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. 
"""
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_task_num(task_number):
    print(f'task number is: {task_number}')

with DAG(
    'DAG_HW_3_ponomareva',
    default_args=default_args,
    description='tasks for HW_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 10),
    catchup=False,
    tags=['tag_HW_3'],
) as dag:

    for step in range(10):
        task_bash = BashOperator(
            task_id='print_command_' + str(step),
            bash_command=f'echo{step}',
            dag=dag
        )

    for i in range(20):
        task_python = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i}
        )

    task_bash >> task_python
