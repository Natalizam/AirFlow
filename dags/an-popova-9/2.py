"""
Напишите DAG, который будет содержать BashOperator и PythonOperator. В функции PythonOperator примите аргумент ds и распечатайте его. Можете распечатать дополнительно любое другое сообщение.
В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow. Результат может оказаться неожиданным, не пугайтесь - Airflow может запускать ваши задачи на разных машинах или контейнерах с разными настройками и путями по умолчанию.
Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
with DAG(
    'hw_2_an-popova-9',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='DAG for task 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    def print_date(ds):
        print(ds)

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )


    t1 >> t2