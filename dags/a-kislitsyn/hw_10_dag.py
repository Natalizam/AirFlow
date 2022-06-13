from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_10_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'В лекции говорилось, что любой вывод return уходит неявно в XCom. Давайте это проверим. '
                  'Создайте новый DAG, содержащий два PythonOperator. Первый оператор должен вызвать функцию, '
                  'возвращающую строку "Airflow tracks everything". Второй оператор должен получить эту строку '
                  'через XCom. Вспомните по лекции, какой должен быть ключ. '
                  'Настройте правильно последовательность операторов.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-10'],
) as dag:
    def return_text_for_xcom():
        return "Airflow tracks everything"

    def task_pull_key(ti):
        print_key_value = ti.xcom_pull(
            key = 'return_value',
	        task_ids = 'call_text_stroke'
        )
        # print(print_key_value)
    task1 = PythonOperator(
        task_id = 'call_text_stroke',
        python_callable = return_text_for_xcom,
    )

    task2 = PythonOperator(
        task_id = 'print_key_xcom',
        python_callable = task_pull_key,
    )

    task1 >> task2