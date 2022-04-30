from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "hw1",
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_entry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description="promlem_1",
) as dag:
    t2 = BashOperator(
        task_id='print pwd',
        bash_command='pwd'
    )

    def print_context(df):
        print(ds)

    t1 = PythonOperator(
        task_id='print ds',
        python_callable=print_context 
    )
    t1 >> t2