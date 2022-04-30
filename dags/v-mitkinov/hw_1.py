from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_1_v_mit',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_entry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description="promlem_1",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    tags=['hw_1_v_mit'],
    ) as dag:

    t2 = BashOperator(
        task_id='hw_1_v_mit_pwd',
        bash_command='pwd',
    )

    def print_context(ds, **kwargs):
        print(ds)

    t1 = PythonOperator(
        task_id='hw_1_v_mit_print_ds',
        python_callable=print_context,
        )
    t2 >> t1