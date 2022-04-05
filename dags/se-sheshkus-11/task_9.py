from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_9_se-sheshkus-11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Less_11_task_9_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_9_se-sheshkus-11'],
) as dag:



    def return_string():
        return "Airflow tracks everything"


    def pull_xcom(ti):
        testing_pull = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(testing_pull)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=return_string,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2