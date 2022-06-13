from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'dm-kuznetsov_hw_10',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='dm-kuznetsov_hw_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['task_1'],
) as dag:

    def push_xcom(ti):
        return 'Airflow tracks everything'


    def pull_xcom(ti):
        pull_res = ti.xcom_pull(
        key="return_value",
        task_ids='push_xcom',
        )
        print(f'pull_res = {pull_res}')

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom
    )

    t1 >> t2