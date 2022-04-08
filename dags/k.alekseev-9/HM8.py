from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'hw_8_k.alekseev-9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='Eight DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['T8'],
) as dag:

    def f_xcom(ur):
        ur.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def s_xcom(ur):
        tp = ur.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(tp)

    t1 = PythonOperator(
        task_id='f_xcom',
        python_callable=f_xcom,
    )

    t2 = PythonOperator(
        task_id='s_xcom',
        python_callable=s_xcom,
    )

    t1 >> t2
