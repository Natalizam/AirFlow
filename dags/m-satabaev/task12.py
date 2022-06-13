from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator



with DAG(
        'branching',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_sixth_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:

    def branch_if():
        if Variable.get('is_startml'):
            task_id = "startml_desc"
        else:
            task_id = "not_startml_desc"
        return task_id


    def print_1():
        print("StartML is a starter course for ambitious people")


    def print_2():
        print("Not a startML course, sorry")


    t1 = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_if,
    )

    t2 = PythonOperator(
        task_id='is_startml',
        python_callable=print_1,
    )

    t3 = PythonOperator(
        task_id='is_not_startml',
        python_callable=print_2,
    )


    t1 >> [t2, t3]
