from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


with DAG(
    'hw_12_t-volkov-5',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    def is_startml_course():
        is_startml = Variable.get("is_startml")
        if is_startml:
            return 'ml_true'
        else:
            return 'ml_false'
    def ml_true():
        print('StartML is a starter course for ambitious people')

    def ml_false():
        print('Not a startML course, sorry')    

    t0 = DummyOperator(task_id='Before_branching')

    t1 = BranchPythonOperator(
        task_id='get_startml_status',
        python_callable=is_startml_course
    )

    t2 = PythonOperator(
        task_id='ml_true',
        python_callable= ml_true
    )
    
    t3 = PythonOperator(
        task_id='ml_false',
        python_callable= ml_false
    )
    t4 = DummyOperator(task_id='After_branching')


    t0>>t1>>[t2,t3]>>t4