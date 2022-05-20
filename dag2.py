"""
 Test documentation
 `code 2`
 `Monospace`
 # bold text 2
 **bold text 2**
 ## italicized text 2
 *italicized text 2*
 _italicized text 2_
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    'm-isaeva_HW_t2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW2 m-isaeva',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw2'],
) as dag:
        date = "{{ ds }}"
        for i in range(10):
                t_bash = BashOperator(
                        task_id="echo_" + str(i),
                        bash_command='f"echo ' + str(i) + '"',
                        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
                        env={"DATA_INTERVAL_START": date},
                )
                if i == 0:
                        t = t_bash
                else:
                        t >> t_bash
                        t = t_bash

        dag.doc_md = __doc__

        def print_str(task_number):
                print("task number is:", task_number)

        for j in range(10, 30):
                t_pth = PythonOperator(
                        task_id='print_str_' + str(j),
                        python_callable=print_str,
                        op_kwargs={'task_number': j},
                )
                if j == 0:
                        t = t_bash
                else:
                        t >> t_pth
                        t = t_pth
