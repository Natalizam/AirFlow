from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


with DAG(
    "task_12",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    def choose_course():
        return (
            "startml_desc"
            if Variable.get("is_startml") == "True"
            else "not_startml_desc"
        )

    branching = BranchPythonOperator(
        task_id="choose_course",
        python_callable=choose_course,
    )

    def message_printer(message, **kwargs):
        print(message)

    start_ml = PythonOperator(
        task_id="startml_desc",
        python_callable=message_printer,
        op_kwargs={"message": "StartML is a starter course for ambitious people"},
        dag=dag,
    )
    not_start_ml = PythonOperator(
        task_id="not_startml_desc",
        python_callable=message_printer,
        op_kwargs={"message": "Not a startML course, sorry"},
        dag=dag,
    )
    end = DummyOperator(task_id="end")

    start >> branching >> [start_ml, not_start_ml] >> end
