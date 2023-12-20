from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from flights_etl import run_flights_etl

default_args = {
    "owner": "matsouto",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 20),
    "email": ["matsouto55@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "flight_dag",
    default_args=default_args,
    description="Flight data ETL for Embraer aircrafts",
)

run_etl = PythonOperator(
    task_id="complete_flights_etl",
    python_callable=run_flights_etl,
    dag=dag,
)

run_etl
