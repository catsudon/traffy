from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("👋 Hello from Airflow inside Docker!")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=greet,
    )
