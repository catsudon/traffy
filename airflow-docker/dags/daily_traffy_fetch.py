from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_traffy_data(execution_date, **kwargs):
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = execution_date.strftime("%Y-%m-%d")

    url = f"https://publicapi.traffy.in.th/teamchadchart-stat-api/statistic-rank/top-rank-view?limit=10000&start={start_date}&end={end_date}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        filename = f"/tmp/traffy_{start_date}.json"
        with open(filename, "w") as f:
            json.dump(data, f)
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

with DAG(
    dag_id="traffy_daily_fetch",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval='@daily',
    catchup=False,
    description="Fetches daily data from Traffy API",
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_traffy_data',
        python_callable=fetch_traffy_data,
        op_kwargs={"execution_date": "{{ ds | ds_to_date }}"},
    )
