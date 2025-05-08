from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging  # <-- import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_traffy_data(**kwargs):
    logger = logging.getLogger("airflow.task")  # Optional: named logger
    execution_date = datetime.strptime(kwargs['templates_dict']['execution_date'], "%Y-%m-%d")
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = execution_date.strftime("%Y-%m-%d")

    logger.info(f"Fetching data from {start_date} to {end_date}")

    # Ensure the directory exists
    output_dir = "/opt/airflow/data"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Ensured directory exists: {output_dir}")

    url = (
        f"https://publicapi.traffy.in.th/teamchadchart-stat-api/statistic-rank/top-rank-view"
        f"?limit=10000&start={start_date}&end={end_date}"
    )
    logger.info(f"Requesting URL: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()  # raises HTTPError if not 200
        data = response.json()
        filename = f"{output_dir}/traffy_{start_date}.json"
        with open(filename, "w") as f:
            json.dump(data, f)
        logger.info(f"Successfully wrote data to {filename}")
    except Exception as e:
        logger.error(f"Error fetching or saving data: {e}")
        raise

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
        templates_dict={'execution_date': '{{ ds }}'},
        provide_context=True
    )
