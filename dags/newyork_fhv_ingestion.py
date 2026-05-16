import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

BRONZE_PATH = "/opt/airflow/data/bronze"
API_URL = "https://data.cityofnewyork.us/resource/2v9c-2k7f.json"
OUTPUT_FILE = "newyork_fhv_base_aggregate_raw.json"


def ingest_data():
    """Fetch FHV Base Aggregate data from NYC Open Data API and save to Bronze layer."""
    os.makedirs(BRONZE_PATH, exist_ok=True)

    print(f"[Ingestion] Fetching data from: {API_URL}")

    params = {"$limit": 50000}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    print(f"[Ingestion] Records fetched: {len(data)}")

    output_path = os.path.join(BRONZE_PATH, OUTPUT_FILE)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[Ingestion] Saved to: {output_path}")
    return len(data)


with DAG(
    dag_id="newyork_fhv_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Ingest FHV Base Aggregate data from NYC Open Data API → Bronze Layer",
    tags=["newyork", "fhv", "bronze", "ingestion"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_fhv_base_aggregate",
        python_callable=ingest_data,
    )

    ingest