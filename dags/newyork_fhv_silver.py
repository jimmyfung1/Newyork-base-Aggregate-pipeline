import json
import os
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"
INPUT_FILE = "newyork_fhv_base_aggregate_raw.json"
OUTPUT_FILE = "newyork_fhv_base_aggregate_cleaned.json"


def clean_and_transform():
    """Clean and transform Bronze data → Silver Layer."""
    os.makedirs(SILVER_PATH, exist_ok=True)

    input_path = os.path.join(BRONZE_PATH, INPUT_FILE)
    print(f"[Silver] Reading from: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"[Silver] Raw records: {len(data)}")

    cleaned = []
    for row in data:
        # Skip rows missing critical fields
        if not row.get("base_number") or not row.get("year") or not row.get("month"):
            continue

        record = {
            # Identity
            "base_number": str(row.get("base_number", "")).strip().upper(),
            "base_name": str(row.get("base_name", "")).strip().title(),
            # Time
            "year": int(row["year"]),
            "month": int(row["month"]),
            "year_month": f"{int(row['year'])}-{int(row['month']):02d}",
            # Metrics — cast to int safely
            "total_dispatched_trips": int(float(row.get("total_dispatched_trips") or 0)),
            "total_dispatched_shared_trips": int(float(row.get("total_dispatched_shared_trips") or 0)),
            "unique_dispatched_vehicles": int(float(row.get("unique_dispatched_vehicles") or 0)),
            # Metadata
            "ingested_at": dt.utcnow().isoformat(),
        }
        cleaned.append(record)

    # Remove duplicates: keep last occurrence per base_number + year + month
    seen = {}
    for record in cleaned:
        key = (record["base_number"], record["year"], record["month"])
        seen[key] = record
    deduped = list(seen.values())

    print(f"[Silver] After cleaning   : {len(cleaned)}")
    print(f"[Silver] After dedup      : {len(deduped)}")
    print(f"[Silver] Removed (dupes)  : {len(cleaned) - len(deduped)}")

    output_path = os.path.join(SILVER_PATH, OUTPUT_FILE)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, ensure_ascii=False, indent=2)

    print(f"[Silver] Saved to: {output_path}")
    return len(deduped)


with DAG(
    dag_id="newyork_fhv_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Clean & transform FHV Base Aggregate data → Silver Layer",
    tags=["newyork", "fhv", "silver"],
) as dag:

    transform = PythonOperator(
        task_id="clean_and_transform",
        python_callable=clean_and_transform,
    )

    transform