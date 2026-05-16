import json
import os
from collections import defaultdict
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"
INPUT_FILE = "newyork_fhv_base_aggregate_cleaned.json"
OUTPUT_FILE = "newyork_fhv_base_aggregate_metrics.json"


def build_gold_metrics():
    """Aggregate Silver data → Gold Layer metrics."""
    os.makedirs(GOLD_PATH, exist_ok=True)

    input_path = os.path.join(SILVER_PATH, INPUT_FILE)
    print(f"[Gold] Reading from: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"[Gold] Silver records: {len(data)}")

    # --- Metric: Total dispatched trips per base (all-time) ---
    trips_per_base = defaultdict(lambda: {
        "base_number": "",
        "base_name": "",
        "total_dispatched_trips": 0,
        "total_dispatched_shared_trips": 0,
        "unique_dispatched_vehicles": 0,
        "months_reported": 0,
    })

    for row in data:
        key = row["base_number"]
        trips_per_base[key]["base_number"] = row["base_number"]
        trips_per_base[key]["base_name"] = row["base_name"]
        trips_per_base[key]["total_dispatched_trips"] += row["total_dispatched_trips"]
        trips_per_base[key]["total_dispatched_shared_trips"] += row["total_dispatched_shared_trips"]
        trips_per_base[key]["unique_dispatched_vehicles"] = max(
            trips_per_base[key]["unique_dispatched_vehicles"],
            row["unique_dispatched_vehicles"],
        )
        trips_per_base[key]["months_reported"] += 1

    trips_per_base_list = sorted(
        trips_per_base.values(),
        key=lambda x: x["total_dispatched_trips"],
        reverse=True,
    )

    # --- Summary stats ---
    total_bases = len(trips_per_base_list)
    grand_total_trips = sum(b["total_dispatched_trips"] for b in trips_per_base_list)
    grand_total_shared = sum(b["total_dispatched_shared_trips"] for b in trips_per_base_list)

    metrics = {
        "generated_at": dt.utcnow().isoformat(),
        "summary": {
            "total_bases": total_bases,
            "grand_total_dispatched_trips": grand_total_trips,
            "grand_total_shared_trips": grand_total_shared,
            "total_records_processed": len(data),
        },
        "total_dispatched_trips_per_base": trips_per_base_list,
    }

    output_path = os.path.join(GOLD_PATH, OUTPUT_FILE)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # Log summary
    print(f"[Gold] Total bases        : {total_bases}")
    print(f"[Gold] Grand total trips  : {grand_total_trips:,}")
    print(f"[Gold] Grand total shared : {grand_total_shared:,}")
    print(f"[Gold] Saved to           : {output_path}")

    return metrics["summary"]


with DAG(
    dag_id="newyork_fhv_gold",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Build Gold metrics: Total dispatched trips per FHV base",
    tags=["newyork", "fhv", "gold"],
) as dag:

    aggregate = PythonOperator(
        task_id="build_gold_metrics",
        python_callable=build_gold_metrics,
    )

    aggregate