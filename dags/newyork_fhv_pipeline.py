from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="newyork_fhv_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
    description="Main pipeline for NYC FHV Base Aggregate Report (Medallion Architecture)",
    tags=["newyork", "fhv", "tlc", "pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="newyork_fhv_ingestion",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_quality_check = TriggerDagRunOperator(
        task_id="trigger_quality_check",
        trigger_dag_id="newyork_fhv_quality_check",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="newyork_fhv_silver",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="newyork_fhv_gold",
        wait_for_completion=True,
        poke_interval=10,
    )

    start >> trigger_ingestion >> trigger_quality_check >> trigger_silver >> trigger_gold >> end