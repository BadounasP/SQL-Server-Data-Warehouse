"""
Airflow Environment Smoke Test

Purpose:
    Verify that the Airflow scheduler and worker
    can execute a Python task inside Docker.

No database connections or warehouse tables
are modified by this test.
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="warehouse_environment_check",
    description="Verify the Docker-based Airflow environment",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "setup"],
)
def warehouse_environment_check():

    @task
    def verify_airflow():
        print("Apache Airflow is running successfully!")
        print("Docker task execution verified.")
        return "SUCCESS"

    verify_airflow()


warehouse_environment_check()
