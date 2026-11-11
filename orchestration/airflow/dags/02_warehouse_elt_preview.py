"""
SQL Server Data Warehouse — Airflow ELT Preview

Orchestrates:
    1. Bronze source validation
    2. Silver staging models
    3. Customer key synchronization
    4. Product key synchronization
    5. Gold dimensional models
    6. dbt data quality tests

Bronze ingestion will be integrated separately
after its database permissions are configured.
"""

import os
import subprocess
import tempfile

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Connection, dag, task


DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/warehouse/dbt"
DBT_PROFILES = "/opt/warehouse/profiles"


@dag(
    dag_id="warehouse_elt_pipeline_preview",
    description="Orchestrate SQL Server and dbt warehouse transformations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "elt", "dbt", "sqlserver"],
)
def warehouse_elt_pipeline_preview():

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def execute_dbt(command: list[str]):

        # Retrieve the SQL Server password during execution,
        # never when Airflow imports the DAG file.
        connection = Connection.get("warehouse_sqlserver")

        if not connection.password:
            raise ValueError("Missing SQL Server password.")

        environment = os.environ.copy()

        environment["DBT_ENV_SECRET_SQL_PASSWORD"] = (
            connection.password
        )

        # dbt's generated files must not be written
        # to our read-only project mount.
        with tempfile.TemporaryDirectory(
            prefix="warehouse_dbt_"
        ) as temporary_directory:

            temporary = Path(temporary_directory)

            environment["DBT_ENGINE_TARGET_PATH"] = str(
                temporary / "target"
            )

            environment["DBT_ENGINE_LOG_PATH"] = str(
                temporary / "logs"
            )

            dbt_command = [
                DBT_EXECUTABLE,
                *command,
                "--project-dir", DBT_PROJECT,
                "--profiles-dir", DBT_PROFILES,
            ]

            print("Running dbt operation:", " ".join(command))

            # A failed dbt process raises an exception,
            # allowing Airflow to mark the task as failed.
            subprocess.run(
                dbt_command,
                env=environment,
                check=True,
                timeout=900,
            )

        print("dbt operation completed successfully.")

    # STEP 1: Verify Bronze source data.
    bronze = execute_dbt.override(
        task_id="verify_bronze"
    )(["run-operation", "assert_bronze_ready"])

    # STEP 2: Rebuild the Silver staging views.
    silver = execute_dbt.override(
        task_id="build_silver"
    )(["run", "--select", "path:models/staging"])

    # STEP 3: Preserve customer surrogate keys.
    customer_keys = execute_dbt.override(
        task_id="sync_customer_keys"
    )(["run-operation", "sync_customer_keys"])

    # STEP 4: Preserve product surrogate keys.
    product_keys = execute_dbt.override(
        task_id="sync_product_keys"
    )(["run-operation", "sync_product_keys"])

    # STEP 5: Build the Gold star schema.
    gold = execute_dbt.override(
        task_id="build_gold"
    )(["run", "--select", "path:models/marts"])

    # STEP 6: Validate warehouse quality.
    quality = execute_dbt.override(
        task_id="run_quality_tests"
    )(["test"])

    # Explicit task dependencies establish the DAG.
    (
        bronze
        >> silver
        >> customer_keys
        >> product_keys
        >> gold
        >> quality
    )


warehouse_elt_pipeline_preview()
