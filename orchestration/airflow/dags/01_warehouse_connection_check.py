"""
Data Warehouse — Containerized dbt Connection Test

Purpose:
    Verify that an actual Airflow task can:
    1. Retrieve SQL Server credentials securely.
    2. Access the mounted dbt project.
    3. Execute dbt from its Linux virtual environment.
    4. Authenticate to the Windows-hosted DataWarehouse.

This DAG does not modify warehouse data.
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="warehouse_dbt_connection_check",
    description="Validate SQL Server connectivity through dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "dbt", "sqlserver"],
)
def warehouse_dbt_connection_check():

    @task(retries=1)
    def verify_dbt_connection():

        import os
        import subprocess
        import tempfile
        from pathlib import Path
        from airflow.sdk import Connection

        # Retrieve credentials only during task execution.
        connection = Connection.get("warehouse_sqlserver")

        if not connection.password:
            raise ValueError("SQL Server password is missing.")

        # Pass the secret to the dbt subprocess.
        environment = os.environ.copy()
        environment["DBT_ENV_SECRET_SQL_PASSWORD"] = (
            connection.password
        )

        # Use temporary directories for generated dbt files.
        with tempfile.TemporaryDirectory(prefix="dbt_check_") as tmp:

            environment["DBT_ENGINE_TARGET_PATH"] = str(
                Path(tmp) / "target"
            )

            environment["DBT_ENGINE_LOG_PATH"] = str(
                Path(tmp) / "logs"
            )

            command = [
                "/opt/dbt-venv/bin/dbt",
                "debug",
                "--project-dir",
                "/opt/warehouse/dbt",
                "--profiles-dir",
                "/opt/warehouse/profiles",
            ]

            subprocess.run(
                command,
                env=environment,
                check=True,
                timeout=120,
            )

        print("Airflow-to-dbt connection verified successfully.")

    verify_dbt_connection()


warehouse_dbt_connection_check()
