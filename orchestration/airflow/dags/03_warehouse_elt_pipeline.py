"""
===============================================================================
SQL Server Data Warehouse - End-to-End Airflow ELT Pipeline
===============================================================================

Purpose:
    Orchestrate a complete Data Engineering ELT pipeline
    using Apache Airflow, Docker, SQL Server, and dbt Core.

Pipeline Tasks:
    1. stage_source_files
       Validate and synchronize CRM and ERP source CSV files
       into the SQL Server ingestion directory.

    2. load_bronze
       Execute the SQL Server stored procedure to load
       the six raw source datasets into the Bronze layer.

    3. verify_bronze
       Validate Bronze source data using dbt.

    4. build_silver
       Execute dbt staging models for data cleaning,
       standardization, and transformation.

    5. sync_customer_keys
       Synchronize persistent customer surrogate keys.

    6. sync_product_keys
       Synchronize persistent product surrogate keys.

    7. build_gold
       Build the customer and product dimensions
       and the sales fact model using dbt.

    8. run_quality_tests
       Execute dbt data quality tests to validate
       the transformed warehouse data.

Monitoring Tasks:
    9. record_success_audit
       Write a SUCCESS event only after the eight ELT tasks pass.

   10. record_failure_audit
       On an ELT failure, attempt to write a FAILED event and
       preserve the failed DAG run status.

Architecture:
    CRM/ERP CSV Sources
           |
           v
    Bronze Layer (SQL Server)
           |
           v
    Silver Layer (dbt)
           |
           v
    Gold Dimensional Models (dbt)
           |
           v
    Data Quality Validation

Orchestration:
    Apache Airflow 3.4 running through Docker Compose.

Scheduling:
    Daily at 13:30 (Europe/Athens).

Monitoring:
    DAG-level success and failure callbacks.
    Task-level failure callbacks.

===============================================================================
"""


import os
import subprocess
import tempfile

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Connection, dag, task

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable

import logging

DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/warehouse/dbt"
DBT_PROFILES = "/opt/warehouse/profiles"


# =====================================================
# Warehouse Pipeline Monitoring Callbacks
# =====================================================

def log_pipeline_success(context):
    """Record a successful warehouse DAG execution."""

    dag_run = context["dag_run"]

    logging.getLogger("warehouse.audit").info(
        "WAREHOUSE_RUN_SUCCESS | dag=%s | run=%s",
        dag_run.dag_id,
        dag_run.run_id,
    )


def log_pipeline_failure(context):
    """Record a failed warehouse DAG execution."""

    dag_run = context["dag_run"]

    logging.getLogger("warehouse.audit").error(
        "WAREHOUSE_RUN_FAILED | dag=%s | run=%s",
        dag_run.dag_id,
        dag_run.run_id,
    )


def log_task_failure(context):
    """Identify the task responsible for a failure."""

    task_instance = context["task_instance"]

    logging.getLogger("warehouse.audit").error(
        "WAREHOUSE_TASK_FAILED | dag=%s | task=%s | run=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
    )


def write_sql_audit_event(event_type, details):
    """
    Insert an execution event using the restricted audit account.

    This function must be called from an Airflow task,
    not directly from a DAG callback.
    """

    import logging
    import os
    import subprocess

    from airflow.sdk import Connection, get_current_context

    logger = logging.getLogger("warehouse.audit")

    try:
        context = get_current_context()
        connection = Connection.get("warehouse_audit")

        if not all((
            connection.host,
            connection.login,
            connection.password,
        )):
            raise ValueError("Incomplete warehouse_audit connection")

        environment = os.environ.copy()
        environment.update({
            "AUDIT_SQL_HOST": connection.host,
            "AUDIT_SQL_PORT": str(connection.port or 14333),
            "AUDIT_SQL_DATABASE": connection.schema or "DataWarehouse",
            "AUDIT_SQL_USER": connection.login,
            "AUDIT_SQL_PASSWORD": connection.password,
            "AUDIT_DAG_ID": context["dag_run"].dag_id,
            "AUDIT_RUN_ID": context["dag_run"].run_id,
            "AUDIT_EVENT_TYPE": event_type,
            "AUDIT_DETAILS": details,
        })

        sql_runner = r'''
import os
import pyodbc

def escape_odbc(value):
    return "{" + str(value).replace("}", "}}") + "}"

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{os.environ['AUDIT_SQL_HOST']},"
    f"{os.environ['AUDIT_SQL_PORT']};"
    f"DATABASE={escape_odbc(os.environ['AUDIT_SQL_DATABASE'])};"
    f"UID={escape_odbc(os.environ['AUDIT_SQL_USER'])};"
    f"PWD={escape_odbc(os.environ['AUDIT_SQL_PASSWORD'])};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

database = pyodbc.connect(
    connection_string,
    autocommit=True,
    timeout=20,
)

try:
    cursor = database.cursor()

    cursor.execute(
        """
        INSERT INTO audit.pipeline_run_events (
            dag_id,
            run_id,
            event_type,
            task_id,
            details
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        os.environ["AUDIT_DAG_ID"],
        os.environ["AUDIT_RUN_ID"],
        os.environ["AUDIT_EVENT_TYPE"],
        None,
        os.environ["AUDIT_DETAILS"],
    )

finally:
    database.close()
'''

        subprocess.run(
            ["/opt/dbt-venv/bin/python", "-c", sql_runner],
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )

        logger.info(
            "WAREHOUSE_SQL_AUDIT_WRITTEN | event=%s | run=%s",
            event_type,
            context["dag_run"].run_id,
        )

    except Exception:
        # Keep the root cause in Airflow logs, then fail the audit task.
        # A missing SQL audit record must not appear as a successful task.
        logger.exception(
            "WAREHOUSE_SQL_AUDIT_WRITE_FAILED | event=%s",
            event_type,
        )
        raise


@dag(
    dag_id="warehouse_elt_pipeline",

    description=(
        "End-to-end SQL Server ELT pipeline orchestrated "
        "with Apache Airflow and dbt"
    ),

    # Allow manual executions starting November 8.
    start_date=pendulum.datetime(
        2026, 11, 8, 14, 0,
        tz="Europe/Athens"
    ),

    # Run every day at 13:30 Athens time.
    schedule=CronTriggerTimetable(
        "30 13 * * *",
        timezone="Europe/Athens"
    ),

    # Do not create a backlog of historical runs.
    catchup=False,

    # Prevent overlapping runs of this DAG.
    max_active_runs=1,

    tags=[
        "data-engineering",
        "elt",
        "dbt",
        "sqlserver",
        "scheduled"
    ],
    on_success_callback=log_pipeline_success,
    on_failure_callback=log_pipeline_failure,
    default_args={
        "on_failure_callback": log_task_failure,
    },
)
def warehouse_elt_pipeline():

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

    @task(retries=0)
    def load_bronze():
        """
        Execute the SQL Server Bronze ingestion procedure.

        Uses the dedicated airflow_ingest connection.
        The procedure manages its own SQL transaction.
        """

        import os
        import subprocess
        from airflow.sdk import Connection

        # Retrieve the separate ingestion credentials.
        connection = Connection.get("warehouse_ingestion")

        if not all((
            connection.host,
            connection.login,
            connection.password,
        )):
            raise ValueError(
                "The warehouse_ingestion connection is incomplete."
            )

        # Pass credentials only to the child process.
        environment = os.environ.copy()
        environment.update({
            "INGEST_SQL_HOST": connection.host,
            "INGEST_SQL_PORT": str(connection.port or 14333),
            "INGEST_SQL_DATABASE": (
                connection.schema or "DataWarehouse"
            ),
            "INGEST_SQL_USER": connection.login,
            "INGEST_SQL_PASSWORD": connection.password,
        })

        # pyodbc is installed in the separate dbt virtual environment.
        # Keep the Python code independent of Airflow packages.
        sql_runner = r'''
import os
import pyodbc

def odbc_escape(value):
    return "{" + value.replace("}", "}}") + "}"

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{os.environ['INGEST_SQL_HOST']},"
    f"{os.environ['INGEST_SQL_PORT']};"
    f"DATABASE={odbc_escape(os.environ['INGEST_SQL_DATABASE'])};"
    f"UID={odbc_escape(os.environ['INGEST_SQL_USER'])};"
    f"PWD={odbc_escape(os.environ['INGEST_SQL_PASSWORD'])};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

# The stored procedure manages BEGIN/COMMIT/ROLLBACK.

database = pyodbc.connect(
    connection_string,
    autocommit=True,
    timeout=20,
)

# Apply the SQL query timeout to the connection.
database.timeout = 300

try:
    cursor = database.cursor()

    cursor.execute("EXEC bronze.load_bronze;")


    # Consume all result sets, including the duration result.
    # SQL errors propagate instead of being silently ignored.
    while True:
        if cursor.description is not None:
            for row in cursor.fetchall():
                print("Procedure result:", tuple(row))

        if not cursor.nextset():
            break

    print("Bronze ingestion completed successfully.")

finally:
    database.close()
'''

        subprocess.run(
            ["/opt/dbt-venv/bin/python", "-c", sql_runner],
            env=environment,
            check=True,
            timeout=450,
        )


    @task(retries=1)
    def stage_source_files():
        """
        Synchronize the six source CSVs into the SQL Server
        ingestion directory before Bronze loading.

        - Validate every source file before copying.
        - Skip identical files.
        - Copy changed files using temporary filenames.
        - Never modify the original source datasets.
        """

        import csv
        import hashlib
        import os
        import shutil
        import uuid
        from pathlib import Path

        source_root = Path("/opt/warehouse/datasets")
        ingest_root = Path("/opt/warehouse/ingest")

        files = [
            "source_crm/cust_info.csv",
            "source_crm/prd_info.csv",
            "source_crm/sales_details.csv",
            "source_erp/CUST_AZ12.csv",
            "source_erp/LOC_A101.csv",
            "source_erp/PX_CAT_G1V2.csv",
        ]

        def trailing_newline_bytes(path):
            """Return bytes needed for a terminated CSV last record."""
            with path.open("rb") as file:
                file.seek(-1, os.SEEK_END)
                last_byte = file.read(1)

            if last_byte == b"\n":
                return b""
            if last_byte == b"\r":
                return b"\n"
            return b"\r\n"

        def file_hash(path, suffix=b""):
            """Hash file content plus optional virtual trailing bytes."""
            digest = hashlib.sha256()

            with path.open("rb") as file:
                for chunk in iter(
                    lambda: file.read(1024 * 1024), b""
                ):
                    digest.update(chunk)

            digest.update(suffix)
            return digest.hexdigest()

        # Validate all source files before replacing any.
        for filename in files:
            source = source_root / filename

            if not source.is_file():
                raise FileNotFoundError(
                    f"Source CSV missing: {source}"
                )

            if source.stat().st_size == 0:
                raise ValueError(
                    f"Source CSV is empty: {source}"
                )

            with source.open(
                "r", encoding="utf-8-sig", newline=""
            ) as handle:
                reader = csv.reader(handle)

                if not next(reader, None):
                    raise ValueError(
                        f"CSV header missing: {source}"
                    )

                if not next(reader, None):
                    raise ValueError(
                        f"CSV has no data rows: {source}"
                    )

        # Copy only changed files.
        for filename in files:
            source = source_root / filename
            destination = ingest_root / filename

            destination.parent.mkdir(
                parents=True, exist_ok=True
            )

            # Compare against the exact staged bytes (including a
            # virtual final newline) to keep subsequent runs idempotent.
            final_newline = trailing_newline_bytes(source)
            expected_hash = file_hash(source, final_newline)

            if (
                destination.is_file()
                and expected_hash == file_hash(destination)
            ):
                print(f"UNCHANGED: {filename}", flush=True)
                continue

            temporary = destination.with_name(
                f".{destination.name}.{uuid.uuid4().hex}.tmp"
            )

            try:
                shutil.copyfile(source, temporary)

                # Normalize only the staged copy. SQL Server BULK INSERT
                # must be able to recognize the final CSV record.
                if final_newline:
                    with temporary.open("ab") as handle:
                        handle.write(final_newline)

                if expected_hash != file_hash(temporary):
                    raise RuntimeError(
                        f"Copy verification failed: {filename}"
                    )

                os.replace(temporary, destination)

                print(f"UPDATED: {filename}", flush=True)

            finally:
                if temporary.exists():
                    temporary.unlink()

        print(
            "All six source files synchronized successfully.",
            flush=True,
        )

    
    # =====================================================
    # SQL Server Audit Tasks (defined inside the DAG)
    # =====================================================
    @task(retries=0)
    def record_success_audit():
        """Persist a SUCCESS event after all eight ELT steps pass."""
        write_sql_audit_event(
            event_type="SUCCESS",
            details="All eight warehouse ELT tasks completed successfully",
        )

    @task(retries=0, trigger_rule="one_failed")
    def record_failure_audit():
        """Persist a FAILED event if an upstream ELT step fails."""
        write_sql_audit_event(
            event_type="FAILED",
            details="At least one warehouse ELT task failed",
        )
        # Keep this as a failed leaf task: otherwise an upstream
        # data failure could be hidden by a successful watcher.
        raise RuntimeError(
            "Warehouse ELT failed: at least one upstream task failed."
        )

    # STEP 1: Load the Bronze layer.
    stage = stage_source_files()
    ingest = load_bronze()

    # STEP 2: Verify Bronze source data.
    bronze = execute_dbt.override(
        task_id="verify_bronze"
    )(["run-operation", "assert_bronze_ready"])

    # STEP 3: Build the Silver staging models.
    silver = execute_dbt.override(
        task_id="build_silver"
    )(["run", "--select", "path:models/staging"])

    # STEP 4: Synchronize customer surrogate keys.
    customer_keys = execute_dbt.override(
        task_id="sync_customer_keys"
    )(["run-operation", "sync_customer_keys"])

    # STEP 5: Synchronize product surrogate keys.
    product_keys = execute_dbt.override(
        task_id="sync_product_keys"
    )(["run-operation", "sync_product_keys"])

    # STEP 6: Build the Gold dimensional models.
    gold = execute_dbt.override(
        task_id="build_gold"
    )(["run", "--select", "path:models/marts"])

    # STEP 7: Execute dbt data quality tests.
    quality = execute_dbt.override(
        task_id="run_quality_tests"
    )(["test"])

    # Define the complete dependency chain.
    
    (
        stage
        >> ingest
        >> bronze
        >> silver
        >> customer_keys
        >> product_keys
        >> gold
        >> quality
    )


    # Create audit tasks.
    success_audit = record_success_audit()
    failure_audit = record_failure_audit()

    # Record SUCCESS only after all warehouse processing
    # and quality validation have completed.
    quality >> success_audit

    # Watch every warehouse task for failures.
    # A failure anywhere in the ELT chain triggers
    # the dedicated failure audit task.
    for pipeline_task in (
        stage,
        ingest,
        bronze,
        silver,
        customer_keys,
        product_keys,
        gold,
        quality,
    ):
        pipeline_task >> failure_audit


warehouse_elt_pipeline()
