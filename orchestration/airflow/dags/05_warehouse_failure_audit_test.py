"""
SQL Server Data Warehouse - isolated failure-auditing test.

Purpose:
    Confirm that a failed Airflow task produces a FAILED audit event in
    SQL Server and the overall Airflow DAG remains FAILED.

Safety:
    - This DAG never invokes the real ELT pipeline.
    - It does not modify Bronze, Silver, Gold, or the source CSVs.
    - It only inserts one FAILED event into audit.pipeline_run_events.
    - It intentionally shows as FAILED in the Airflow UI.

Usage:
    Trigger manually once, inspect its logs, then verify the audit event
    using an administrator SQL Server connection.
"""

import os
import subprocess

import pendulum
from airflow.sdk import Connection, dag, get_current_context, task


@dag(
    dag_id="warehouse_failure_audit_test",
    start_date=pendulum.datetime(2026, 11, 8, 0, 0, tz="Europe/Athens"),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "audit", "testing"],
)
def warehouse_failure_audit_test():
    @task(retries=0)
    def simulate_failure():
        """Fail intentionally; do not touch any warehouse data."""
        raise RuntimeError("INTENTIONAL TEST FAILURE: warehouse data was not changed")

    @task(retries=0, trigger_rule="one_failed")
    def record_failure_audit():
        """Write the FAILED audit event, then keep the test DAG failed."""
        context = get_current_context()
        connection = Connection.get("warehouse_audit")

        if not all((connection.host, connection.login, connection.password)):
            raise ValueError("warehouse_audit connection is incomplete")

        environment = os.environ.copy()
        environment.update(
            {
                "AUDIT_SQL_HOST": connection.host,
                "AUDIT_SQL_PORT": str(connection.port or 14333),
                "AUDIT_SQL_DATABASE": connection.schema or "DataWarehouse",
                "AUDIT_SQL_USER": connection.login,
                "AUDIT_SQL_PASSWORD": connection.password,
                "AUDIT_DAG_ID": context["dag_run"].dag_id,
                "AUDIT_RUN_ID": context["dag_run"].run_id,
            }
        )

        # Same parameterized audit-insert approach as the successful
        # warehouse_audit_connection_test DAG.
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
            dag_id, run_id, event_type, task_id, details
        ) VALUES (?, ?, ?, ?, ?)
        """,
        os.environ["AUDIT_DAG_ID"],
        os.environ["AUDIT_RUN_ID"],
        "FAILED",
        None,
        "Intentional isolated failure test; warehouse data not modified",
    )
finally:
    database.close()
'''

        subprocess.run(
            ["/opt/dbt-venv/bin/python", "-c", sql_runner],
            env=environment,
            check=True,
            timeout=60,
        )

        print("FAILED audit event inserted successfully.", flush=True)

        # Preserve the failed overall DAG result. This is intentional.
        raise RuntimeError("INTENTIONAL TEST: failure watcher completed its audit")

    failed_task = simulate_failure()
    audit_task = record_failure_audit()
    failed_task >> audit_task


warehouse_failure_audit_test()
