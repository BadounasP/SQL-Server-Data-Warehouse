"""
SQL Server Audit Connection Test

Verifies that an Airflow task can:
1. Retrieve the warehouse_audit connection.
2. Connect to SQL Server using ODBC Driver 18.
3. Insert a test event into audit.pipeline_run_events.

This DAG does not modify Bronze, Silver, or Gold data.
"""

import os
import subprocess

import pendulum

from airflow.sdk import Connection, dag, get_current_context, task


@dag(
    dag_id="warehouse_audit_connection_test",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "audit", "testing"],
)
def warehouse_audit_connection_test():

    @task(retries=0)
    def write_audit_test():

        context = get_current_context()
        connection = Connection.get("warehouse_audit")

        if not all((
            connection.host,
            connection.login,
            connection.password
        )):
            raise ValueError(
                "The warehouse_audit connection is incomplete."
            )

        environment = os.environ.copy()

        environment.update({
            "AUDIT_SQL_HOST": connection.host,
            "AUDIT_SQL_PORT": str(connection.port or 14333),
            "AUDIT_SQL_DATABASE": (
                connection.schema or "DataWarehouse"
            ),
            "AUDIT_SQL_USER": connection.login,
            "AUDIT_SQL_PASSWORD": connection.password,
            "AUDIT_DAG_ID": context["dag_run"].dag_id,
            "AUDIT_RUN_ID": context["dag_run"].run_id,
        })

        # Execute SQL with the dedicated dbt Python environment,
        # which already contains pyodbc.
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
        "SUCCESS",
        "write_audit_test",
        "Audit connection smoke test - not a warehouse run",
    )

    print("Audit event inserted successfully.")

finally:
    database.close()
'''

        subprocess.run(
            ["/opt/dbt-venv/bin/python", "-c", sql_runner],
            env=environment,
            check=True,
            timeout=60,
        )

        print("Warehouse audit connection test completed.")

    write_audit_test()


warehouse_audit_connection_test()
