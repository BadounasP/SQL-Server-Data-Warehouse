# ============================================================
# Custom Apache Airflow Image
#
# Includes:
#   - Apache Airflow 3.4.0
#   - Microsoft ODBC Driver 18
#   - dbt Core + dbt-sqlserver
#
# dbt runs in a separate virtual environment so its Python
# dependencies do not interfere with Apache Airflow.
# ============================================================

FROM apache/airflow:3.4.0

# Install the Linux dependencies needed by SQL Server ODBC.
USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unixodbc-dev \
        libgssapi-krb5-2; \
    curl -fsSL \
        https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb \
        -o /tmp/packages-microsoft-prod.deb; \
    dpkg -i /tmp/packages-microsoft-prod.deb; \
    rm /tmp/packages-microsoft-prod.deb; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18; \
    rm -rf /var/lib/apt/lists/*

# Create an isolated Python environment for dbt.
RUN python -m venv /opt/dbt-venv \
    && chown -R airflow:0 /opt/dbt-venv

# Run Python package installation as the Airflow user.
USER airflow

RUN /opt/dbt-venv/bin/python -m pip install \
    --no-cache-dir \
    "dbt-sqlserver[pyodbc]==1.12.2"
