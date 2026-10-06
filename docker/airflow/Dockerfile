FROM apache/airflow:2.10.5-python3.11

# A C compiler: some pinned packages (e.g. lz4) have no prebuilt wheel for ARM (Apple Silicon).
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# dbt in its own virtualenv, so its dependencies never conflict with Airflow's.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 \
        dbt-clickhouse==1.10.3
USER airflow

# Our pipeline's packages, installed against the versions Airflow is tested with.
# Keep the pins in sync with pyproject.toml. pytest is only for the DAG integrity test.
RUN pip install --no-cache-dir \
        pandas==2.1.4 \
        clickhouse-connect==1.9.0 \
        kaggle==2.2.4 \
        pytest==8.3.4 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
