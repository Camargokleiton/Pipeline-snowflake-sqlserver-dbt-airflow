FROM apache/airflow:3.3.2

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.13

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends unixodbc-dev \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /tmp/airflow-requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/airflow-requirements.txt
