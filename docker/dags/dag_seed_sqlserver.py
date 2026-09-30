from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dag_seed_sqlserver",
    description="Gera dados ficticios no SQL Server a cada 20 minutos",
    start_date=datetime(2026, 9, 28),
    schedule="*/20 * * * *",
    catchup=False,
    default_args={
        "owner": "data_engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["sqlserver", "seed", "fake-data"],
) as dag:
    generate_fake_data = BashOperator(
        task_id="generate_fake_data",
        bash_command=(
            "python /opt/airflow/data_font/fake_ingest_sqlserver_database/"
            "seed_database_sqlserver.py"
        ),
    )

    trigger_bronze_ingestion = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="dag_sqlserver_to_snowflake_bronze",
    )

    generate_fake_data >> trigger_bronze_ingestion