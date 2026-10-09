import logging
import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator


LOGGER = logging.getLogger(__name__)
PROJECT_DIR = Path("/opt/airflow/dbt_ecommerce")
SNOWFLAKE_CONN_ID = "snowflake"
DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"


def run_dbt_snapshots() -> None:
    connection = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = connection.extra_dejson
    account = extra.get("account") or connection.host
    warehouse = extra.get("warehouse")
    role = extra.get("role")

    required_fields = {
        "account": account,
        "username": connection.login,
        "password": connection.password,
        "warehouse": warehouse,
        "role": role,
    }
    missing_fields = [name for name, value in required_fields.items() if not value]
    if missing_fields:
        raise ValueError(
            f"Airflow connection '{SNOWFLAKE_CONN_ID}' is missing required "
            f"Snowflake settings: {', '.join(missing_fields)}."
        )

    profile = {
        "dbt_ecommerce": {
            "target": "airflow",
            "outputs": {
                "airflow": {
                    "type": "snowflake",
                    "account": "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}",
                    "user": connection.login,
                    "password": "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}",
                    "role": role,
                    "database": "ERP_DATABASE",
                    "warehouse": warehouse,
                    "schema": "BRONZE",
                    "threads": 4,
                }
            },
        }
    }
    environment = os.environ.copy()
    environment["DBT_SNOWFLAKE_ACCOUNT"] = account
    environment["DBT_SNOWFLAKE_PASSWORD"] = connection.password

    with tempfile.TemporaryDirectory(prefix="dbt-snapshot-") as temp_dir:
        profiles_dir = Path(temp_dir) / "profiles"
        profiles_dir.mkdir()
        (profiles_dir / "profiles.yml").write_text(
            yaml.safe_dump(profile, sort_keys=False),
            encoding="utf-8",
        )

        command = [
            DBT_EXECUTABLE,
            "snapshot",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(Path(temp_dir) / "target"),
            "--log-path",
            str(Path(temp_dir) / "logs"),
            "--no-use-colors",
        ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

    if result.stdout:
        LOGGER.info("dbt snapshot output:\n%s", result.stdout)
    if result.stderr:
        LOGGER.info("dbt snapshot diagnostics:\n%s", result.stderr)
    if result.returncode:
        raise RuntimeError(
            f"dbt snapshot failed with exit code {result.returncode}; "
            "see task logs for dbt output."
        )


with DAG(
    dag_id="dag_dbt_snapshots",
    description="Capture changed customer and product versions in Snowflake with dbt snapshots.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["dbt", "snowflake", "snapshots", "scd-type-2"],
) as dag:
    snapshot_dimensions = PythonOperator(
        task_id="snapshot_customer_and_product_changes",
        python_callable=run_dbt_snapshots,
    )
