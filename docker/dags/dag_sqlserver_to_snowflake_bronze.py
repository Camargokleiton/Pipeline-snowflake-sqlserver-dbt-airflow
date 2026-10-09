import os
import uuid
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

TABLES_CONFIG = {
    "customers": "id_customer",
    "categories": "id_category",
    "products": "id_product",
    "orders": "id_order",
    "order_items": "id_item",
    "payments": "id_payment",
}

TEMP_DIR = "/tmp/parquet_ingestion"
SNOWFLAKE_DATABASE = "ERP_DATABASE"
BRONZE_SCHEMA = "BRONZE"
SNOWFLAKE_CONN_ID = "snowflake"
SOURCE_SQL_TYPES = {
    "bigint": "NUMBER",
    "int": "NUMBER",
    "smallint": "NUMBER",
    "tinyint": "NUMBER",
    "decimal": "NUMBER",
    "numeric": "NUMBER",
    "money": "NUMBER",
    "smallmoney": "NUMBER",
    "float": "FLOAT",
    "real": "FLOAT",
    "bit": "BOOLEAN",
    "date": "TIMESTAMP_NTZ",
    "datetime": "TIMESTAMP_NTZ",
    "datetime2": "TIMESTAMP_NTZ",
    "smalldatetime": "TIMESTAMP_NTZ",
    "time": "TIME",
    "binary": "BINARY",
    "varbinary": "BINARY",
    "image": "BINARY",
}


def _serialize_timestamps_for_parquet(
    dataframe: pd.DataFrame,
    snowflake_types: dict[str, str],
) -> None:
    for column, snowflake_type in snowflake_types.items():
        if snowflake_type == "TIMESTAMP_NTZ":
            values = pd.to_datetime(dataframe[column], errors="raise")
            if values.dt.tz is not None:
                values = values.dt.tz_convert("UTC").dt.tz_localize(None)
            dataframe[column] = values.dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def _snowflake_type_from_sql_type(
    sql_type: str,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
) -> str:
    normalized_type = sql_type.lower()
    if normalized_type in {"decimal", "numeric"} and numeric_precision is not None:
        return f"NUMBER({numeric_precision}, {numeric_scale or 0})"
    if normalized_type == "money":
        return "NUMBER(19, 4)"
    if normalized_type == "smallmoney":
        return "NUMBER(10, 4)"
    return SOURCE_SQL_TYPES.get(normalized_type, "VARCHAR")


def create_snowflake_bronze_target():
    """Create Snowflake targets and enable SQL Server change tracking."""
    from data_font.fake_ingest_sqlserver_database import seed_database_sqlserver as seed_module

    seed_module.ensure_schema()
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}")
        cursor.execute(
            f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.BRONZE_PARQUET_STAGE"
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.INGESTION_WATERMARKS (
                TABLE_NAME VARCHAR NOT NULL,
                LAST_SYNC_VERSION NUMBER(38, 0) NOT NULL
            )
            """
        )

        mssql_hook = MsSqlHook(mssql_conn_id="mssql_default")
        for table_name in TABLES_CONFIG:
            columns = mssql_hook.get_records(
                f"""
                SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
                """
            )
            if not columns:
                continue

            columns_sql = ", ".join(
                f'"{column_name.lower()}" {_snowflake_type_from_sql_type(sql_type, precision, scale)}'
                for column_name, sql_type, precision, scale in columns
            )
            raw_table_name = f"RAW_{table_name.upper()}"
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.{raw_table_name} '
                f'({columns_sql}, "_extracted_at" TIMESTAMP_NTZ)'
            )

        print("Snowflake bronze target created successfully.")
    finally:
        cursor.close()
        conn.close()


def extract_to_parquet_and_load_snowflake(table_name: str, primary_key: str):
    """Load SQL Server inserts, updates, and deletes since the saved CT version."""
    mssql_hook = MsSqlHook(mssql_conn_id="mssql_default")
    table_columns = mssql_hook.get_records(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
    )
    if not table_columns:
        raise RuntimeError(f"SQL Server source table dbo.{table_name} has no columns.")
    column_names = [column[0].lower() for column in table_columns]
    source_types = {
        column_name.lower(): _snowflake_type_from_sql_type(sql_type, precision, scale)
        for column_name, sql_type, precision, scale in table_columns
    }

    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    raw_table_name = f"RAW_{table_name.upper()}"
    bronze_table = f"{SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.{raw_table_name}"
    watermark_table = f"{SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.INGESTION_WATERMARKS"
    qualified_source = f"dbo.{table_name}"
    parquet_path = None
    stage_path = None

    try:
        versions = mssql_hook.get_first(
            f"""
            SELECT
                CHANGE_TRACKING_CURRENT_VERSION(),
                CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(N'{qualified_source}'))
            """
        )
        upper_version, min_valid_version = versions
        if upper_version is None:
            raise RuntimeError("SQL Server Change Tracking is not enabled for the source database.")
        upper_version = int(upper_version)

        cursor.execute(
            f"SELECT LAST_SYNC_VERSION FROM {watermark_table} WHERE TABLE_NAME = %s",
            (table_name,),
        )
        saved_watermark = cursor.fetchone()
        last_version = int(saved_watermark[0]) if saved_watermark else None
        full_refresh = (
            last_version is None
            or min_valid_version is None
            or last_version < int(min_valid_version)
            or last_version > upper_version
        )

        if full_refresh:
            dataframe = mssql_hook.get_pandas_df(f"SELECT * FROM {qualified_source}")
            dataframe["_operation"] = "I"
        else:
            selected_columns = ", ".join(
                f"source.[{column_name}]"
                if column_name != primary_key.lower()
                else f"changes.[{primary_key}] AS [{column_name}]"
                for column_name in column_names
            )
            changes_query = f"""
                SELECT {selected_columns}, changes.SYS_CHANGE_OPERATION AS [_operation]
                FROM CHANGETABLE(CHANGES {qualified_source}, {last_version}) AS changes
                LEFT JOIN {qualified_source} AS source
                    ON source.[{primary_key}] = changes.[{primary_key}]
                ORDER BY changes.SYS_CHANGE_VERSION, changes.[{primary_key}]
            """
            dataframe = mssql_hook.get_pandas_df(changes_query)

        dataframe.columns = [str(column).lower() for column in dataframe.columns]
        if not dataframe.empty:
            dataframe = dataframe.drop_duplicates(
                subset=[primary_key.lower()],
                keep="last",
            )
            dataframe["_extracted_at"] = datetime.utcnow().strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )

        if not full_refresh and dataframe.empty:
            cursor.execute("BEGIN")
            try:
                cursor.execute(
                    f"""
                    MERGE INTO {watermark_table} AS target
                    USING (
                        SELECT %s AS TABLE_NAME, %s AS LAST_SYNC_VERSION
                    ) AS source
                        ON target.TABLE_NAME = source.TABLE_NAME
                    WHEN MATCHED THEN UPDATE
                        SET LAST_SYNC_VERSION = source.LAST_SYNC_VERSION
                    WHEN NOT MATCHED THEN INSERT (TABLE_NAME, LAST_SYNC_VERSION)
                        VALUES (source.TABLE_NAME, source.LAST_SYNC_VERSION)
                    """,
                    (table_name, upper_version),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            print(
                f"No changes found for dbo.{table_name}; "
                f"watermark advanced to {upper_version}."
            )
            return

        dataframe = dataframe.reindex(
            columns=[*column_names, "_operation", "_extracted_at"]
        )
        timestamp_columns = {
            column: "TIMESTAMP_NTZ"
            for column, snowflake_type in source_types.items()
            if snowflake_type == "TIMESTAMP_NTZ"
        }
        timestamp_columns["_extracted_at"] = "TIMESTAMP_NTZ"
        _serialize_timestamps_for_parquet(dataframe, timestamp_columns)

        os.makedirs(TEMP_DIR, exist_ok=True)
        run_token = uuid.uuid4().hex
        file_name = f"{table_name}_{run_token}.parquet"
        parquet_path = os.path.join(TEMP_DIR, file_name)
        dataframe.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        stage_path = (
            f"@{SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.BRONZE_PARQUET_STAGE/"
            f"{table_name}/{run_token}/"
        )
        temp_table = f"INGEST_STAGE_{table_name.upper()}_{run_token.upper()}"

        business_columns = [
            column for column in column_names if column != "_extracted_at"
        ]
        update_assignments = ", ".join(
            f'target."{column}" = source."{column}"'
            for column in business_columns
        )
        update_assignments += ', target."_extracted_at" = source."_extracted_at"'
        insert_columns = [*business_columns, "_extracted_at"]
        insert_column_sql = ", ".join(f'"{column}"' for column in insert_columns)
        insert_values_sql = ", ".join(
            f'source."{column}"' for column in insert_columns
        )

        cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} LIKE {bronze_table}")
        cursor.execute(f'ALTER TABLE {temp_table} ADD COLUMN "_operation" VARCHAR')
        cursor.execute(f"PUT file://{parquet_path} {stage_path} OVERWRITE = TRUE")
        cursor.execute(
            f"""
            COPY INTO {temp_table}
            FROM {stage_path}
            FILES = ('{file_name}')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'ABORT_STATEMENT';
            """
        )
        copy_results = cursor.fetchall()
        loaded_rows = sum(int(result[3]) for result in copy_results)
        if loaded_rows != len(dataframe):
            raise RuntimeError(
                f"Expected to stage {len(dataframe)} changed rows for {bronze_table}, "
                f"but Snowflake loaded {loaded_rows}."
            )

        cursor.execute("BEGIN")
        try:
            cursor.execute(
                f"""
                MERGE INTO {bronze_table} AS target
                USING {temp_table} AS source
                    ON target."{primary_key.lower()}" = source."{primary_key.lower()}"
                WHEN MATCHED AND source."_operation" = 'D' THEN DELETE
                WHEN MATCHED THEN UPDATE SET {update_assignments}
                WHEN NOT MATCHED AND source."_operation" <> 'D' THEN
                    INSERT ({insert_column_sql})
                    VALUES ({insert_values_sql})
                """
            )
            if full_refresh:
                cursor.execute(
                    f"""
                    DELETE FROM {bronze_table} AS target
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {temp_table} AS source
                        WHERE source."{primary_key.lower()}" =
                            target."{primary_key.lower()}"
                    )
                    """
                )
            cursor.execute(
                f"""
                MERGE INTO {watermark_table} AS target
                USING (
                    SELECT %s AS TABLE_NAME, %s AS LAST_SYNC_VERSION
                ) AS source
                    ON target.TABLE_NAME = source.TABLE_NAME
                WHEN MATCHED THEN UPDATE
                    SET LAST_SYNC_VERSION = source.LAST_SYNC_VERSION
                WHEN NOT MATCHED THEN INSERT (TABLE_NAME, LAST_SYNC_VERSION)
                    VALUES (source.TABLE_NAME, source.LAST_SYNC_VERSION)
                """,
                (table_name, upper_version),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        cursor.execute(f"REMOVE {stage_path}")
        print(
            f"Applied {loaded_rows} {'full-refresh' if full_refresh else 'incremental'} "
            f"rows to {bronze_table}; watermark is {upper_version}."
        )
    finally:
        if parquet_path and os.path.exists(parquet_path):
            os.remove(parquet_path)
        cursor.close()
        conn.close()


with DAG(
    dag_id="dag_sqlserver_to_snowflake_bronze",
    default_args=default_args,
    description="Pipeline ELT via Parquet: Extrai do SQL Server, gera Parquet e carrega no Snowflake Bronze",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "sqlserver", "snowflake", "bronze", "parquet"],
) as dag:
    create_target = PythonOperator(
        task_id="create_snowflake_bronze_target",
        python_callable=create_snowflake_bronze_target,
    )

    ingest_tasks = []
    for table_name, pk in TABLES_CONFIG.items():
        task = PythonOperator(
            task_id=f"ingest_{table_name}_to_bronze",
            python_callable=extract_to_parquet_and_load_snowflake,
            op_kwargs={"table_name": table_name, "primary_key": pk},
        )
        ingest_tasks.append(task)

    trigger_dbt_snapshots = TriggerDagRunOperator(
        task_id="trigger_dbt_snapshots",
        trigger_dag_id="dag_dbt_snapshots",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    create_target >> ingest_tasks
    ingest_tasks >> trigger_dbt_snapshots