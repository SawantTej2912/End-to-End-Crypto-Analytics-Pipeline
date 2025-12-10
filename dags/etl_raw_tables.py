from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="etl_raw_tables",
    description="ETL DAG to import raw tables from Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hw6", "etl"],
)
def etl_raw_tables():

    @task
    def count_rows(table_name: str):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM RAW.{table_name}")
        (n,) = cur.fetchone()
        print(f"RAW.{table_name} → {n} rows found.")
        cur.close()
        conn.close()
        return n

    # Give each invocation a custom task_id and pass only the table name
    count_user_session_channel = count_rows.override(
        task_id="count_user_session_channel"
    )("USER_SESSION_CHANNEL")

    count_session_timestamp = count_rows.override(
        task_id="count_session_timestamp"
    )("SESSION_TIMESTAMP")

etl_raw_tables = etl_raw_tables()
