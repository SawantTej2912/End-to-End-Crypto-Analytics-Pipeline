from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="elt_session_summary",
    description="ELT: build ANALYTICS.SESSION_SUMMARY from RAW tables",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hw6", "elt"],
)
def elt_session_summary():

    @task(task_id="build_session_summary")
    def build_session_summary():
        sql_statements = [
            "CREATE SCHEMA IF NOT EXISTS ANALYTICS",
            """
            CREATE OR REPLACE TABLE ANALYTICS.SESSION_SUMMARY AS
            WITH dedup AS (
              SELECT
                u.userId,
                u.sessionId,
                u.channel,
                s.ts
              FROM RAW.USER_SESSION_CHANNEL u
              JOIN RAW.SESSION_TIMESTAMP s
                ON u.sessionId = s.sessionId
              /* Remove duplicates at the event-level */
              QUALIFY ROW_NUMBER()
                OVER (PARTITION BY u.sessionId, s.ts ORDER BY s.ts) = 1
            )
            SELECT
              userId,
              sessionId,
              channel,
              MIN(ts) AS session_start,
              MAX(ts) AS session_end,
              COUNT(*) AS event_count
            FROM dedup
            GROUP BY userId, sessionId, channel
            """
        ]
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN;")
            for stmt in sql_statements:
                cur.execute(stmt)
            cur.execute("COMMIT;")
            print("Built ANALYTICS.SESSION_SUMMARY successfully.")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
        finally:
            cur.close(); conn.close()

    @task(task_id="count_summary_rows")
    def count_summary_rows():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ANALYTICS.SESSION_SUMMARY")
        (n,) = cur.fetchone()
        print(f"ANALYTICS.SESSION_SUMMARY rows → {n}")
        cur.close(); conn.close()
        return n

  
    @task(task_id="check_for_duplicates")
    def check_for_duplicates():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) AS dup_sessions
            FROM (
              SELECT sessionId
              FROM ANALYTICS.SESSION_SUMMARY
              GROUP BY sessionId
              HAVING COUNT(*) > 1
            )
        """)
        (dup_sessions,) = cur.fetchone()
        cur.close(); conn.close()
        if dup_sessions and int(dup_sessions) > 0:
            raise AirflowException(f"Duplicate sessionId rows found: {dup_sessions}")
        print("No duplicate sessionId rows found in ANALYTICS.SESSION_SUMMARY.")

    # Order: build → count → duplicate-check
    build_session_summary() >> count_summary_rows() >> check_for_duplicates()

elt_session_summary = elt_session_summary()
