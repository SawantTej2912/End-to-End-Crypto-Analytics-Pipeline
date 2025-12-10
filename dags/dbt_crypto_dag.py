from datetime import datetime, timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

# Paths *inside the container*
DBT_PROJECT_DIR = "/opt/airflow/crypto_analytics_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_bash_command(cmd: str):
    """
    Safely run a bash command with try/except.
    Raises AirflowException on error so Airflow knows the task failed.
    """
    try:
        print(f"\n[INFO] Running command:\n{cmd}\n")
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True
        )

        print("[INFO] STDOUT:\n", result.stdout)
        print("[INFO] STDERR:\n", result.stderr)

        if result.returncode != 0:
            raise Exception(f"Command failed with exit code {result.returncode}")

        return result.stdout

    except Exception as e:
        print(f"[ERROR] Exception during command: {e}")
        raise


with DAG(
    dag_id="dbt_crypto_pipeline",
    description="Run dbt models (run, test, snapshot) for crypto analytics.",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["lab2", "dbt"],
):

    dbt_run_cmd = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_test_cmd = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_snapshot_cmd = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_bash_command,
        op_kwargs={"cmd": dbt_run_cmd},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_bash_command,
        op_kwargs={"cmd": dbt_test_cmd},
    )

    dbt_snapshot = PythonOperator(
        task_id="dbt_snapshot",
        python_callable=run_bash_command,
        op_kwargs={"cmd": dbt_snapshot_cmd},
    )

    dbt_run >> dbt_test >> dbt_snapshot
