# airflow/dags/fetch_crypto_data_dag.py
#
# ETL: yfinance (BTC-USD, ETH-USD, etc.) -> RAW.CRYPTO_PRICES (Snowflake)
# Airflow Variables (Admin → Variables):
#   crypto_tickers  e.g. "BTC-USD,ETH-USD"
#   lookback_days   e.g. "365"
#   raw_schema      e.g. "PARROT_RAW" (or YOURNAME_RAW)
#
# Airflow Connection (Admin → Connections):
#   snowflake_default  (account, user, password, warehouse, database, role)
#
# Idempotency:
#   - Transaction with BEGIN/COMMIT/ROLLBACK
#   - MERGE on (SYMBOL, PRICE_DATE)
#
# Requirements in the Airflow container:
#   yfinance
#   pandas
#   apache-airflow-providers-snowflake
#   "snowflake-connector-python[pandas]"

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# ---------- Config (Airflow Variables with sensible defaults) ----------
TICKERS = [
    t.strip().upper()
    for t in Variable.get("crypto_tickers", default_var="BTC-USD,ETH-USD").split(",")
    if t.strip()
]
LOOKBACK_DAYS = int(Variable.get("lookback_days", default_var="365"))
RAW_SCHEMA = Variable.get("raw_schema", default_var="RAW")  # e.g., PARROT_RAW / YOURNAME_RAW
TMP_DIR = os.environ.get("AIRFLOW_TMP_DIR", "/tmp")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_crypto_data_dag",
    description="Fetch daily crypto OHLCV from yfinance and upsert into Snowflake RAW.CRYPTO_PRICES",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 1 * * *",  # daily at 01:15 UTC
    catchup=False,
    max_active_runs=1,
    tags=["lab2", "etl", "crypto", "snowflake", "yfinance"],
) as dag:

    def _extract_prices(**kwargs):
        """
        Download OHLCV for all tickers for LOOKBACK_DAYS ending at ds-1.
        Robust to yfinance MultiIndex columns and missing 'Adj Close'.
        """
        import os
        import pandas as pd
        import yfinance as yf

        ds = kwargs["ds"]
        end = pd.Timestamp(ds)
        start = end - pd.Timedelta(days=LOOKBACK_DAYS)

        os.makedirs(TMP_DIR, exist_ok=True)

        frames = []
        if not TICKERS:
            print("[WARN] No crypto_tickers defined. Set Airflow Variable 'crypto_tickers'.")

        for ticker in TICKERS:
            try:
                # group_by=None avoids MultiIndex; threads=False for stability
                hist = yf.download(
                    ticker,
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),  # end is exclusive; range still fine
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    group_by=None,
                    threads=False,
                )

                if hist is None or hist.empty:
                    print(f"[WARN] No data for {ticker} in {start.date()}..{end.date()}")
                    continue

                # If columns are MultiIndex, collapse to last level
                if isinstance(hist.columns, pd.MultiIndex):
                    try:
                        hist = hist.xs(ticker, axis=1, level=0)
                    except Exception:
                        hist.columns = hist.columns.get_level_values(-1)

                # Normalize column names
                hist = hist.reset_index()
                hist.columns = [str(c).strip().lower().replace(" ", "_") for c in hist.columns]

                # Rename to expected names
                rename_map = {
                    "date": "price_date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "adj_close": "adj_close",
                    "volume": "volume",
                }
                df = hist.rename(columns=rename_map)

                # Ensure required columns
                if "adj_close" not in df.columns and "close" in df.columns:
                    df["adj_close"] = df["close"]
                if "volume" not in df.columns:
                    df["volume"] = 0

                # Type cleanup
                num_cols = ["open", "high", "low", "close", "adj_close", "volume"]
                for c in num_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")

                df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
                df["symbol"] = ticker
                df["ingested_at"] = pd.Timestamp.utcnow()

                # Reorder to expected uppercase schema
                df = df[
                    [
                        "symbol",
                        "price_date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "adj_close",
                        "volume",
                        "ingested_at",
                    ]
                ]
                df.columns = [c.upper() for c in df.columns]

                frames.append(df)

            except Exception as e:
                print(f"[ERROR] Failed to download {ticker}: {e}")

        if not frames:
            print("[WARN] No frames collected; writing empty CSV (load step will no-op).")
            df_all = pd.DataFrame(
                columns=[
                    "SYMBOL",
                    "PRICE_DATE",
                    "OPEN",
                    "HIGH",
                    "LOW",
                    "CLOSE",
                    "ADJ_CLOSE",
                    "VOLUME",
                    "INGESTED_AT",
                ]
            )
        else:
            df_all = pd.concat(frames, ignore_index=True).dropna(subset=["PRICE_DATE", "SYMBOL"])

        out_path = os.path.join(TMP_DIR, f"crypto_{kwargs['ds_nodash']}.csv")
        df_all.to_csv(out_path, index=False)
        print(f"[INFO] Wrote {len(df_all)} rows to {out_path}")
        return out_path

    extract_task = PythonOperator(
        task_id="extract_crypto_to_csv",
        python_callable=_extract_prices,
    )

    def _load_merge_to_snowflake(csv_path: str, **kwargs):
        """Create RAW.CRYPTO_PRICES if needed, bulk load to temp table, MERGE into target inside a transaction."""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        conn.autocommit = False

        # Read CSV and normalize columns/types
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
        else:
            df = pd.DataFrame(
                columns=[
                    "SYMBOL",
                    "PRICE_DATE",
                    "OPEN",
                    "HIGH",
                    "LOW",
                    "CLOSE",
                    "ADJ_CLOSE",
                    "VOLUME",
                    "INGESTED_AT",
                ]
            )

        expected = ["SYMBOL", "PRICE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME", "INGESTED_AT"]
        df.columns = [str(c).strip().upper() for c in df.columns]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        df = df[expected]

        if not df.empty:
            df["PRICE_DATE"] = pd.to_datetime(df["PRICE_DATE"]).dt.date
            for c in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        target = f"{RAW_SCHEMA}.CRYPTO_PRICES"
        stage = f"{RAW_SCHEMA}.CRYPTO_PRICES_STAGE"

        create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

        CREATE TABLE IF NOT EXISTS {target} (
            SYMBOL      STRING      NOT NULL,
            PRICE_DATE  DATE        NOT NULL,
            OPEN        NUMBER(38,8),
            HIGH        NUMBER(38,8),
            LOW         NUMBER(38,8),
            CLOSE       NUMBER(38,8),
            ADJ_CLOSE   NUMBER(38,8),
            VOLUME      NUMBER(38,8),
            INGESTED_AT TIMESTAMP_NTZ,
            CONSTRAINT PK_CRYPTO_PRICES PRIMARY KEY (SYMBOL, PRICE_DATE)
        );
        """

        create_stage_sql = f"CREATE OR REPLACE TEMPORARY TABLE {stage} LIKE {target};"

        merge_sql = f"""
        MERGE INTO {target} AS T
        USING {stage} AS S
          ON  T.SYMBOL = S.SYMBOL
          AND T.PRICE_DATE = S.PRICE_DATE
        WHEN MATCHED THEN UPDATE SET
            OPEN        = S.OPEN,
            HIGH        = S.HIGH,
            LOW         = S.LOW,
            CLOSE       = S.CLOSE,
            ADJ_CLOSE   = S.ADJ_CLOSE,
            VOLUME      = S.VOLUME,
            INGESTED_AT = S.INGESTED_AT
        WHEN NOT MATCHED THEN INSERT (
            SYMBOL, PRICE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, INGESTED_AT
        ) VALUES (
            S.SYMBOL, S.PRICE_DATE, S.OPEN, S.HIGH, S.LOW, S.CLOSE, S.ADJ_CLOSE, S.VOLUME, S.INGESTED_AT
        );
        """

        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            # DDLs
            for stmt in [create_sql, create_stage_sql]:
                for s in [x.strip() for x in stmt.split(";") if x.strip()]:
                    cur.execute(s)

            # Stage load (only if we have data)
            if not df.empty:
                write_pandas(
                    conn,
                    df,
                    table_name="CRYPTO_PRICES_STAGE",
                    schema=RAW_SCHEMA,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=False,
                )

            # Upsert
            cur.execute(merge_sql)
            cur.execute("COMMIT")
            print("[INFO] Upsert to RAW.CRYPTO_PRICES complete.")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
            conn.close()

    load_task = PythonOperator(
        task_id="load_and_merge_to_snowflake",
        python_callable=_load_merge_to_snowflake,
        op_kwargs={"csv_path": "{{ ti.xcom_pull(task_ids='extract_crypto_to_csv') }}"},
    )

    def _dq_recent_count(**kwargs):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        with hook.get_conn() as conn, conn.cursor() as cur:
            sql = f"""
            SELECT COUNT(*) FROM {RAW_SCHEMA}.CRYPTO_PRICES
            WHERE PRICE_DATE >= DATEADD(day, -{LOOKBACK_DAYS}, CURRENT_DATE());
            """
            cur.execute(sql)
            c = cur.fetchone()[0]
            print(f"[DQ] Recent rows present (last {LOOKBACK_DAYS}d): {c}")

    dq_task = PythonOperator(
        task_id="dq_rowcount_window",
        python_callable=_dq_recent_count,
    )

    extract_task >> load_task >> dq_task
