
# End-to-End Crypto Analytics Pipeline  
### Snowflake • Apache Airflow • dbt • Preset (BI Dashboard)

This project implements a full **data engineering + ELT + analytics** pipeline for cryptocurrency price analytics using:

- **Apache Airflow** for ETL orchestration  
- **Snowflake** for cloud warehousing  
- **dbt** for transformations, testing, and snapshots  
- **Preset (Apache Superset)** for data visualization  
- **yfinance** as the data source for BTC-USD and ETH-USD  

This project extends concepts from **Lab-1: Stock Price Predictor using Snowflake**, originally implemented by my teammate **Vaheedur Rehman**, and builds a more complete ELT workflow.

---

## 📌 Project Overview

Cryptocurrency markets generate continuous and highly volatile price data.  
This project builds an automated pipeline to:

1. **Ingest raw crypto OHLCV data** (BTC-USD & ETH-USD)
2. **Store it in Snowflake RAW schema**
3. **Transform it with dbt** into analytics-ready models
4. **Run data tests**
5. **Snapshot historical data** for versioning
6. **Visualize insights** using Preset dashboards

This delivers a modern, production-style data stack with automation, quality checks, and BI reporting.

---

## 🏗️ System Architecture

```
      +-------------------------+
      |      yfinance API       |
      +-----------+-------------+
                  |
                  v
      +-----------+-------------+
      |   Airflow ETL DAG       |
      | fetch_crypto_data_dag   |
      +-----------+-------------+
                  |
                  v
    RAW Layer: PARROT_RAW.CRYPTO_PRICES
                  |
            (dbt run/test)
                  |
                  v
    FEAT Layer: PARROT_FEAT.STAGING / FEATURES
                  |
             MART Layer:
   PARROT_FEAT.MART_CRYPTO_SUMMARY   <--- Dashboard uses this
                  |
            (dbt snapshot)
                  |
                  v
   SNAP Layer: PARROT_SNAP.CRYPTO_PRICES_SNAPSHOT
                  |
                  v
        Preset / Superset Dashboard

```

## 🧩 Components

### **1. Airflow ETL – `fetch_crypto_data_dag.py`**
- Fetches daily OHLCV data for BTC-USD and ETH-USD  
- Cleans and normalizes data  
- Writes to temporary CSV  
- Loads into Snowflake with **MERGE** (idempotent)  
- Stores in:

```

USER_DB_PARROT.PARROT_RAW.CRYPTO_PRICES

````

**Includes:**
- Try/except error handling  
- SQL transaction control  
- Data quality checks (row counts, null checks)

---

### **2. dbt ELT Pipeline – `crypto_analytics_dbt`**

dbt transforms RAW data into analytics-ready tables.

#### ✔ Models:

| Layer | Schema | Table | Description |
|------|--------|--------|-------------|
| RAW | PARROT_RAW | CRYPTO_PRICES | Raw OHLCV crypto data |
| STAGING | PARROT_FEAT | STG_CRYPTO_PRICES | Cleaned, typed staging data |
| FEATURE | PARROT_FEAT | INT_CRYPTO_FEATURES | SMA5/20/50, momentum |
| MART ⭐ | PARROT_FEAT | MART_CRYPTO_SUMMARY | Final BI-ready table |
| SNAPSHOT | PARROT_SNAP | CRYPTO_PRICES_SNAPSHOT | Historical versioning |

#### ✔ Tests:
- `symbol` not null  
- `price_date` not null  
- dbt schema tests for all layers  

#### ✔ Snapshot:
```sql
CRYPTO_PRICES_SNAPSHOT
````

Tracks historical price changes using `INGESTED_AT`.

---

## ⚙️ **3. Airflow dbt Pipeline – `dbt_crypto_dag.py`**

Automates dbt execution:

1. `dbt run`
2. `dbt test`
3. `dbt snapshot`

Each command runs inside a PythonOperator with **try/except** for clean failure handling and logging.

Runs daily after ETL.

---

## 🗂️ Snowflake Schemas Used

```
USER_DB_PARROT.PARROT_RAW     – Raw crypto OHLCV data  
USER_DB_PARROT.PARROT_FEAT    – Staging, features, marts  
USER_DB_PARROT.PARROT_SNAP    – Snapshots
```

### ⭐ Final analytics table for BI:

```
USER_DB_PARROT.PARROT_FEAT.MART_CRYPTO_SUMMARY
```

---

## 📊 Dashboard (Preset / Superset)

Preset visualizes the final mart table.

### ✔ Dataset:

```
PARROT_FEAT.MART_CRYPTO_SUMMARY
```

### Dashboard includes:

* Line chart: Close price + SMA5 + SMA20 + SMA50
* Momentum 5-day % chart
* Bullish signal indicator (SMA5 > SMA20)
* Filters:

  * Symbol (BTC/ETH)
  * Date range

### Required screenshots:

1. Full dashboard
2. Dashboard with filters applied (different date range or symbol)

---

## 🗂️ Repository Structure

```
lab2_repo/
│
├── fetch_crypto_data_dag.py
├── dbt_crypto_dag.py
├── docker-compose.yml
├── dbt_project.yml
├── profiles.yml
├── crypto_prices_snapshot.sql
└── README.md
```

---

## ▶️ How to Run the Project

### **1. Start Airflow**

```bash
docker compose up -d
```

### **2. Run ETL**

Airflow UI →
`fetch_crypto_data_dag` → **Trigger DAG**

### **3. Run dbt Pipeline**

Airflow UI →
`dbt_crypto_pipeline` → **Trigger DAG**

### **4. Verify in Snowflake**

```sql
USE DATABASE USER_DB_PARROT;
USE SCHEMA PARROT_FEAT;
SELECT * FROM MART_CRYPTO_SUMMARY LIMIT 20;
```

### **5. Build Dashboard**

Connect Preset → Add dataset → Create charts.

---

## 📘 Example SQL Queries

```sql
-- BTC trend (mart layer)
SELECT *
FROM PARROT_FEAT.MART_CRYPTO_SUMMARY
WHERE SYMBOL='BTC-USD'
ORDER BY PRICE_DATE DESC;

-- Compare RAW vs FEATURE
SELECT r.SYMBOL, r.PRICE_DATE, r.CLOSE AS RAW_CLOSE,
       f.SMA_20, f.MOMENTUM_5D_PCT
FROM PARROT_RAW.CRYPTO_PRICES r
JOIN PARROT_FEAT.INT_CRYPTO_FEATURES f
  ON r.SYMBOL=f.SYMBOL
 AND r.PRICE_DATE=f.PRICE_DATE
ORDER BY PRICE_DATE DESC;
```

---

## 📚 References

* YFinance: [https://pypi.org/project/yfinance/](https://pypi.org/project/yfinance/)
* Apache Airflow: [https://airflow.apache.org/docs/](https://airflow.apache.org/docs/)
* dbt Docs: [https://docs.getdbt.com/](https://docs.getdbt.com/)
* Snowflake Docs: [https://docs.snowflake.com/](https://docs.snowflake.com/)
* Preset/Superset: [https://preset.io](https://preset.io)

---

## 👥 Authors

* **Tejas Sawant** – Lab 2 Development
* **Vaheedur Rehman** – Lab 1 Foundation (Referenced)
* Department of Applied Data Intelligence
* San Jose State University

