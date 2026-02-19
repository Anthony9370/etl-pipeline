# README: ETL Pipeline for Transaction Insights

## 1. Overview
This ETL pipeline extracts core tables from a Postgres database, applies transformations, and loads them into BigQuery for analytics and AI insights.

## 2. Directory Structure
- etl/config.py — Configuration for Postgres and BigQuery
- etl/etl_main.py — Main ETL script (extract, transform, load)
- etl/transformations.py — Transformation utilities (currency normalization, date handling, etc.)
- etl/bigquery_ddl_templates.py — Example DDLs for BigQuery models

## 3. Setup
1. Install dependencies:
   ```bash
   pip install pandas psycopg2-binary google-cloud-bigquery
   ```
2. Update `etl/config.py` with your Postgres and BigQuery credentials.
3. Place your GCP service account key file in a secure location and update the path in `etl/config.py`.

## 4. Running the ETL
Run the main script:
```bash
python etl/etl_main.py
```

## 5. Transformations
- Add transformation logic in `etl/etl_main.py` using functions from `etl/transformations.py`.
- Example: Normalize currencies, standardize dates, enrich with dimension keys, etc.

## 6. BigQuery Modeling
- Use the DDLs in `etl/bigquery_ddl_templates.py` to create dimension, fact, and aggregate tables in BigQuery.
- Replace `{project}` and `{dataset}` with your actual values.
- You can run these DDLs in the BigQuery console or via Python using the BigQuery client.

## 7. AI Insights Layer
- Once data models are in place, you can build an AI layer (LLM, anomaly detection, etc.) on top of the curated tables.
- Ensure all metrics and features needed for AI are materialized in BigQuery.

## 8. Notes
- For incremental loads, modify extraction queries to use `updatedAt` fields.
- Always store both original and normalized currency values.
- Document all models and fields for downstream consumers.

---

For further customization or automation (e.g., Airflow orchestration, dbt modeling), extend this structure as needed.
