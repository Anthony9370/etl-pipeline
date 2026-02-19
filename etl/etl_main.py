import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from config import POSTGRES, BIGQUERY, TABLES

# Utility: Connect to Postgres
def get_postgres_conn():
    return psycopg2.connect(
        host=POSTGRES['host'],
        port=POSTGRES['port'],
        dbname=POSTGRES['database'],
        user=POSTGRES['user'],
        password=POSTGRES['password']
    )

# Utility: Connect to BigQuery
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        BIGQUERY['credentials']
    )
    return bigquery.Client(credentials=credentials, project=BIGQUERY['project'])


# Extract table from Postgres to DataFrame
def extract_table(table_name):
    with get_postgres_conn() as conn:
        return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)

# Load DataFrame to BigQuery
def load_to_bigquery(df, table_name):
    client = get_bq_client()
    table_id = f"{BIGQUERY['project']}.{BIGQUERY['dataset']}.{table_name.lower()}"
    job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")


# Import transformation functions
from transformations import standardize_dates, map_status, remove_duplicates, auto_fill_nulls

# Main ETL for all tables
def etl_all_tables():
    for table in TABLES:
        print(f"Extracting {table}...")
        df = extract_table(table)
        # Example transformation usage (customize as needed):
        if 'date' in df.columns:
            df = standardize_dates(df, [col for col in df.columns if 'date' in col.lower()])
        if 'status' in df.columns:
            status_mapping = {0: 'Inactive', 1: 'Active'}  # Example mapping
            df = map_status(df, 'status', status_mapping)
        # Remove duplicates only on 'id' column if it exists
        if 'id' in df.columns:
            df = remove_duplicates(df, subset=['id'])
        # Automatically fill nulls based on column dtypes
        df = auto_fill_nulls(df)
        load_to_bigquery(df, table)

if __name__ == "__main__":
    etl_all_tables()
