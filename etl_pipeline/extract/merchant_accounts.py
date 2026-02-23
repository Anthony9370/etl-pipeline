
import pandas as pd
from etl_pipeline.config.database import get_postgres_engine
from dotenv import load_dotenv
import logging
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CHECKPOINT_FILE = "merchant_accounts_checkpoint.txt"

def get_last_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None

def save_checkpoint(value):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(value))

def extract_merchant_accounts():
    engine = get_postgres_engine()
    last_checkpoint = get_last_checkpoint()
    if last_checkpoint:
        query = f"""
            SELECT id, merchant_id, provider_id, external_account_id, status, account_type,
                   display_name, country, created_at, updated_at
            FROM merchant_accounts
            WHERE updated_at > '{last_checkpoint}'
            ORDER BY updated_at
        """
    else:
        query = """
            SELECT id, merchant_id, provider_id, external_account_id, status, account_type,
                   display_name, country, created_at, updated_at
            FROM merchant_accounts
            ORDER BY updated_at
        """
    chunksize = 20000
    total_rows = 0
    for i, chunk in enumerate(pd.read_sql_query(query, engine, chunksize=chunksize)):
        logging.info(f"Processed chunk {i+1}, rows in this chunk: {len(chunk)}")
        total_rows += len(chunk)
        if not chunk.empty:
            save_checkpoint(chunk['updated_at'].max())
        # Optionally: chunk.to_csv(f"merchant_accounts_chunk_{i+1}.csv", index=False)
    logging.info(f"Extraction complete. Total rows extracted: {total_rows}")


if __name__ == "__main__":
    extract_merchant_accounts()

if __name__ == "__main__":
    extract_merchant_accounts()

if __name__ == "__main__":
    main()