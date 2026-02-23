
import pandas as pd
from etl_pipeline.config.database import get_postgres_engine
from dotenv import load_dotenv
import logging
from datetime import timedelta
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CHECKPOINT_FILE = "transactions_checkpoint.txt"

def get_last_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None

def save_checkpoint(value):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(value))

def extract_transactions_by_date():
    engine = get_postgres_engine()
    last_checkpoint = get_last_checkpoint()
    if last_checkpoint:
        query = f"""
            SELECT id, amount, currency, fee_total, platform_fee, provider_fee, net_amount, status,
                   transaction_date, merchant_id, merchant_account_id, connected_account_id, provider_id,
                   payment_method_type, card_brand, card_country, card_funding, reconciliation_status,
                   created_at, updated_at
            FROM transactions
            WHERE updated_at > '{last_checkpoint}'
            ORDER BY updated_at
        """
    else:
        query = """
            SELECT id, amount, currency, fee_total, platform_fee, provider_fee, net_amount, status,
                   transaction_date, merchant_id, merchant_account_id, connected_account_id, provider_id,
                   payment_method_type, card_brand, card_country, card_funding, reconciliation_status,
                   created_at, updated_at
            FROM transactions
            ORDER BY updated_at
        """
    chunksize = 20000
    total_rows = 0
    for i, chunk in enumerate(pd.read_sql_query(query, engine, chunksize=chunksize)):
        logging.info(f"Processed chunk {i+1}, rows in this chunk: {len(chunk)}")
        total_rows += len(chunk)
        if not chunk.empty:
            save_checkpoint(chunk['updated_at'].max())
        # Optionally: chunk.to_csv(f"transactions_chunk_{i+1}.csv", index=False)
    logging.info(f"Extraction complete. Total rows extracted: {total_rows}")

def main():
    extract_transactions_by_date()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    extract_transactions_by_date()