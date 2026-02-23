
import pandas as pd
from etl_pipeline.config.database import get_postgres_engine
from dotenv import load_dotenv
import logging
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CHECKPOINT_FILE = "merchants_checkpoint.txt"

def get_last_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None

def save_checkpoint(value):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(value))

def extract_merchants():
    query = """
        SELECT id, display_name, email, status, created_at, updated_at
        FROM merchants
        ORDER BY updated_at
    """
    engine = get_postgres_engine()
    chunksize = 20000
    total_rows = 0
    last_checkpoint = get_last_checkpoint()
    if last_checkpoint:
        query = f"""
            SELECT id, display_name, email, status, created_at, updated_at
            FROM merchants
            WHERE updated_at > '{last_checkpoint}'
            ORDER BY updated_at
        """
    else:
        query = """
            SELECT id, display_name, email, status, created_at, updated_at
            FROM merchants
            ORDER BY updated_at
        """
    for i, chunk in enumerate(pd.read_sql_query(query, engine, chunksize=chunksize)):
        logging.info(f"Processed chunk {i+1}, rows in this chunk: {len(chunk)}")
        total_rows += len(chunk)
        if not chunk.empty:
            save_checkpoint(chunk['updated_at'].max())
        # Process/save chunk here if needed
    logging.info(f"Extraction complete. Total rows extracted: {total_rows}")

if __name__ == "__main__":
    extract_merchants()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    extract_merchants()

if __name__ == "__main__":
    main()