



import pandas as pd
from etl_pipeline.config.database import get_postgres_engine
from dotenv import load_dotenv
import logging
import os
from pathlib import Path
from datetime import timedelta

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BATCH_DAYS = int(os.getenv("EXTRACTION_BATCH_DAYS", "1"))
CHUNKSIZE = int(os.getenv("BATCH_CHUNKSIZE", "5000"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "extracted/transactions"))
CHECKPOINT_FILE = Path(os.getenv("CHECKPOINT_FILE", "transactions_checkpoint_date.txt"))

def read_checkpoint():
    if CHECKPOINT_FILE.exists():
        return pd.to_datetime(CHECKPOINT_FILE.read_text().strip())
    return None

def write_checkpoint(dt):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(pd.to_datetime(dt).date().isoformat())

def extract_by_date():
    engine = get_postgres_engine()
    rng = pd.read_sql_query("SELECT MIN(transaction_date) AS min_date, MAX(transaction_date) AS max_date FROM transactions", engine)
    start = pd.to_datetime(rng.loc[0, "min_date"])
    end = pd.to_datetime(rng.loc[0, "max_date"])
    if pd.isna(start) or pd.isna(end):
        logging.info("No data in transactions")
        return

    last_cp = read_checkpoint()
    current = last_cp + timedelta(days=1) if last_cp is not None else start
    total = 0

    while current <= end:
        window_end = current + timedelta(days=BATCH_DAYS)
        window_dir = OUTPUT_DIR / current.strftime("%Y-%m-%d")
        window_dir.mkdir(parents=True, exist_ok=True)

        # Check if any Parquet file exists for this window
        existing_files = list(window_dir.glob(f"transactions_{current.date()}_chunk_*.parquet"))
        if existing_files:
            logging.info(f"Parquet files already exist for {current.date()}, skipping extraction.")
            write_checkpoint(current)
            current = window_end
            continue

        query = f"""
            SELECT *
            FROM transactions
            WHERE transaction_date >= '{current.date()}' AND transaction_date < '{window_end.date()}'
            ORDER BY transaction_date
        """

        rows_this_window = 0
        try:
            for i, chunk in enumerate(pd.read_sql_query(query, engine, chunksize=CHUNKSIZE)):
                n = len(chunk)
                rows_this_window += n
                total += n
                file_name = window_dir / f"transactions_{current.date()}_chunk_{i+1}.parquet"
                chunk.to_parquet(file_name, index=False, compression="snappy")
                logging.info(f"Window {current.date()} chunk {i+1}: {n} rows -> {file_name.name}")
                if "transaction_date" in chunk.columns and not chunk["transaction_date"].isna().all():
                    last_dt = pd.to_datetime(chunk["transaction_date"]).max()
                    write_checkpoint(last_dt)
        except Exception as e:
            logging.error(f"Error extracting window {current} -> {window_end}: {e}")
            raise

        logging.info(f"Extracted {rows_this_window} rows for {current.date()} -> {window_end.date()}")
        current = window_end

    logging.info(f"Extraction complete. Total rows extracted: {total}")

def main():
    extract_by_date()

if __name__ == "__main__":
    main()