
import pandas as pd
from etl_pipeline.config.database import get_postgres_engine
from dotenv import load_dotenv
import logging
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def extract_providers():
    query = "SELECT id, name FROM providers"
    engine = get_postgres_engine()
    chunksize = 20000
    total_rows = 0
    for i, chunk in enumerate(pd.read_sql_query(query, engine, chunksize=chunksize)):
        logging.info(f"Processed chunk {i+1}, rows in this chunk: {len(chunk)}")
        total_rows += len(chunk)
        # Process/save chunk here if needed
    logging.info(f"Extraction complete. Total rows extracted: {total_rows}")

def main():
    extract_providers()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    extract_providers()

if __name__ == "__main__":
    main()