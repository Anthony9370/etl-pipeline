

from pathlib import Path
import pandas as pd
from etl_pipeline.config.database import get_postgres_engine

def extract_connected_accounts():
    engine = get_postgres_engine()
    query = """
    SELECT * FROM connected_accounts
    """
    df = pd.read_sql_query(query, engine)
    output_dir = Path("extracted/connected_accounts")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = output_dir / "connected_accounts.parquet"
    df.to_parquet(file_name, index=False, compression="snappy")
    print(f"Extracted {len(df)} rows to {file_name}")

if __name__ == "__main__":
    extract_connected_accounts()