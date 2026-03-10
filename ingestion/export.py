import os
import duckdb
import pandas as pd

DB_PATH = "data/crypto_warehouse.duckdb"
EXPORTS_DIR = "data/exports"

TABLES = [
    "crypto_market_summary",
    "crypto_volume_trends",
    "crypto_price_movement",
    "crypto_market_dominance",
    "top_gainers_losers",
]


def export_gold_tables():
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    for table in TABLES:
        df = conn.execute(f"SELECT * FROM main_gold.{table}").df()
        df.to_csv(f"{EXPORTS_DIR}/{table}.csv", index=False)
        print(f"exported {table}.csv")
    conn.close()


if __name__ == "__main__":
    export_gold_tables()
