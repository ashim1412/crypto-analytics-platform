import os
import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
DB_PATH = "data/crypto_warehouse.duckdb"
TOP_N_COINS = 100


def fetch_market_data():
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N_COINS,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h,7d",
    }
    headers = {"x-cg-demo-api-key": API_KEY} if API_KEY else {}

    print(f"[{datetime.now(timezone.utc)}] fetching top {TOP_N_COINS} coins...")
    response = requests.get(API_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"[{datetime.now(timezone.utc)}] fetched {len(data)} coins")
    return data


def prepare_dataframe(raw_data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw_data)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return df


def load_to_bronze(df: pd.DataFrame) -> None:
    print(f"[{datetime.now(timezone.utc)}] loading to bronze...")

    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_coin_markets AS
        SELECT * FROM df WHERE 1=0
    """)

    # add any columns the API introduced since the table was created
    existing_cols = {
        row[0]
        for row in conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = 'raw_coin_markets'
        """).fetchall()
    }
    for col in df.columns:
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE bronze.raw_coin_markets ADD COLUMN "{col}" VARCHAR')

    conn.execute("INSERT INTO bronze.raw_coin_markets BY NAME SELECT * FROM df")

    row_count = conn.execute("SELECT COUNT(*) FROM bronze.raw_coin_markets").fetchone()[0]
    conn.close()
    print(f"[{datetime.now(timezone.utc)}] done — {row_count} total rows in bronze")


def main():
    raw_data = fetch_market_data()
    df = prepare_dataframe(raw_data)
    load_to_bronze(df)


if __name__ == "__main__":
    main()
