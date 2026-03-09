# Crypto Analytics Platform

A production-quality crypto analytics data platform built on a modern data stack.

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python + CoinGecko API |
| Warehouse | DuckDB |
| Transformations | dbt with dbt-duckdb adapter |
| Orchestration | GitHub Actions (every 6 hours) |

## Architecture — Lakehouse (3 layers)

```
Bronze  →  Silver  →  Gold
raw        cleaned     business-ready
append     deduplicated marts/aggregations
-only      typed
           views       tables
```

**Bronze** (`bronze.raw_coin_markets`) — Raw API payloads, append-only. The only field added at this layer is `ingested_at`.

**Silver** (`silver.stg_coin_markets`) — dbt staging view. Renames columns to snake_case, casts types, deduplicates to one row per coin per day (latest snapshot wins), adds `snapshot_date`.

**Gold** (`gold.*`) — dbt mart tables. Business-ready, always filtered to latest snapshot date.

## Gold Mart Models

| Model | Business Question |
|---|---|
| `crypto_market_summary` | What is the current state of the crypto market? |
| `crypto_volume_trends` | How is trading volume behaving over time per coin? |
| `crypto_price_movement` | Which coins are moving the most in price? |
| `crypto_market_dominance` | What % of total market cap does each coin control? |
| `top_gainers_losers` | Which coins gained or lost the most today? |

## Project Structure

```
crypto-analytics-platform/
├── ingestion/
│   └── ingest.py             # CoinGecko API → DuckDB Bronze
├── data/                     # DuckDB warehouse (gitignored)
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_coin_markets.sql
│   │   └── marts/
│   │       ├── crypto_market_summary.sql
│   │       ├── crypto_volume_trends.sql
│   │       ├── crypto_price_movement.sql
│   │       ├── crypto_market_dominance.sql
│   │       └── top_gainers_losers.sql
│   ├── tests/
│   │   └── generic_tests.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── .github/
│   └── workflows/
│       └── pipeline.yml      # Runs every 6 hours
├── .env.example
├── requirements.txt
└── README.md
```

## Setup

**1. Clone and install dependencies**
```bash
pip install -r requirements.txt
```

**2. Configure your API key**
```bash
cp .env.example .env
# Edit .env and set COINGECKO_API_KEY=your_key
```

**3. Run ingestion (from project root)**
```bash
python ingestion/ingest.py
```

**4. Run dbt transformations**
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

## GitHub Actions

Add `COINGECKO_API_KEY` as a repository secret. The pipeline runs automatically every 6 hours and can also be triggered manually via `workflow_dispatch`.

## Engineering Constraints

- Bronze is **append-only** — never overwritten
- Silver deduplicates with `ROW_NUMBER()` on `ingested_at` per coin per day
- Gold always filters to the **latest `snapshot_date`**
- All dbt models use `ref()` and `source()` — no raw table names
- Staging materialized as **views**, marts as **tables**
- `.env` is gitignored; `.env.example` is committed
