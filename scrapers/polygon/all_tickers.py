import datetime as dt
import os

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pandas as pd
import pandas_gbq

from polygon import RESTClient

# Constants
BQ_TABLE_NAME = "raw.all_tickers"  # dataset.table
CSV_FILE_NAME = "all_tickers.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME
PROJECT_ID = "new-life-400922"
WORKERS = 50


def fetch_ticker_info(ticker):
    ticker_detail = {
        "active": ticker.active,
        "cik": ticker.cik,
        "composite_figi": ticker.composite_figi,
        "currency_symbol": ticker.currency_symbol,
        "base_currency_symbol": ticker.base_currency_symbol,
        "currency_name": ticker.currency_name,
        "base_currency_name": ticker.base_currency_name,
        "delisted_utc": ticker.delisted_utc,
        "locale": ticker.locale,
        "title": ticker.name,
        "primary_exchange": ticker.primary_exchange,
        "share_class_figi": ticker.share_class_figi,
        "ticker": ticker.ticker,
        "type": ticker.type,
        "source_feed": ticker.source_feed,
    }
    with dfs_lock:
        dfs.append(ticker_detail)
    return ticker_detail


if __name__ == "__main__":
    # Set up client connections
    polygon_secret = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_secret, retries=10, trace=False)

    # Fetch Ticker Info
    tickers_active = polygon_client.list_tickers()
    dfs = []
    dfs_lock = Lock()

    # Using ThreadPoolExecutor to fetch stock histories concurrently
    print(f"Scraping web data")
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_ticker_info, tickers_active)

    all_tickers_df = pd.DataFrame(dfs)
    all_tickers_df["active"] = all_tickers_df["active"].astype(bool)
    all_tickers_df["cik"] = all_tickers_df["cik"].astype(str)
    all_tickers_df["composite_figi"] = all_tickers_df["composite_figi"].astype(str)
    all_tickers_df["currency_symbol"] = all_tickers_df["currency_symbol"].astype(str)
    all_tickers_df["base_currency_symbol"] = all_tickers_df[
        "base_currency_symbol"
    ].astype(str)
    all_tickers_df["currency_name"] = all_tickers_df["currency_name"].astype(str)
    all_tickers_df["base_currency_name"] = all_tickers_df["base_currency_name"].astype(
        str
    )
    all_tickers_df["locale"] = all_tickers_df["locale"].astype(str)
    all_tickers_df["title"] = all_tickers_df["title"].astype(str)
    all_tickers_df["primary_exchange"] = all_tickers_df["primary_exchange"].astype(str)
    all_tickers_df["share_class_figi"] = all_tickers_df["share_class_figi"].astype(str)
    all_tickers_df["ticker"] = all_tickers_df["ticker"].astype(str)
    all_tickers_df["type"] = all_tickers_df["type"].astype(str)
    all_tickers_df["source_feed"] = all_tickers_df["source_feed"].astype(str)

    # Write a CSV
    print(f"Writing {GCP_FILE_NAME}")
    all_tickers_df.to_csv(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    # upload the data to Bigquery
    print(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_tickers_df,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
        table_schema=[
            {"name": "active", "type": "BOOL"},
            {"name": "cik", "type": "STRING"},
            {"name": "composite_figi", "type": "STRING"},
            {"name": "currency_symbol", "type": "STRING"},
            {"name": "base_currency_symbol", "type": "STRING"},
            {"name": "currency_name", "type": "STRING"},
            {"name": "base_currency_name", "type": "STRING"},
            {"name": "locale", "type": "STRING"},
            {"name": "title", "type": "STRING"},
            {"name": "primary_exchange", "type": "STRING"},
            {"name": "share_class_figi", "type": "STRING"},
            {"name": "ticker", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "source_feed", "type": "STRING"},
        ],
    )
