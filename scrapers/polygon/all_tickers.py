import datetime as dt
import logging as log

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import pandas_gbq

from constants import BUCKET_NAME, PROJECT_ID, SECRET_ID, WORKERS
from google.cloud import logging, secretmanager, storage
from polygon import RESTClient

# Constants
BQ_TABLE_NAME = "raw.all_tickers"  # dataset.table
CSV_FILE_NAME = "all_tickers.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME


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
    logging_client = logging.Client()
    logging_client.setup_logging()
    secrets_client = secretmanager.SecretManagerServiceClient()
    polygon_secret = secrets_client.access_secret_version(
        request={"name": f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"}
    )
    polygon_client = RESTClient(
        polygon_secret.payload.data.decode("UTF-8"), retries=10, trace=False
    )
    storage_client = storage.Client()

    # Fetch Ticker Info
    tickers = polygon_client.list_tickers()
    dfs = []
    dfs_lock = Lock()

    # Using ThreadPoolExecutor to fetch stock histories concurrently
    log.info(f"Scraping web data")
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_ticker_info, tickers)

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
    log.info(f"Writing {GCP_FILE_NAME}")
    all_tickers_df.to_csv(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    # upload the data to Bigquery
    log.info(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_tickers_df, BQ_TABLE_NAME, project_id=PROJECT_ID, if_exists="replace"
    )
