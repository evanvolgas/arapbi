import datetime as dt
import os

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pandas as pd

from google.cloud import storage
from polygon import RESTClient

# TODO Make this incremental

# Constants
BQ_TABLE_NAME = "raw.all_tickers_history"
BUCKET_NAME = "arapbi-polygon"
GCP_FOLDER_NAME = "polygon/tickers/"
PROJECT_ID = "new-life-400922"
WORKERS = 50

# Scrape Polygon's website for stocks history for every ticker, make a dataframe out of the result,
# and append that dataframe to a list of all dataframes for all stocks. It will be concatenated to one dataframe below.
def fetch_stock_history(d):
    aggs = []
    for a in polygon_client.get_grouped_daily_aggs(d):
        aggs.append(a)

    if aggs:
        daily = [
            {
                "ticker": y.ticker,
                "timestamp": int(y.timestamp),
                "open": y.open,
                "close": y.close,
                "volume_weighted_average_price": y.vwap,
                "volume": y.volume,
                "transactions": y.transactions,
                "date": d,
            }
            for y in aggs
        ]
        df = pd.DataFrame(daily)
        df.to_csv(f"gs://arapbi-polygon/polygon/tickers/{d}.csv")
        return df
    else:
        print(f"No ticker data for {d}")


def scrape_gcp_for_csvs(file):
    print(f"Downloading {file.name}")
    file_path = f"gs://{file.bucket.name}/{file.name}"
    df = pd.read_csv(file_path)
    with dfs_lock:
        dfs.append(df)


if __name__ == "__main__":
    # Set up client connections
    polygon_secret = os.getenv('POLYGON_API_KEY')
    polygon_client = RESTClient(
        polygon_secret, retries=10, trace=False
    )
    storage_client = storage.Client()

    # Determine how many days worth of data to scrape from the API.
    # These will be feed to fetch_stock_history
    today = dt.date.today().strftime("%Y-%m-%d")
    sql = f"SELECT max(date) as dt FROM {BQ_TABLE_NAME};"
    max_date = pd.read_gbq(sql, project_id=PROJECT_ID, dialect="standard")
    max_stored_date = max_date["dt"].to_list()[0].strftime("%Y-%m-%d")
    dates = [str(d)[:10] for d in pd.date_range(max_stored_date, today)]
    #dates = [str(d)[:10] for d in pd.date_range('2021-10-24', today)]

    # Retrieving stocks csv data from google storage and making a list of dataframes.
    # These will be fed to scrape_gcp_for_csvs
    dfs = []
    dfs_lock = Lock()  # Lock for ensuring thread safety when appending to dfs
    files = list(
        storage_client.list_blobs(bucket_or_name=BUCKET_NAME, prefix=GCP_FOLDER_NAME)
    )

    print(f"Scraping web data")
    # Using ThreadPoolExecutor to fetch stock histories concurrently
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_stock_history, dates)

    print(f"Scraping GCP data")
    # Using ThreadPoolExecutor to download CSVs of stock history concurrently. _csvs
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(scrape_gcp_for_csvs, files)

    # Create the master dataframe of all stocks and their price history
    print(f"Making stocks DataFrame")
    all_tickers_history = pd.concat(dfs)
    all_tickers_history = all_tickers_history.drop("timestamp", axis=1)
    all_tickers_history["ticker"] = all_tickers_history["ticker"].astype(str)
    all_tickers_history["open"] = all_tickers_history["open"].fillna(0).astype(float)
    all_tickers_history["close"] = all_tickers_history["close"].fillna(0).astype(float)
    all_tickers_history["volume_weighted_average_price"] = (
        all_tickers_history["volume_weighted_average_price"].fillna(0).astype(float)
    )
    all_tickers_history["volume"] = all_tickers_history["volume"].fillna(0).astype(int)
    all_tickers_history["transactions"] = (
        all_tickers_history["transactions"].fillna(0).astype(int)
    )
    all_tickers_history["date"] = pd.to_datetime(all_tickers_history["date"])

    # upload the data to Bigquery
    print(f"Uploading to BQ")
    pandas_gbq.to_gbq(
        all_tickers_history,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
        table_schema=[
            {"name": "ticker", "type": "STRING"},
            {"name": "open", "type": "FLOAT"},
            {"name": "close", "type": "FLOAT"},
            {"name": "volume_weighted_average_price", "type": "FLOAT"},
            {"name": "volume", "type": "INT64"},
            {"name": "transactions", "type": "INT64"},
            {"name": "date", "type": "DATE"},
        ],
    )
