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
BQ_TABLE_NAME = "raw.all_tickers_detail"  # dataset.table
CSV_FILE_NAME = "all_tickers_detail.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME

# Scrape Polygon's website for stocks details for every ticker, make a dataframe out of the result,
# and append that dataframe to a list of all dataframes for all stocks. It will be concatenated to one dataframe below


def fetch_ticker_details(ticker):
    # logger.log_text(f'Retrieving stocks data for {ticker} // {title}')

    aggs = []
    a = polygon_client.get_ticker_details(ticker)
    if a:
        aggs.append(a)

    if aggs:
        t = [
            {
                "active": y.active,
                "address1": y.address.address1 if y.address else "",
                "address2": y.address.address2 if y.address else "",
                "city": y.address.city if y.address else "",
                "state": y.address.state if y.address else "",
                "country": y.address.country if y.address else "",
                "postal_code": y.address.postal_code if y.address else "",
                "cik": y.cik,
                "composite_figi": y.composite_figi,
                "currency_name": y.currency_name,
                "description": y.description,
                "ticker_root": y.ticker_root,
                "title": y.name,
                "homepage_url": y.homepage_url,
                "list_date": y.list_date,
                "locale": y.locale,
                "market": y.market,
                "market_cap": y.market_cap,
                "phone_number": y.phone_number,
                "primary_exchange": y.primary_exchange,
                "share_class_figi": y.share_class_figi,
                "share_class_shares_outstanding": y.share_class_shares_outstanding,
                "sic_code": y.sic_code,
                "sic_description": y.sic_description,
                "ticker": y.ticker,
                "total_employees": y.total_employees,
                "type": y.type,
                "weighted_shares_outstanding": y.weighted_shares_outstanding,
            }
            for y in aggs
        ]
        df = pd.DataFrame(t)

        with dfs_lock:
            dfs.append(df)
    else:
        log.warn(f"No Response for {ticker}")


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

    log.info(f"Querying all tickers")
    sql = """
        SELECT distinct(ticker) as ticker
        FROM `raw.all_tickers` ORDER BY 1;
        """
    df = pd.read_gbq(sql, dialect="standard")
    df = pd.read_gbq(sql, project_id=PROJECT_ID, dialect="standard")

    all_tickers = list(df["ticker"])

    dfs = []
    dfs_lock = Lock()

    log.info(f"Scraping web data")
    # Using ThreadPoolExecutor to fetch stock histories concurrently
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_ticker_details, all_tickers)

    all_stocks = pd.concat(dfs)
    all_stocks["active"] = all_stocks["active"].astype(bool)
    all_stocks["address1"] = all_stocks["address1"].astype(str)
    all_stocks["address2"] = all_stocks["address2"].astype(str)
    all_stocks["city"] = all_stocks["city"].astype(str)
    all_stocks["state"] = all_stocks["state"].astype(str)
    all_stocks["country"] = all_stocks["country"].astype(str)
    all_stocks["postal_code"] = all_stocks["postal_code"].astype(str)
    all_stocks["cik"] = all_stocks["cik"].astype(str)
    all_stocks["composite_figi"] = all_stocks["composite_figi"].astype(str)
    all_stocks["currency_name"] = all_stocks["currency_name"].astype(str)
    all_stocks["description"] = all_stocks["description"].astype(str)
    all_stocks["ticker_root"] = all_stocks["ticker_root"].astype(str)
    all_stocks["title"] = all_stocks["title"].astype(str)
    all_stocks["homepage_url"] = all_stocks["homepage_url"].astype(str)
    all_stocks["list_date"] = pd.to_datetime(all_stocks["list_date"])
    all_stocks["locale"] = all_stocks["locale"].astype(str)
    all_stocks["market"] = all_stocks["market"].astype(str)
    all_stocks["market_cap"] = all_stocks["market_cap"].fillna(0).astype(float)
    all_stocks["phone_number"] = all_stocks["phone_number"].astype(str)
    all_stocks["primary_exchange"] = all_stocks["primary_exchange"].astype(str)
    all_stocks["share_class_figi"] = all_stocks["share_class_figi"].astype(str)
    all_stocks["share_class_shares_outstanding"] = (
        all_stocks["share_class_shares_outstanding"].fillna(0).astype(int)
    )
    all_stocks["sic_code"] = all_stocks["sic_code"].astype(str)
    all_stocks["sic_description"] = all_stocks["sic_description"].astype(str)
    all_stocks["ticker"] = all_stocks["ticker"].astype(str)
    all_stocks["total_employees"] = all_stocks["total_employees"].fillna(0).astype(int)
    all_stocks["type"] = all_stocks["type"].astype(str)
    all_stocks["weighted_shares_outstanding"] = (
        all_stocks["weighted_shares_outstanding"].fillna(0).astype(int)
    )

    # Write a CSV to Google Storage
    log.info(f"Writing {GCP_FILE_NAME}")
    all_stocks.to_csv(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    log.info(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_stocks,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
    )
