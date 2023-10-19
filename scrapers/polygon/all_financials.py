import datetime as dt
import os

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import numpy as np
import pandas as pd
import pandas_gbq

from polygon import RESTClient

# Constants
BQ_TABLE_NAME = "raw.all_financials_history"
PARQUET_FILE_NAME = "all_financials_history.parquet"
GCP_FILE_NAME = "polygon/financials/" + PARQUET_FILE_NAME
PROJECT_ID = "new-life-400922"
WORKERS = 50


# Scrape Polygon's website for each ticker's dividend history, make a dataframe out of the result,
# and append that dataframe to a list of all dataframes for all dividends.
# It will be concatenated to one dataframe below.
def fetch_financials(ticker="AAPL"):
    for d in polygon_client.vx.list_stock_financials(ticker):
        daily = {
            "ticker": ticker,
            "start_date": d.start_date,
            "end_date": d.end_date,
            "filing_date": d.filing_date,
            "fiscal_period": d.fiscal_period,
            "fiscal_year": d.fiscal_year,
            "cik": d.cik,
            "company_name": d.company_name,
            "financials": d.financials,
        }
        # Cash Flow Statement
        if daily.get("financials").cash_flow_statement:
            daily["cash_flow_label"] = daily.get(
                "financials"
            ).cash_flow_statement.net_cash_flow.label
            daily["cash_flow_value"] = daily.get(
                "financials"
            ).cash_flow_statement.net_cash_flow.value
            daily["cash_flow_unit"] = daily.get(
                "financials"
            ).cash_flow_statement.net_cash_flow.unit
        else:
            daily["cash_flow_label"] = None
            daily["cash_flow_value"] = None
            daily["cash_flow_unit"] = None

        daily.pop("financials", None)

        with dfs_lock:
            dfs.append(daily)

if __name__ == "__main__":
    # Set up client connection
    polygon_secret = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_secret, retries=10, trace=False)

    print(f"Querying all tickers")
    sql = """
        SELECT distinct(ticker) as ticker
        FROM `raw.all_tickers` ORDER BY 1;
        """
    df = pd.read_gbq(sql, dialect="standard")
    df = pd.read_gbq(sql, project_id=PROJECT_ID, dialect="standard")

    all_tickers = list(df["ticker"])
    dfs = []
    dfs_lock = Lock()

    # Using ThreadPoolExecutor to fetch dividends concurrently
    print(f"Scraping web data")
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_financials, all_tickers)

    # Create the master dataframe of all dividends and their price history
    print(f"Making Dividend DataFrame")
    all_financials_history = pd.DataFrame(dfs)
    all_financials_history["ticker"] = all_financials_history["ticker"].astype(str)
    all_financials_history["start_date"] = pd.to_datetime(
        all_financials_history["start_date"]
    )
    all_financials_history["end_date"] = pd.to_datetime(
        all_financials_history["end_date"]
    )
    all_financials_history["filing_date"] = pd.to_datetime(
        all_financials_history["filing_date"]
    )
    all_financials_history["fiscal_period"] = all_financials_history[
        "fiscal_period"
    ].astype(str)
    all_financials_history["fiscal_year"] = all_financials_history[
        "fiscal_year"
    ].replace(r'^\s*$', 0, regex=True).fillna(0).astype(int)
    all_financials_history["cik"] = all_financials_history["cik"].astype(str)
    all_financials_history["company_name"] = all_financials_history[
        "company_name"
    ].astype(str)
    all_financials_history["cash_flow_label"] = all_financials_history[
        "cash_flow_label"
    ].astype(str)
    all_financials_history["cash_flow_value"] = all_financials_history[
        "cash_flow_value"
    ].replace(r'^\s*$', 0, regex=True).fillna(0).astype(int)
    all_financials_history["cash_flow_unit"] = all_financials_history[
        "cash_flow_unit"
    ].astype(str)

    # Write a CSV to Google Storage
    print(f"Writing {GCP_FILE_NAME}")
    all_financials_history.to_parquet(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    # upload the data to Bigquery
    print(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_financials_history,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
        table_schema=[
            {"name": "ticker", "type": "STRING"},
            {"name": "start_date", "type": "DATE"},
            {"name": "end_date", "type": "DATE"},
            {"name": "filing_date", "type": "DATE"},
            {"name": "fiscal_period", "type": "STRING"},
            {"name": "fiscal_year", "type": "INT64"},
            {"name": "cik", "type": "STRING"},
            {"name": "company_name", "type": "STRING"},
            {"name": "cash_flow_label", "type": "STRING"},
            {"name": "cash_flow_value", "type": "FLOAT"},
            {"name": "cash_flow_unit", "type": "STRING"},
        ],
    )
