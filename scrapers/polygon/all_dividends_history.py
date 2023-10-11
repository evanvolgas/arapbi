import datetime as dt

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import pandas_gbq

from constants import PROJECT_ID, SECRET_ID, WORKERS
from google.cloud import logging, secretmanager, storage
from polygon import RESTClient

# Constants
BQ_TABLE_NAME = 'raw.all_dividends_history'
CSV_FILE_NAME = "all_dividends_history.csv"
GCP_FILE_NAME = "polygon/dividends/" + CSV_FILE_NAME
LOG_NAME = "polygon_all_dividends_history"

# Scrape Polygon's website for each ticker's dividend history, make a dataframe out of the result,
# and append that dataframe to a list of all dataframes for all dividends. It will be concatenated to one dataframe below
def fetch_dividend_history(ticker):
    print(f'Retrieving dividend data for {ticker}')
    for d in polygon_client.list_dividends(ticker):
        print(f'Retrieved dividend data for {ticker}')
        daily  = { 'ticker': ticker,
                    'cash_amount': d.cash_amount,
                    'currency': d.currency,
                    'declaration_date': d.declaration_date,
                    'dividend_type': d.dividend_type,
                    'ex_dividend_date': d.ex_dividend_date,
                    'frequency': d.frequency,
                    'pay_date': d.record_date}
        with dfs_lock:
            dfs.append(daily)
        print(f'Assembled dataframe of dividend history for {ticker}')

if __name__ == "__main__":
    # Set up client connections
    # Rewrite this with https://cloud.google.com/logging/docs/reference/libraries#client-libraries-usage-python
    logging_client = logging.Client()
    logger = logging_client.logger(LOG_NAME)
    secrets_client = secretmanager.SecretManagerServiceClient()
    polygon_secret = secrets_client.access_secret_version(request={"name": f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"})
    polygon_client = RESTClient(polygon_secret.payload.data.decode("UTF-8"), retries=10, trace=True)
    storage_client = storage.Client()

    sql = """
        SELECT distinct(ticker) as ticker
        FROM `raw.all_tickers` ORDER BY 1;
        """
    df = pd.read_gbq(sql, dialect="standard")
    df = pd.read_gbq(sql,
                    project_id=PROJECT_ID,
                    dialect="standard"
                    )

    all_tickers =  list(df['ticker'])
    dfs = []
    dfs_lock = Lock()

    # Using ThreadPoolExecutor to fetch stock histories concurrently
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_dividend_history, all_tickers)

    # Create the master dataframe of all dividends and their price history
    # logger.log_text(f'Making Dividend DataFrame')
    print(f'Making Dividend DataFrame')
    all_dividends_history = pd.DataFrame(dfs)
    all_dividends_history['ticker'] = all_dividends_history['ticker'].astype(str)
    all_dividends_history['cash_amount'] = all_dividends_history['cash_amount'].fillna(0).astype(float)
    all_dividends_history['currency'] = all_dividends_history['currency'].fillna(0).astype(str)
    all_dividends_history['declaration_date'] = pd.to_datetime(all_dividends_history['declaration_date'])
    all_dividends_history['dividend_type'] = all_dividends_history['dividend_type'].fillna(0).astype(str)
    all_dividends_history['ex_dividend_date'] =  pd.to_datetime(all_dividends_history['ex_dividend_date'])
    all_dividends_history['frequency'] = all_dividends_history['frequency'].fillna(0).astype(int)
    all_dividends_history['pay_date'] =  pd.to_datetime(all_dividends_history['pay_date'])

    # Write a CSV to Google Storage
    # logger.log_text(f'Writing Dividend CSV')
    print(f'Writing {GCP_FILE_NAME}')
    all_dividends_history.to_csv(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    # upload the data to Bigquery
    # logger.log_text(f'Uploading to BQ')
    print(f'Uploading to BQ')
    # add schema table_schema   https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_gbq.html
    pandas_gbq.to_gbq(all_dividends_history,
                    BQ_TABLE_NAME,
                    project_id=PROJECT_ID,
                    if_exists='replace',
                    table_schema=[{'name': 'ticker', 'type': 'STRING'},
                                    {'name': 'cash_amount', 'type': 'FLOAT'},
                                    {'name': 'currency', 'type': 'STRING'},
                                    {'name': 'declaration_date', 'type': 'DATE'},
                                    {'name': 'dividend_type', 'type': 'STRING'},
                                    {'name': 'ex_dividend_date', 'type': 'DATE'},
                                    {'name': 'frequency', 'type': 'INT64'},
                                    {'name': 'pay_date', 'type': 'DATE'}
                                    ],)


