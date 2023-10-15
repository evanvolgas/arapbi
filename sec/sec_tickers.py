import logging as log

import pandas as pd
import pandas_gbq
import requests

from google.cloud import logging, storage

BQ_TABLE_NAME = "raw.sec_tickers"  # dataset.table
BUCKET_NAME = "arapbi-polygon"
COMPANY_TICKERS_URL = 'https://www.sec.gov/files/company_tickers.json'
CSV_FILE_NAME = "sec_tickers.csv"
PROJECT_ID = "new-life-400922"


def string_pad(cik_str):
    c = str(cik_str)
    cik = c.zfill(10)
    return cik


if __name__ == "__main__":
    logging_client = logging.Client()
    logging_client.setup_logging()
    storage_client = storage.Client()

    # Assemlble a dictionary of stock tickers from the SEC's listing of them
    log.info(f"Retriving SEC data")
    r = requests.get(COMPANY_TICKERS_URL)

    log.info(f"Assembling SEC dataframe")
    tickers_df = pd.DataFrame(dict(r.json())).T
    tickers_df['cik'] = tickers_df['cik_str'].apply(string_pad)
    tickers_df = tickers_df.drop('cik_str', axis=1)

    log.info(f"Writing {CSV_FILE_NAME}")
    tickers_df.to_csv(f"gs://arapbi-sec/tickers/{CSV_FILE_NAME}")

    log.info(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        tickers_df,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
    )
