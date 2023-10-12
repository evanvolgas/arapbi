import datetime as dt
import logging as log

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import pandas_gbq

from constants import BUCKET_NAME, PROJECT_ID, SECRET_ID
from google.cloud import logging, secretmanager, storage
from polygon import RESTClient

# Constants
BQ_TABLE_NAME = "etl.all_ticker_types"  # dataset.table
CSV_FILE_NAME = "all_ticker_types.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME

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

    # Assemlble a dataframe of ticker types
    log.info(f"Scraping web data")
    polygon_client.get_ticker_types()
    ticker_types_resp = polygon_client.get_ticker_types()
    all_ticker_types = [
        {
            "asset_class": y.asset_class,
            "code": y.code,
            "description": y.description,
            "locale": y.locale,
        }
        for y in ticker_types_resp
    ]
    all_ticker_types_df = pd.DataFrame(all_ticker_types)

    # Write a CSV to Google Storage
    print(f"Writing {GCP_FILE_NAME}")
    all_ticker_types_df.to_csv(f"gs://arapbi-polygon/{GCP_FILE_NAME}")

    # upload the data to Bigquery
    log.info(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_ticker_types_df, BQ_TABLE_NAME, project_id=PROJECT_ID, if_exists="replace"
    )
