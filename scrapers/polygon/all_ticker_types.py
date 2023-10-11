import datetime as dt

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import pandas_gbq

from constants import BUCKET_NAME, PROJECT_ID, SECRET_ID
from google.cloud import logging, secretmanager, storage
from polygon import RESTClient

# Constants
BQ_TABLE_NAME = 'raw.all_ticker_types' # dataset.table
CSV_FILE_NAME = "all_ticker_types.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME
LOG_NAME = "polygon_all_stock_detail"

if __name__ == "__main__":
    # Set up client connections
    # Rewrite this with https://cloud.google.com/logging/docs/reference/libraries#client-libraries-usage-python
    logging_client = logging.Client()
    logger = logging_client.logger(LOG_NAME)
    secrets_client = secretmanager.SecretManagerServiceClient()
    polygon_secret = secrets_client.access_secret_version(request={"name": f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"})
    polygon_client = RESTClient(polygon_secret.payload.data.decode("UTF-8"), retries=10, trace=True)
    storage_client = storage.Client()

    # Assemlble a dataframe of ticker types
    polygon_client.get_ticker_types()
    ticker_types_resp = polygon_client.get_ticker_types()
    all_ticker_types  = [{ 'asset_class': y.asset_class,
                        'code': y.code,
                        'description': y.description,
                        'locale': y.locale} for y in ticker_types_resp]
    all_ticker_types_df = pd.DataFrame(all_ticker_types)


    # Write a CSV
    # logger.log_text(f'Writing  CSV')
    print(f'Writing tickers CSV')
    all_ticker_types_df.to_csv(CSV_FILE_NAME)

    # Upload the CSV to Google Storage
    # logger.log_text(f'Uploading stocks file')
    print(f'Uploading tickers file')
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCP_FILE_NAME)
    blob.upload_from_filename(CSV_FILE_NAME, timeout=3600)

    # upload the data to Bigquery
    # logger.log_text(f'Uploading to BQ')
    print(f'Uploading to BQ')
    pandas_gbq.to_gbq(all_ticker_types_df,
                    BQ_TABLE_NAME,
                    project_id=PROJECT_ID,
                    if_exists='replace')