import datetime as dt
import os

import pandas as pd
import pandas_gbq

from constants import *
from polygon import RESTClient

# Constants
BQ_TABLE_NAME = "raw.all_ticker_types"
CSV_FILE_NAME = "all_ticker_types.csv"
GCP_FILE_NAME = "polygon/ticker_details/" + CSV_FILE_NAME

if __name__ == "__main__":
    polygon_secret = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_secret, retries=10, trace=False)

    # Assemlble a dataframe of ticker types
    print(f"Scraping web data")
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
    print(f"Uploading {BQ_TABLE_NAME} to BQ")
    pandas_gbq.to_gbq(
        all_ticker_types_df,
        BQ_TABLE_NAME,
        project_id=PROJECT_ID,
        if_exists="replace",
        table_schema=[
            {"name": "asset_class", "type": "STRING"},
            {"name": "code", "type": "FLOAT"},
            {"name": "description", "type": "STRING"},
            {"name": "locale", "type": "DATE"},
        ],
    )
