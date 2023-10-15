import logging as log

import pandas as pd
import pandas_gbq
import requests

from io import BytesIO, StringIO
from zipfile import ZipFile

from bs4 import BeautifulSoup
from google.cloud import logging, storage

BQ_TABLE_NAME = "raw.sec_financials"  # dataset.table
BUCKET_NAME = "arapbi-polygon"
COMPANY_FINANCIALS_URL = "https://www.sec.gov/dera/data/financial-statement-data-sets"
CSV_FILE_NAME = "sec_financials.csv"
PROJECT_ID = "new-life-400922"


def string_pad(cik_str):
    c = str(cik_str)
    cik = c.zfill(10)
    return cik


def unzip_response(r):
    zipfile = ZipFile(BytesIO(r.content))
    files = [zipfile.open(file_name) for file_name in zipfile.namelist()]
    return files


if __name__ == "__main__":
    logging_client = logging.Client()
    logging_client.setup_logging()
    storage_client = storage.Client()

    # Assemlble a dictionary of stock tickers from the SEC's listing of them
    log.info(f"Retriving SEC data")
    r = requests.get(COMPANY_FINANCIALS_URL)
    soup = BeautifulSoup(r.content)

    links = []
    for link in soup.find_all("a", href=True):
        if ".zip" in link["href"]:
            links.append(f"https://www.sec.gov/{link['href']}")

    for link in links[0:22]:
        zip_file = requests.get(link)
        unzipped = unzip_response(zip_file)
        for file in unzipped:
            if "sub" in file.name or "num" in file.name:
                print(f"Assembling data frame from {file.name}")
                df = pd.read_csv(StringIO(file.read().decode("utf-8")), delimiter="\t")
                df["year"] = int(link[-10:-6])
                df["quarter"] = int(link[-5:-4])

                if "cik" in df.columns:
                    df["cik"] = df["cik"].apply(string_pad)

                print(
                    f"Writing {file.name} to gs://arapbi-sec/financials/{link[-10:-4]}_{file.name}"
                )
                log.info(f"Writing {file.name}")
                df.to_csv(f"gs://arapbi-sec/financials/{link[-10:-4]}_{file.name}")

                print(f"Uploading {file.name} to BQ")
                table_name = file.name[0:-4]
                pandas_gbq.to_gbq(
                    df,
                    f"raw.sec_financials_{table_name}",
                    project_id=PROJECT_ID,
                    if_exists="append",
                )
