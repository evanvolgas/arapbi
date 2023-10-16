import pandas as pd
import pandas_gbq
import requests

from io import BytesIO, StringIO
from zipfile import ZipFile

from bs4 import BeautifulSoup

COMPANY_FINANCIALS_URL = "https://www.sec.gov/dera/data/financial-statement-data-sets"
PROJECT_ID = "new-life-400922"

SUB_DTYPE = {
    "adsh": "string",
    "cik": int,
    "name": "string",
    "sic": float,
    "countryba": "string",
    "stprba": "string",
    "cityba": "string",
    "zipba": "string",
    "bas1": "string",
    "bas2": "string",
    "baph": "string",
    "countryma": "string",
    "stprma": "string",
    "cityma": "string",
    "zipma": "string",
    "mas1": "string",
    "mas2": "string",
    "countryinc": "string",
    "stprinc": "string",
    "ein": "Int64",
    "former": "string",
    "changed": float,
    "afs": "string",
    "wksi": "string",
    "fye": float,
    "form": "string",
    "period": float,
    "fy": float,
    "fp": "string",
    "filed": "string",
    "accepted": "string",
    "prevrpt": "string",
    "detail": "string",
    "instance": "string",
    "nciks": "int64",
    "aciks": "string",
    "year": int,
    "quarter": int,
}

NUM_DTYPE = {
    "adsh": "string",
    "tag": "string",
    "version": "string",
    "coreg": "string",
    "ddate": int,
    "qtrs": int,
    "uom": "string",
    "value": float,
    "footnote": "string",
    "year": int,
    "quarter": int,
}


def string_pad(cik_str):
    c = str(cik_str)
    cik = c.zfill(10)
    return cik


def unzip_response(r):
    zipfile = ZipFile(BytesIO(r.content))
    files = [zipfile.open(file_name) for file_name in zipfile.namelist()]
    return files


if __name__ == "__main__":
    # Assemlble a dictionary of stock tickers from the SEC's listing of them
    print(f"Retriving SEC data")
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
                if "sub" in file.name:
                    print("processing the sub file")
                    df = pd.read_csv(
                        StringIO(file.read().decode("utf-8")),
                        delimiter="\t",
                        dtype=SUB_DTYPE,
                    )
                    df["cik"] = df["cik"].apply(string_pad)
                    df["year"] = int(link[-10:-6])
                    df["quarter"] = int(link[-5:-4])
                if "num" in file.name:
                    print("processing the num file")
                    df = pd.read_csv(
                        StringIO(file.read().decode("utf-8")),
                        delimiter="\t",
                        dtype=NUM_DTYPE,
                    )
                    df["year"] = int(link[-10:-6])
                    df["quarter"] = int(link[-5:-4])

                print(f"Writing {file.name}")
                df.to_csv(f"gs://arapbi-sec/financials/{link[-10:-4]}_{file.name}")

                print(f"Uploading {file.name} to BQ")
                table_name = file.name[0:-4]
                pandas_gbq.to_gbq(
                    df,
                    f"raw.sec_financials_{table_name}",
                    project_id=PROJECT_ID,
                    if_exists="append",
                )
