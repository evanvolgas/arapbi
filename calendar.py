import logging as log

import pandas as pd
import pandas_gbq

PROJECT_ID = "new-life-400922"


dates = pd.DataFrame(pd.date_range("1970-01-01", "2070-01-01"), columns=["date"])
dates["year"] = dates["date"].dt.year
dates["quarter"] = dates["date"].dt.quarter
dates["month"] = dates["date"].dt.month
dates["day"] = dates["date"].dt.day
dates["day_name"] = dates["date"].dt.day_name()
dates["day_of_week"] = dates["date"].dt.day_of_week

dates.to_csv(f"gs://arapbi-sec/calendar.csv")

pandas_gbq.to_gbq(
    dates,
    "raw.calendar",
    project_id=PROJECT_ID,
    if_exists="replace",
)
