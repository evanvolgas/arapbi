#!/bin/sh

# Use the Docker run argument to construct an actual task
if [ "$1" = "all_dividends_history" ]; then
    python scrapers/polygon/all_dividends_history.py
elif [ "$1" = "all_ticker_types" ]; then
    python scrapers/polygon/all_ticker_types.py
elif [ "$1" = "all_tickers" ]; then
    python scrapers/polygon/all_tickers.py
elif [ "$1" = "all_tickers_detail" ]; then
    python scrapers/polygon/all_tickers_detail.py
elif [ "$1" = "all_tickers_history" ]; then
    python scrapers/polygon/all_tickers_history.py
elif [ "$1" = "sec_financials" ]; then
    python scrapers/sec/sec_financials.py
elif [ "$1" = "sec_tickers" ]; then
    python scrapers/sec/sec_tickers.py
elif [ "$1" = "run" ]; then
    cd data_models && && dbt deps && dbt run
elif [ "$1" = "test" ]; then
    cd data_models && dbt deps && dbt test
else
    echo "Unknown command: $1"
    exit 1
fi