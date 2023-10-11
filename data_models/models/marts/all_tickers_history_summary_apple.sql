select *
from {{ ref('all_tickers_history_summary') }}
where ticker = "AAPL"
