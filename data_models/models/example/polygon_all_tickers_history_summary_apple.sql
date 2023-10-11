
-- Use the `ref` function to select from other models

select *
from {{ ref('polygon_all_tickers_history_summary') }}
where ticker = "AAPL"
