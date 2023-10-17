with source_data as (

    SELECT
        count(distinct ticker) as distinct_tickers,
        count(*) as n_rows,
        min(date) as min_dt,
        max(date) as max_dt
    FROM raw.all_tickers_history
)

select *
from source_data
