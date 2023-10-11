{{ config(materialized='table') }}

with source_data as (

    SELECT
        ticker,
        count(*) as n_days,
        min(date) as min_dt,
        max(date) as max_dt,
        avg(volume_weighted_average_price) as avg_volume_weighted_average_price,
        avg(volume) as avg_volume,
        avg(transactions) as avg_transactions
    FROM raw.all_tickers_history
    GROUP BY 1
)

select *
from source_data