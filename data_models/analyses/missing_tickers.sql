SELECT
count(distinct b.ticker ) as ticker
FROM {{ ref('stg_all_tickers') }} as a
full outer join {{ ref('stg_all_tickers_historical') }} as b
on a.ticker = b.ticker
where a.ticker is null