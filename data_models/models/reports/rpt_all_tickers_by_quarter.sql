SELECT
    a.ticker,
    a.title,
    a.type,
    a.cik,
    a.sic_description,
    a.year,
    a.quarter,
    a.volume_weighted_average_price,
    a.avg_cash_amount,
    a.avg_weighted_shares_outstanding,
    a.avg_transactions,
    a.avg_volume,
    a.min_dt,
    a.max_dt,
    a.number_days_in_quarter,
    b.earnings_per_share_basic,
    b.earnings_per_share_diluted,
    b.profit_loss
FROM {{ ref('int_all_tickers_by_quarter') }} AS a
LEFT JOIN {{ ref('int_eps_pl_by_ticker') }} AS b
ON a.cik = b.cik
AND a.year = b.year
AND a.quarter = b.quarter
ORDER BY a.ticker, a.title, a.type, a.min_dt