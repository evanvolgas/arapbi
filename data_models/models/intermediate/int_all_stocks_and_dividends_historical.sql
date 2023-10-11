with detail as (
SELECT
    ticker,
    title,
    type,
    description,
    sic_description,
    total_employees,
    market,
    market_cap,
    primary_exchange,
    currency_name,
    share_class_shares_outstanding,
    weighted_shares_outstanding,
FROM {{ ref('stg_all_tickers_detail') }}
), history as
(
SELECT
    ticker,
    open,
    close,
    volume_weighted_average_price,
    transactions,
    date,
    year
FROM {{ ref('stg_all_tickers_historical') }}
), dividends AS
(
SELECT
    ticker,
    cash_amount,
    currency,
    declaration_date,
    declaration_yr,
    dividend_type,
    ex_dividend_date,
    ex_dividend_yr,
    frequency,
    pay_date,
    pay_yr
FROM {{ ref('stg_all_dividends_historical') }}
WHERE ex_dividend_date >= '2018-10-12'
)
SELECT
    detail.*,
    open,
    close,
    volume_weighted_average_price,
    transactions,
    date,
    year,
    cash_amount,
    currency,
    declaration_date,
    declaration_yr,
    dividend_type,
    ex_dividend_date,
    ex_dividend_yr,
    frequency,
    pay_date,
    pay_yr
FROM  detail
LEFT JOIN history
    ON detail.ticker = history.ticker
LEFT JOIN dividends
    ON  history.ticker = dividends.ticker
    AND history.date = ex_dividend_date
