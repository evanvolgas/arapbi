with ticker_types as (
SELECT
    asset_class,
    code,
    description,
    locale
FROM {{ ref('stg_all_ticker_types') }}
), all_tickers AS (
SELECT
    ticker,
    title,
    type
FROM {{ ref('stg_all_tickers') }}
), detail as (
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
    all_tickers.ticker,
    all_tickers.title,
    ticker_types.description AS type,
    detail.description,
    detail.sic_description,
    detail.total_employees,
    detail.market,
    detail.market_cap,
    detail.primary_exchange,
    detail.currency_name,
    detail.share_class_shares_outstanding,
    detail.weighted_shares_outstanding,
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
FROM all_tickers
LEFT JOIN detail
    ON all_tickers.ticker = detail.ticker
    AND all_tickers.type = detail.type
LEFT JOIN history
    ON all_tickers.ticker = history.ticker
LEFT JOIN dividends
    ON  all_tickers.ticker = dividends.ticker
    AND history.date = dividends.ex_dividend_date
LEFT JOIN ticker_types
    ON all_tickers.type = ticker_types.code
