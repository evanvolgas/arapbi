with
apple AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_apple,
    volume AS volume_apple,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "AAPL"
),
adobe AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_adobe,
    volume AS volume_adobe,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "ADBE"
),
alphabet AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_alphabet,
    volume AS volume_alphabet,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "GOOGL"
),
amd AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_amd,
    volume AS volume_amd,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "AMD"
),
amazon AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_amazon,
    volume AS volume_amazon,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "AMZN"
),
asml AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_asml,
    volume AS volume_asml,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "ASML"
),
broadcom AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_broadcom,
    volume AS volume_broadcom,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "AVGO"
),
salesforce AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_salesforce,
    volume AS volume_salesforce,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "CRM"
),
intel AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_intel,
    volume AS volume_intel,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "INTC"
),
intuit AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_intuit,
    volume AS volume_intuit,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "INTU"
),
meta as (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_meta,
    volume AS volume_meta
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "META"
),
microsoft AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_microsoft,
    volume AS volume_microsoft,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "MSFT"
),
netflix AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_netflix,
    volume AS volume_netflix,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "NFLX"
),
nvidia AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_nvidia,
    volume AS volume_nvidia,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "NVDA"
),
oracle AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_oracle,
    volume AS volume_oracle,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "ORCL"
),
tesla AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_tesla,
    volume AS volume_tesla,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "TSLA"
),
tsm AS (
SELECT
    date,
    volume_weighted_average_price as volume_weighted_average_price_tsm,
    volume AS volume_tsm,
FROM `new-life-400922.etl.res_tickers_history`
WHERE ticker = "TSM"
)
SELECT
    a.date,
    a.volume_weighted_average_price_apple,
    a.volume_apple,
    b.volume_weighted_average_price_adobe,
    b.volume_adobe,
    c.volume_weighted_average_price_alphabet,
    c.volume_alphabet,
    d.volume_weighted_average_price_amd,
    d.volume_amd,
    e.volume_weighted_average_price_amazon,
    e.volume_amazon,
    f.volume_weighted_average_price_asml,
    f.volume_asml,
    g.volume_weighted_average_price_broadcom,
    g.volume_broadcom,
    h.volume_weighted_average_price_salesforce,
    h.volume_salesforce,
    i.volume_weighted_average_price_intel,
    i.volume_intel,
    j.volume_weighted_average_price_intuit,
    j.volume_intuit,
    k.volume_weighted_average_price_meta,
    k.volume_meta,
    l.volume_weighted_average_price_microsoft,
    l.volume_microsoft,
    m.volume_weighted_average_price_netflix,
    m.volume_netflix,
    n.volume_weighted_average_price_nvidia,
    n.volume_nvidia,
    o.volume_weighted_average_price_oracle,
    o.volume_oracle,
    p.volume_weighted_average_price_tesla,
    p.volume_tesla,
    q.volume_weighted_average_price_tsm,
    q.volume_tsm

FROM apple as a
LEFT JOIN adobe as b
    ON a.date = b.date
LEFT JOIN alphabet as c
    ON a.date = c.date
LEFT JOIN amd as d
    ON a.date = d.date
LEFT JOIN amazon as e
    ON a.date = e.date
LEFT JOIN asml as f
    ON a.date = f.date
LEFT JOIN broadcom as g
    ON a.date = g.date
LEFT JOIN salesforce as h
    ON a.date = h.date
LEFT JOIN intel as i
    ON a.date = i.date
LEFT JOIN intuit as j
    ON a.date = j.date
LEFT JOIN meta as k
    ON a.date = k.date
LEFT JOIN microsoft as l
    ON a.date = l.date
LEFT JOIN netflix as m
    ON a.date = m.date
LEFT JOIN nvidia as n
    ON a.date = n.date
LEFT JOIN oracle as o
    ON a.date = o.date
LEFT JOIN tesla as p
    ON a.date = p.date
LEFT JOIN tsm as q
    ON a.date = q.date
WHERE EXTRACT(ISOYEAR FROM a.date) > 2014