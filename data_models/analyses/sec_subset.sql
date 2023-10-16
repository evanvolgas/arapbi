SELECT count(distinct adsh) FROM `new-life-400922.raw.sec_financials_num`  where tag in

('NetIncomeLoss',
'StockholdersEquity',
'EarningsPerShareBasic',
'Assets',
'EarningsPerShareDiluted',
'Liabilities',
'ProfitLoss',
'CommonStockValue')
limit 1000;
