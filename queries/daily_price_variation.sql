
SELECT 
  h.Company,
  h.date,
  h.close,
  ROUND(((h.close - lag(h.close, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) / lag(h.close, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) * 100,2) AS price_variation
FROM daily_stock_data h;