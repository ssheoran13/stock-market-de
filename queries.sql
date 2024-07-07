-- Daily variation in prices

SELECT 
  h.Company,
  h.date,
  h.close,
  ((h.close - lag(h.close, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) / lag(h.close, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) * 100 AS price_variation
FROM historical_stock_data h;


-- Daily variation in volume

SELECT 
  h.Company,
  h.date,
  h.volume,
  ROUND(((h.volume - lag(h.volume, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) / lag(h.volume, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) * 100, 2) AS volume_variation
FROM historical_stock_data h limit 10;
