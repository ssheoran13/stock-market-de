SELECT 
  h.Company,
  h.date,
  h.volume,
  ROUND(((h.volume - lag(h.volume, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) / lag(h.volume, 1) OVER (PARTITION BY h.Company ORDER BY h.date)) * 100, 2) AS volume_variation
FROM daily_stock_data h;