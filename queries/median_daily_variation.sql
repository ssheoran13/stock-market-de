WITH previous_close AS (
    SELECT 
        company,
        date,
        close,
        LAG(close) OVER (PARTITION BY company ORDER BY date) AS prev_close_price
    FROM 
        historical_stock_data
    -- you can specify the date range here for which the median has to be found
    where date>=date_sub(current_date(),interval 70 day) and date<=current_date()
),
daily_variations AS (
    SELECT 
        company,
        date,
        (close - prev_close_price) / prev_close_price * 100.0 AS percent_variation
    FROM 
        previous_close
    WHERE 
        prev_close_price IS NOT NULL
),

ranked_variations AS (
    SELECT 
        company,
        percent_variation,
        ROW_NUMBER() OVER (partition by company ORDER BY percent_variation) AS row_num,
        COUNT(*) OVER (partition by company) AS total_count
    FROM 
        daily_variations
)
SELECT 
    company,percent_variation AS median_percent_variation
FROM 
    ranked_variations
WHERE 
    row_num IN ((total_count + 1) / 2, (total_count + 2) / 2);
