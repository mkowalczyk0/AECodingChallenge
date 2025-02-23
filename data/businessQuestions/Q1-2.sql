

-- 1. "What are the top 5 brands by receipts scanned for most recent month?"
-- 2. "How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?"

-- These 2 questions with the current ingestion are incredibly difficult to answer, not IMPOSSIBLE, just difficult and time consuming because of how much more processing is required
-- We would need to add multiple checks to not only create records for missing brand attribution for the join, but do so by parsing the item description
-- I show an example of how to handle it


-- technically this is the correct query, 
-- without populating barcords in brands table with what its picking up it will return nothing for the LATEST month
-- but if we assume the data is pretty as a princess (maybe one day), this will give us our answer
WITH latest_month AS (
    SELECT 
        MAX(DATE_FORMAT(date_scanned, '%Y-%m-01')) AS latest_month
    FROM fct_receipts
)
SELECT
    b.name AS brand_name,
    COUNT(*) AS receipts_scanned
FROM 
    fct_receipts r
JOIN 
    fct_items i ON r.receipt_id = i.receipt_id
JOIN 
    dim_brands b ON i.barcode = b.barcode
WHERE 
    DATE_FORMAT(r.date_scanned, '%Y-%m-01') = (SELECT latest_month FROM latest_month)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5; 


-- example of using description 
WITH latest_month AS (
    SELECT 
        MAX(DATE_FORMAT(date_scanned, '%Y-%m-01')) AS latest_month
    FROM fct_receipts
)
SELECT
    DATE_FORMAT(r.date_scanned, '%Y-%m-01') AS latest_month,
    CASE
        WHEN LOWER(i.description) LIKE '%mueller austria%' THEN 'Mueller Austria'
        WHEN LOWER(i.description) LIKE '%thindust%' THEN 'Thindust'
        ELSE i.description -- we can use a case when here after identifying the brand from the description
    END AS brand_name,
    COUNT(DISTINCT r.receipt_id) AS receipts_scanned
FROM 
    fct_receipts r
JOIN fct_items i ON r.receipt_id = i.receipt_id
WHERE 
    DATE_FORMAT(r.date_scanned, '%Y-%m-01') = (SELECT latest_month FROM latest_month)
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 5; 


-- proper ref 
-- using 'most recent' date that the join allows from brands
WITH latest_month AS (
    SELECT 
        MAX(DATE_FORMAT(r.date_scanned, '%Y-%m-01')) AS latest_month
    FROM fct_receipts r
    JOIN fct_items i ON r.receipt_id = i.receipt_id
    JOIN dim_brands b ON i.barcode = b.barcode
    WHERE b.name IS NOT NULL -- returns the actual 'latest' date it can for brand name that exists
)
SELECT
    DATE_FORMAT(r.date_scanned, '%Y-%m-01') AS latest_month,
    b.name AS brand_name,
    COUNT(*) AS receipts_scanned
FROM 
    fct_receipts r
JOIN fct_items i ON r.receipt_id = i.receipt_id
JOIN dim_brands b ON i.barcode = b.barcode
WHERE 
    DATE_FORMAT(r.date_scanned, '%Y-%m-01') = (SELECT latest_month FROM latest_month)
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 5; 


-- compare rankings example query assuming proper data, wont do an example with description beacuse proof is above
WITH max_date_cte AS (
    SELECT DATE_FORMAT(MAX(date_scanned), '%Y-%m-01') AS max_month
    FROM fct_receipts
),
months AS (
    SELECT 
        DATE_FORMAT(date_scanned, '%Y-%m-01') as month
    FROM fct_receipts, max_date_cte
    WHERE date_scanned >= DATE_SUB(max_month, INTERVAL 1 MONTH)
    GROUP BY DATE_FORMAT(date_scanned, '%Y-%m-01')
),
rankings AS (
    SELECT 
        DATE_FORMAT(r.date_scanned, '%Y-%m-01') as month,
        b.name as brand_name,
        COUNT(DISTINCT r.receipt_id) as receipt_count,
        DENSE_RANK() OVER (PARTITION BY DATE_FORMAT(r.date_scanned, '%Y-%m-01') ORDER BY COUNT(DISTINCT r.receipt_id) DESC) as rank_num -- using DENSE_RANK to better look at ties
    FROM fct_receipts r
    JOIN fct_items i ON r.receipt_id = i.receipt_id
    JOIN dim_brands b ON i.barcode = b.barcode
    WHERE DATE_FORMAT(r.date_scanned, '%Y-%m-01') IN (SELECT month FROM months)
    GROUP BY 
        DATE_FORMAT(r.date_scanned, '%Y-%m-01'),
        b.name
)
SELECT 
    brand_name,
    MAX(
        CASE 
            WHEN month = (SELECT MAX(month) FROM months) THEN rank_num 
        END) as current_rank,
    MAX(
        CASE 
            WHEN month = (SELECT MIN(month) FROM months) THEN rank_num 
        END) as previous_rank
FROM rankings
WHERE rank_num <= 5
GROUP BY brand_name
ORDER BY current_rank;



