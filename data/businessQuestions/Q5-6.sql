
-- 5. Which brand has the most spend among users who were created within the past 6 months?
-- 6. Which brand has the most transactions among users who were created within the past 6 months?

-- we will do something similar to Q1-2 where we have to assume that the 'most recent' date is the 'last' date since we have no data in the actual 'last 6 months'


-- correct query assuming data in the last 6 months:
SELECT 
    b.name as brand_name,
    CONCAT('$', SUM(r.total_spent)) as total_spend,
    COUNT(r.receipt_id) AS total_transactions
FROM dim_users u
JOIN fct_receipts r ON u.user_id = r.user_id
JOIN fct_items i ON r.receipt_id = i.receipt_id
JOIN dim_brands b ON i.barcode = b.barcode
WHERE u.created_date >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
GROUP BY b.name
ORDER BY SUM(r.total_spent) DESC
LIMIT 1;


-- Q5-6 adjusted query starting at most recent:
WITH latest AS (
    SELECT
        MAX(DATE_FORMAT(u.created_date, '%Y-%m-01')) as latest_date
    FROM
        dim_users u
)
SELECT 
    b.name as brand_name,
    CONCAT('$', SUM(r.total_spent)) as total_spend,
    COUNT(r.receipt_id) AS total_transactions
FROM dim_users u
JOIN fct_receipts r ON u.user_id = r.user_id
JOIN fct_items i ON r.receipt_id = i.receipt_id
JOIN dim_brands b ON i.barcode = b.barcode
JOIN latest on 1=1
WHERE latest.latest_date >= DATE_SUB(latest.latest_date, INTERVAL 6 MONTH)
    -- '2021-02-01' >= '2020-08-01'
GROUP BY b.name
ORDER BY SUM(r.total_spent) DESC, COUNT(r.receipt_id) DESC 
-- order/limit combo works here because both columns 2 and 3 are valid for answer the question.b
-- rather than having 2 separate query's one using ORDER BY SUM(r.total_spent) and one with COUNT(r.receipt_id) we get the right answer in 1
-- i will still write out those query's for best practice since the data does not like to behave....
LIMIT 1;



-- JUST Q5
WITH latest AS (
    SELECT
        MAX(DATE_FORMAT(u.created_date, '%Y-%m-01')) as latest_date
    FROM
        dim_users u
)
SELECT 
    b.name as brand_name,
    CONCAT('$', SUM(r.total_spent)) as total_spend
FROM dim_users u
JOIN fct_receipts r ON u.user_id = r.user_id
JOIN fct_items i ON r.receipt_id = i.receipt_id
JOIN dim_brands b ON i.barcode = b.barcode
JOIN latest on 1=1
WHERE latest.latest_date >= DATE_SUB(latest.latest_date, INTERVAL 6 MONTH)
    -- '2021-02-01' >= '2020-08-01'
GROUP BY b.name
ORDER BY SUM(r.total_spent) DESC
LIMIT 1;


-- JUST Q6
WITH latest AS (
    SELECT
        MAX(DATE_FORMAT(u.created_date, '%Y-%m-01')) as latest_date
    FROM
        dim_users u
)
SELECT 
    b.name as brand_name,
    COUNT(r.receipt_id) AS total_transactions
FROM dim_users u
JOIN fct_receipts r ON u.user_id = r.user_id
JOIN fct_items i ON r.receipt_id = i.receipt_id
JOIN dim_brands b ON i.barcode = b.barcode
JOIN latest on 1=1
WHERE latest.latest_date >= DATE_SUB(latest.latest_date, INTERVAL 6 MONTH)
    -- '2021-02-01' >= '2020-08-01'
GROUP BY b.name
ORDER BY COUNT(r.receipt_id) DESC
LIMIT 1;