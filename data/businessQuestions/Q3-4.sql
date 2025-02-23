

-- 3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
-- 4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

SELECT 
    rewards_receipt_status,
    SUM(purchased_item_count) AS total_items_purchased,
    CONCAT("$", ROUND(AVG(total_spent),2)) as avg_spend
FROM fct_receipts
WHERE rewards_receipt_status IN ('ACCEPTED', 'REJECTED')
GROUP BY rewards_receipt_status;