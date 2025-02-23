-- Test 4.1: Receipt Status Distribution
SELECT 
    rewards_receipt_status,
    COUNT(*) as count,
    CONCAT(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), '%') as percentage
FROM fct_receipts
GROUP BY rewards_receipt_status;
