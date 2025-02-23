-- Test 4.2: Receipt Items Quantity Check
SELECT 
    'Receipts with Item Count Mismatch' as test,
    COUNT(*) as count
FROM (
    SELECT 
        r.receipt_id,
        r.purchased_item_count as reported_count,
        SUM(i.quantity_purchased) as actual_count
    FROM fct_receipts r
    LEFT JOIN fct_items i ON r.receipt_id = i.receipt_id
    GROUP BY r.receipt_id, r.purchased_item_count
    HAVING reported_count != actual_count
) t;