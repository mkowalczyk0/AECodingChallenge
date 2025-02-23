
-- Test 3.2: Numeric Value Tests
SELECT 
    'Invalid Numeric Values' as test,
    COUNT(*) as count
FROM fct_receipts
WHERE total_spent < 0
    OR purchased_item_count < 0
    OR points_earned < 0;