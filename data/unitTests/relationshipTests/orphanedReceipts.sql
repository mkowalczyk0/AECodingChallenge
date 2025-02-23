-- Test 2.1: Orphaned Receipts
SELECT 
    'Orphaned Receipts' as test,
    COUNT(*) as count
FROM fct_receipts r
LEFT JOIN dim_users u ON r.user_id = u.user_id
WHERE u.user_id IS NULL;