-- Test 2.2: Orphaned Items
SELECT 
    'Orphaned Items' as test,
    COUNT(*) as count
FROM fct_items i
LEFT JOIN fct_receipts r ON i.receipt_id = r.receipt_id
WHERE r.receipt_id IS NULL;