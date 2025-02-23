-- Test 1.2: Verify no duplicate primary keys
SELECT 'dim_users duplicates' as test, COUNT(*) - COUNT(DISTINCT user_id) as duplicate_count FROM dim_users
UNION ALL
SELECT 'fct_receipts duplicates', COUNT(*) - COUNT(DISTINCT receipt_id) FROM fct_receipts
UNION ALL
SELECT 'fct_items duplicates', COUNT(*) - COUNT(DISTINCT item_id) FROM fct_items
UNION ALL
SELECT 'dim_brands duplicates', COUNT(*) - COUNT(DISTINCT barcode) FROM dim_brands;
