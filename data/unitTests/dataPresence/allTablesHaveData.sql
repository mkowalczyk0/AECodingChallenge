-- Test 1.1: Verify all tables have data
SELECT 
    'dim_users' as table_name, COUNT(*) as record_count FROM dim_users
UNION ALL
SELECT 'fct_receipts', COUNT(*) FROM fct_receipts
UNION ALL
SELECT 'fct_items', COUNT(*) FROM fct_items
UNION ALL
SELECT 'dim_brands', COUNT(*) FROM dim_brands;