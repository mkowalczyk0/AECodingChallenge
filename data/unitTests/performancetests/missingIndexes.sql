-- Test 5.1: Check for missing indexes
SELECT 
    'Missing user_id index' as test,
    NOT EXISTS (
        SELECT 1 
        FROM information_schema.statistics 
        WHERE table_schema = 'fetchTest' 
        AND table_name = 'fct_receipts' 
        AND index_name = 'idx_user_id'
    ) as result
UNION ALL
SELECT 
    'Missing purchase_date index',
    NOT EXISTS (
        SELECT 1 
        FROM information_schema.statistics 
        WHERE table_schema = 'fetchTest' 
        AND table_name = 'fct_receipts' 
        AND index_name = 'idx_purchase_date'
    );