-- Test 3.1: Date Logic Tests
SELECT 
    'Invalid Date Records' as test,
    COUNT(*) as count
FROM fct_receipts
WHERE create_date > date_scanned
    OR date_scanned > finished_date
    OR purchase_date > date_scanned;
