-- Test 2.3: No Brands
SELECT 
    'Items without Brands' as test,
    COUNT(*) as count
FROM fct_items i
LEFT JOIN dim_brands b ON i.barcode = b.barcode
WHERE b.barcode IS NULL;