
USE fetchTest;

DROP TABLE IF EXISTS dim_users;
DROP TABLE IF EXISTS fct_receipts;
DROP TABLE IF EXISTS fct_items;
DROP TABLE IF EXISTS dim_brands;


-- will be treating this like bigquery or any other warehouse and not enforce Foreign Key Constraints
-- users -> receipts -> items -> brands

CREATE TABLE dim_users (
    user_id VARCHAR(255) PRIMARY KEY,
    created_date TIMESTAMP,
    last_login TIMESTAMP,
    state VARCHAR(7),
    role VARCHAR(10),
    active BOOLEAN
);

CREATE TABLE fct_receipts (
    receipt_id VARCHAR(255) PRIMARY KEY,
    create_date TIMESTAMP,
    date_scanned TIMESTAMP,
    finished_date TIMESTAMP,
    modify_date TIMESTAMP,
    points_awarded_date TIMESTAMP,
    purchase_date TIMESTAMP,
    bonus_points_earned INT,
    points_earned INT,
    purchased_item_count INT,
    total_spent DECIMAL(10,2),
    rewards_receipt_status VARCHAR(50),
    user_id VARCHAR(255),
    bonus_points_earned_reason TEXT,
    INDEX idx_user_id (user_id),
    INDEX idx_purchase_date (purchase_date),
    INDEX idx_rewards_receipt_status (rewards_receipt_status)
    );

CREATE TABLE fct_items (
    item_id VARCHAR(255) PRIMARY KEY,
    receipt_id VARCHAR(255),
    barcode VARCHAR(255),
    description VARCHAR(255),
    final_price DECIMAL(10,2),
    item_price DECIMAL(10,2),
    quantity_purchased INT,
    partner_item_id INT,
    INDEX idx_barcode (barcode)
    );


CREATE TABLE dim_brands (
    barcode VARCHAR(255) PRIMARY KEY,
    brand_id VARCHAR(255),
    brand_code VARCHAR(255),
    category VARCHAR(255),
    category_code VARCHAR(225),
    cpg_id VARCHAR(255),
    cpg_ref VARCHAR(255),
    top_brand BOOLEAN,
    name VARCHAR(255),
    UNIQUE INDEX idx_brand_id (brand_id)
    );


-- Populate Tables
INSERT INTO dim_users (user_id, created_date, last_login, state, role, active)
SELECT 
    DISTINCT _id,
    `createdDate`,
    `lastLogin`,
    state,
    role,
    active
FROM stg_users;

INSERT INTO fct_receipts (
    receipt_id,
    create_date,
    date_scanned,
    finished_date,
    modify_date,
    points_awarded_date,
    purchase_date,
    bonus_points_earned,
    points_earned,
    purchased_item_count,
    total_spent,
    rewards_receipt_status,
    user_id,
    bonus_points_earned_reason
)
SELECT 
    _id,                           
    createDate,                    
    dateScanned,                   
    finishedDate,                  
    modifyDate,                    
    pointsAwardedDate,             
    purchaseDate,                  
    bonusPointsEarned,             
    pointsEarned,                  
    purchasedItemCount,            
    totalSpent,                    
    CASE 
        WHEN rewardsReceiptStatus = 'FINISHED' THEN 'ACCEPTED' -- lets surmise FINISHED is ACCEPTED here
        ELSE `rewardsReceiptStatus`
    END,          
    userId,                        
    bonusPointsEarnedReason    
FROM stg_receipts;


INSERT INTO fct_items (
    item_id,
    receipt_id,
    barcode,
    description,
    final_price,
    item_price,
    quantity_purchased,
    partner_item_id
)
SELECT 
    UUID(), -- generate unique id for items
    receipt_id,
    barcode,
    description,
    `finalPrice`,
    `itemPrice`,
    `quantityPurchased`,
    `partnerItemId`
FROM
    stg_items;

INSERT INTO dim_brands (barcode, brand_id, brand_code, category, category_code, cpg_id, cpg_ref, top_brand, name)
WITH verified_brands AS ( -- missed the duplicate barcodes in clean step, handling here
    SELECT 
        b.barcode,
        b._id,
        b.brandCode,
        b.category,
        b.categoryCode,
        b.cpg_id,
        b.cpg_ref,
        b.topBrand,
        b.name,
        -- Check if this brand's barcode appears in items with matching description
        EXISTS (
            SELECT 1 
            FROM stg_items i 
            WHERE i.barcode = b.barcode 
            AND UPPER(i.description) LIKE CONCAT('%', UPPER(b.brandCode), '%')
        ) as is_verified,
        -- handle remaining duplicates
        ROW_NUMBER() OVER (PARTITION BY b.barcode ORDER BY 
            CASE WHEN EXISTS (
                SELECT 1 
                FROM stg_items i 
                WHERE i.barcode = b.barcode 
                AND UPPER(i.description) LIKE CONCAT('%', UPPER(b.brandCode), '%')
            ) THEN 0 ELSE 1 END,
            b._id
        ) as rn
    FROM stg_brands b
)
SELECT 
    barcode,
    _id,
    brandCode,
    category,
    categoryCode,
    cpg_id,
    cpg_ref,
    topBrand,
    name
FROM verified_brands
WHERE rn = 1;



