-- ================================================
-- STAGING DATA BATCH LOAD SCRIPT WITH DUPLICATE DETECTION
-- ================================================
USE stg_sales_management;

-- creating the duplicate_checker table if it doesn't exist
CREATE TABLE IF NOT EXISTS duplicate_checker (
	id INT AUTO_INCREMENT PRIMARY KEY, 
    table_name_ VARCHAR(50),
    duplicate_status VARCHAR(50),
    duplicate_count INT DEFAULT 0,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Creating store procedure
DELIMITER //
DROP PROCEDURE IF EXISTS stg_load_stp//
CREATE PROCEDURE stg_load_stp()
BEGIN
    -- Declaring variables
    DECLARE v_duplicate_count INT DEFAULT 0;
    
    -- Declaring handlers for error tracking
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @error_message = MESSAGE_TEXT;
        SET @has_error = 1;
    END;

    -- Defining a batch tag for traceability
    SET @batch_tag = CONCAT('StgBatch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i'));

    -- Temporary log for this run
    DROP TEMPORARY TABLE IF EXISTS tmp_log;
    CREATE TEMPORARY TABLE tmp_log (
        table_name_ VARCHAR(50), 
        load_status VARCHAR(50), 
        message TEXT
    );

-- =========================================================
-- 1. STG_BRANDS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT brand_id) INTO v_duplicate_count -- checking number of duplicates
    FROM raw_sales_management.raw_brands;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_brands', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
                ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_brands;
    INSERT INTO stg_brands
    SELECT DISTINCT
        brand_id,
        TRIM(brand_name) AS brand_name
    FROM raw_sales_management.raw_brands;
    
   INSERT INTO tmp_log VALUES ('stg_brands', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 2. STG_CATEGORIES
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT category_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_categories;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_categories', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE'
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
			WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
            ELSE 'NO DUPLICATE' 
		END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_categories;
    INSERT INTO stg_categories
    SELECT DISTINCT
        category_id,
        TRIM(category_name) AS category_name
    FROM raw_sales_management.raw_categories;
    
   INSERT INTO tmp_log VALUES ('stg_categories', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 3. STG_PRODUCTS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT product_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_products
    WHERE product_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_products', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
                ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_products;
    INSERT INTO stg_products
    SELECT DISTINCT
        product_id,
        TRIM(product_name) AS product_name,
        brand_id,
        category_id,
        list_price
    FROM raw_sales_management.raw_products
    WHERE product_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_products', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));
        
-- =========================================================
-- 4. STG_CUSTOMERS
-- =========================================================
    SET @has_error = 0;
    
    -- Check for duplicates in raw data
    SELECT COUNT(*) - COUNT(DISTINCT customer_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_customers
    WHERE customer_id IS NOT NULL;
    
    -- Update duplicate checker
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_customers', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
			WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
            ELSE 'NO DUPLICATE' 
		END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_customers;
    INSERT INTO stg_customers
    SELECT DISTINCT
        customer_id,
        TRIM(CONCAT(first_name, ' ', last_name)) AS customer_name,
        TRIM(city) AS city,
        CASE
            WHEN state = 'NY' THEN 'New York'
            WHEN state = 'CA' THEN 'Carlifornia'
            ELSE 'Texas'
        END AS state
    FROM raw_sales_management.raw_customers
    WHERE customer_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_customers', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 5. STG_ORDERS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT order_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_orders
    WHERE order_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_orders', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
            END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
            END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_orders;
    INSERT INTO stg_orders
    SELECT DISTINCT
        order_id,
        customer_id,
        DATE(order_date) AS order_date,
        COALESCE(DATE(shipped_date), DATE(required_date)) AS shipped_date,
        store_id,
        staff_id
    FROM raw_sales_management.raw_orders
    WHERE order_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_orders', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 6. STG_ORDER_ITEMS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '-', item_id, '-', product_id)) INTO v_duplicate_count
    FROM raw_sales_management.raw_order_items
    WHERE order_id IS NOT NULL AND item_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_order_items', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
            END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_order_items;
    INSERT INTO stg_order_items
    SELECT DISTINCT
        order_id,
        item_id,
        product_id,
        quantity,
        list_price,
        discount,
        ROUND((list_price * quantity) * (1 - discount), 2) AS sales
    FROM raw_sales_management.raw_order_items
    WHERE order_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_order_items', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 7. STG_STORES
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT store_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_stores
    WHERE store_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_stores', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
			WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
            ELSE 'NO DUPLICATE' 
		END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_stores;
    INSERT INTO stg_stores
    SELECT DISTINCT
        store_id,
        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        CASE
            WHEN state = 'NY' THEN 'New York'
            WHEN state = 'CA' THEN 'Carlifornia'
            ELSE 'Texas'
        END AS state
    FROM raw_sales_management.raw_stores
    WHERE store_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_stores', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 8. STG_STAFFS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT staff_id) INTO v_duplicate_count
    FROM raw_sales_management.raw_staffs
    WHERE staff_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_staffs', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
				ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE 
			WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
			ELSE 'NO DUPLICATE' 
		END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_staffs;
    INSERT INTO stg_staffs
    SELECT DISTINCT
        staff_id,
        TRIM(CONCAT(first_name, ' ', last_name)) AS staff_name,
        store_id,
        COALESCE(manager_id, 0) AS manager_id
    FROM raw_sales_management.raw_staffs
    WHERE staff_id IS NOT NULL;
    
    INSERT INTO tmp_log VALUES ('stg_staffs', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));

-- =========================================================
-- 9. STG_STOCKS
-- =========================================================
    SET @has_error = 0;
    
    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(store_id, '-', product_id)) INTO v_duplicate_count
    FROM raw_sales_management.raw_stocks
    WHERE store_id IS NOT NULL AND product_id IS NOT NULL;
    
    INSERT INTO duplicate_checker (table_name_, duplicate_status, duplicate_count)
    VALUES ('stg_stocks', 
            CASE 
				WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' 
                ELSE 'NO DUPLICATE' 
			END,
            v_duplicate_count)
    ON DUPLICATE KEY UPDATE 
        duplicate_status = CASE WHEN v_duplicate_count > 0 THEN 'DUPLICATE FOUND' ELSE 'NO DUPLICATE' END,
        duplicate_count = v_duplicate_count;
    
    TRUNCATE TABLE stg_stocks;
    INSERT INTO stg_stocks
    SELECT DISTINCT
        store_id,
        product_id,
        quantity
    FROM raw_sales_management.raw_stocks
    WHERE store_id IS NOT NULL AND product_id IS NOT NULL;
    
   INSERT INTO tmp_log VALUES ('stg_stocks', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Transformation failed: ', @error_message), 'Transformed successfully'));
        
-- =========================================================
-- Write all logs to master log
-- =========================================================
    INSERT INTO raw_sales_management.load_log (
        table_name_,
        load_status,
        message,
        batch_tag)
    SELECT 
        table_name_, 
        load_status, 
        message, 
        @batch_tag 
    FROM tmp_log;

    DROP TEMPORARY TABLE tmp_log;

 -- Show duplicate checker results
    SELECT * FROM duplicate_checker;
    
-- View verification on load_log
    SELECT * FROM raw_sales_management.load_log;

END //

DELIMITER ;

CALL stg_load_stp();