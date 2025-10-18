/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'staging' layer. It includes checks for:
    - Null or duplicate primary keys (for composite keys, checks both columns together).
    - compare raw table records counts with staging table records counts
    - Allows up to 5% variance (marked as WARNING if exceeded)

Usage Notes:
    - These checks should be run after data loading in staging layer.
    - Investigate and resolve any discrepancies found during the checks.
    - Execution pass rate: 100.00 pass rate
    - check for product without stock info not being 100.00 pass is expected due to various business reasons:
		- New products
        - Discontinued products
        - Seasonal products
===============================================================================
*/

-- ================================================
-- STAGING DATA QUALITY CHECK SCRIPT
-- ================================================
USE stg_sales_management;

-- Create quality check results table
CREATE TABLE IF NOT EXISTS quality_check_results (
    check_id INT AUTO_INCREMENT PRIMARY KEY,
    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name_ VARCHAR(50),
    check_type VARCHAR(50),
    check_description VARCHAR(255),
    check_status VARCHAR(20),
    record_count INT DEFAULT 0,
    failed_count INT DEFAULT 0,
    pass_rate DECIMAL(5,2),
    batch_tag VARCHAR(100)
);

-- Creating quality check store procedure
DELIMITER //
DROP PROCEDURE IF EXISTS stg_quality_check_stp//
CREATE PROCEDURE stg_quality_check_stp()
BEGIN
    -- Declare variables
    DECLARE v_total_count INT DEFAULT 0;
    DECLARE v_failed_count INT DEFAULT 0;
    DECLARE v_pass_rate DECIMAL(5,2);
    
    -- Define a batch tag for traceability
    SET @batch_tag = CONCAT('QualityCheck_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i'));
    
    -- Temporary table for quality check results
    DROP TEMPORARY TABLE IF EXISTS tmp_quality_checks;
    CREATE TEMPORARY TABLE tmp_quality_checks (
        table_name_ VARCHAR(50),
        check_type VARCHAR(50),
        check_description VARCHAR(255),
        check_status VARCHAR(20),
        record_count INT DEFAULT 0,
        failed_count INT DEFAULT 0,
        pass_rate DECIMAL(5,2)
    );

-- =========================================================
-- 1. STG_CUSTOMERS QUALITY CHECKS
-- =========================================================
    
    -- Check 1.1: NULL checks in required fields
    SELECT COUNT(*) INTO v_total_count FROM stg_customers;
    SELECT COUNT(*) INTO v_failed_count FROM stg_customers 
    WHERE customer_id IS NULL OR customer_name IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_customers', 'NULL_CHECK', 'Check for NULL in required fields (customer_id, customer_name)',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 1.2: Duplicate check on primary key
    SELECT COUNT(*) INTO v_total_count FROM stg_customers;
    SELECT COUNT(*) - COUNT(DISTINCT customer_id) INTO v_failed_count FROM stg_customers;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_customers', 'DUPLICATE_CHECK', 'Check for duplicate customer_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 1.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_customers WHERE customer_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_customers;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_customers', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 2. STG_PRODUCTS QUALITY CHECKS
-- =========================================================
    
    -- Check 2.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_products;
    SELECT COUNT(*) INTO v_failed_count FROM stg_products 
    WHERE product_id IS NULL OR product_name IS NULL OR brand_id IS NULL OR category_id IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_products', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 2.2: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT product_id) INTO v_failed_count FROM stg_products;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_products', 'DUPLICATE_CHECK', 'Check for duplicate product_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 2.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_products WHERE product_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_products;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_products', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 3. STG_ORDERS QUALITY CHECKS
-- =========================================================
    
    -- Check 3.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_orders;
    SELECT COUNT(*) INTO v_failed_count FROM stg_orders 
    WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 3.2: Invalid date range - shipped_date before order_date
    SELECT COUNT(*) INTO v_failed_count FROM stg_orders 
    WHERE shipped_date < order_date;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'INVALID_DATE_RANGE', 'Check shipped_date is not before order_date',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 3.3: Invalid date range - future dates
    SELECT COUNT(*) INTO v_failed_count FROM stg_orders 
    WHERE order_date > CURDATE() OR shipped_date > CURDATE();
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'INVALID_DATE_RANGE', 'Check for future order/shipped dates',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 3.4: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT order_id) INTO v_failed_count FROM stg_orders;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'DUPLICATE_CHECK', 'Check for duplicate order_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 3.5: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_orders WHERE order_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_orders;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 4. STG_ORDER_ITEMS QUALITY CHECKS
-- =========================================================
    
    -- Check 4.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_order_items;
    SELECT COUNT(*) INTO v_failed_count FROM stg_order_items 
    WHERE order_id IS NULL OR product_id IS NULL OR quantity IS NULL OR list_price IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_order_items', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 4.2: Duplicate check on composite key
    SELECT COUNT(*) INTO v_total_count FROM stg_order_items;
    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '-', product_id)) INTO v_failed_count 
    FROM stg_order_items;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_order_items', 'DUPLICATE_CHECK', 'Check for duplicate (order_id, product_id)',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 4.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_order_items WHERE order_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_order_items;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_order_items', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 5. STG_STORES QUALITY CHECKS
-- =========================================================
    
    -- Check 5.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_stores;
    SELECT COUNT(*) INTO v_failed_count FROM stg_stores 
    WHERE store_id IS NULL OR store_name IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stores', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 5.2: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT store_id) INTO v_failed_count FROM stg_stores;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stores', 'DUPLICATE_CHECK', 'Check for duplicate store_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 5.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_stores WHERE store_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_stores;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stores', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 6. STG_STAFFS QUALITY CHECKS
-- =========================================================
    
    -- Check 6.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_staffs;
    SELECT COUNT(*) INTO v_failed_count FROM stg_staffs 
    WHERE staff_id IS NULL OR staff_name IS NULL OR store_id IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_staffs', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 6.2: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT staff_id) INTO v_failed_count FROM stg_staffs;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_staffs', 'DUPLICATE_CHECK', 'Check for duplicate staff_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 6.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_staffs WHERE staff_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_staffs;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_staffs', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 7. STG_STOCKS QUALITY CHECKS
-- =========================================================
    
    -- Check 7.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_stocks;
    SELECT COUNT(*) INTO v_failed_count FROM stg_stocks 
    WHERE store_id IS NULL OR product_id IS NULL OR quantity IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stocks', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 7.2: Duplicate check on composite key
    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(store_id, '-', product_id)) INTO v_failed_count 
    FROM stg_stocks;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stocks', 'DUPLICATE_CHECK', 'Check for duplicate (store_id, product_id)',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 7.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_stocks 
    WHERE store_id IS NOT NULL AND product_id IS NOT NULL;
    SELECT COUNT(*) INTO @stg_count FROM stg_stocks;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stocks', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 8. STG_BRANDS QUALITY CHECKS
-- =========================================================
    
    -- Check 8.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_brands;
    SELECT COUNT(*) INTO v_failed_count FROM stg_brands 
    WHERE brand_id IS NULL OR brand_name IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_brands', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 8.2: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT brand_id) INTO v_failed_count FROM stg_brands;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_brands', 'DUPLICATE_CHECK', 'Check for duplicate brand_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 8.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_brands;
    SELECT COUNT(*) INTO @stg_count FROM stg_brands;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_brands', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 9. STG_CATEGORIES QUALITY CHECKS
-- =========================================================
    
    -- Check 9.1: NULL checks
    SELECT COUNT(*) INTO v_total_count FROM stg_categories;
    SELECT COUNT(*) INTO v_failed_count FROM stg_categories 
    WHERE category_id IS NULL OR category_name IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_categories', 'NULL_CHECK', 'Check for NULL in required fields',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 9.2: Duplicate check
    SELECT COUNT(*) - COUNT(DISTINCT category_id) INTO v_failed_count FROM stg_categories;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_categories', 'DUPLICATE_CHECK', 'Check for duplicate category_id',
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 9.3: Count validation
    SELECT COUNT(*) INTO @raw_count FROM raw_sales_management.raw_categories;
    SELECT COUNT(*) INTO @stg_count FROM stg_categories;
    SET v_failed_count = ABS(@raw_count - @stg_count);
    SET v_pass_rate = CASE WHEN @raw_count > 0 
                           THEN (@stg_count / @raw_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_categories', 'COUNT_VALIDATION', 
        CONCAT('Raw count: ', @raw_count, ', Staging count: ', @stg_count),
        CASE WHEN v_failed_count <= (@raw_count * 0.05) THEN 'PASS' ELSE 'WARNING' END,
        @raw_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- 10. COMPLETENESS CHECKS (WARNINGS ONLY)
-- =========================================================
    
    -- Check 10.1: Products without stock information
    SELECT COUNT(DISTINCT p.product_id) INTO v_total_count FROM stg_products p;
    SELECT COUNT(DISTINCT p.product_id) INTO v_failed_count 
    FROM stg_products p
    LEFT JOIN stg_stocks s ON p.product_id = s.product_id
    WHERE s.product_id IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_products', 'COMPLETENESS_CHECK', 
        CONCAT('Products without stock info: ', v_failed_count, ' out of ', v_total_count),
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'WARNING' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 10.2: Orders without order items (orphaned orders)
    SELECT COUNT(DISTINCT o.order_id) INTO v_total_count FROM stg_orders o;
    SELECT COUNT(DISTINCT o.order_id) INTO v_failed_count 
    FROM stg_orders o
    LEFT JOIN stg_order_items oi ON o.order_id = oi.order_id
    WHERE oi.order_id IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_orders', 'COMPLETENESS_CHECK', 
        CONCAT('Orders without order items: ', v_failed_count, ' out of ', v_total_count),
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'WARNING' END,
        v_total_count, v_failed_count, v_pass_rate
    );
    
    -- Check 10.3: Stores without staff
    SELECT COUNT(DISTINCT s.store_id) INTO v_total_count FROM stg_stores s;
    SELECT COUNT(DISTINCT s.store_id) INTO v_failed_count 
    FROM stg_stores s
    LEFT JOIN stg_staffs st ON s.store_id = st.store_id
    WHERE st.store_id IS NULL;
    SET v_pass_rate = CASE WHEN v_total_count > 0 
                           THEN ((v_total_count - v_failed_count) / v_total_count) * 100 
                           ELSE 0 END;
    
    INSERT INTO tmp_quality_checks VALUES (
        'stg_stores', 'COMPLETENESS_CHECK', 
        CONCAT('Stores without staff: ', v_failed_count, ' out of ', v_total_count),
        CASE WHEN v_failed_count = 0 THEN 'PASS' ELSE 'WARNING' END,
        v_total_count, v_failed_count, v_pass_rate
    );

-- =========================================================
-- Write all quality check results to permanent table
-- =========================================================
    INSERT INTO quality_check_results (
        table_name_,
        check_type,
        check_description,
        check_status,
        record_count,
        failed_count,
        pass_rate,
        batch_tag
    )
    SELECT 
        table_name_,
        check_type,
        check_description,
        check_status,
        record_count,
        failed_count,
        pass_rate,
        @batch_tag 
    FROM tmp_quality_checks;

    DROP TEMPORARY TABLE tmp_quality_checks;

-- =========================================================
-- Display Quality Check Summary
-- =========================================================
    
    -- Overall summary by table
    SELECT 
        table_name_,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) AS passed_checks,
        SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
        SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_checks,
        ROUND(AVG(pass_rate), 2) AS avg_pass_rate,
        CASE 
            WHEN SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'HEALTHY'
            WHEN SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) <= 2 THEN 'NEEDS ATTENTION'
            ELSE 'CRITICAL'
        END AS data_quality_status
    FROM quality_check_results
    WHERE batch_tag = @batch_tag
    GROUP BY table_name_
    ORDER BY failed_checks DESC, table_name_;
    
    -- Summary by check type
    SELECT 
        check_type,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        ROUND(AVG(pass_rate), 2) AS avg_pass_rate
    FROM quality_check_results
    WHERE batch_tag = @batch_tag
    GROUP BY check_type
    ORDER BY check_type;
    
    -- Failed checks details
    SELECT 
        table_name_,
        check_type,
        check_description,
        check_status,
        record_count,
        failed_count,
        pass_rate
    FROM quality_check_results
    WHERE batch_tag = @batch_tag
    AND check_status IN ('FAIL', 'WARNING')
    ORDER BY check_status DESC, failed_count DESC;
    
    -- Complete detailed report
    SELECT 
        table_name_,
        check_type,
        check_description,
        check_status,
        record_count,
        failed_count,
        pass_rate,
        check_date
    FROM quality_check_results
    WHERE batch_tag = @batch_tag
    ORDER BY table_name_, check_type, check_status;

END //

DELIMITER ;

-- Execute the quality check procedure
CALL stg_quality_check_stp();