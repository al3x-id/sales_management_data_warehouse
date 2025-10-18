/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the modelling Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
	- These checks should be run after data loading in staging layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- =====================================================
-- DATA WAREHOUSE QUALITY CHECK
-- =====================================================
USE dw_sales_management;

-- Create quality check results table
DROP TABLE IF EXISTS dw_quality_check_results;
CREATE TABLE dw_quality_check_results (
    check_id INT AUTO_INCREMENT PRIMARY KEY,
    check_category VARCHAR(50),
    check_name VARCHAR(100),
    table_name VARCHAR(50),
    test_result VARCHAR(20),
    total_rows INT,
    issue_count INT,
    issue_percentage DECIMAL(5,2),
    message TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- STORED PROCEDURE: COMPREHENSIVE QUALITY CHECKS
-- =====================================================
DELIMITER //
DROP PROCEDURE IF EXISTS dw_quality_checks//
CREATE PROCEDURE dw_quality_checks()
BEGIN
    -- Clear previous results
    TRUNCATE TABLE dw_quality_check_results;
    
    -- =====================================================
    -- 1. SURROGATE KEY QUALITY CHECKS
    -- =====================================================
    
    -- Check 1.1: Primary Key Uniqueness - DIM_CUSTOMERS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys' AS check_category,
        'Primary Key Uniqueness' AS check_name,
        'dim_customers' AS table_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'PASS' ELSE 'FAIL' END AS test_result,
        COUNT(*) AS total_rows,
        COUNT(*) - COUNT(DISTINCT customer_id) AS issue_count,
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'All customer_id values are unique'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT customer_id), ' duplicate customer_id values')
        END AS message
    FROM dim_customers;
    
    -- Check 1.2: Primary Key Uniqueness - DIM_PRODUCTS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys',
        'Primary Key Uniqueness',
        'dim_products',
        CASE WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT product_id),
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'All product_id values are unique'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT product_id), ' duplicate product_id values')
        END
    FROM dim_products;
    
    -- Check 1.3: Primary Key Uniqueness - DIM_STORES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys',
        'Primary Key Uniqueness',
        'dim_stores',
        CASE WHEN COUNT(*) = COUNT(DISTINCT store_id) THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT store_id),
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT store_id) THEN 'All store_id values are unique'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT store_id), ' duplicate store_id values')
        END
    FROM dim_stores;
    
    -- Check 1.4: Primary Key Uniqueness - DIM_STAFFS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys',
        'Primary Key Uniqueness',
        'dim_staffs',
        CASE WHEN COUNT(*) = COUNT(DISTINCT staff_id) THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT staff_id),
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT staff_id) THEN 'All staff_id values are unique'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT staff_id), ' duplicate staff_id values')
        END
    FROM dim_staffs;
    
    -- Check 1.5: Primary Key Uniqueness - DIM_DATES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys',
        'Primary Key Uniqueness',
        'dim_dates',
        CASE WHEN COUNT(*) = COUNT(DISTINCT date_id) THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT date_id),
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT date_id) THEN 'All date_id values are unique'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT date_id), ' duplicate date_id values')
        END
    FROM dim_dates;
    
    -- Check 1.6: NULL Primary Keys Check - All Dimensions
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Surrogate Keys',
        'NULL Primary Key Check',
        'dim_customers',
        CASE WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
        CASE 
            WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'No NULL customer_id values found'
            ELSE CONCAT('Found ', SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), ' NULL customer_id values')
        END
    FROM dim_customers;
    
    -- =====================================================
    -- 2. REFERENTIAL INTEGRITY CHECKS
    -- =====================================================
    
    -- Check 2.1: Orphaned Records - FACT_SALES -> DIM_CUSTOMERS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Customer Records',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All customer_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' fact records with customer_id not in dim_customers')
        END
    FROM fact_sales f
    LEFT JOIN dim_customers d ON f.customer_id = d.customer_id
    WHERE d.customer_id IS NULL AND f.customer_id IS NOT NULL;
    
    -- Check 2.2: Orphaned Records - FACT_SALES -> DIM_PRODUCTS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Product Records',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All product_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' fact records with product_id not in dim_products')
        END
    FROM fact_sales f
    LEFT JOIN dim_products d ON f.product_id = d.product_id
    WHERE d.product_id IS NULL AND f.product_id IS NOT NULL;
    
    -- Check 2.3: Orphaned Records - FACT_SALES -> DIM_STORES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Store Records',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All store_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' fact records with store_id not in dim_stores')
        END
    FROM fact_sales f
    LEFT JOIN dim_stores d ON f.store_id = d.store_id
    WHERE d.store_id IS NULL AND f.store_id IS NOT NULL;
    
    -- Check 2.4: Orphaned Records - FACT_SALES -> DIM_STAFFS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Staff Records',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All staff_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' fact records with staff_id not in dim_staffs')
        END
    FROM fact_sales f
    LEFT JOIN dim_staffs d ON f.staff_id = d.staff_id
    WHERE d.staff_id IS NULL AND f.staff_id IS NOT NULL;
    
    -- Check 2.5: Orphaned Records - FACT_SALES -> DIM_DATES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Date Records',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All date_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' fact records with date_id not in dim_dates')
        END
    FROM fact_sales f
    LEFT JOIN dim_dates d ON f.date_id = d.date_id
    WHERE d.date_id IS NULL AND f.date_id IS NOT NULL;
    
    -- Check 2.6: Orphaned Records - FACT_INVENTORY -> DIM_PRODUCTS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Product Records',
        'fact_inventory',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_inventory),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_inventory), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All product_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' inventory records with product_id not in dim_products')
        END
    FROM fact_inventory f
    LEFT JOIN dim_products d ON f.product_id = d.product_id
    WHERE d.product_id IS NULL AND f.product_id IS NOT NULL;
    
    -- Check 2.7: Orphaned Records - FACT_INVENTORY -> DIM_STORES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'Orphaned Store Records',
        'fact_inventory',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_inventory),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_inventory), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All store_id values have matching dimension records'
            ELSE CONCAT('Found ', COUNT(*), ' inventory records with store_id not in dim_stores')
        END
    FROM fact_inventory f
    LEFT JOIN dim_stores d ON f.store_id = d.store_id
    WHERE d.store_id IS NULL AND f.store_id IS NOT NULL;
    
    -- Check 2.8: NULL Foreign Keys - FACT_SALES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Referential Integrity',
        'NULL Foreign Keys',
        'fact_sales',
        CASE 
            WHEN SUM(CASE WHEN customer_id IS NULL OR product_id IS NULL OR store_id IS NULL OR staff_id IS NULL OR date_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
            ELSE 'WARNING'
        END,
        COUNT(*),
        SUM(CASE WHEN customer_id IS NULL OR product_id IS NULL OR store_id IS NULL OR staff_id IS NULL OR date_id IS NULL THEN 1 ELSE 0 END),
        ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL OR product_id IS NULL OR store_id IS NULL OR staff_id IS NULL OR date_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2),
        CONCAT('NULL FKs - Customer: ', SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
               ', Product: ', SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
               ', Store: ', SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END),
               ', Staff: ', SUM(CASE WHEN staff_id IS NULL THEN 1 ELSE 0 END),
               ', Date: ', SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END))
    FROM fact_sales;
    
    -- =====================================================
    -- 3. DIMENSIONAL MODEL RELATIONSHIP VALIDATION
    -- =====================================================
    
    -- Check 3.1: Cardinality - FACT_SALES to DIM_CUSTOMERS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Relationships',
        'Cardinality Check',
        'fact_sales -> dim_customers',
        CASE 
            WHEN COUNT(*) > COUNT(DISTINCT customer_id) THEN 'PASS'
            WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'WARNING'
            ELSE 'FAIL'
        END,
        COUNT(*),
        NULL,
        CONCAT('Many-to-One relationship confirmed. ',
               'Avg facts per customer: ', ROUND(CAST(COUNT(*) AS DECIMAL) / NULLIF(COUNT(DISTINCT customer_id), 0), 2))
    FROM fact_sales
    WHERE customer_id IS NOT NULL;
    
    -- Check 3.2: Unused Dimension Records - DIM_CUSTOMERS
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Relationships',
        'Unused Dimension Records',
        'dim_customers',
        CASE WHEN COUNT(DISTINCT d.customer_id) - COUNT(DISTINCT f.customer_id) = 0 THEN 'PASS' ELSE 'WARNING' END,
        COUNT(DISTINCT d.customer_id),
        COUNT(DISTINCT d.customer_id) - COUNT(DISTINCT f.customer_id),
        ROUND(100.0 * (COUNT(DISTINCT d.customer_id) - COUNT(DISTINCT f.customer_id)) / COUNT(DISTINCT d.customer_id), 2),
        CONCAT(COUNT(DISTINCT d.customer_id) - COUNT(DISTINCT f.customer_id), ' customers have no sales transactions')
    FROM dim_customers d
    LEFT JOIN fact_sales f ON d.customer_id = f.customer_id;
    
    -- Check 3.3: Unused Dimension Records - DIM_PRODUCTS (in sales)
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Relationships',
        'Unused Products in Sales',
        'dim_products',
        CASE WHEN COUNT(DISTINCT d.product_id) - COUNT(DISTINCT f.product_id) = 0 THEN 'PASS' ELSE 'WARNING' END,
        COUNT(DISTINCT d.product_id),
        COUNT(DISTINCT d.product_id) - COUNT(DISTINCT f.product_id),
        ROUND(100.0 * (COUNT(DISTINCT d.product_id) - COUNT(DISTINCT f.product_id)) / COUNT(DISTINCT d.product_id), 2),
        CONCAT(COUNT(DISTINCT d.product_id) - COUNT(DISTINCT f.product_id), ' products have no sales transactions')
    FROM dim_products d
    LEFT JOIN fact_sales f ON d.product_id = f.product_id;
    
    -- Check 3.4: Date Dimension Continuity
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Relationships',
        'Date Dimension Continuity',
        'dim_dates',
        CASE WHEN COUNT(DISTINCT full_date) = COUNT(*) THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT full_date),
        CASE 
            WHEN COUNT(DISTINCT full_date) = COUNT(*) THEN 'All dates are unique, no duplicates found'
            ELSE CONCAT('Found ', COUNT(*) - COUNT(DISTINCT full_date), ' duplicate dates')
        END
    FROM dim_dates;
    
    -- Check 3.5: Staff-Store Relationship
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Relationships',
        'Staff-Store Relationship',
        'dim_staffs',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM dim_staffs),
        COUNT(*),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All staff members have valid store assignments'
            ELSE CONCAT('Found ', COUNT(*), ' staff members with invalid store_id')
        END
    FROM dim_staffs s
    LEFT JOIN dim_stores st ON s.store_id = st.store_id
    WHERE st.store_id IS NULL AND s.store_id IS NOT NULL;
    
    -- =====================================================
    -- 4. DATA QUALITY & BUSINESS RULES
    -- =====================================================
    
    -- Check 4.1: Negative Quantities in FACT_SALES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Data Quality',
        'Negative Quantities',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'No negative quantities found'
            ELSE CONCAT('Found ', COUNT(*), ' records with negative quantities')
        END
    FROM fact_sales
    WHERE quantity < 0;
    
    -- Check 4.2: Negative Prices in FACT_SALES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Data Quality',
        'Negative Prices',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'No negative prices found'
            ELSE CONCAT('Found ', COUNT(*), ' records with negative list_price')
        END
    FROM fact_sales
    WHERE list_price < 0;
    
    -- Check 4.3: Invalid Discount Range (should be between 0 and 1)
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Data Quality',
        'Invalid Discount Values',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All discounts are in valid range (0-1)'
            ELSE CONCAT('Found ', COUNT(*), ' records with discount outside 0-1 range')
        END
    FROM fact_sales
    WHERE discount < 0 OR discount > 1;
    
    -- Check 4.4: Total Amount Calculation Accuracy
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Data Quality',
        'Total Amount Calculation',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_sales), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'All total_amount calculations are accurate'
            ELSE CONCAT('Found ', COUNT(*), ' records with total_amount calculation variance > 0.01')
        END
    FROM fact_sales
    WHERE ABS(total_amount - ((quantity * list_price) - (discount * quantity * list_price))) > 0.01;
    
    -- Check 4.5: Negative Stock Quantities
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, issue_percentage, message)
    SELECT 
        'Data Quality',
        'Negative Stock Quantities',
        'fact_inventory',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
        (SELECT COUNT(*) FROM fact_inventory),
        COUNT(*),
        ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_inventory), 0), 2),
        CASE 
            WHEN COUNT(*) = 0 THEN 'No negative stock quantities found'
            ELSE CONCAT('Found ', COUNT(*), ' records with negative stock_quantity')
        END
    FROM fact_inventory
    WHERE stock_quantity < 0;
    
    -- Check 4.6: Fact Grain Validation - FACT_SALES
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Data Quality',
        'Fact Grain Validation',
        'fact_sales',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_sales),
        COUNT(*),
        CASE 
            WHEN COUNT(*) = 0 THEN 'No duplicate grain combinations found (order_id + product_id is unique)'
            ELSE CONCAT('Found ', COUNT(*), ' duplicate combinations at fact grain level')
        END
    FROM (
        SELECT order_id, product_id, COUNT(*) AS cnt
        FROM fact_sales
        GROUP BY order_id, product_id
        HAVING COUNT(*) > 1
    ) AS grain_check;
    
    -- Check 4.7: Fact Grain Validation - FACT_INVENTORY
    INSERT INTO dw_quality_check_results (check_category, check_name, table_name, test_result, total_rows, issue_count, message)
    SELECT 
        'Data Quality',
        'Fact Grain Validation',
        'fact_inventory',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM fact_inventory),
        COUNT(*),
        CASE 
            WHEN COUNT(*) = 0 THEN 'No duplicate grain combinations found (store_id + product_id is unique)'
            ELSE CONCAT('Found ', COUNT(*), ' duplicate combinations at fact grain level')
        END
    FROM (
        SELECT store_id, product_id, COUNT(*) AS cnt
        FROM fact_inventory
        GROUP BY store_id, product_id
        HAVING COUNT(*) > 1
    ) AS grain_check;
    
END //
DELIMITER ;

-- =====================================================
-- EXECUTE QUALITY CHECKS
-- =====================================================
CALL dw_quality_checks();

-- =====================================================
-- VIEW RESULTS - SUMMARY BY CATEGORY
-- =====================================================
SELECT 
    check_category,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN test_result = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN test_result = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 1), '%') AS pass_rate
FROM dw_quality_check_results
GROUP BY check_category;

-- =====================================================
-- VIEW RESULTS - FAILED CHECKS ONLY
-- =====================================================
SELECT 
    check_category,
    check_name,
    table_name,
    test_result,
    issue_count,
    issue_percentage,
    message,
    checked_at
FROM dw_quality_check_results
WHERE test_result IN ('FAIL', 'WARNING');

-- =====================================================
-- VIEW RESULTS - ALL CHECKS
-- =====================================================
SELECT 
    check_id,
    check_category,
    check_name,
    table_name,
    test_result,
    total_rows,
    issue_count,
    issue_percentage,
    message,
    checked_at
FROM dw_quality_check_results;

-- =====================================================
-- DETAILED ORPHANED RECORDS INVESTIGATION
-- =====================================================
-- Run this to see actual orphaned records if any failures 
-- Expectation: No results
--
SELECT f.order_id, f.customer_id, f.product_id, f.quantity, f.total_amount
FROM fact_sales f
LEFT JOIN dim_customers d ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL AND f.customer_id IS NOT NULL
LIMIT 100;


