-- ================================================
-- RAW DATA BATCH LOAD SCRIPT WITH ERROR HANDLING
-- ================================================
 USE raw_sales_management;

-- Defining a batch tag manually each time after running this
SET @batch_tag = CONCAT('RawBatch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i'));

-- Helper: safely insert a log record
DROP TEMPORARY TABLE IF EXISTS tmp_log;
CREATE TEMPORARY TABLE tmp_log (
table_name_ VARCHAR(50),
load_status VARCHAR(50),
 message TEXT
 );
 
-- ================================================
-- Function to simplify logging
-- Inserting to main log after all loads. 
-- ================================================

-- Set up for error handling
SET @error_count = 0;

-- 1. BRANDS
SET @load_error = 0;
TRUNCATE raw_brands;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/brands.csv'
INTO TABLE raw_brands
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_brands', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 2. CATEGORIES
SET @load_error = 0;
TRUNCATE raw_categories;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/categories.csv'
INTO TABLE raw_categories
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_categories', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 3. PRODUCTS
SET @load_error = 0;
TRUNCATE raw_products;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/products.csv'
INTO TABLE raw_products
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_products', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 4. CUSTOMERS
SET @load_error = 0;
TRUNCATE raw_customers;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/customers.csv'
INTO TABLE raw_customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_customers', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 5. ORDERS
SET @load_error = 0;
TRUNCATE raw_orders;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/orders.csv'
INTO TABLE raw_orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_orders', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 6. ORDER ITEMS
SET @load_error = 0;
TRUNCATE raw_order_items;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/order_items.csv'
INTO TABLE raw_order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_order_items', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 7. STORES
SET @load_error = 0;
TRUNCATE raw_stores;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/stores.csv'
INTO TABLE raw_stores
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_stores', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 8. STAFFS
SET @load_error = 0;
TRUNCATE raw_staffs;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/staffs.csv'
INTO TABLE raw_staffs
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_staffs', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- 9. STOCKS
SET @load_error = 0;
TRUNCATE raw_stocks;
LOAD DATA LOCAL INFILE 'C:/Users/cw_86/Desktop/data_source/stocks.csv'
INTO TABLE raw_stocks
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET @load_error = @@error_count;
INSERT INTO tmp_log VALUES ('raw_stocks', 
    IF(@load_error > 0, 'FAILED', 'SUCCESS'), 
    IF(@load_error > 0, 'Load failed - check file path and format', 'Loaded successfully'));

-- Insert everything into main log table
INSERT INTO load_log (
	table_name_, 
	load_status, 
	message, 
	batch_tag)
SELECT table_name_, 
	load_status, 
	message, 
	@batch_tag 
FROM tmp_log;

-- Clean up
DROP TEMPORARY TABLE tmp_log;

-- View Verification
SELECT * FROM load_log;