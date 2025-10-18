-- ==========================================
-- ETL PIPELINE: STAGING → DATA WAREHOUSE
-- ==========================================
USE dw_sales_management;
-- =========================
-- Creating Stored Procedure
-- =========================
DELIMITER //
DROP PROCEDURE IF EXISTS dw_load_stp//
CREATE PROCEDURE dw_load_stp()
BEGIN
	-- Declare handlers for error tracking
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@error_message = MESSAGE_TEXT;
		SET @has_error = 1;
	END;

-- =========================
-- Defining a batch tag for traceability
-- =========================
	SET @batch_tag = CONCAT('DWBatch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i'));

-- Creating Temporary log table
	DROP TEMPORARY TABLE IF EXISTS tmp_log;
	CREATE TEMPORARY TABLE tmp_log (
		table_name_ VARCHAR(50),
		load_status VARCHAR(20),
		message TEXT
	);
    
-- ==========================================
-- 1 DIM_PRODUCT
-- ==========================================
	SET @has_error = 0;
	TRUNCATE TABLE dim_products;

	INSERT INTO dim_products(
		product_id, product_name, brand_name, category_name, list_price
	)
	SELECT
		p.product_id,
		p.product_name,
		b.brand_name,
		c.category_name,
		p.list_price
	FROM stg_sales_management.stg_products p
	LEFT JOIN stg_sales_management.stg_brands b ON p.brand_id = b.brand_id
	LEFT JOIN stg_sales_management.stg_categories c ON p.category_id = c.category_id;

	 INSERT INTO tmp_log VALUES ('dim_products', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));

-- ==========================================
-- 2 DIM_CUSTOMER
-- ==========================================
		SET @has_error = 0;
		TRUNCATE TABLE dim_customers;

		INSERT INTO dim_customers(
			customer_id, customer_name, 
			city, state
		)
		SELECT
			customer_id, 
			customer_name,
			city, 
			state
		FROM stg_sales_management.stg_customers;
        
        INSERT INTO tmp_log VALUES ('dim_customers', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));

-- ==========================================
-- 3 DIM_STORE
-- ==========================================
	SET @has_error = 0;
	TRUNCATE TABLE dim_stores;

	INSERT INTO dim_stores(store_id, store_name, city, state
	)
	SELECT
		store_id, 
		store_name,
		city, 
		state
	FROM stg_sales_management.stg_stores;

	 INSERT INTO tmp_log VALUES ('dim_stores', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));
        
-- ==========================================
-- 4 DIM_STAFF
-- ==========================================
	SET @has_error = 0;
	TRUNCATE TABLE dim_staffs;

	INSERT INTO dim_staffs(staff_id, staff_name, store_id, manager_id
	)
	SELECT
		staff_id, 
		staff_name, 
		store_id, 
		manager_id
	FROM stg_sales_management.stg_staffs;

	 INSERT INTO tmp_log VALUES ('dim_staffs', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));
        
-- ==========================================
-- 5️ DIM_DATES
-- ==========================================
	SET @has_error = 0;
	TRUNCATE TABLE dim_dates;

	INSERT INTO dim_dates (date_id, full_date, day_, month_, month_name, quarter_, year_, week_of_year)
	SELECT
		ROW_NUMBER() OVER (ORDER BY DATE(order_date)) AS date_id,
		DATE(order_date) AS full_date,
		DAY(DATE(order_date)) AS day_,
		MONTH(DATE(order_date)) AS month_,
		MONTHNAME(DATE(order_date)) AS month_name,
		QUARTER(DATE(order_date)) AS quarter_,
		YEAR(DATE(order_date)) AS year_,
		WEEK(DATE(order_date)) AS week_of_year
	FROM stg_sales_management.stg_orders;

	 INSERT INTO tmp_log VALUES ('dim_dates', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));

-- ==========================================
-- 6 FACT_SALES
-- ==========================================
	SET @has_error = 0;

	TRUNCATE TABLE fact_sales;
	INSERT INTO fact_sales (
		order_id, item_id, customer_id, product_id,store_id, staff_id, date_id,
		quantity, list_price, sales, discount, total_amount
	)
	SELECT
		oi.order_id,
        oi.item_id,
		o.customer_id,
		oi.product_id,
		o.store_id,
		o.staff_id,
		date_id,
		oi.quantity,
		oi.list_price,
		oi.sales,
		oi.discount,
		(oi.quantity * oi.list_price) - (oi.discount * oi.quantity * oi.list_price) AS total_amount
	FROM stg_sales_management.stg_order_items oi
	JOIN stg_sales_management.stg_orders o ON oi.order_id = o.order_id
	JOIN dw_sales_management.dim_dates d ON DATE(o.order_date) = d.full_date;

	 INSERT INTO tmp_log VALUES ('fact_sales', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));
        
-- ==========================================
-- 7️ FACT_INVENTORY
-- ==========================================
	SET @has_error = 0;

	TRUNCATE TABLE fact_inventory;

	INSERT INTO fact_inventory (
		store_id, product_id, stock_quantity, last_updated
	)
	SELECT
		store_id,
		product_id,
		quantity AS stock_quantity,
		CURDATE() AS last_updated
	FROM stg_sales_management.stg_stocks;

	 INSERT INTO tmp_log VALUES ('fact_inventory', 
		IF(@has_error = 1, 'FAILED', 'SUCCESS'), 
		IF(@has_error = 1, CONCAT('Load failed: ', @error_message), 'Loaded into DW'));
        
-- ==========================================
-- Write all logs to master log
-- ==========================================
	INSERT INTO raw_sales_management.load_log (table_name_, load_status, message, batch_tag)
	SELECT 
		table_name_, 
		load_status, 
		message, 
		@batch_tag 
	FROM tmp_log;

	DROP TEMPORARY TABLE tmp_log;

-- View Verification on load_log
	SELECT * FROM raw_sales_management.load_log;

END //
DELIMITER ;

CALL dw_load_stp();