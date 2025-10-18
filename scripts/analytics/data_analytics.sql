-- ==========================================
-- Data Analysis
-- ==========================================

USE dw_sales_management;

DELIMITER //
DROP PROCEDURE IF EXISTS analytics_stp//
CREATE PROCEDURE analytics_stp()
BEGIN

-- Validating the Warehouse
-- Checking fact table volume
	SELECT 
		COUNT(*) AS sales_volume
    FROM fact_sales;
	SELECT 
		COUNT(*) AS inventory_volume
    FROM fact_inventory;

-- Check number of distinct customers
	SELECT 
		COUNT(customer_id) AS no_of_customers
    FROM dim_customers;

-- Sales by months
	CREATE TABLE sales_by_month AS
	SELECT
		YEAR(d.full_date) AS year,
		MONTH(d.full_date) AS month,
		s.store_name,
		SUM(f.total_amount) AS monthly_sales
	FROM fact_sales f
	JOIN dim_stores s ON f.store_id = s.store_id
	JOIN dim_dates d ON f.date_id = d.date_id
	GROUP BY year, month, s.store_name;
    
    SELECT * FROM sales_by_month;

-- Creating Analytical Views
	CREATE OR REPLACE VIEW vw_sales_by_store AS
	SELECT s.store_name, 
		SUM(f.total_amount) AS total_sales
	FROM fact_sales f
	JOIN dim_stores s ON f.store_id = s.store_id
	GROUP BY s.store_name;
    
    SELECT * FROM vw_sales_by_store;

	CREATE OR REPLACE VIEW vw_sales_summary AS
	SELECT
		s.store_name,
		p.product_name,
		c.customer_name,
		d.full_date,
		f.quantity,
		f.list_price,
		f.total_amount
	FROM fact_sales f
	JOIN dim_products p ON f.product_id = p.product_id
	JOIN dim_customers c ON f.customer_id = c.customer_id
	JOIN dim_stores s ON f.store_id = s.store_id
	JOIN dim_dates d ON f.date_id = d.date_id;

	SELECT * FROM vw_sales_summary;
    
	CREATE OR REPLACE VIEW vw_inventory_details AS
	SELECT
		i.inventory_id,
		p.product_name,
		s.store_name,
		i.stock_quantity
	FROM fact_inventory i
	JOIN dim_products p ON i.product_id = p.product_id
	JOIN dim_stores s ON i.store_id = s.store_id;
    
	SELECT * FROM vw_inventory_details;
    
	CREATE OR REPLACE VIEW vw_inventory_by_store AS
	SELECT
		s.store_name,
		SUM(i.stock_quantity) AS current_stock
	FROM fact_inventory i
	JOIN dim_stores s ON i.store_id = s.store_id
	GROUP BY s.store_name
	ORDER BY current_stock DESC;
	
    SELECT * FROM vw_inventory_by_store;
    
END //

DELIMITER ;

CALL analytics_stp;