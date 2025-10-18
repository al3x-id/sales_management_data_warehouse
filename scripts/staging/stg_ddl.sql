DROP DATABASE IF EXISTS stg_sales_management;

CREATE DATABASE stg_sales_management;

USE stg_sales_management;

CREATE TABLE stg_brands AS
	SELECT 
		brand_id,
		TRIM(brand_name) AS brand_name
    FROM  raw_sales_management.raw_brands;
    
CREATE TABLE stg_customers AS
	SELECT
		customer_id,
		TRIM(CONCAT(first_name, ' ', last_name)) AS customer_name,
		TRIM(city) AS city,
		CASE
			WHEN state = 'NY' THEN 'New York'
			WHEN state = 'CA' THEN 'Carlifornia'
			ELSE 'Texas'
		END AS state
	FROM  raw_sales_management.raw_customers;
    
CREATE TABLE stg_categories AS
	SELECT
		category_id,
		TRIM(category_name) AS category_name
	FROM  raw_sales_management.raw_categories;
        
CREATE TABLE stg_order_items AS
	SELECT
		order_id,
        item_id,
		product_id,
		quantity,
		list_price,
		discount,
        ROUND((list_price * quantity) * (1 - discount), 2) AS sales
	FROM raw_sales_management.raw_order_items;
    
CREATE TABLE stg_orders AS
	SELECT 
		order_id,
        customer_id,
        DATE(order_date) AS order_date,
        COALESCE(DATE(shipped_date), DATE(required_date)) AS shipped_date,
        store_id,
        staff_id
	FROM raw_sales_management.raw_orders;
    
CREATE TABLE stg_products AS
	SELECT 
		product_id,
        TRIM(product_name) AS product_name,
        brand_id,
        category_id,
        list_price
	FROM raw_sales_management.raw_products;
        
CREATE TABLE stg_staffs AS
	SELECT
		staff_id,
        TRIM(CONCAT(first_name, ' ', last_name)) AS staff_name,
        store_id,
        COALESCE(manager_id, 0) AS manager_id
	FROM raw_sales_management.raw_staffs;
    
    CREATE TABLE stg_stocks AS
		SELECT
			store_id,
            product_id,
            quantity
		FROM raw_sales_management.raw_stocks;
            
CREATE TABLE stg_stores AS
	SELECT
		store_id,
        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
		CASE
			WHEN state = 'NY' THEN 'New York'
			WHEN state = 'CA' THEN 'Carlifornia'
			ELSE 'Texas'
		END AS state
	FROM raw_sales_management.raw_stores;