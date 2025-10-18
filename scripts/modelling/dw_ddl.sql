DROP DATABASE IF EXISTS dw_sales_management;

CREATE DATABASE dw_sales_management;

USE dw_sales_management;

-- ===================================
-- Creating Fact Sales, Inventory & dimension Table
-- ===================================

CREATE TABLE dim_customers(
customer_id INT PRIMARY KEY,
customer_name VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50)
);

-- combines products, brands & categories
CREATE TABLE dim_products(
product_id INT PRIMARY KEY,
product_name VARCHAR(50),
brand_name VARCHAR(50),
category_name VARCHAR(50),
list_price DECIMAL(10, 2)
);

CREATE TABLE dim_stores(
store_id INT PRIMARY KEY,
store_name VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50)
);

CREATE TABLE dim_staffs(
staff_id INT PRIMARY KEY,
staff_name VARCHAR(50),
store_id INT,
manager_id INT
);

CREATE TABLE dim_dates(
date_id INT PRIMARY KEY,
full_date DATE,
day_ INT,
month_ INT,
month_name VARCHAR(50),
quarter_ INT,
year_ INT,
week_of_year INT
);

CREATE TABLE fact_sales(
sales_id INT AUTO_INCREMENT PRIMARY KEY,
order_id INT,
item_id INT,
customer_id INT,
product_id INT,
store_id INT,
staff_id INT,
date_id INT,
quantity INT,
list_price DECIMAL(10, 2),
sales DECIMAL(10, 2),
discount DECIMAL(10, 2),
total_amount DECIMAL (10, 2)
);

CREATE TABLE fact_inventory(
inventory_id INT AUTO_INCREMENT PRIMARY KEY,
store_id INT,
product_id INT,
stock_quantity INT,
last_updated DATE
);

