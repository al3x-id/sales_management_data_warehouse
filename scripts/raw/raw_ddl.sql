DROP DATABASE IF EXISTS raw_sales_management;

-- separating database keeps raw data immutable and clearly separated from transforms
CREATE DATABASE raw_sales_management CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE raw_sales_management;

CREATE TABLE raw_brands(
brand_id INT,
brand_name VARCHAR(50)
);

CREATE TABLE raw_categories(
category_id INT,
category_name VARCHAR(50)
);

CREATE TABLE raw_customers(
customer_id INT,
first_name VARCHAR(50),
last_name VARCHAR(50),
phone VARCHAR(50),
email VARCHAR (50),
street VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50),
zipcode INT
);

CREATE TABLE raw_stores(
store_id INT,
store_name VARCHAR(50),
phone VARCHAR(50),
email VARCHAR(50),
street VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50),
zipcode INT
);

CREATE TABLE raw_orders(
order_id INT,
customer_id INT,
order_status INT,
order_date DATE,
required_date DATE,
shipped_date DATE,
store_id INT,
staff_id INT
);

CREATE TABLE raw_staffs(
staff_id INT,
first_name VARCHAR(50),
last_name VARCHAR(50),
email VARCHAR(50),
phone VARCHAR(50),
active_ INT,
store_id INT,
manager_id INT
);

CREATE TABLE raw_products(
product_id INT,
product_name VARCHAR(50),
brand_id INT,
category_id INT,
model_year YEAR,
list_price DECIMAL(10,2)
);

CREATE TABLE raw_order_items(
order_id INT,
item_id INT,
product_id INT,
quantity INT,
list_price DECIMAL(10,2),
discount DECIMAL(10,2)
);

CREATE TABLE raw_stocks(
store_id INT,
product_id INT,
quantity INT
);

-- Creating logging control: one centralized table to record load attempts across the project
CREATE TABLE IF NOT EXISTS load_log(
load_id INT AUTO_INCREMENT PRIMARY KEY,
table_name_ VARCHAR(50),
batch_tag VARCHAR(50),
load_status VARCHAR(50),
message TEXT,
load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

 
