/*
Goal : Simulate a mini ETL pipeline (Extract–Transform–Load) and build analytical SQL views.

Steps :
1.Extract & Load → Load raw CSV-style data into staging tables.
2.Transform → Clean and load into production tables (fact & dimension).
3.Validate Data Quality → Compare row counts between staging and production.
4.Analytics Views → Create reporting views using joins + window functions.
*/


-- Database Schema Design
/*
We’ll use a Sales Analytics Warehouse:

1. Staging Tables (Raw Data)
stg_customers
stg_orders

2. Dimension & Fact Tables
dim_customer
dim_product
fact_sales
*/



-- Step 1: Create Database & Staging Tables
CREATE DATABASE sales_warehouse;
USE sales_warehouse;


-- Raw staging data (simulated CSV load)
CREATE TABLE stg_customers (
    customer_id INT,
    full_name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50)
);


CREATE TABLE stg_orders (
    order_id INT,
    order_date DATE,
    customer_id INT,
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2)
);


-- Step 2: Insert Sample Data
INSERT INTO stg_customers VALUES
(1, 'Alice Johnson', 'alice@mail.com', 'Thailand'),
(2, 'Bob Tan', 'bob@mail.com', 'Malaysia'),
(3, 'Chen Wei', 'chen@mail.com', 'Singapore'),
(4, 'Agar David', 'david@gmail.com', 'Vietnam'),
(5, 'Lom Loon', 'loom@gmail.com', 'Laos'),
(6, 'Byrone Tamse', 'byrone@gmail.com','Phillipine'),
(7, 'Mg Zaw', 'zaw@gamil.com','Myanmar'),
(8, 'Kha Mar', 'mar@gamil.com','Cambodia'),
(9, 'Quao', 'quao@gmail.com','Indonesia'),
(10,'Abudabi', 'abu@gamil.com','Brunei');


INSERT INTO stg_orders (order_id, order_date, customer_id, product_name, quantity, price) VALUES
(101, '2025-09-01', 1, 'Laptop', 1, 1000.00),
(102, '2025-09-03', 1, 'Mouse', 2, 25.00),
(103, '2025-09-10', 2, 'Keyboard', 1, 45.00),
(104, '2025-09-12', 3, 'Laptop', 1, 950.00),
(105, '2025-09-14', 3, 'Headset', 1, 70.00),
(106, '2025-09-18', 5, 'Tablet', 1, 200.00),
(107, '2025-09-25', 7,'Smart Watch', 1, 150.00),
(108, '2025-09-29',8,'Bluetooth', 1, 300.00),
(109, '2025-09-30',10,'Smart Phone', 1, 700.00);



-- Step 3: Transform → Load into Dimension & Fact Tables

-- Dimension tables
CREATE TABLE dim_customer AS
SELECT DISTINCT
    customer_id,
    full_name,
    email,
    country
FROM stg_customers;


CREATE TABLE dim_product AS
SELECT DISTINCT
    product_name,
    CASE
        WHEN product_name LIKE '%Laptop%' THEN 'Electronics'
        WHEN product_name LIKE '%Mouse%' THEN 'Accessories'
        WHEN product_name LIKE '%Keyboard%' THEN 'Accessories'
        WHEN product_name LIKE '%Headset%' THEN 'Accessories'
        WHEN product_name LIKE '%Smart Phone%' THEN 'Electronics'       
        ELSE 'Other'
    END AS category
FROM stg_orders;



-- Fact table
CREATE TABLE fact_sales AS
SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    p.product_name,
    o.quantity,
    o.price,
    (o.quantity * o.price) AS total_amount
FROM stg_orders o
JOIN dim_product p ON o.product_name = p.product_name;



-- Step 4: Data Quality Check — Row Count Comparison
-- Compare record counts between staging and warehouse
SELECT
    'stg_customers' AS table_name,
    COUNT(*) AS row_count
FROM stg_customers
UNION ALL
SELECT
    'dim_customer',
    COUNT(*)
FROM dim_customer
UNION ALL
SELECT
    'stg_orders',
    COUNT(*)
FROM stg_orders
UNION ALL
SELECT
    'fact_sales',
    COUNT(*)
FROM fact_sales;



-- Step 5: Create Analytical Reporting Views
-- 1️ Customer Revenue Summary (using JOIN + GROUP BY)

CREATE VIEW view_customer_revenue AS
SELECT
    c.full_name,
    c.country,
    SUM(f.total_amount) AS total_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.full_name, c.country;


-- 2️ Monthly Sales Rank (using Window Functions)
CREATE VIEW view_monthly_sales_rank AS
SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    customer_id,
    SUM(total_amount) AS monthly_revenue,
    RANK() OVER (PARTITION BY DATE_FORMAT(order_date, '%Y-%m') ORDER BY SUM(total_amount) DESC) AS rank_in_month
FROM fact_sales
GROUP BY month, customer_id;



-- Step 6: Query the Analytical Views
-- Top revenue by customer
SELECT * FROM view_customer_revenue ORDER BY total_revenue DESC;

-- Monthly ranking of customers
SELECT * FROM view_monthly_sales_rank ORDER BY month, rank_in_month;





