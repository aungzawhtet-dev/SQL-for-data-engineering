use world;

-- Create Tables
-- Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(100),
    region VARCHAR(50),
    signup_date DATE
);


-- Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);


-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    product_id INT,
    order_date DATE,
    quantity INT,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- check talbes
SELECT COUNT(*) FROM customers;
select * from customers;
SELECT COUNT(*) FROM products;
select * from products;
SELECT COUNT(*) FROM orders;
select * from orders;


-- Step 4: Analyze Query Performance (Without Indexes)
EXPLAIN ANALYZE
SELECT 
    c.region,
    p.category,
    SUM(o.total_amount) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY c.region, p.category
ORDER BY total_sales DESC; -- Total execution time is 34.2 ms


-- Create Indexes and Compare
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_customer_product ON orders(customer_id, product_id);
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_products_category ON products(category);

-- Drop Index Statements
SHOW INDEXES FROM orders;
SHOW CREATE TABLE orders;

DROP INDEX idx_orders_order_date ON orders;
DROP INDEX idx_customers_region ON customers;
DROP INDEX idx_products_category ON products;

ALTER TABLE orders DROP FOREIGN KEY orders_ibfk_1;
DROP INDEX idx_orders_customer_product ON orders;


-- rerun the same query after index 
EXPLAIN ANALYZE
SELECT 
    c.region,
    p.category,
    SUM(o.total_amount) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY c.region, p.category
ORDER BY total_sales DESC; -- The total execution time is approximately 22.1 milliseconds.


-- Simulate Materialized View
-- MySQL doesn’t have true materialized views, but we can simulate one using
-- a table + scheduled refresh.

CREATE TABLE mv_sales_summary AS
SELECT 
    c.region,
    p.category,
    SUM(o.total_amount) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.region, p.category;

-- Later: refresh it (simulate daily)
TRUNCATE TABLE mv_sales_summary;
INSERT INTO mv_sales_summary
SELECT 
    c.region,
    p.category,
    SUM(o.total_amount),
    COUNT(DISTINCT o.customer_id)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.region, p.category;


-- Partition the orders table by YEAR(order_date) (for large datasets).
/*
Why Partition?
When your orders table has millions of rows, queries, don’t need to scan all partitions — only the relevant year(s).
That means less I/O, faster query times, and better maintenance.
*/

-- Confirm  MySQL Engine Supports Partitioning
SHOW ENGINES;
select * from orders;
select max(order_date), min(order_date) from orders;

-- Copy Structure and Data from original table and create new one
-- But in this exercise, I don't use these approach
CREATE TABLE orders_new AS SELECT * FROM orders;
select * from orders_new;

-- Create Partation Table
CREATE TABLE orders_partitioned (
    order_id INT NOT NULL AUTO_INCREMENT,
    customer_id INT,
    product_id INT,
    order_date DATE,
    quantity INT,
    total_amount DECIMAL(10,2),
    order_year INT GENERATED ALWAYS AS (YEAR(order_date)) STORED,
    PRIMARY KEY (order_id, order_year)
)
PARTITION BY RANGE (order_year) (
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION pmax  VALUES LESS THAN MAXVALUE
);

-- Migrate Data from Old Table
INSERT INTO orders_partitioned (order_id, customer_id, product_id, order_date, quantity, total_amount)
SELECT order_id, customer_id, product_id, order_date, quantity, total_amount FROM orders;


-- Check Partition Metadata
SELECT
    PARTITION_NAME,
    TABLE_ROWS,
    PARTITION_EXPRESSION,
    PARTITION_DESCRIPTION
FROM information_schema.PARTITIONS
WHERE TABLE_NAME = 'orders_partitioned'
  AND TABLE_SCHEMA = DATABASE();


-- Test After Partition Routing or Result
SELECT  *
FROM orders_partitioned
PARTITION (p2024);





