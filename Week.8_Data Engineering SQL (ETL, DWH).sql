/*
simulating a small ETL pipeline (Extract → Transform → Load)
Extract & Load: Load CSV data into a staging table.
Transform: Clean and enrich the data.
Load: Insert into a data warehouse fact table (fact_sales).
Handle Upserts: Avoid duplicates when reloading data.
*/


use world;
-- STEP 1: Create Database & Tables

-- STAGING: raw CSV data from external source
CREATE TABLE staging_sales (
    sale_id INT,
    customer_name VARCHAR(100),
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2),
    sale_date DATE
);


-- DIMENSIONS
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100) UNIQUE
);


CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) UNIQUE
);


-- FACT TABLE
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    sale_date DATE,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);


-- STEP 3: Load CSV → Staging Table & check staging table
select * from staging_sales;


-- STEP 4: Clean & Transform Data
-- Remove invalid rows
DELETE FROM staging_sales
WHERE quantity <= 0 OR price <= 0 OR customer_name IS NULL;

-- Standardize capitalization
UPDATE staging_sales
SET 
    customer_name = CONCAT(UCASE(LEFT(customer_name, 1)), LCASE(SUBSTRING(customer_name, 2))),
    product_name  = CONCAT(UCASE(LEFT(product_name, 1)), LCASE(SUBSTRING(product_name, 2)));


-- STEP 5: Load into Dimension Tables
-- IGNORE keyword: Prevents errors if a duplicate customer_name already exists. Instead of failing, MySQL skips that row.
INSERT IGNORE INTO dim_customer (customer_name)
SELECT DISTINCT customer_name
FROM staging_sales;

INSERT IGNORE INTO dim_product (product_name)
SELECT DISTINCT product_name
FROM staging_sales;


-- Check Tables
SELECT * FROM dim_customer;
SELECT * FROM dim_product;



-- STEP 6: Load into Fact Table (ETL Load + UPSERT)

INSERT INTO fact_sales (sale_id, customer_id, product_id, quantity, price, total_amount, sale_date)
SELECT 
    s.sale_id,
    c.customer_id,
    p.product_id,
    s.quantity,
    s.price,
    s.quantity * s.price AS total_amount,
    s.sale_date
FROM staging_sales s
JOIN dim_customer c ON s.customer_name = c.customer_name
JOIN dim_product p ON s.product_name = p.product_name
ON DUPLICATE KEY UPDATE 
    quantity = VALUES(quantity),
    price = VALUES(price),
    total_amount = VALUES(total_amount),
    sale_date = VALUES(sale_date);


-- Verify:
SELECT * FROM fact_sales;



-- STEP 7: Sample Analysis Queries
-- Total sales summary
SELECT COUNT(*) AS total_records, SUM(total_amount) AS total_sales
FROM fact_sales;

-- Top 3 customers
SELECT c.customer_name, SUM(f.total_amount) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC
LIMIT 3;

-- Product performance
SELECT p.product_name, SUM(f.total_amount) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC;



-- STEP 8: Optional Clean-up (after ETL)
TRUNCATE TABLE staging_sales;



-- Summary of Layers

| Layer     | Table                         | Purpose                                    |
| --------- | ----------------------------- | ------------------------------------------ |
| Staging   | `staging_sales`               | Raw CSV data                               |
| Dimension | `dim_customer`, `dim_product` | Lookup tables for relationships            |
| Fact      | `fact_sales`                  | Core sales metrics (quantity, total, etc.) |




