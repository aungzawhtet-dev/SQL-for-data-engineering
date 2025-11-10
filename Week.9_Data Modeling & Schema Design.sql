/*
build a mini Data Warehouse for “E-commerce Analytics” in MySQL
Star Schema design
Dimension & Fact tables
Surrogate keys
Sample data insertion
Example analytical queries
*/


-- design a Star Schema for a small E-commerce Analytics Warehouse
--  to analyze orders, customers, products, and time-based sales metrics.
/*
Main Entities:

1.Fact table: fact_sales

2.Dimension tables:
dim_customer
dim_product
dim_date
dim_region
*/


use db_test;

-- Create Dimension Tables

-- dim_customers
CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(10),
    customer_name VARCHAR(100),
    gender VARCHAR(10),
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50)
);



-- din_product
CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(10),
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2)
);


-- dim_date
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday VARCHAR(15)
);



-- dim_region
CREATE TABLE dim_region (
    region_key INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(50),
    country VARCHAR(50)
);



-- Step 3. Create Fact Table
CREATE TABLE fact_sales (
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_key INT,
    product_key INT,
    date_key INT,
    region_key INT,
    quantity INT,
    total_amount DECIMAL(12,2),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (region_key) REFERENCES dim_region(region_key)
);



-- Step 4. Insert Sample Data
-- Insert dim_customer
INSERT INTO dim_customer (customer_id, customer_name, gender, email, city, country)
VALUES
('C001', 'Alice Wong', 'Female', 'alice@example.com', 'Bangkok', 'Thailand'),
('C002', 'John Tan', 'Male', 'john@example.com', 'Singapore', 'Singapore'),
('C003', 'Mya Aung', 'Female', 'myaaung@example.com', 'Yangon', 'Myanmar');



-- Insert dim_product
INSERT INTO dim_product (product_id, product_name, category, brand, price)
VALUES
('P001', 'Laptop', 'Electronics', 'Dell', 1200.00),
('P002', 'Smartphone', 'Electronics', 'Samsung', 800.00),
('P003', 'Headphones', 'Accessories', 'Sony', 150.00);



-- Insert dim_region
INSERT INTO dim_region (region_name, country)
VALUES
('Southeast Asia', 'Thailand'),
('Southeast Asia', 'Singapore'),
('Southeast Asia', 'Myanmar');



-- Insert dim_date
INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, weekday)
VALUES
(20250110, '2025-01-10', 2025, 1, 1, 10, 'Friday'),
(20250215, '2025-02-15', 2025, 1, 2, 15, 'Saturday'),
(20250320, '2025-03-20', 2025, 1, 3, 20, 'Thursday');



-- Insert Fact Table
INSERT INTO fact_sales (customer_key, product_key, date_key, region_key, quantity, total_amount)
VALUES
(1, 1, 20250110, 1, 2, 2400.00),
(2, 2, 20250215, 2, 1, 800.00),
(3, 3, 20250320, 3, 3, 450.00);



-- Step 5. Practice Analytical Queries

-- 1. Total Sales by Product
SELECT 
    p.product_name,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name;


-- 2. Sales by Country and Month
SELECT 
    r.country,
    d.month,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_region r ON f.region_key = r.region_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY r.country, d.month;


-- 3. Top Customer by Total Spending
SELECT 
    c.customer_name,
    SUM(f.total_amount) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_name
ORDER BY total_spent DESC
LIMIT 1;



-- Step 6. Optional Extension: Snowflake Schema Example
/*
If we want to extend to a Snowflake schema, we can normalize further — for example:
Split dim_product into dim_product + dim_category
Split dim_region into dim_region + dim_country
*/



-- check data inside the tables
select * from dim_customer;
select * from dim_product;
select * from dim_region;
select * from dim_date;
select * from fact_sales;  

