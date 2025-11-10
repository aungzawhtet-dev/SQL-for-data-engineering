/*
Goal: Learn to summarize and analyze data.
Topics
•	GROUP BY, HAVING, aggregate functions (SUM, AVG, COUNT, MAX, MIN)
•	Combining filters and aggregates
•	DISTINCT and data deduplication
•	Calculate total sales per region, average order size, etc.
*/

use world;

-- Create sales table
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50),
    product VARCHAR(50),
    quantity INT,
    price NUMERIC(10,2),
    sale_date DATE
);


-- Insert sample data
INSERT INTO sales (region, product, quantity, price, sale_date) VALUES
('North', 'Laptop', 3, 800.00, '2025-10-01'),
('North', 'Mouse', 10, 20.00, '2025-10-01'),
('South', 'Keyboard', 5, 50.00, '2025-10-02'),
('South', 'Laptop', 2, 850.00, '2025-10-02'),
('East', 'Monitor', 4, 200.00, '2025-10-03'),
('West', 'Mouse', 15, 25.00, '2025-10-03'),
('East', 'Laptop', 1, 900.00, '2025-10-04'),
('West', 'Keyboard', 8, 45.00, '2025-10-04');

-- check sales table
select * from sales;

-- Total sales (quantity × price)
select sum(quantity * price) as total_sales from sales;


-- Average order price
select avg(price) as avg_price from sales;


-- Maximum and Minimum product price
select max(price) as  max_price,
min(price) as min_price
from sales;



-- GROUP BY — summarize by region or product
-- a. Total sales per region
select sum(price * quantity) as total_sales
from sales
group by region 
order by total_sales desc;


-- b. Average price per product
select avg(price) as avg_price from sales 
group by product
order by avg_price desc;


-- HAVING — filtering after aggregation
-- Show only regions where total sales > 1000.
select  sum(price * quantity) as total_price
from sales
group by region
having  sum(price * quantity) > 1000;


-- Combining WHERE + GROUP BY + HAVING
-- Show only sales in October after the 2nd day, and only products with total quantity > 5.
select product,
sum(quantity) as total_quantity
from sales 
where sale_date > "2025-10-02"
group by product
having sum(quantity) > 5;


-- DISTINCT & Data Deduplication
-- Get unique regions
select distinct (region) as unique_region from sales;


-- Count how many unique products were sold
SELECT COUNT(DISTINCT product) AS unique_products FROM sales;


-- Mini Project — Sales Summary Dashboard Queries
-- a. Top 3 selling products by revenue
select product,
sum(price * quantity) as total_revenue
from sales
group by product
order by total_revenue desc
limit 3;


-- b. Average daily revenue
select 
avg(price * quantity) as avg_revenue,
sale_date
from sales
group by sale_date
order by sale_date asc;


-- c. Total sales per region and top product in each
select product,
region,
sum(price * quantity) as total_sales
from sales 
group by region,product
order by region,total_sales desc;





